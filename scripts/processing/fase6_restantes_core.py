#!/usr/bin/env python3
"""Catalogo y validadores PUROS para Silver restante BVL.

NO otorga autorizacion ni ejecuta Spark/GCS; Quality firma cada intento.
"""
from __future__ import annotations
from dataclasses import dataclass
from hashlib import sha256
import json
import re

SILVER_BUCKET = 'bvl26-silver-7f3a9c'
CONTROL_BUCKET = 'bvl26-control-7f3a9c'
BRONZE_BUCKET = 'bvl26-bronze-7f3a9c'
RUN_REFERENCE = 'bvl-bronze-inicial-20261125-001'
GATE_APPROVED_COMMIT = '41266f0efba4477c30266e19ca3280838ff3d802'
QUALITY_SERVICE_ACCOUNT = 'sa-bvl-silver@bvl-analytics-2026.iam.gserviceaccount.com'
HEX = re.compile(r'[0-9a-f]{64}\Z')
PART = re.compile(r'[a-z0-9][a-z0-9_-]*\Z')

# Inventario RATIFICADO de acuerdo con el documento de continuidad F5;
# no equivale a aprobacion del procesamiento propuesto en F6.
SOURCES = {
    'bcrp/embig': ('macroeconomia/embig',),
    'smv/cambios_patrimonio': ('finanzas_empresariales/cambios_patrimonio',),
    'smv/estado_resultados': ('finanzas_empresariales/estado_resultados',),
    'smv/flujo_efectivo': ('finanzas_empresariales/flujo_efectivo',),
    'smv/situacion_financiera': ('finanzas_empresariales/situacion_financiera',),
    'smv/principales_cuentas': ('finanzas_empresariales/principales_cuentas',),
    'smv/cotizaciones': ('mercado_diario/cotizaciones',),
    'smv/montos_sab': ('mercado_diario/montos_sab',),
    'smv/valores_inscritos': ('referencias_smv/valores_inscritos', 'referencias_smv/puente_ruc_nemonico'),
    'smv/hechos_valores_consulta': ('referencias_smv/hechos_valores_consulta',),
    'smv/hechos_importancia': ('hechos_importancia/eventos',),
}
FILE_COUNTS = {
    'bcrp/embig': 1, 'smv/cambios_patrimonio': 1,
    'smv/estado_resultados': 1, 'smv/flujo_efectivo': 1,
    'smv/situacion_financiera': 1, 'smv/principales_cuentas': 20,
    'smv/cotizaciones': 60, 'smv/montos_sab': 1221,
    'smv/valores_inscritos': 290, 'smv/hechos_valores_consulta': 23,
    'smv/hechos_importancia': 1979,
}
DERIVED = 'hechos_importancia/identidad_empresarial'
DEPENDENCIES = (
    'hechos_importancia/eventos',
    'finanzas_empresariales/principales_cuentas',
    'referencias_smv/puente_ruc_nemonico',
    'mercado_diario/cotizaciones',
    'referencias_smv/hechos_valores_consulta',
)
# Implementaciones puras iniciales: 1 BCRP + 4 financieros +
# Cotizaciones y Principales Cuentas. Requieren validacion con Bronze real.
CANDIDATE_TRANSFORMS = frozenset({
    'bcrp/embig', 'smv/cambios_patrimonio', 'smv/estado_resultados',
    'smv/flujo_efectivo', 'smv/situacion_financiera',
    'smv/principales_cuentas', 'smv/cotizaciones', 'smv/montos_sab',
})

class ScopeError(ValueError):
    pass


def require(cond, msg):
    if not cond:
        raise ScopeError(msg)


def canonical(obj):
    return json.dumps(obj, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def valid_segment(s):
    return isinstance(s, str) and PART.fullmatch(s) is not None and len(s) <= 100


def expected_prefix(product, run_id, attempt_id):
    require(isinstance(product, str) and len(product.split('/')) == 2 and
            all(valid_segment(x) for x in product.split('/')), 'Producto Silver invalido')
    require(valid_segment(run_id) and valid_segment(attempt_id), 'run/attempt invalido')
    return (f'gs://{SILVER_BUCKET}/staging/{product}/'
            f'run_id={run_id}/attempt_id={attempt_id}/')


def validate_authorized_scope(auth, dataset):
    """SOLO examina alcance DESPUES de verificar firmas con puerta F5.

    No valida firma por si sola; nunca tratar auth arbitraria como autoridad.
    """
    require(dataset in SOURCES, 'Dataset no esta en catalogo ratificado')
    require(isinstance(auth, dict) and auth.get('dataset') == dataset and
            auth.get('mode') == 'SINGLE_SOURCE' and
            auth.get('operation') == 'BUILD_SILVER_STAGING', 'Alcance no coincide')
    require(auth.get('service_account') == QUALITY_SERVICE_ACCOUNT,
            'Principal fuera de alcance')
    run_id, attempt_id = auth.get('run_id'), auth.get('attempt_id')
    require(valid_segment(run_id) and valid_segment(attempt_id), 'Intento no canonical')
    outputs = auth.get('silver_outputs')
    require(isinstance(outputs, list) and len(outputs) == len(SOURCES[dataset]),
            'Cantidad de productos no coincide')
    actual = {o.get('name'): o.get('staging_prefix') for o in outputs if isinstance(o, dict)}
    expected = {p: expected_prefix(p, run_id, attempt_id) for p in SOURCES[dataset]}
    require(actual == expected and len(actual) == len(outputs),
            'Salidas Silver o staging fuera del intento firmado')
    return expected


def validate_source_entries(dataset, files):
    """Reconciliacion de datos de manifest YA VALIDADO criptograficamente.

    El manifest no es una fuente de confianza por si solo. No lee GCS.
    """
    require(dataset in SOURCES, 'Dataset desconocido')
    require(isinstance(files, list) and len(files) == FILE_COUNTS[dataset],
            'Numero de objetos Bronze distinto al inventario oficial')
    seen = set()
    for entry in files:
        require(isinstance(entry, dict), 'Entrada de manifest invalida')
        uri = entry.get('uri')
        require(isinstance(uri, str) and
                uri.startswith(f'gs://{BRONZE_BUCKET}/{dataset}/run_id={RUN_REFERENCE}/') and
                '..' not in uri and '\\' not in uri and uri not in seen,
                'URI Bronze fuera de inventario / repetido')
        seen.add(uri)
        gen, size, h = entry.get('generation'), entry.get('size_bytes'), entry.get('sha256')
        require(str(gen).isascii() and str(gen).isdecimal() and int(gen) > 0,
                'Generacion de archivo no fijada')
        require(type(size) is int and size >= 0, 'Tamano Bronze invalido')
        require(isinstance(h, str) and HEX.fullmatch(h), 'Hash Bronze invalido')
    return True


def validate_derived_dependencies(deps):
    """Exige cinco versiones oficiales INMUTABLES: sin staging ni latest.

    Prohibe utilizar `current.json` como referencia sin resolucion previa.
    """
    require(isinstance(deps, list) and len(deps) == 5, 'Cinco dependencias requeridas')
    names = [d.get('product') for d in deps if isinstance(d, dict)]
    require(len(names) == 5 and set(names) == set(DEPENDENCIES),
            'Dependencias distintas al anexo')
    for d in deps:
        uri, h, gen = d.get('manifest_uri'), d.get('manifest_sha256'), d.get('manifest_generation')
        require(isinstance(uri, str) and
                uri.startswith(f'gs://{CONTROL_BUCKET}/manifests/silver/') and
                uri.endswith('/manifest.json') and '/staging/' not in uri and
                '/current.json' not in uri, 'Manifest Silver no oficial')
        require(isinstance(h, str) and HEX.fullmatch(h), 'SHA Silver no fijado')
        require(str(gen).isascii() and str(gen).isdecimal() and int(gen) > 0,
                'Generacion Silver no fijada')
        require(isinstance(d.get('version_id'), str) and len(d['version_id']) > 0,
                'Version Silver ausente')
    return True


def validate_request_without_authorizing(manifest):
    """Validador de solicitud de lote; no emite firma, claim ni batch."""
    require(isinstance(manifest, dict) and manifest.get('execution_enabled') is False,
            'La solicitud no puede habilitar ejecucion')
    tasks = manifest.get('datasets')
    require(isinstance(tasks, list) and set(tasks) == set(SOURCES) and
            len(tasks) == len(SOURCES), 'Inventario pendiente incorrecto')
    return {'status': 'PLAN_CANDIDATO_NO_AUTORIZADO',
            'procesos': len(SOURCES), 'productos_directos': sum(map(len, SOURCES.values())),
            'derivado': DERIVED, 'request_sha256': sha256(canonical(manifest)).hexdigest()}
