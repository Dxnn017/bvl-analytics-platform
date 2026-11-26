#!/usr/bin/env python3
"""Integracion LOCAL de los 10/11 procesos Bronze restantes con adaptador.

NUNCA obtiene credenciales, toca GCS, firma, ejecuta Dataproc, consume claim
ni produce Silver oficial. Su entrada es una copia de bytes *ya fijados*;
comprobar hashes locales NO autentica el manifest de origen. Para Cloud se
exige la puerta KMS/GCS de F5 y un ejecutor NUEVO autorizado por Quality.
"""
from __future__ import annotations
from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import PurePosixPath

from fase6_restantes_core import (
    BRONZE_BUCKET, RUN_REFERENCE, FILE_COUNTS, SOURCES, DEPENDENCIES,
    CANDIDATE_TRANSFORMS, ScopeError, require, validate_source_entries,
)
from fase6_restantes_preflight import build_source_dataframe
from fase6_restantes_transform import load_json_records, dispatch_transform
from fase6_especializados_valores import transform_valores_inscritos
from fase6_especializados_hechos_valores import reconcile as reconcile_hechos_valores
from fase6_especializados_hechos_metadata import execute_hechos_events
from fase6_valores_conciliacion import reconcile_valores

READY_LOCAL = frozenset(CANDIDATE_TRANSFORMS | {
    'smv/valores_inscritos', 'smv/hechos_valores_consulta'
})
BLOCKED_DATASET = 'smv/hechos_importancia'
DERIVED = 'hechos_importancia/identidad_empresarial'
MAX_TOTAL_BYTES = 256 * 1024 * 1024
MAX_OBJECT_BYTES = 32 * 1024 * 1024

@dataclass(frozen=True)
class LocalCandidate:
    dataset: str
    products: dict
    evidence: dict
    quality_status: str = 'CANDIDATO_PENDIENTE_QUALITY'


def roadmap():
    """Plan determinista (sin credenciales ni operaciones cloud)."""
    batches = []
    for ds in SOURCES:
        batches.append({
            'dataset': ds,
            'products': list(SOURCES[ds]),
            'mode': 'SINGLE_SOURCE',
            'local_transform_status': ('PROBADO_LOCAL' if ds in READY_LOCAL
                                       else 'BLOQUEADO_PROCEDENCIA_METADATA'),
            'quality_authorized': False,
            'cloud_execution_enabled': False,
        })
    batches.append({
        'dataset': DERIVED, 'products': [DERIVED], 'mode': 'MULTI_SOURCE',
        'local_transform_status': 'BLOQUEADO_CINCO_SILVER_OFICIALES',
        'dependencies': list(DEPENDENCIES), 'quality_authorized': False,
        'cloud_execution_enabled': False,
    })
    return {'schema': 'BVL-F6-INTEGRACION-LOCAL-v1',
            'status': 'PLAN_CANDIDATO_NO_AUTORIZA',
            'processing_count': len(SOURCES),
            'direct_output_count': sum(map(len, SOURCES.values())),
            'remaining_output_count': 1 + sum(map(len, SOURCES.values())),
            'local_candidate_processes': len(READY_LOCAL),
            'blocked_processes': 1,
            'batches': batches,
            'execution_enabled': False,
            'quality_approved': False,
            'gold_consumption_allowed': False}


def checked_snapshot(dataset, objects):
    """Hash/size/generation local de objetos; NO verifica firmas KMS.

    objects es lista de dict: uri,generation,size_bytes,sha256,data (bytes).
    Se exige inventario COMPLETO de la fuente antes de transformar.
    """
    require(dataset in READY_LOCAL, 'Transformacion pendiente de fuente/Quality')
    require(isinstance(objects, (list, tuple)) and len(objects) == FILE_COUNTS[dataset],
            'Numero de entradas distinto del manifest ratificado')
    refs = []
    raw_by_uri = {}
    total = 0
    for obj in objects:
        require(isinstance(obj, dict) and set(obj) == {
                'uri', 'generation', 'size_bytes', 'sha256', 'data'},
                'Objeto local con campos inesperados')
        raw = obj['data']
        require(type(raw) is bytes and 0 < len(raw) <= MAX_OBJECT_BYTES,
                'Objeto Bronze vacio o sobre limite de memoria')
        total += len(raw)
        require(total <= MAX_TOTAL_BYTES, 'Lote excede limite de memoria controlado')
        require(obj['size_bytes'] == len(raw) and obj['sha256'] == sha256(raw).hexdigest(),
                'Bytes distintos de tamaño/hash fijados')
        uri = obj['uri']
        require(uri not in raw_by_uri, 'URI Bronze duplicada')
        raw_by_uri[uri] = raw
        refs.append({k: obj[k] for k in ('uri','generation','size_bytes','sha256')})
    validate_source_entries(dataset, refs)
    return [(obj['uri'], raw_by_uri[obj['uri']]) for obj in objects], {
        'dataset': dataset, 'objects': refs, 'object_count': len(refs),
        'total_bytes': total,
        'status': 'BYTES_LOCALES_VERIFICADOS_NO_ES_FIRMA_KMS',
    }


def _records_to_frame(spark, rows):
    from pyspark.sql.types import StructType, StructField, StringType
    require(isinstance(rows, list) and bool(rows), 'Sin registros para transformar')
    columns = sorted({k for row in rows for k in row})
    require(bool(columns) and len(columns) <= 300, 'Cantidad de columnas invalida')
    data = []
    for row in rows:
        require(isinstance(row, dict), 'Fila JSON no es objeto')
        out = []
        for name in columns:
            value = row.get(name)
            require(value is None or isinstance(value, (str,int,float,bool)),
                    'Valor JSON anidado; requiere esquema aprobado')
            out.append(None if value is None else str(value))
        data.append(tuple(out))
    schema = StructType([StructField(name, StringType(), True) for name in columns])
    return spark.createDataFrame(data, schema)


def _make_candidate(dataset, products, qa, provenance):
    require(dataset in SOURCES, 'Dataset desconocido')
    require(set(products) == set(SOURCES[dataset]),
            'Salidas diferentes al mapeo ratificado')
    require(all(hasattr(df, 'schema') for df in products.values()),
            'Salida no es DataFrame Spark')
    for name, df in products.items():
        names = df.columns
        require(bool(names) and len(set(names)) == len(names),
                f'Esquema vacio/duplicado para {name}')
        require(df.limit(1).count() == 1, f'Salida vacia: {name}')
    return LocalCandidate(dataset=dataset, products=products,
        evidence={'source': provenance, 'transformation': qa,
                  'products': sorted(products), 'publish_allowed': False,
                  'claim_consumed': False})


def local_transform_from_snapshot(spark, dataset, objects):
    """Procesa lote completo local; NO obtiene datos desde GCS.

    Se debe integrar posteriormente con check_gcs_signed_bundle y un
    contexto de lanzamiento firmado específico de los nuevos procesadores.
    """
    if dataset == BLOCKED_DATASET:
        return execute_hechos_events()  # siempre bloqueado
    selected, audit = checked_snapshot(dataset, objects)
    if dataset in CANDIDATE_TRANSFORMS:
        df = build_source_dataframe(spark, dataset, [raw for _, raw in selected])
        output, quality = dispatch_transform(dataset, df)
        return _make_candidate(dataset, {SOURCES[dataset][0]:output}, quality, audit)

    xml_items = [(uri, raw) for uri, raw in selected if uri.lower().endswith('.xml')]
    json_items = [(uri, raw) for uri, raw in selected if uri.lower().endswith('.json')]
    if dataset == 'smv/valores_inscritos':
        require(len(xml_items) == 289 and len(json_items) == 1,
                'Valores: manifest no contiene 289 XML + 1 JSON')
        records, reconciliation = reconcile_valores(xml_items, json_items[0][1])
        df = _records_to_frame(spark, records)
        valores, puente, quality = transform_valores_inscritos(df)
        require(quality['silver_valores_rows'] == reconciliation['json_records'],
                'Conteo de Valores distinto a la conciliacion XML/JSON')
        return _make_candidate(dataset, dict(zip(SOURCES[dataset], (valores, puente))),
                               {'reconciliation': reconciliation, 'transform': quality}, audit)

    if dataset == 'smv/hechos_valores_consulta':
        require(len(xml_items) == 22 and len(json_items) == 1,
                'Hechos Valores: manifest no contiene 22 XML + 1 JSON')
        records, quality = reconcile_hechos_valores(xml_items, json_items[0][1])
        cleaned = []
        for rec in records:
            # Lineage XML sigue íntegro como JSON string, no se elimina.
            refs = rec.get('_xml_sources')
            require(isinstance(refs, list) and all(isinstance(x,str) for x in refs),
                    'Lineage XML mal formado')
            new = {k:v for k,v in rec.items() if k != '_xml_sources'}
            new['xml_sources_json'] = json.dumps(refs, ensure_ascii=False,
                                                  sort_keys=True, separators=(',',':'))
            cleaned.append(new)
        df = _records_to_frame(spark, cleaned)
        return _make_candidate(dataset, {SOURCES[dataset][0]: df}, quality, audit)

    raise ScopeError('No existe proceso aprobado para esta fuente')
