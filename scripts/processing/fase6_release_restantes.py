#!/usr/bin/env python3
"""REPRODUCIBLE local release for Quality review, not a GCS deployer.

Without --from-commit generates only an UNSIGNED candidate from working files.
With --from-commit requires clean pinned Git HEAD and F5 gateway unchanged.
Never connects to network, creates claims, signs or executes Dataproc.
"""
from __future__ import annotations

import argparse
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[2]
ENTRY = 'scripts/processing/fase6_restantes_batch.py'
# Top-level import names for Spark --py-files; all paths from the same Git SHA.
MODULES = {
    'fase6_launch_restantes.py': 'scripts/processing/fase6_launch_restantes.py',
    'fase6_restantes_core.py': 'scripts/processing/fase6_restantes_core.py',
    'fase6_restantes_preflight.py': 'scripts/processing/fase6_restantes_preflight.py',
    'fase6_restantes_transform.py': 'scripts/processing/fase6_restantes_transform.py',
    'fase6_restantes_writer.py': 'scripts/processing/fase6_restantes_writer.py',
    'fase6_integracion_10_local.py': 'scripts/processing/fase6_integracion_10_local.py',
    'fase6_valores_conciliacion.py': 'scripts/processing/fase6_valores_conciliacion.py',
    'fase6_especializados_valores.py': 'scripts/processing/fase6_especializados_valores.py',
    'fase6_especializados_hechos_valores.py': 'scripts/processing/fase6_especializados_hechos_valores.py',
    'fase6_especializados_hechos_metadata.py': 'scripts/processing/fase6_especializados_hechos_metadata.py',
    'bronze_to_silver_dataproc_candidato.py': 'scripts/processing/bronze_to_silver_dataproc_candidato.py',
    'fase6_contexto_lanzamiento.py': 'scripts/processing/fase6_contexto_lanzamiento.py',
    'puerta_silver_operativa_v1_candidata.py': 'scripts/pipeline/puerta_silver_operativa_v1_candidata.py',
    'puerta_silver_v1_candidata.py': 'scripts/pipeline/puerta_silver_v1_candidata.py',
    'adaptador_gcs_silver_v1.py': 'scripts/pipeline/adaptador_gcs_silver_v1.py',
}
GATE_FILES = (
    'scripts/pipeline/puerta_silver_operativa_v1_candidata.py',
    'scripts/pipeline/puerta_silver_v1_candidata.py',
    'scripts/pipeline/adaptador_gcs_silver_v1.py',
)


def hash_bytes(value):
    return sha256(value).hexdigest()


def deterministic_zip(files):
    """fixed timestamp, order, mode; byte-for-byte reproducible."""
    if not isinstance(files, dict) or not files:
        raise ValueError('ZIP sin archivos')
    for name, value in files.items():
        if '/' in name or '\\' in name or not name.endswith('.py') or type(value) is not bytes:
            raise ValueError('Archivo de dependencia invalido')
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        for name, value in sorted(files.items()):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            z.writestr(info, value)
    return buffer.getvalue()


def _git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, stderr=subprocess.PIPE)


def _source_from_disk(name):
    path = ROOT / name
    if not path.is_file() or path.is_symlink() or ROOT not in path.resolve().parents:
        raise ValueError(f'Archivo no seguro o ausente: {name}')
    return path.read_bytes()


def _source_from_commit(name):
    data = _git('show', 'HEAD:'+name)
    if _source_from_disk(name) != data:
        raise ValueError(f'Archivo local difiere del commit: {name}')
    return data


def _verify_gate(original_source):
    for path in GATE_FILES:
        baseline = _git('show', '41266f0:'+path)
        if original_source(path) != baseline:
            raise ValueError(f'ALTO: puerta Fase 5 modificada: {path}')


def _write_exclusive(path, raw):
    if path.exists():
        if path.read_bytes() != raw:
            raise ValueError('Destino existente distinto: '+str(path))
    else:
        # Preserve preexisting files; no overwrite of different evidence.
        with path.open('xb') as f:
            f.write(raw)


def prepare(*, from_commit=False, out_dir=None):
    if _git('branch','--show-current').decode().strip() != 'pipeline-daniela':
        raise ValueError('Rama distinta de pipeline-daniela')
    _git('merge-base', '--is-ancestor', '41266f0', 'HEAD')
    base = _source_from_commit if from_commit else _source_from_disk
    if from_commit:
        paths = [ENTRY,*MODULES.values(), 'scripts/processing/fase6_release_restantes.py']
        changes = _git('status','--porcelain','--', *paths).decode().strip()
        if changes:
            raise ValueError('Archivos de release sin registrar o editados: '+changes)
    _verify_gate(base)
    entry = base(ENTRY)
    modules = {name:base(path) for name,path in MODULES.items()}
    deps = deterministic_zip(modules)
    commit = _git('rev-parse','HEAD').decode().strip()
    if len(commit) != 40:
        raise ValueError('SHA Git invalido')
    suffix = commit[:12] if from_commit else 'SIN_COMMIT_'+hash_bytes(entry+deps)[:12]
    output = (out_dir or (Path.home()/'Descargas')) / ('BVL_FASE6_RESTANTES_'+suffix)
    output.mkdir(parents=True, exist_ok=True)
    entryname, depsname = 'fase6_restantes_batch.py', 'fase6_restantes_dependencias.zip'
    _write_exclusive(output/entryname, entry)
    _write_exclusive(output/depsname, deps)
    request = {
        'schema_version': 'BVL-F6-REST-RELEASE-REQUEST-v1',
        'status': ('PROPUESTA_NO_FIRMADA_NO_AUTORIZA' if from_commit else
                   'BORRADOR_LOCAL_SIN_COMMIT_NO_AUTORIZA'),
        'execution_enabled':False, 'gold_consumption_allowed':False,
        'quality_approved':False, 'processor_commit': (commit if from_commit else None),
        'source_head_informational': commit,
        'expected_launch_schema_proposal': 'BVL-SILVER-LAUNCH-CONTEXT-REST-v1',
        'source_inventory': {'processes_candidate':10, 'processes_blocked':1,
                             'direct_outputs_candidate':11,
                             'remaining_total_products':13},
        'entrypoint_sha256':hash_bytes(entry),
        'dependencies_sha256':hash_bytes(deps),
        'module_sha256':{name:hash_bytes(raw) for name,raw in sorted(modules.items())},
        'storage_proposal': (f'gs://bvl26-dataproc-staging-7f3a9c/bvl-phase6/releases/{commit}/'
                             if from_commit else None),
        'quality_gate': [
            'Ratificar nuevo schema de contexto SINGLE_SOURCE por intento',
            'Reproducir hashes de entrypoint + py-files ZIP y fijar generaciones GCS',
            'Revisar capacidad: 256MiB por fuente, 32MiB por objeto, 250000 filas',
            'Aprobar esquemas, particiones y formato de manifest candidato por producto',
            'Emitir autorizaciones temporales independientes por dataset/attempt',
            'Corroborar permiso de lectura de manifiestos y escritura create-only',
            'No reintentar tras consumir claim; reconciliar fallos con Quality',
            'Valores Inscritos: Quality debe validar ambas salidas como unidad',
            'Resolver procedencia 2 CSV Hechos antes de su autorización',
            'Identidad: cinco manifests Silver oficiales versionados, no staging',
        ],
    }
    payload = (json.dumps(request, ensure_ascii=False, sort_keys=True,
                          indent=2, allow_nan=False)+'\n').encode('utf-8')
    _write_exclusive(output/'solicitud_quality_NO_FIRMADA.json', payload)
    print('PAQUETE_LOCAL:', output)
    print('ENTRYPOINT_SHA256:',request['entrypoint_sha256'])
    print('DEPENDENCIAS_SHA256:',request['dependencies_sha256'])
    print('ESTADO:',request['status'])
    print('GCS:NO | CLAIM:NO | DATAPROC:NO')
    return request


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--from-commit', action='store_true',
                    help='Solo cuando todos los archivos estén en HEAD y aprobados localmente')
    ap.add_argument('--out-dir', type=Path)
    args = ap.parse_args()
    prepare(from_commit=args.from_commit, out_dir=args.out_dir)


if __name__ == '__main__':
    main()
