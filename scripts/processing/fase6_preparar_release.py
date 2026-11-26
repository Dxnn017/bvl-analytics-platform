#!/usr/bin/env python3
"""Prepara artefactos reproducibles desde Git HEAD. SOLO ARCHIVOS LOCALES.

Uso: python3 scripts/processing/fase6_preparar_release.py
Crea una carpeta en ~/Descargas sin tocar GCS ni firmas.
"""
from __future__ import annotations
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
from pathlib import Path
import json
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[2]
MAIN = 'scripts/processing/fase6_tipo_cambio_batch.py'
MODULES = {
    'fase6_tipo_cambio_seguro.py': 'scripts/processing/fase6_tipo_cambio_seguro.py',
    'fase6_contexto_lanzamiento.py': 'scripts/processing/fase6_contexto_lanzamiento.py',
    'bronze_to_silver_dataproc_candidato.py': 'scripts/processing/bronze_to_silver_dataproc_candidato.py',
    'puerta_silver_operativa_v1_candidata.py': 'scripts/pipeline/puerta_silver_operativa_v1_candidata.py',
    'puerta_silver_v1_candidata.py': 'scripts/pipeline/puerta_silver_v1_candidata.py',
    'adaptador_gcs_silver_v1.py': 'scripts/pipeline/adaptador_gcs_silver_v1.py',
}


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT)


def read_committed(path):
    return git('show', 'HEAD:' + path)


def create_deps(contents):
    memory = BytesIO()
    with zipfile.ZipFile(memory, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        for name, data in sorted(contents.items()):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            z.writestr(info, data)
    return memory.getvalue()


def main():
    assert git('branch', '--show-current').decode().strip() == 'pipeline-daniela', 'Rama distinta'
    commit = git('rev-parse', 'HEAD').decode().strip()
    assert len(commit) == 40, 'Commit incompleto'
    subprocess.check_call(['git', 'merge-base', '--is-ancestor', '41266f0', 'HEAD'], cwd=ROOT)
    candidates = [MAIN, *MODULES.values()]
    # Solo los fuentes del release deben estar intactos en el workspace.
    changes = git('status', '--porcelain', '--', *candidates).decode()
    assert not changes.strip(), 'Cambios locales en artefactos del release: ' + changes
    assert all((ROOT / p).read_bytes() == read_committed(p) for p in candidates), 'Archivos difieren del commit'
    main_bytes = read_committed(MAIN)
    dep_bytes = create_deps({name: read_committed(path) for name, path in MODULES.items()})
    out = Path.home() / 'Descargas' / ('BVL_FASE6_RELEASE_' + commit[:12])
    out.mkdir(parents=True, exist_ok=True)
    for name, raw in (("fase6_tipo_cambio_batch.py", main_bytes),
                      ("fase6_dependencias.zip", dep_bytes)):
        target = out / name
        if target.exists() and target.read_bytes() != raw:
            raise SystemExit(f'ALTO: artefacto existente distinto: {target}')
        target.write_bytes(raw)
    base_uri = (f'gs://bvl26-dataproc-staging-7f3a9c/'
                f'bvl-phase6/releases/{commit}')
    draft = {
        'status': 'PROPUESTA_NO_FIRMADA_NO_AUTORIZA_LANZAMIENTO',
        'schema_version_requested': 'BVL-SILVER-LAUNCH-CONTEXT-v1',
        'commit': commit,
        'artifact_hashes': {
            'entrypoint_sha256': sha256(main_bytes).hexdigest(),
            'dependencies_sha256': sha256(dep_bytes).hexdigest(),
        },
        'artifact_uris_proposed': {
            'entrypoint_uri': base_uri + '/fase6_tipo_cambio_batch.py',
            'dependencies_uri': base_uri + '/fase6_dependencias.zip',
        },
        'release_file_sha256': {name: sha256(read_committed(path)).hexdigest()
                                for name, path in MODULES.items()},
        'dataproc': {
            'region': 'southamerica-west1', 'runtime': '2.2',
            'driver_vcpus': 4, 'executor_vcpus_each': 4, 'executor_instances': 2,
            'dynamic_allocation': False, 'max_total_vcpu': 12,
            'staging_bucket': 'bvl26-dataproc-staging-7f3a9c',
            'subnet': 'PENDIENTE_CONFIRMACIÓN_QUALITY',
            'batch_id': 'PENDIENTE_CONTEXT0_QUALITY',
        },
        'quality_must': [
            'verificar que los artefactos cargados por create-only correspondan al commit',
            'fijar generaciones GCS de ambas rutas',
            'conciliar autorizacion/claim; generar nuevo intento si corresponde',
            'crear y firmar launch_context.json canónico con KMS autorizado',
            'suministrar launch_context.sig y referencias independientes',
            'autorizar manualmente UNA ejecucion supervisada tras validar runtime y red',
        ],
    }
    (out / 'solicitud_quality_NO_FIRMADA.json').write_text(
        json.dumps(draft, indent=2, sort_keys=True, ensure_ascii=False) + '\n', encoding='utf-8')
    print('RELEASE_PREPARADO:', out)
    print('PROCESSOR_COMMIT:', commit)
    print('ENTRYPOINT_SHA256:', draft['artifact_hashes']['entrypoint_sha256'])
    print('DEPENDENCIAS_SHA256:', draft['artifact_hashes']['dependencies_sha256'])
    print('NO_GCS_NO_AUTH_NO_BATCH')


if __name__ == '__main__':
    main()
