#!/usr/bin/env python3
"""Genera propuesta local y nunca habilita cloud ni solicita credenciales."""
import argparse
from hashlib import sha256
from pathlib import Path
import json
from fase6_integracion_10_local import roadmap

FILES = [
    'scripts/processing/fase6_restantes_core.py',
    'scripts/processing/fase6_restantes_preflight.py',
    'scripts/processing/fase6_restantes_transform.py',
    'scripts/processing/fase6_restantes_writer.py',
    'scripts/processing/fase6_especializados_valores.py',
    'scripts/processing/fase6_especializados_hechos_valores.py',
    'scripts/processing/fase6_especializados_hechos_metadata.py',
    'scripts/processing/fase6_valores_conciliacion.py',
    'scripts/processing/fase6_integracion_10_local.py',
]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--output', type=Path, default=Path.home()/'Descargas'/'BVL_FASE6_INTEGRACION_10_ESTADO.json')
    args = parser.parse_args()
    repo = args.repo.resolve()
    result = roadmap()
    refs = {}
    for name in FILES:
        path = repo / name
        if not path.is_file():
            raise SystemExit('ALTO: falta módulo ' + name)
        refs[name] = sha256(path.read_bytes()).hexdigest()
    result['source_hashes_local'] = refs
    result['notes'] = [
        '10 transformadores locales candidatos NO equivalen a batches cloud',
        'Hechos de Importancia bloqueado hasta metadatos oficiales ratificados',
        'Derivado requiere cinco versiones Silver oficiales por hash/generacion',
        'No usar contexto de lanzamiento del piloto Tipo Cambio para otros datasets',
        'Un intento y un claim por procesamiento; Valores requiere dos salidas Quality como unidad',
    ]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + '\n',encoding='utf-8')
    print('PLAN_LOCAL:',args.output)
    print('PROCESOS_CANDIDATOS:',result['local_candidate_processes'])
    print('PROCESOS_BLOQUEADOS:',result['blocked_processes'])
    print('PRODUCTOS_SILVER_PENDIENTES:',result['remaining_output_count'])
    print('PUBLICACION: NO | GCS: NO | CLAIM: NO | DATAPROC: NO | GIT: SIN_CAMBIOS')

if __name__ == '__main__':
    main()
