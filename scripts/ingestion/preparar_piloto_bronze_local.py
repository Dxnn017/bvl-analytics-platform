#!/usr/bin/env python3
"""Prepara una propuesta de piloto Bronze SIN crear reservas ni transferencias.

Seguridad: se examina exclusivamente el plan local; el resultado no es una
aprobacion Quality ni una liberacion del Paso 101.7. Nunca llama APIs Cloud.
"""
import argparse
import csv
import hashlib
import io
import json
import re
from collections import Counter
from datetime import date
from pathlib import Path

EXPECTED_DATASETS = 12
EXPECTED_FILES = 3599
EXPECTED_BYTES = 393623640
RUN_RE = re.compile(r"bvl-bronze-inicial-\d{8}-\d{3}\Z")
DATASET_RE = re.compile(r"(?:bcrp|smv)/[a-z0-9_]+\Z")
SHA_RE = re.compile(r"[0-9a-f]{64}\Z")
BUCKET = "gs://bvl26-bronze-7f3a9c/landing/"

class PreparacionError(ValueError):
    pass

def exigir(cond, msg):
    if not cond:
        raise PreparacionError(msg)

def validar_plan(plan):
    """Valida inventario entero antes de seleccionar un solo fichero."""
    exigir(isinstance(plan, dict), "PLAN_TIPO")
    run_id = plan.get('run_id')
    exigir(isinstance(run_id, str) and RUN_RE.fullmatch(run_id), 'RUN_ID_INVALIDO')
    exigir(plan.get('tipo') == 'PLAN_LOCAL_BRONZE', 'PLAN_TIPO_INVALIDO')
    exigir(plan.get('publicacion') == 'NO_PUBLICADO', 'PLAN_PUBLICACION_INVALIDA')
    exigir(type(plan.get('transferencias')) is int and plan['transferencias'] == 0, 'PLAN_NO_LOCAL')
    exigir(plan.get('datasets') == EXPECTED_DATASETS and plan.get('archivos') == EXPECTED_FILES
           and plan.get('bytes') == EXPECTED_BYTES, 'PLAN_CABECERA')
    ops = plan.get('operaciones')
    exigir(isinstance(ops, list) and len(ops) == EXPECTED_FILES, 'PLAN_OPERACIONES')
    destinos = set()
    origenes = set()
    por_dataset = {}
    conteos = Counter()
    total = 0
    for i, op in enumerate(ops):
        exigir(isinstance(op, dict), f'OP_TIPO:{i}')
        ds = op.get('dataset')
        exigir(isinstance(ds, str) and DATASET_RE.fullmatch(ds), f'DATASET:{i}')
        exigir(op.get('run_id') == run_id, f'OP_RUN_ID:{i}')
        exigir(op.get('estado') == 'PLANIFICADO_NO_EJECUTADO', f'OP_ESTADO:{i}')
        hdfs = op.get('origen')
        prefijo_hdfs = f'/datalake/bvl/bronze/{ds}/'
        exigir(isinstance(hdfs, str) and hdfs.startswith(prefijo_hdfs), f'OP_ORIGEN:{i}')
        relativo = hdfs[len(prefijo_hdfs):]
        segmentos = relativo.split('/')
        exigir(relativo and all(s not in ('', '.', '..') for s in segmentos)
               and all(not s.startswith('..') for s in segmentos), f'OP_TRAVERSAL:{i}')
        fechas = [j for j, segmento in enumerate(segmentos[:-1])
                  if re.fullmatch(r'ingestion_date=\d{4}-\d{2}-\d{2}', segmento)]
        exigir(len(fechas) == 1, f'OP_INGESTION_DATE:{i}')
        try:
            date.fromisoformat(segmentos[fechas[0]].split('=', 1)[1])
        except ValueError:
            raise PreparacionError(f'OP_INGESTION_DATE_CALENDARIO:{i}')
        destino = f'{BUCKET}{ds}/run_id={run_id}/{relativo}'
        exigir(op.get('destino') == destino, f'OP_DESTINO:{i}')
        exigir(destino not in destinos and hdfs not in origenes, f'OP_DUPLICADO:{i}')
        size = op.get('size_bytes')
        exigir(type(size) is int and size > 0, f'OP_SIZE:{i}')
        sha = op.get('sha256')
        exigir(isinstance(sha, str) and SHA_RE.fullmatch(sha), f'OP_SHA:{i}')
        destinos.add(destino)
        origenes.add(hdfs)
        conteos[ds] += 1
        total += size
        mejor = por_dataset.get(ds)
        if mejor is None or (size, destino) < (mejor['size_bytes'], mejor['destino']):
            por_dataset[ds] = op
    exigir(len(conteos) == EXPECTED_DATASETS and total == EXPECTED_BYTES, 'INVENTARIO_INCONSISTENTE')
    return run_id, por_dataset, conteos

def preparar(plan, plan_sha256):
    run_id, por_dataset, conteos = validar_plan(plan)
    elegidos = []
    for ds, op in sorted(por_dataset.items()):
        elegidos.append({
            'run_id': run_id,
            'dataset': ds,
            'origen_hdfs': op['origen'],
            'destino_gcs': op['destino'],
            'bytes_esperados': op['size_bytes'],
            'sha256_esperado': op['sha256'],
            'bytes_observados': None,
            'sha256_gcs': None,
            'generacion_gcs': None,
            'estado': 'PENDIENTE_AUTORIZACION',
            'error': None,
        })
    return {
        'tipo': 'PROPUESTA_PILOTO_BRONZE_LOCAL',
        'run_id': run_id,
        'estado': 'CANDIDATO_NO_AUTORIZADO',
        'plan_sha256_local': plan_sha256,
        'seleccion': 'Menor size_bytes por cada dataset; desempate por URI GCS',
        'datasets': len(elegidos),
        'archivos': len(elegidos),
        'bytes_esperados': sum(x['bytes_esperados'] for x in elegidos),
        'inventario_completo_validado': {'datasets': len(conteos), 'archivos': EXPECTED_FILES, 'bytes': EXPECTED_BYTES},
        'quality_approved': False,
        'transfer_allowed': False,
        'reservas_cloud': 0,
        'transferencias_cloud': 0,
        'publicacion': 'NO_PUBLICADO',
        'nota': ('Los SHA provienen del plan LOCAL; no son prueba de identidad, '
                 'autenticidad Quality, integridad GCS ni autorizacion del Paso 101.7.'),
        'archivos_piloto': elegidos,
    }

def documentos(piloto):
    datos = (json.dumps(piloto, indent=2, ensure_ascii=False) + '\n').encode('utf-8')
    cabeceras = ('run_id', 'dataset', 'origen_hdfs', 'destino_gcs', 'bytes_esperados',
                 'sha256_esperado', 'bytes_observados', 'sha256_gcs',
                 'generacion_gcs', 'estado', 'error')
    cadena = io.StringIO(newline='')
    writer = csv.DictWriter(cadena, fieldnames=cabeceras, lineterminator='\n')
    writer.writeheader()
    writer.writerows(piloto['archivos_piloto'])
    return datos, cadena.getvalue().encode('utf-8')

def guardar_sin_sobrescribir(carpeta, piloto):
    """Reejecucion idempotente: si ya existe salida distinta, ERROR sin cambios."""
    run_id = piloto['run_id']
    nombre = f'PILOTO_CANDIDATO_{run_id}'
    js, csv_bytes = documentos(piloto)
    shas = (f'{hashlib.sha256(js).hexdigest()}  {nombre}.json\n'
            f'{hashlib.sha256(csv_bytes).hexdigest()}  {nombre}.csv\n').encode()
    salidas = [
        (carpeta / (nombre + '.json'), js),
        (carpeta / (nombre + '.csv'), csv_bytes),
        (carpeta / (nombre + '.sha256'), shas),
    ]
    exigir(carpeta.is_dir(), 'CARPETA_SALIDA_NO_EXISTE')
    for ruta, contenido in salidas:
        exigir(not ruta.exists() or ruta.read_bytes() == contenido,
               f'CONFLICTO_ARCHIVO_EXISTENTE:{ruta}')
    for ruta, contenido in salidas:
        if not ruta.exists():
            with ruta.open('xb') as salida:
                salida.write(contenido)
    return [str(ruta) for ruta, _ in salidas]

def main():
    cli = argparse.ArgumentParser(description='Propuesta piloto Bronze OFFLINE; nunca autoriza transferencias')
    cli.add_argument('--plan', required=True, type=Path)
    cli.add_argument('--out-dir', type=Path, help='Carpeta de salida (por defecto, directorio del plan)')
    args = cli.parse_args()
    try:
        plan_bytes = args.plan.read_bytes()
        plan = json.loads(plan_bytes)
        piloto = preparar(plan, hashlib.sha256(plan_bytes).hexdigest())
        paths = guardar_sin_sobrescribir(args.out_dir or args.plan.parent, piloto)
    except (ValueError, OSError, TypeError, KeyError) as exc:
        cli.exit(1, f'RECHAZADO|{exc}\n')
    print('===== PROPUESTA PILOTO LOCAL (NO AUTORIZADA) =====')
    print('RUN_ID:', piloto['run_id'])
    print('DATASETS:', piloto['datasets'])
    print('ARCHIVOS:', piloto['archivos'])
    print('BYTES:', piloto['bytes_esperados'])
    print('TRANSFERENCIAS_GCS: 0')
    for p in paths:
        print('EVIDENCIA:', p)
    print('RESULTADO: PILOTO_BRONZE_CANDIDATO_LISTO')

if __name__ == '__main__':
    main()
