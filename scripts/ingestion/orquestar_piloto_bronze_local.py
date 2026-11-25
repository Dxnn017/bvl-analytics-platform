#!/usr/bin/env python3
"""Cierre local de preparación piloto BVL Bronze, sin transferencias ni Cloud.

NUNCA autoriza transferencias. Integra inventario, selección y evidencia real HDFS.
No acepta objetos Cloud, identidades o flags locales como autorización.
"""
import argparse
import csv
import hashlib
import io
import json
import re
from collections import Counter
from pathlib import Path

from preparar_piloto_bronze_local import validar_plan
from transferir_hdfs_gcs import validar_operacion, TRANSFERENCIAS_HABILITADAS

RUN_RE = re.compile(r'bvl-bronze-inicial-\d{8}-\d{3}\Z')


class OrquestacionError(ValueError):
    pass


def exigir(condicion, codigo):
    if not condicion:
        raise OrquestacionError(codigo)


def leer(ruta):
    return json.loads(Path(ruta).read_bytes())


def integrar(plan_bytes, piloto, evidencia):
    """Devuelve reporte de preparación NO operativa. No ejecuta transferencia."""
    exigir(TRANSFERENCIAS_HABILITADAS is False, 'NUCLEO_CLOUD_DESBLOQUEADO')
    plan = json.loads(plan_bytes)
    run, _, conteos = validar_plan(plan)
    exigir(bool(RUN_RE.fullmatch(run)), 'RUN_ID')
    exigir(piloto.get('run_id') == run and evidencia.get('run_id') == run, 'RUN_MISMATCH')
    exigir(piloto.get('tipo') == 'PROPUESTA_PILOTO_BRONZE_LOCAL', 'PILOTO_TIPO')
    exigir(piloto.get('estado') == 'CANDIDATO_NO_AUTORIZADO', 'PILOTO_ESTADO')
    exigir(piloto.get('plan_sha256_local') == hashlib.sha256(plan_bytes).hexdigest(), 'PLAN_SHA_NO_COINCIDE')
    exigir(piloto.get('quality_approved') is False and piloto.get('transfer_allowed') is False, 'PILOTO_APROBACION_INVALIDA')
    exigir(piloto.get('transferencias_cloud') == 0 and piloto.get('reservas_cloud') == 0, 'PILOTO_YA_OPERADO')
    exigir(evidencia.get('tipo') == 'EVIDENCIA_LOCAL_HDFS', 'HDFS_EVIDENCIA_TIPO')
    exigir(evidencia.get('quality_approved') is False and evidencia.get('transfer_allowed') is False, 'HDFS_APROBACION_INVALIDA')
    exigir(evidencia.get('transferencias_gcs') == 0, 'HDFS_CLOUD_NO_ES_CERO')
    items = piloto.get('archivos_piloto')
    checks = evidencia.get('archivos')
    exigir(isinstance(items, list) and len(items) == 12, 'PILOTO_NO_12')
    exigir(isinstance(checks, list) and len(checks) == 12, 'HDFS_NO_12')
    exigir(piloto.get('datasets') == 12 and piloto.get('archivos') == 12, 'PILOTO_CABECERA')
    exigir(evidencia.get('archivos_esperados') == 12 and evidencia.get('archivos_verificados') == 12 and evidencia.get('archivos_con_error') == 0, 'HDFS_CABECERA')

    ops = {(op['origen'], op['destino']): op for op in plan['operaciones']}
    exigir(len(ops) == 3599, 'OPERACIONES_DUPLICADAS')
    por_ds = Counter()
    vistos = set()
    detalles = []
    for i, (p, h) in enumerate(zip(items, checks), 1):
        exigir(isinstance(p, dict) and isinstance(h, dict), f'ITEM_TIPO:{i}')
        key = (p.get('origen_hdfs'), p.get('destino_gcs'))
        exigir(key in ops and key not in vistos, f'ARCHIVO_NO_AUTORIZADO_O_DUPLICADO:{i}')
        vistos.add(key)
        op = ops[key]
        validar_operacion(op)  # Mismo validador de rutas exactas del núcleo GCS
        ds = op['dataset']
        por_ds[ds] += 1
        exigir(p.get('run_id') == run and p.get('dataset') == ds, f'PILOTO_IDENTIDAD:{i}')
        exigir(p.get('bytes_esperados') == op['size_bytes'] and p.get('sha256_esperado') == op['sha256'], f'PILOTO_INTEGRIDAD:{i}')
        exigir(p.get('estado') == 'PENDIENTE_AUTORIZACION' and p.get('sha256_gcs') is None and p.get('generacion_gcs') is None, f'PILOTO_YA_TRANSFERIDO:{i}')
        exigir(h.get('run_id') == run and h.get('dataset') == ds and (h.get('origen_hdfs'), h.get('destino_gcs')) == key, f'HDFS_IDENTIDAD:{i}')
        exigir(h.get('estado') == 'VERIFICADO_HDFS' and h.get('error') is None, f'HDFS_NO_VERIFICADO:{i}')
        exigir(h.get('bytes_esperados') == op['size_bytes'] and h.get('bytes_observados') == op['size_bytes'], f'HDFS_TAMANO:{i}')
        exigir(h.get('sha256_esperado') == op['sha256'] and h.get('sha256_hdfs_observado') == op['sha256'], f'HDFS_SHA:{i}')
        detalles.append({
            'run_id': run, 'dataset': ds, 'origen_hdfs': op['origen'],
            'destino_gcs': op['destino'], 'bytes_esperados': op['size_bytes'],
            'bytes_hdfs_verificados': h['bytes_observados'],
            'sha256_hdfs_verificado': h['sha256_hdfs_observado'],
            'estado': 'PREPARADO_LOCAL_SIN_PERMISO_CLOUD',
            'generacion_gcs': None, 'bytes_transferidos_gcs': 0,
            'error': None,
        })
    exigir(len(por_ds) == 12 and all(v == 1 for v in por_ds.values()), 'DATASETS_PILOTO')
    exigir(set(por_ds) == set(conteos), 'DATASETS_NO_COINCIDEN')
    total = sum(x['bytes_esperados'] for x in detalles)
    exigir(piloto.get('bytes_esperados') == evidencia.get('bytes_esperados') == total, 'BYTES_TOTAL_NO_COINCIDE')
    return {
        'schema_version': '0.1-local', 'tipo': 'PREFLIGHT_ORQUESTADOR_PILOTO',
        'run_id': run, 'estado': 'LISTO_PARA_REVISION_QUALITY_NO_OPERATIVO',
        'datasets': 12, 'archivos': 12, 'bytes_totales': total,
        'plan_sha256': hashlib.sha256(plan_bytes).hexdigest(),
        'hdfs_verificados': 12, 'gcs_transferidos': 0,
        'reservas_cloud': 0, 'aprobaciones_cloud': 0,
        'reserva_autenticada': False, 'aprobacion_autenticada': False,
        'liberacion_101_7_verificada': False, 'transfer_allowed': False,
        'publicacion': 'NO_PUBLICADO', 'detalle': detalles,
        'bloqueos_pendientes': [
            'CONFORMIDAD_QUALITY_RUTAS_PDF',
            'IAM_LECTURA_CONTROL',
            'RESERVA_GCS_AUTENTICADA',
            'QUALITY_APPROVED_AUTENTICA',
            'REFERENCIAS_TRUSTED_CHANNEL',
            'LIBERACION_OPERATIVA_101_7',
            'PILOTO_TRANSFERENCIA_AUTORIZADA',
        ],
    }


def guardar(reporte, directorio):
    directorio = Path(directorio)
    exigir(directorio.is_dir(), 'DIRECTORIO_NO_EXISTE')
    stem = f"ORQUESTADOR_PILOTO_LOCAL_{reporte['run_id']}"
    js = (json.dumps(reporte, indent=2, ensure_ascii=False) + '\n').encode('utf-8')
    cols = ('run_id', 'dataset', 'origen_hdfs', 'destino_gcs', 'bytes_esperados',
            'bytes_hdfs_verificados', 'sha256_hdfs_verificado', 'estado',
            'generacion_gcs', 'bytes_transferidos_gcs', 'error')
    file_csv = io.StringIO(newline='')
    w = csv.DictWriter(file_csv, fieldnames=cols, lineterminator='\n')
    w.writeheader(); w.writerows(reporte['detalle'])
    csv_data = file_csv.getvalue().encode('utf-8')
    sha = (f'{hashlib.sha256(js).hexdigest()}  {stem}.json\n'
           f'{hashlib.sha256(csv_data).hexdigest()}  {stem}.csv\n').encode('utf-8')
    files = [(directorio / (stem + '.json'), js),
             (directorio / (stem + '.csv'), csv_data),
             (directorio / (stem + '.sha256'), sha)]
    for p, data in files:
        exigir(not p.exists() or p.read_bytes() == data, f'EVIDENCIA_PREEXISTENTE_DISTINTA:{p}')
    for p, data in files:
        if not p.exists():
            p.write_bytes(data)  # Salidas nuevas únicamente; validación completa previa
    return [str(p) for p, _ in files]


def main():
    parser = argparse.ArgumentParser(description='Preflight piloto Bronze 100% offline, bloqueado por defecto')
    parser.add_argument('--plan', required=True, type=Path)
    parser.add_argument('--piloto', required=True, type=Path)
    parser.add_argument('--hdfs-evidencia', required=True, type=Path)
    parser.add_argument('--salida-dir', required=True, type=Path)
    args = parser.parse_args()
    try:
        reporte = integrar(args.plan.read_bytes(), leer(args.piloto), leer(args.hdfs_evidencia))
        salidas = guardar(reporte, args.salida_dir)
    except (OrquestacionError, ValueError, KeyError, TypeError, OSError) as exc:
        parser.exit(1, f'RECHAZADO|{exc}\n')
    print('===== ORQUESTADOR LOCAL / NO CLOUD =====')
    print('DATASETS:', reporte['datasets'])
    print('ARCHIVOS:', reporte['archivos'])
    print('BYTES:', reporte['bytes_totales'])
    print('HDFS_VERIFICADOS:', reporte['hdfs_verificados'])
    print('TRANSFER_ALLOWED:', reporte['transfer_allowed'])
    print('TRANSFERENCIAS_GCS:', reporte['gcs_transferidos'])
    for p in salidas:
        print('EVIDENCIA:', p)
    print('RESULTADO: ORQUESTADOR_PILOTO_LOCAL_LISTO')


if __name__ == '__main__':
    main()
