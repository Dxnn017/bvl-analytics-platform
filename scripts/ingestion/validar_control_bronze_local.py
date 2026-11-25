#!/usr/bin/env python3
"""Validacion ESTRUCTURAL offline del contrato Bronze v0.1.

NO autentica GCS, IAM, Quality ni la liberacion operativa del Paso 101.7.
NO importa Google Cloud SDK ni permite transferencias.
"""
import argparse
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

BUCKET = 'gs://bvl26-bronze-7f3a9c/landing/'
TEMPLATE = BUCKET + '{source}/{dataset}/run_id={run_id}/ingestion_date={ingestion_date}/{filename}'
QUALITY = 'sa-bvl-quality@bvl-analytics-2026.iam.gserviceaccount.com'
RUN_PATTERN = r'bvl-bronze-inicial-\d{8}-\d{3}'
HEX = r'[0-9a-f]{64}'


class ContratoError(ValueError):
    pass


def exigir(condicion, mensaje):
    if not condicion:
        raise ContratoError(mensaje)


def leer_json(ruta):
    return json.loads(Path(ruta).read_bytes())


def canon_manifests(items):
    exigir(isinstance(items, list) and len(items) == 12, 'MANIFESTS_CANTIDAD')
    salida = {}
    for m in items:
        exigir(isinstance(m, dict), 'MANIFEST_INVALIDO')
        d = m.get('dataset')
        exigir(isinstance(d, str) and re.fullmatch(r'(smv|bcrp)/[a-z0-9_]+', d), 'MANIFEST_DATASET')
        origen = d.split('/')[0]
        exigir(m.get('source') == origen, 'MANIFEST_SOURCE')
        exigir(isinstance(m.get('manifest_id'), str) and m['manifest_id'], 'MANIFEST_ID')
        exigir(isinstance(m.get('official_uri'), str) and m['official_uri'].startswith('gs://'), 'MANIFEST_URI')
        exigir(isinstance(m.get('manifest_sha256'), str) and re.fullmatch(HEX, m['manifest_sha256']), 'MANIFEST_SHA')
        exigir(d not in salida, 'MANIFEST_DUPLICADO')
        salida[d] = {
            'manifest_id': m['manifest_id'],
            'source': origen,
            'official_uri': m['official_uri'],
            'manifest_sha256': m['manifest_sha256'],
            'target_prefix': m.get('target_prefix'),
        }
    return salida


def validar_local(plan, control, reserva_bytes, aprobacion_bytes, reserva_generacion):
    """Comprueba estructura y consistencia; NUNCA devuelve habilitacion de transferencia."""
    exigir(isinstance(plan, dict) and isinstance(control, dict), 'TIPO_DOCUMENTO')
    run = plan.get('run_id')
    exigir(isinstance(run, str) and re.fullmatch(RUN_PATTERN, run), 'RUN_ID')
    exigir(control.get('run_id') == run and control.get('status') == 'CANDIDATO', 'CONTROL_CANDIDATO')
    exigir(control.get('validation_approved') is False, 'CONTROL_NO_ES_LABORATORIO')
    exigir(plan.get('tipo') == 'PLAN_LOCAL_BRONZE' and plan.get('publicacion') == 'NO_PUBLICADO', 'PLAN_NO_ES_LABORATORIO')
    exigir(plan.get('transferencias') == 0, 'PLAN_TRANSFERENCIAS')
    ds = control.get('datasets')
    exigir(isinstance(ds, list) and len(ds) == 12, 'CONTROL_DATASETS')
    hashes = {}
    prefijos = {}
    for d in ds:
        nombre = d.get('dataset')
        exigir(isinstance(nombre, str) and re.fullmatch(r'(smv|bcrp)/[a-z0-9_]+', nombre), 'DATASET_CONTROL')
        exigir(nombre not in hashes, 'DATASET_CONTROL_DUPLICADO')
        exigir(d.get('run_id_operativo') == run, 'CONTROL_RUN')
        sha = d.get('manifest_sha256')
        exigir(isinstance(sha, str) and re.fullmatch(HEX, sha), 'CONTROL_SHA')
        hashes[nombre] = sha
        prefijos[nombre] = BUCKET + nombre + f'/run_id={run}/'
        exigir(d.get('landing_prefix_operativo') == prefijos[nombre], 'CONTROL_PREFIJO')
    exigir(len(hashes) == 12, 'CONTROL_DATASETS_CANTIDAD')

    ops = plan.get('operaciones')
    exigir(isinstance(ops, list) and len(ops) == 3599, 'PLAN_3599')
    exigir(plan.get('datasets') == 12 and plan.get('archivos') == 3599 and plan.get('bytes') == 393623640, 'PLAN_CONTEOS')
    destinos = set()
    conteos = Counter()
    total = 0
    for op in ops:
        exigir(isinstance(op, dict) and op.get('run_id') == run, 'OP_RUN')
        nombre = op.get('dataset')
        exigir(nombre in hashes and op.get('estado') == 'PLANIFICADO_NO_EJECUTADO', 'OP_ESTADO')
        origen = op.get('origen')
        exigir(isinstance(origen, str) and origen.startswith('/datalake/bvl/bronze/' + nombre + '/'), 'OP_ORIGEN')
        relativo = origen[len('/datalake/bvl/bronze/' + nombre + '/'):]
        exigir(relativo and all(x not in ('', '.', '..') for x in relativo.split('/')), 'OP_TRAVERSAL')
        destino = prefijos[nombre] + relativo
        exigir(op.get('destino') == destino and destino not in destinos, 'OP_DESTINO')
        exigir(type(op.get('size_bytes')) is int and op['size_bytes'] > 0, 'OP_SIZE')
        exigir(isinstance(op.get('sha256'), str) and re.fullmatch(HEX, op['sha256']), 'OP_SHA')
        destinos.add(destino)
        conteos[nombre] += 1
        total += op['size_bytes']
    exigir(set(conteos) == set(hashes) and total == plan['bytes'], 'PLAN_INVENTARIO')

    reserva = json.loads(reserva_bytes)
    aprobacion = json.loads(aprobacion_bytes)
    exigir(isinstance(reserva, dict) and isinstance(aprobacion, dict), 'RESERVA_APROBACION_TIPO')
    exigir(reserva.get('contract_version') in ('v0.1', 'V02'), 'RESERVA_VERSION')
    exigir(reserva.get('run_id') == run and reserva.get('layer') == 'bronze' and reserva.get('status') == 'RESERVED', 'RESERVA_ESTADO')
    exigir(reserva.get('owner_service_account') == QUALITY, 'RESERVA_OWNER_DECLARADO')
    exigir(reserva.get('target_base_prefix', reserva.get('target_prefix')) == BUCKET, 'RESERVA_BASE')
    exigir(reserva.get('target_path_template') == TEMPLATE, 'RESERVA_TEMPLATE')
    prefijos_reserva = reserva.get('target_prefixes', reserva.get('authorized_dataset_prefixes'))
    exigir(isinstance(prefijos_reserva, list) and len(prefijos_reserva) == 12 and set(prefijos_reserva) == set(prefijos.values()), 'RESERVA_PREFIJOS')
    if 'target_prefixes' in reserva and 'authorized_dataset_prefixes' in reserva:
        exigir(set(reserva['target_prefixes']) == set(reserva['authorized_dataset_prefixes']), 'RESERVA_ALIASES')
    manifest_reserva = canon_manifests(reserva.get('approved_manifests'))
    exigir(set(manifest_reserva) == set(hashes), 'RESERVA_DATASETS')
    for n, m in manifest_reserva.items():
        exigir(m['manifest_sha256'] == hashes[n], 'RESERVA_MANIFEST_SHA')
        if m['target_prefix'] is not None:
            exigir(m['target_prefix'] == prefijos[n], 'RESERVA_MANIFEST_PREFIJO')

    exigir(aprobacion.get('contract_version') == 'v0.1', 'APROBACION_VERSION')
    exigir(aprobacion.get('signal_type') == 'QUALITY_APPROVED' and aprobacion.get('status') == 'QUALITY_APPROVED', 'APROBACION_ESTADO')
    exigir(aprobacion.get('run_id') == run and aprobacion.get('layer') == 'bronze', 'APROBACION_IDENTIDAD')
    exigir(aprobacion.get('approved_by_sa') == QUALITY, 'APROBACION_OWNER_DECLARADO')
    exigir(aprobacion.get('target_base_prefix') == BUCKET and aprobacion.get('target_path_template') == TEMPLATE, 'APROBACION_RUTA')
    manifest_aprobacion = canon_manifests(aprobacion.get('approved_manifests'))
    exigir(set(manifest_aprobacion) == set(manifest_reserva), 'APROBACION_DATASETS')
    for n, m in manifest_aprobacion.items():
        exigir(m['manifest_sha256'] == hashes[n] and m['official_uri'] == manifest_reserva[n]['official_uri'] and m['manifest_id'] == manifest_reserva[n]['manifest_id'], 'APROBACION_MANIFEST')
        exigir(m['target_prefix'] == prefijos[n], 'APROBACION_PREFIJO')
    enlace = aprobacion.get('reservation_binding', {})
    exigir(enlace.get('reservation_uri') == f'gs://bvl26-control-7f3a9c/reservations/bronze/{run}.json', 'APROBACION_RESERVA_URI')
    exigir(type(reserva_generacion) is int and reserva_generacion > 0 and enlace.get('reservation_generation') == reserva_generacion, 'APROBACION_RESERVA_GENERACION')
    exigir(enlace.get('reservation_sha256') == hashlib.sha256(reserva_bytes).hexdigest(), 'APROBACION_RESERVA_HASH')
    return {
        'resultado': 'VALIDACION_ESTRUCTURAL_LOCAL_OK',
        'run_id': run,
        'datasets': len(hashes),
        'archivos': len(destinos),
        'bytes': total,
        'transfer_allowed': False,
        'transferencias_gcs': 0,
        'advertencia': 'NO_VERIFICA_IAM_GCS_AUTENTICIDAD_NI_RELEASE_101_7',
    }


def main():
    parser = argparse.ArgumentParser(description='Validador offline: NUNCA habilita transferencias')
    parser.add_argument('--plan', required=True)
    parser.add_argument('--control', required=True)
    parser.add_argument('--reserva', required=True)
    parser.add_argument('--aprobacion', required=True)
    parser.add_argument('--reserva-generacion-lab', required=True, type=int)
    args = parser.parse_args()
    try:
        resultado = validar_local(leer_json(args.plan), leer_json(args.control), Path(args.reserva).read_bytes(), Path(args.aprobacion).read_bytes(), args.reserva_generacion_lab)
    except (ContratoError, ValueError, KeyError, TypeError) as exc:
        parser.exit(1, f'RECHAZADO_LOCAL|{exc}\n')
    print(json.dumps(resultado, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
