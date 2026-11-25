#!/usr/bin/env python3
"""Adapter ESTRICTAMENTE OFFLINE de reserva Bronze 329A hacia auditoría 329C.

NO usa Google Cloud SDK, no lee HDFS ni GCS, no emite aprobaciones y no
habilita TRANSFER_ALLOWED. Un hash/JSON local nunca acredita autoría IAM.
"""
import argparse
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

RUN = 'bvl-bronze-inicial-20261125-001'
PLAN_SHA = '1397f92911b159e8df257fc82a7dd01593fce0d2e898e796a82efc77ae35d90e'
IDENTIDAD = 'e4a3fb69f25ee87a26e71a40506017c199d43cd13735bef3330462c906205a2c'
RESERVA_GENERACION_REFERENCIAL = 1795640338029794
RESERVA_SHA_REFERENCIAL = '80cb92939b975659d32514fff3a64a30e02148434222f1391b1afd675913c232'
PREFIX = f'gs://bvl26-bronze-7f3a9c/landing/'

class Incompatible(ValueError):
    pass

def exigir(check, why):
    if not check:
        raise Incompatible(why)

def canon(x):
    return json.dumps(x, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')

def validar(reserva, plan, plan_bytes, piloto):
    """Evalúa consistencia local. `reserva` debe ser copia local confiable del payload.

    Este adaptador no compara bytes de reserva contra la huella remota porque una
    copia local sin extracción autenticada NO acredita que sean los mismos bytes.
    """
    exigir(reserva.get('run_id') == plan.get('run_id') == piloto.get('run_id') == RUN,
           'RUN_ID_INCOMPATIBLE')
    exigir(reserva.get('record_type') == 'BRONZE_RUN_RESERVATION', 'TIPO_RESERVA')
    exigir(reserva.get('reservation_status') == 'RESERVADO', 'RESERVA_NO_RESERVADA')
    exigir(reserva.get('status') == 'CANDIDATO', 'RESERVA_ESTADO_CANDIDATO_INESPERADO')
    exigir(reserva.get('validation_approved') is False and
           reserva.get('publication_status') == 'NO_PUBLICADO', 'RESERVA_APROBACION_INESPERADA')
    exigir(reserva.get('schema_version') == '1.0-proposed-329A', 'VERSION_RESERVA')
    exigir(reserva.get('reservation_context', {}).get('identity_to_write') ==
           'sa-bvl-quality@bvl-analytics-2026.iam.gserviceaccount.com',
           'EMISOR_DECLARADO_NO_COINCIDE')
    exigir(reserva.get('reservation_context', {}).get('write_condition') == 'if_generation_match=0',
           'PRECONDICION_RESERVA')
    exigir(reserva.get('batch_identity_sha256') == IDENTIDAD, 'IDENTIDAD_ESPERADA')
    datasets = reserva.get('datasets')
    exigir(isinstance(datasets, list) and len(datasets) == 12, 'RESERVA_12_DATASETS')
    dmap = {}
    for d in datasets:
        nombre = d.get('dataset')
        exigir(isinstance(nombre, str) and re.fullmatch(r'(smv|bcrp)/[a-z0-9_]+', nombre)
               and nombre not in dmap, 'DATASET_REPETIDO_O_INVALIDO')
        exigir(isinstance(d.get('manifest_sha256'), str) and
               re.fullmatch(r'[0-9a-f]{64}', d['manifest_sha256']), 'MANIFEST_SHA_INVALIDO')
        dmap[nombre] = d
    identity = {
        'batch_type': reserva.get('batch_type'),
        'inventory_sha256': reserva.get('inventory_sha256'),
        'datasets': sorted(({k: d[k] for k in ('dataset', 'manifest_sha256',
                        'expected_file_count', 'expected_size_bytes')} for d in datasets),
                        key=lambda r:r['dataset']),
    }
    exigir(hashlib.sha256(canon(identity)).hexdigest() == IDENTIDAD, 'IDENTIDAD_LOTE_NO_COINCIDE')
    exigir(reserva.get('dataset_count') == 12 and reserva.get('file_count') == 3599 and
           reserva.get('total_size_bytes') == 393623640, 'CONTEOS_RESERVA')
    exigir(plan.get('tipo') == 'PLAN_LOCAL_BRONZE' and plan.get('publicacion') == 'NO_PUBLICADO'
           and plan.get('transferencias') == 0, 'PLAN_ESTADO')
    exigir(hashlib.sha256(plan_bytes).hexdigest() == PLAN_SHA, 'PLAN_SHA_NO_COINCIDE')
    exigir(plan.get('datasets') == 12 and plan.get('archivos') == 3599 and
           plan.get('bytes') == 393623640, 'PLAN_CONTEOS')
    ops = plan.get('operaciones')
    exigir(isinstance(ops, list) and len(ops) == 3599, 'OPERACIONES_PLAN')
    totals, sizes, destinations, by_key = Counter(), Counter(), set(), {}
    for op in ops:
        d = op.get('dataset'); origin = op.get('origen'); dest = op.get('destino')
        exigir(d in dmap and op.get('run_id') == RUN and
               op.get('estado') == 'PLANIFICADO_NO_EJECUTADO', 'PLAN_OPERACION_IDENTIDAD')
        raiz = f'/datalake/bvl/bronze/{d}/'
        exigir(isinstance(origin, str) and origin.startswith(raiz), 'PLAN_ORIGEN')
        rel = origin[len(raiz):]
        exigir(rel and all(s not in ('', '.', '..') for s in rel.split('/')), 'RUTA_TRAVERSAL')
        esperado = f'{PREFIX}{d}/run_id={RUN}/{rel}'
        exigir(dest == esperado and dest not in destinations, 'DESTINO_PLAN')
        exigir(type(op.get('size_bytes')) is int and op['size_bytes'] > 0 and
               isinstance(op.get('sha256'), str) and
               re.fullmatch('[0-9a-f]{64}', op['sha256']), 'OPERACION_INTEGRIDAD')
        destinations.add(dest); totals[d] += 1; sizes[d] += op['size_bytes']; by_key[dest] = op
    exigir(all(totals[d] == x['expected_file_count'] and
               sizes[d] == x['expected_size_bytes'] for d,x in dmap.items()), 'PLAN_VS_RESERVA')
    exigir(len(destinations) == 3599 and sum(sizes.values()) == 393623640, 'TOTAL_PLAN')
    exigir(piloto.get('quality_approved') is False and
           piloto.get('transfer_allowed') is False and
           piloto.get('publicacion') == 'NO_PUBLICADO', 'PILOTO_NO_BLOQUEADO')
    exigir(piloto.get('plan_sha256_local') == PLAN_SHA and piloto.get('archivos') == 12 and
           piloto.get('bytes_esperados') == 23052390, 'PILOTO_IDENTIDAD')
    files = piloto.get('archivos_piloto')
    exigir(isinstance(files, list) and len(files)==12, 'PILOTO_12')
    seen, pilot_bytes, pilot_ds = set(), 0, set()
    for f in files:
        dest = f.get('destino_gcs'); op = by_key.get(dest)
        exigir(op is not None and dest not in seen and
               f.get('run_id') == RUN and f.get('dataset') == op['dataset'] and
               f.get('origen_hdfs') == op['origen'] and
               f.get('bytes_esperados') == op['size_bytes'] and
               f.get('sha256_esperado') == op['sha256'], 'PILOTO_VS_PLAN')
        seen.add(dest); pilot_ds.add(f['dataset']); pilot_bytes += f['bytes_esperados']
    exigir(len(pilot_ds) == 12 and pilot_bytes == 23052390, 'PILOTO_DISTRIBUCION')
    return {'resultado':'CONTRATO_329A_COMPATIBLE_LOCAL_PASS', 'run_id':RUN,
            'plan_sha256':PLAN_SHA, 'identidad_lote_sha256':IDENTIDAD,
            'reserva_generacion_referencial':RESERVA_GENERACION_REFERENCIAL,
            'reserva_sha256_referencial':RESERVA_SHA_REFERENCIAL,
            'datasets':12, 'archivos_plan':3599,'bytes_plan':393623640,
            'archivos_piloto':12,'bytes_piloto':23052390,
            'reserva_remota_leida_en_esta_prueba':False,
            'procedencia_quality_autenticada':False,
            'quality_approved':False,'transfer_allowed':False,
            'operaciones_gcp':0}

def main():
    p = argparse.ArgumentParser(description='Validación de reserva 329A, estrictamente local')
    p.add_argument('--reserva-local', type=Path, required=True)
    p.add_argument('--plan-local', type=Path, required=True)
    p.add_argument('--piloto-local', type=Path, required=True)
    args = p.parse_args()
    try:
        plan_bytes = args.plan_local.read_bytes()
        result = validar(json.loads(args.reserva_local.read_bytes()),
                         json.loads(plan_bytes),plan_bytes,
                         json.loads(args.piloto_local.read_bytes()))
    except (OSError, ValueError, TypeError, KeyError) as err:
        p.exit(1, f'RECHAZADO_LOCAL: {err}\n')
    print(json.dumps(result, ensure_ascii=False, indent=2))

if __name__=='__main__':
    main()
