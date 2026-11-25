#!/usr/bin/env python3
"""Auditoría EXCLUSIVAMENTE local del run_id: no GCS, HDFS ni escritura."""
import argparse, json, hashlib, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'scripts'/'ingestion'))
from preparar_piloto_bronze_local import validar_plan


def revisar(config_path, plan_path=None):
    cfg = json.loads(Path(config_path).read_text(encoding='utf-8'))
    assert cfg['cloud_operations_authorized'] is False
    assert cfg['transfer_allowed'] is False
    run = cfg['run_id_oficial_declarado_por_quality']
    print('RUN_ID_OFICIAL_DECLARADO:',run)
    print('RESERVA_URI:',cfg['reservation_uri'])
    print('APROBACION_URI:',cfg['approval_uri'])
    print('TRANSFER_ALLOWED: False')
    print('OPERACIONES_GCS: 0')
    if plan_path is None:
        print('PLAN_OFICIAL: NO_ADJUNTADO')
        print('RESULTADO: AUDITORIA_CODIGO_SIN_PLAN_OPERATIVO')
        return 0
    data = Path(plan_path).read_bytes()
    plan = json.loads(data)
    if plan.get('run_id') != run:
        print('ERROR: PLAN_RUN_ID_NO_COINCIDE_CON_RESERVA_OFICIAL',file=sys.stderr)
        return 2
    validar_plan(plan)
    print('PLAN_SHA256_CALCULADO:',hashlib.sha256(data).hexdigest())
    print('DATASETS:',plan['datasets'],'ARCHIVOS:',plan['archivos'],'BYTES:',plan['bytes'])
    print('RESULTADO: PLAN_ESTRUCTURAL_COMPATIBLE_SIN_AUTORIZACION_CLOUD')
    return 0


if __name__=='__main__':
    p=argparse.ArgumentParser(description='Auditoría local del run oficial, sin operaciones Cloud')
    p.add_argument('--config',required=True,type=Path)
    p.add_argument('--plan',type=Path)
    a=p.parse_args()
    try:
        raise SystemExit(revisar(a.config,a.plan))
    except (OSError,KeyError,ValueError,AssertionError) as exc:
        raise SystemExit('RECHAZADO_LOCAL: '+str(exc))
