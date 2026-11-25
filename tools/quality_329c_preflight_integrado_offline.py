#!/usr/bin/env python3
"""Preflight 329A/329C: lectura local de archivos, nunca GCP/HDFS."""
import argparse, json, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'scripts'/'ingestion'))
from puerta_quality_bronze_v01 import auditar_contrato_329a_preflight_local

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--reserva-fixture-local',required=True,type=Path)
    p.add_argument('--plan-local',required=True,type=Path)
    p.add_argument('--piloto-local',required=True,type=Path)
    a=p.parse_args()
    try:
        resultado=auditar_contrato_329a_preflight_local(
            json.loads(a.reserva_fixture_local.read_bytes()),
            a.plan_local.read_bytes(), json.loads(a.piloto_local.read_bytes()))
    except Exception as e:
        p.exit(1,'RECHAZADO_OFFLINE: '+type(e).__name__+': '+str(e)+'\n')
    print(json.dumps(resultado,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
