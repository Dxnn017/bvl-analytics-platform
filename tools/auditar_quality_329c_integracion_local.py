#!/usr/bin/env python3
"""Quality 329C: cierre DOCUMENTAL local del motor, nunca llama APIs GCP."""
import argparse
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'/'ingestion'))
from orquestar_transferencias_bronze_v01 import revisar_quality_329c_desde_coordinador_local

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--plan', type=Path, required=True)
    p.add_argument('--piloto', type=Path, required=True)
    p.add_argument('--borrador', type=Path, required=True)
    a=p.parse_args()
    try:
        result=revisar_quality_329c_desde_coordinador_local(
            a.plan.read_bytes(),a.piloto.read_bytes(),a.borrador.read_bytes())
    except Exception as exc:
        print(f'RECHAZADO_LOCAL|{type(exc).__name__}|{exc}',file=sys.stderr)
        return 1
    print('===== PREFLIGHT QUALITY 329C / SOLO LOCAL =====')
    print(json.dumps(result,ensure_ascii=False,indent=2))
    print('RESULTADO: INTEGRACION_QUALITY_329C_LOCAL_PASS_SIN_CLOUD')
    return 0
if __name__=='__main__':sys.exit(main())
