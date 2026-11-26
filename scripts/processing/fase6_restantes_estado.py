#!/usr/bin/env python3
"""CLI de inspeccion LOCAL. No lee ni escribe GCS, no consume claim."""
from __future__ import annotations
from pathlib import Path
import argparse
import hashlib
import json
import sys

from fase6_restantes_core import (
    SOURCES, FILE_COUNTS, CANDIDATE_TRANSFORMS, DERIVED, DEPENDENCIES,
)


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument('--repo', type=Path, default=Path.cwd())
    args = p.parse_args(argv)
    repo = args.repo.resolve()
    source_files = {
      'legacy_bcrp_and_financial': 'scripts/processing/bronze_to_silver_dataproc_candidato.py',
      'legacy_principales': 'scripts/processing/principales_cuentas_to_silver.py',
      'legacy_cotizaciones': 'scripts/processing/cotizaciones_to_silver.py',
      'legacy_montos': 'scripts/processing/montos_sab_to_silver.py',
      'legacy_hechos': 'scripts/processing/hechos_importancia_to_silver.py',
      'legacy_valores': 'scripts/processing/valores_smv_to_silver.py',
      'legacy_identidad': 'scripts/processing/hechos_identidad_gcp_candidato.py',
    }
    present = {}
    for key, name in source_files.items():
        path = repo / name
        if not path.is_file():
            raise SystemExit('ALTO: no existe codigo historico ' + name)
        present[key] = {'file': name, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    doc = {
      'status': 'ADAPTADORES_CANDIDATOS_NO_AUTORIZA_EJECUCION',
      'dataset_count': len(SOURCES),
      'products_direct': sum(len(v) for v in SOURCES.values()),
      'products_derived': 1,
      'dataset_with_candidate_transform': len(CANDIDATE_TRANSFORMS),
      'candidate_datasets': sorted(CANDIDATE_TRANSFORMS),
      'not_ready_for_quality': sorted(set(SOURCES) - CANDIDATE_TRANSFORMS),
      'quality_approved': False,
      'execution_enabled': False,
      'claim_consumed': False,
      'gold_consumption_allowed': False,
      'derived_product': DERIVED,
      'derived_dependencies': list(DEPENDENCIES),
      'source_hashes': present,
      'note': 'Faltan pruebas Spark locales por fuente, contraste con Bronze oficial, firmas por intento y publicacion Quality.'
    }
    dest = Path.home() / 'Descargas' / 'BVL_FASE6_ADAPTADORES_ESTADO.json'
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print('PLAN_ADAPTADORES:', dest)
    print('PROCESOS_PENDIENTES:', len(SOURCES))
    print('PRODUCTOS_DIRECTOS:', sum(len(v) for v in SOURCES.values()))
    print('ADAPTADORES_CANDIDATOS:', len(CANDIDATE_TRANSFORMS))
    print('ADAPTADORES_SIN_IMPLEMENTACION_CONFORME:', len(set(SOURCES) - CANDIDATE_TRANSFORMS))
    print('EJECUCION_GCS: NO | CLAIM: NO | GIT: SIN_CAMBIOS')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
