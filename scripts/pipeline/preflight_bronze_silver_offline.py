#!/usr/bin/env python3
"""Inspección offline del BORRADOR Bronze-Silver. NUNCA autoriza ni ejecuta Spark/GCS."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path

PROJECT = 'bvl-analytics-2026'
RUN = 'bvl-bronze-inicial-20261125-001'
BUCKET = 'bvl26-bronze-7f3a9c'
CONTROL = 'bvl26-control-7f3a9c'
FILES = 3599
BYTES = 393623640
EXPECTED = {
    'bcrp/embig': (1, 89858),
    'bcrp/tipo_cambio': (1, 154221),
    'smv/cambios_patrimonio': (1, 160953),
    'smv/cotizaciones': (60, 37360244),
    'smv/estado_resultados': (1, 5686254),
    'smv/flujo_efectivo': (1, 7890131),
    'smv/hechos_importancia': (1979, 322432612),
    'smv/hechos_valores_consulta': (23, 19917),
    'smv/montos_sab': (1221, 7227879),
    'smv/principales_cuentas': (20, 2750808),
    'smv/situacion_financiera': (1, 8569828),
    'smv/valores_inscritos': (290, 1280935),
}
PROCESSORS = {
    'bcrp/embig': 'scripts/processing/bcrp_incremental.py',
    'bcrp/tipo_cambio': 'scripts/processing/bcrp_incremental.py',
    'smv/cambios_patrimonio': 'scripts/processing/finanzas_incremental.py',
    'smv/cotizaciones': 'scripts/processing/cotizaciones_incremental_desde_bronze.py',
    'smv/estado_resultados': 'scripts/processing/finanzas_incremental.py',
    'smv/flujo_efectivo': 'scripts/processing/finanzas_incremental.py',
    'smv/hechos_importancia': 'scripts/processing/hechos_importancia_to_silver.py + candidato staging + hechos_incremental.py',
    'smv/hechos_valores_consulta': 'PENDIENTE: confirmar productor del candidato y tratamiento de archivos de error',
    'smv/montos_sab': 'scripts/processing/montos_sab_incremental.py',
    'smv/principales_cuentas': 'scripts/processing/principales_cuentas_incremental.py',
    'smv/situacion_financiera': 'scripts/processing/finanzas_incremental.py',
    'smv/valores_inscritos': 'scripts/processing/valores_smv_to_silver.py',
}


def check(data: dict) -> dict:
    """Solo valida el contrato *borrador*: no comprueba firmas ni generaciones reales."""
    def must(condition, message):
        if not condition:
            raise ValueError(message)

    must(isinstance(data, dict), 'El contrato no es un objeto JSON')
    expected_root = {
        'schema_version': 'BVL-BRONZE-SILVER-CONTRACT-v1-DRAFT',
        'project_id': PROJECT,
        'run_id': RUN,
        'producer': 'BRONZE',
        'consumer': 'SILVER',
        'status': 'DRAFT_PENDING_TEAM_REVIEW',
        'expected_datasets': 12,
        'expected_files': FILES,
        'expected_bytes': BYTES,
        'required_bronze_status': 'BRONZE_COMPLETADO',
        'required_silver_status': 'SILVER_AUTORIZADO',
        'consume_landing_directly': False,
        'overwrite_original_data': False,
        'verify_input_generations': True,
        'verify_manifest_generation': True,
        'trigger_on_candidate_manifest': False,
        'trigger_on_individual_file': False,
        'quality_approved': False,
        'silver_consumption_allowed': False,
        'published': False,
        'consumer_service_account': f'sa-bvl-silver@{PROJECT}.iam.gserviceaccount.com',
    }
    for key, val in expected_root.items():
        must(type(data.get(key)) is type(val) and data.get(key) == val,
             f'Contrato incompatible o no autorizado: {key}')
    must(data.get('required_workflow_parameters') ==
         ['run_id','dataset','bronze_input_uris','bronze_manifest_uri','bronze_manifest_generation'],
         'Parámetros Workflow distintos')
    must(data.get('required_manifest_fields') ==
         ['run_id','dataset','status','quality_approved','files'],
         'Campos manifest distintos')
    must(data.get('required_file_fields') ==
         ['uri','generation','size_bytes','sha256'],
         'Campos de integridad distintos')
    must(isinstance(data.get('datasets'), list) and len(data['datasets']) == 12,
         'Se esperaban 12 datasets')
    seen = set()
    total_files = total_bytes = 0
    for row in data['datasets']:
        must(isinstance(row,dict), 'Dataset malformado')
        ds=row.get('dataset')
        must(isinstance(ds,str) and ds in EXPECTED and ds not in seen,
             f'Dataset inválido, inesperado o duplicado: {ds}')
        seen.add(ds)
        n, b = EXPECTED[ds]
        must(type(row.get('expected_files')) is int and row['expected_files'] == n,
             f'Archivos incompatibles: {ds}')
        must(type(row.get('expected_bytes')) is int and row['expected_bytes'] == b,
             f'Bytes incompatibles: {ds}')
        expected_landing = f'gs://{BUCKET}/landing/{ds}/run_id={RUN}/'
        expected_official = f'gs://{BUCKET}/{ds}/run_id={RUN}/'
        expected_manifest = f'gs://{CONTROL}/manifests/bronze/{ds}/run_id={RUN}/manifest.json'
        must(row.get('landing_prefix') == expected_landing, f'Landing incompatible: {ds}')
        must(row.get('proposed_official_prefix') == expected_official,
             f'Prefijo oficial propuesto incompatible: {ds}')
        must(row.get('proposed_manifest_uri') == expected_manifest,
             f'Ruta manifest propuesto incompatible: {ds}')
        must(row.get('bronze_input_uris') == [], f'BORRADOR no debe tener entradas activas: {ds}')
        must(row.get('bronze_manifest_uri') is None, f'BORRADOR con manifest oficial: {ds}')
        must(row.get('bronze_manifest_generation') is None,
             f'BORRADOR con generación supuestamente oficial: {ds}')
        must(row.get('quality_approved') is False and row.get('silver_consumption_allowed') is False,
             f'BORRADOR con permisos activos: {ds}')
        total_files += n
        total_bytes += b
    must(seen == set(EXPECTED), 'Datasets incompletos')
    must(total_files == FILES and total_bytes == BYTES, 'Totales incompatibles')
    return {
        'estado': 'BORRADOR_ESTRUCTURALMENTE_COMPATIBLE',
        'datasets': len(seen), 'archivos':total_files, 'bytes':total_bytes,
        'procesadores_identificados': PROCESSORS,
        'rutas_oficiales': 'PROPUESTAS_NO_PUBLICADAS',
        'manifests_y_generaciones': 'AUSENTES_POR_SER_BORRADOR',
        'validacion_firmas_y_gcs': 'NO_REALIZADA',
        'autoriza_silver': False,
        'ejecuta_spark': False, 'opera_gcs': False,
    }


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--contrato', required=True, type=Path)
    args=ap.parse_args()
    b=args.contrato.read_bytes()
    data=json.loads(b.decode('utf-8'))
    result=check(data)
    result['sha256_del_borrador_recibido'] = hashlib.sha256(b).hexdigest()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print('RESULTADO: PREFLIGHT_BORRADOR_COMPATIBLE_SILVER_BLOQUEADO')

if __name__ == '__main__':
    main()
