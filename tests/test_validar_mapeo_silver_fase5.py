"""Pruebas unitarias aisladas de compatibilidad del mapeo; sin GCS ni Spark."""
import copy
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts' / 'pipeline'))
import validar_mapeo_silver_fase5 as mod

SOURCES = [
 ('bcrp/embig','macroeconomia/embig'), ('bcrp/tipo_cambio','macroeconomia/tipo_cambio'),
 ('smv/cambios_patrimonio','finanzas_empresariales/cambios_patrimonio'),
 ('smv/cotizaciones','mercado_diario/cotizaciones'),
 ('smv/estado_resultados','finanzas_empresariales/estado_resultados'),
 ('smv/flujo_efectivo','finanzas_empresariales/flujo_efectivo'),
 ('smv/hechos_importancia','hechos_importancia/eventos'),
 ('smv/hechos_valores_consulta','referencias_smv/hechos_valores_consulta'),
 ('smv/montos_sab','mercado_diario/montos_sab'),
 ('smv/principales_cuentas','finanzas_empresariales/principales_cuentas'),
 ('smv/situacion_financiera','finanzas_empresariales/situacion_financiera'),
 ('smv/valores_inscritos','referencias_smv/valores_inscritos'),
]
RUN = 'run-test-01'
B = 'bvl26-silver-7f3a9c'


def sample():
    ds = []
    src = []
    for name, prod in SOURCES:
        uri = f'gs://control/manifests/bronze/{name}/run_id={RUN}/manifest.json'
        ds.append(dict(dataset=name, files_count=1, bronze_manifest_uri=uri,
                       bronze_manifest_generation='123',bronze_manifest_sha256='a'*64,
                       proposed_silver_product=prod))
        src.append(dict(bronze_dataset=name, files=1, bronze_manifest_uri=uri,
                        bronze_manifest_generation='123',bronze_manifest_sha256='a'*64,
                        silver_product_proposed=prod, mapping_status='PENDIENTE_DE_RATIFICACION_QUALITY',
                        staging_prefix_proposed=f'gs://{B}/staging/{prod}/run_id={RUN}/attempt_id=<attempt_id>/'))
    contract = {'schema_version':'BVL-BRONZE-SILVER-CONTRACT-v1','run_id':RUN,
                'phase_5_closed':False, 'consumer':{'silver_bucket':B,'spark_execution_allowed':False},
                'bronze':{'files_count':12},'datasets':ds}
    mapping = dict(document_type='PROPUESTA_MAPEO_NO_RATIFICADA',
                   contract_schema=contract['schema_version'], run_id=RUN,
                   status='PENDIENTE_DE_RATIFICACION_QUALITY', derived_auth_route_ratified=False,
                   source_datasets=src, derived_products=[
        dict(silver_product='referencias_smv/puente_ruc_nemonico',depends_on_silver=[],
             depends_on_bronze=['smv/valores_inscritos'],
             produced_together_with='referencias_smv/valores_inscritos',
             potential_additional_bronze=[],status='PROPUESTA_PENDIENTE_DE_RATIFICACION_QUALITY'),
        dict(silver_product='hechos_importancia/identidad_empresarial',
             depends_on_silver=['hechos_importancia/eventos','finanzas_empresariales/principales_cuentas',
                                'referencias_smv/puente_ruc_nemonico','mercado_diario/cotizaciones',
                                'referencias_smv/hechos_valores_consulta'],
             status='PROPUESTA_PENDIENTE_DE_RATIFICACION_QUALITY')])
    return contract, mapping

class Tests(unittest.TestCase):
    def setUp(self):
        self.contract, self.mapping = sample()

    def test_ok(self):
        self.assertEqual(mod.validate_mapping(self.contract,self.mapping)['total_productos'],14)

    def test_falta_fuente(self):
        self.mapping['source_datasets'].pop()
        with self.assertRaises(mod.MappingError): mod.validate_mapping(self.contract,self.mapping)

    def test_generacion_distinta(self):
        self.mapping['source_datasets'][0]['bronze_manifest_generation']='999'
        with self.assertRaises(mod.MappingError): mod.validate_mapping(self.contract,self.mapping)

    def test_producto_duplicado(self):
        self.mapping['source_datasets'][1]['silver_product_proposed']=self.mapping['source_datasets'][0]['silver_product_proposed']
        with self.assertRaises(mod.MappingError): mod.validate_mapping(self.contract,self.mapping)

    def test_staging_equivocado(self):
        self.mapping['source_datasets'][0]['staging_prefix_proposed']='gs://bucket/oficial'
        with self.assertRaises(mod.MappingError): mod.validate_mapping(self.contract,self.mapping)

    def test_puente_fuente_erronea(self):
        self.mapping['derived_products'][0]['depends_on_bronze']=['smv/hechos_valores_consulta']
        with self.assertRaises(mod.MappingError): mod.validate_mapping(self.contract,self.mapping)

    def test_identidad_dependencia_extra(self):
        self.mapping['derived_products'][1]['depends_on_silver'].append('desconocido')
        with self.assertRaises(mod.MappingError): mod.validate_mapping(self.contract,self.mapping)

    def test_quality_no_ratificado(self):
        self.mapping['derived_auth_route_ratified']=True
        with self.assertRaises(mod.MappingError): mod.validate_mapping(self.contract,self.mapping)

if __name__=='__main__': unittest.main()
