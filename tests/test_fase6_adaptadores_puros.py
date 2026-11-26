#!/usr/bin/env python3
"""Unittest sin GCS, PyArrow ni Spark; no necesita credenciales."""
import hashlib
import io
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts' / 'processing'))

from fase6_restantes_core import (
    SOURCES, FILE_COUNTS, CANDIDATE_TRANSFORMS, DEPENDENCIES,
    BRONZE_BUCKET, CONTROL_BUCKET, QUALITY_SERVICE_ACCOUNT,
    RUN_REFERENCE, ScopeError, expected_prefix, validate_authorized_scope,
    validate_source_entries, validate_derived_dependencies,
    validate_request_without_authorizing,
)
from fase6_restantes_transform import load_bcrp_csv, load_json_records, dispatch_transform
from fase6_restantes_writer import create_only


class TestCatalogo(unittest.TestCase):
    def test_numero_procesos_productos(self):
        self.assertEqual(len(SOURCES), 11)
        self.assertEqual(sum(len(v) for v in SOURCES.values()), 12)
        self.assertEqual(len(DEPENDENCIES), 5)
        self.assertEqual(len(CANDIDATE_TRANSFORMS), 8)
        self.assertEqual(set(SOURCES) - CANDIDATE_TRANSFORMS,
                         {'smv/valores_inscritos','smv/hechos_valores_consulta','smv/hechos_importancia'})

    def test_nunca_autoriza_por_plan(self):
        out = validate_request_without_authorizing({'execution_enabled': False,
                'datasets': list(SOURCES)})
        self.assertEqual(out['status'], 'PLAN_CANDIDATO_NO_AUTORIZADO')
        with self.assertRaises(ScopeError):
            validate_request_without_authorizing({'execution_enabled': True,
                                                   'datasets': list(SOURCES)})

    def test_alcance_gcs_cotizaciones(self):
        ds = 'smv/cotizaciones'
        auth = {'dataset': ds, 'mode':'SINGLE_SOURCE',
                'operation':'BUILD_SILVER_STAGING',
                'run_id':RUN_REFERENCE, 'attempt_id':'cotizaciones-prueba-001',
                'service_account':QUALITY_SERVICE_ACCOUNT,
                'silver_outputs':[{'name': SOURCES[ds][0],
                                   'staging_prefix': expected_prefix(SOURCES[ds][0],
                                        RUN_REFERENCE,'cotizaciones-prueba-001')}]}
        self.assertEqual(validate_authorized_scope(auth, ds),
                         {SOURCES[ds][0]:auth['silver_outputs'][0]['staging_prefix']})
        auth['silver_outputs'][0]['staging_prefix'] += 'otra-carpeta/'
        with self.assertRaises(ScopeError):
            validate_authorized_scope(auth, ds)

    def test_dos_salidas_un_claim_valores(self):
        ds = 'smv/valores_inscritos'
        auth = {'dataset':ds,'mode':'SINGLE_SOURCE',
                'operation':'BUILD_SILVER_STAGING',
                'run_id':RUN_REFERENCE, 'attempt_id':'valores-prueba-001',
                'service_account':QUALITY_SERVICE_ACCOUNT,
                'silver_outputs':[{'name':p,'staging_prefix':expected_prefix(p,RUN_REFERENCE,'valores-prueba-001')}
                                  for p in SOURCES[ds]]}
        self.assertEqual(len(validate_authorized_scope(auth,ds)),2)
        auth['silver_outputs'].pop()
        with self.assertRaises(ScopeError):
            validate_authorized_scope(auth,ds)

    def test_manifest_rechaza_landing_y_duplicados(self):
        ds='bcrp/embig'
        entry = {'uri':f'gs://{BRONZE_BUCKET}/{ds}/run_id={RUN_REFERENCE}/embig.csv',
                 'generation':'1795600000000001','size_bytes':100,'sha256':'a'*64}
        self.assertTrue(validate_source_entries(ds,[entry]))
        entry2 = dict(entry, uri=f'gs://{BRONZE_BUCKET}/landing/{ds}/x.csv')
        with self.assertRaises(ScopeError):
            validate_source_entries(ds,[entry2])
        with self.assertRaises(ScopeError):
            validate_source_entries(ds,[entry,entry])

    def test_5_dependencias_versionadas(self):
        deps = [{'product':p,
                 'manifest_uri':f'gs://{CONTROL_BUCKET}/manifests/silver/{p}/version=v1/manifest.json',
                 'manifest_sha256':'f'*64,'manifest_generation':'1795649999000000',
                 'version_id':'v1'} for p in DEPENDENCIES]
        self.assertTrue(validate_derived_dependencies(deps))
        deps[0]['manifest_uri'] = f'gs://{CONTROL_BUCKET}/manifests/silver/{DEPENDENCIES[0]}/current.json'
        with self.assertRaises(ScopeError): validate_derived_dependencies(deps)

    def test_sin_retries_ni_publicacion_en_archivos(self):
        source = (ROOT/'scripts/processing/fase6_restantes_writer.py').read_text()
        self.assertIn('if_generation_match=0', source)
        self.assertIn('retry=None', source)
        self.assertNotIn('delete(', source)
        self.assertNotIn('current.json\'(', source)
        candidate = (ROOT/'scripts/processing/fase6_restantes_preflight.py').read_text()
        self.assertNotIn('consume_once(', candidate)


class TestCSVJSON(unittest.TestCase):
    def test_csv_primera_cabecera_vacia(self):
        rows=load_bcrp_csv(b',PD04709XD\n,embig\n02Ene97,600\n','PD04709XD')
        self.assertEqual(rows[1]['_c0'],'02Ene97')

    def test_csv_fila_malformada(self):
        with self.assertRaises(ScopeError):
            load_bcrp_csv(b',PD04709XD\n01Ene97,500,x\n','PD04709XD')

    def test_json_wrapper_y_cruces(self):
        a = load_json_records([b'{"Resultado":[{"CodigoSAB":"X","Monto":3}]}'])
        self.assertEqual(a[0]['CodigoSAB'], 'X')
        with self.assertRaises(ScopeError):
            load_json_records([b'{"data":[],"data":[]}'])

    def test_json_rechaza_codigo_error(self):
        with self.assertRaises(ScopeError):
            load_json_records([b'{"ResponseCode":107,"data":[]}'])

    def test_json_rechaza_nested_no_inventar(self):
        with self.assertRaises(ScopeError):
            load_json_records([b'{"Resultado":{"foo":"bar"}}'])

    def test_transformador_no_implementado_bloquea(self):
        with self.assertRaises(ScopeError):
            dispatch_transform('smv/hechos_importancia', object())


class MockBlob:
    def __init__(self): self.generation = None
    def upload_from_file(self, obj, *, rewind, size, if_generation_match, retry, checksum, content_type):
        assert if_generation_match == 0 and retry is None
        assert checksum == 'crc32c'
        assert len(obj.read()) == size
        self.generation = 123
class MockBucket:
    def __init__(self): self.blobname = None
    def blob(self,key):
        self.blobname = key
        return MockBlob()
class MockClient:
    def __init__(self): self.b = MockBucket()
    def bucket(self,name):
        assert name == 'bvl26-silver-7f3a9c'
        return self.b

class TestWriter(unittest.TestCase):
    def test_create_only_gcs_simulado(self):
        client = MockClient()
        r=create_only(client,
            'gs://bvl26-silver-7f3a9c/staging/macroeconomia/embig/run_id=x/attempt_id=y/part-00000.snappy.parquet', b'parquet')
        self.assertEqual(r['generation'], '123')
        self.assertEqual(r['sha256'], hashlib.sha256(b'parquet').hexdigest())

    def test_prohibe_publicacion_oficial(self):
        client = MockClient()
        with self.assertRaises(ScopeError):
            create_only(client,
                'gs://bvl26-silver-7f3a9c/versions/macroeconomia/embig/version=v1/x.snappy.parquet',b'x')

if __name__ == '__main__':
    unittest.main()
