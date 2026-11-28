"""Pruebas offline: datos sinteticos, sin GCS/KMS/claims ni Spark."""
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from hashlib import sha256
import copy
import io
import json
from pathlib import Path
import sys
import unittest
from uuid import UUID

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'/'processing'))
import fase6_hechos_launcher_documental_v1 as launch
import fase6_release_hechos_documental_v1 as rel
import fase6_hechos_launch_scope_candidato as scope


def inventory_with_bytes():
    items=[]; values={}
    for i in range(990):
        guid=str(UUID(int=i+1)).upper()
        name=f'20250109_{guid}.pdf'
        pdf=(b'PDF0' if i==989 else f'PDF{i}'.encode())
        p_uri=scope.PREFIX+f'pdf/ingestion_date=2026-10-04/{name}'
        items.append({'uri':p_uri,'source_hdfs':f'/datalake/bvl/bronze/smv/hechos_importancia/pdf/ingestion_date=2026-10-04/{name}',
                      'generation':'1795680000000000','size_bytes':len(pdf),'sha256':sha256(pdf).hexdigest()})
        values[p_uri]=pdf
        if i<989:
            tname=name[:-4]+'.txt'; txt=f'TXT{i}'.encode()
            t_uri=scope.PREFIX+f'texto_extraido/ingestion_date=2026-10-04/{tname}'
            items.append({'uri':t_uri,'source_hdfs':f'/datalake/bvl/bronze/smv/hechos_importancia/texto_extraido/ingestion_date=2026-10-04/{tname}',
                          'generation':'1795680000000000','size_bytes':len(txt),'sha256':sha256(txt).hexdigest()})
            values[t_uri]=txt
    return items,values


def candidate_context():
    now=datetime.now(timezone.utc)
    c={key:None for key in launch.CONTEXT_KEYS}
    c.update({
        'schema_version':launch.SCHEMA,
        'status':'QUALITY_APPROVED_DOCUMENTAL_SUPERVISED_LAUNCH',
        'quality_approved':True,'project_id':launch.PROJECT,
        'region':launch.REGION,'source_dataset':launch.DATASET,'product':launch.PRODUCT,
        'bronze_run_id':launch.RUN,
        'bronze_manifest_uri':f'gs://{launch.CONTROL_BUCKET}/manifests/bronze/smv/hechos_importancia/manifest.json',
        'bronze_manifest_generation':launch.BRONZE_MANIFEST_GENERATION,
        'bronze_manifest_sha256':launch.BRONZE_MANIFEST_SHA,
        'run_id':'bvl-bronze-inicial-20261125-001','attempt_id':'attempt-abc123',
        'batch_id':'batch-doc123','processor_commit':'a'*40,
        'authorization_fingerprint':'b'*64,'entrypoint_sha256':'c'*64,
        'dependencies_sha256':'d'*64,'entrypoint_generation':'123',
        'dependencies_generation':'456','service_account':'sa-bvl-silver',
        'staging_prefix':launch.expected_staging('bvl-bronze-inicial-20261125-001','attempt-abc123'),
        'not_before_utc':(now-timedelta(minutes=2)).isoformat(),
        'expires_at_utc':(now+timedelta(minutes=30)).isoformat(),
        'schema_id':'BVL-HECHOS-DOCUMENTAL-RESTRINGIDO-CANDIDATO-v1',
        'runtime_version':'2.2','overwrite_allowed':False,'delete_allowed':False,
        'automatic_retry_allowed':False,'official_publication_allowed':False,
        'gold_consumption_allowed':False,'operation':'BUILD_SILVER_STAGING','mode':'SINGLE_SOURCE',
    })
    root=f'gs://{launch.ARTIFACT_BUCKET}/bvl-phase6/releases/'+c['processor_commit']+'/'
    c['entrypoint_uri']=root+'fase6_hechos_launcher_documental_v1.py'
    c['dependencies_uri']=root+'fase6_hechos_documental_dependencias.zip'
    return c


class TestDocumentalLauncher(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.items,cls.data=inventory_with_bytes()

    def test_default_no_execution(self):
        self.assertEqual(launch.main([]),0)

    def test_no_execute_without_f5(self):
        with self.assertRaisesRegex(ValueError,'SUPERVISION'):
            launch.main(['--execute'])

    def test_context_good_is_shape_only(self):
        self.assertEqual(launch.validate_context(candidate_context())['product'],launch.PRODUCT)

    def test_historical_events_rejected(self):
        x=candidate_context();x['product']='hechos_importancia/eventos'
        with self.assertRaisesRegex(ValueError,'product'):
            launch.validate_context(x)

    def test_wrong_project_and_region(self):
        for key,val in [('project_id','falso'),('region','us-east1')]:
            x=candidate_context(); x[key]=val
            with self.subTest(key=key),self.assertRaisesRegex(ValueError,key):
                launch.validate_context(x)

    def test_manifest_pin_changed(self):
        for key,val in [('bronze_manifest_sha256','a'*64),('bronze_manifest_generation','1')]:
            x=candidate_context();x[key]=val
            with self.subTest(key=key),self.assertRaisesRegex(ValueError,key):
                launch.validate_context(x)

    def test_invalid_staging(self):
        x=candidate_context();x['staging_prefix']+= '/otro'
        with self.assertRaisesRegex(ValueError,'STAGING'):
            launch.validate_context(x)

    def test_output_and_retry_prohibited(self):
        for key in ('overwrite_allowed','delete_allowed','automatic_retry_allowed',
                    'official_publication_allowed','gold_consumption_allowed'):
            x=candidate_context();x[key]=True
            with self.subTest(key=key),self.assertRaisesRegex(ValueError,key):
                launch.validate_context(x)

    def test_expired_context(self):
        x=candidate_context();x['expires_at_utc']=(datetime.now(timezone.utc)-timedelta(minutes=1)).isoformat()
        with self.assertRaisesRegex(ValueError,'EXPIRADO'):
            launch.validate_context(x)

    def test_no_extra_fields(self):
        x=candidate_context();x['impersonate_quality']=True
        with self.assertRaisesRegex(ValueError,'CAMPOS'):
            launch.validate_context(x)

    def test_invalid_entrypoint_release(self):
        x=candidate_context();x['entrypoint_uri']=x['entrypoint_uri'].replace('fase6_hechos_launcher','fase6_restantes')
        with self.assertRaisesRegex(ValueError,'ARTEFACTOS'):
            launch.validate_context(x)

    def test_valid_official_inventory_1979(self):
        m={'dataset':launch.DATASET,'run_id':launch.RUN,
           'quality_approved':True,'status':'BRONZE_COMPLETADO','files':self.items}
        r=launch.validate_official_manifest(m,self.items)
        self.assertEqual((r['pdf_count'],r['txt_count']), (990,989))

    def test_unsigned_manifest_rejected(self):
        m={'dataset':launch.DATASET,'run_id':launch.RUN,
           'quality_approved':False,'status':'BRONZE_COMPLETADO','files':self.items}
        with self.assertRaisesRegex(ValueError,'NO_OFICIAL'):
            launch.validate_official_manifest(m,self.items)

    def test_missing_txt_rejected(self):
        m={'dataset':launch.DATASET,'run_id':launch.RUN,
           'quality_approved':True,'status':'BRONZE_COMPLETADO','files':self.items[:-1]}
        with self.assertRaisesRegex(ValueError,'1979'):
            launch.validate_official_manifest(m,self.items[:-1])

    def test_tampered_bytes_detected(self):
        from fase6_hechos_documental_cloud_candidato import build_verified_document_rows
        def tampered(uri,generation,sha,size):
            raw=self.data[uri]
            return b'Z'*len(raw) if uri==self.items[0]['uri'] else raw
        with self.assertRaisesRegex(ValueError,'SHA'):
            build_verified_document_rows(self.items,read_exact=tampered)

    def test_exact_reader_and_txt_reuse_and_label(self):
        calls=set()
        def reader(uri,generation,sha,size):
            raw=self.data[uri]
            self.assertEqual(sha256(raw).hexdigest(),sha)
            self.assertEqual(size,len(raw))
            calls.add(uri)
            return raw
        rows,metrics=launch.verify_rows_after_exact_gcs(self.items,reader)
        self.assertEqual(len(rows),990)
        self.assertEqual(len(calls),1979)
        self.assertEqual(metrics['txt_unicos_verificados'],989)
        self.assertEqual(sum(r['duplicado_por_sha256'] for r in rows),1)
        self.assertEqual(sum(r['origen_texto']=='CANONICO_POR_SHA_DOCUMENTAL' for r in rows),1)
        self.assertTrue(all(r['verificacion']=='BYTES_GCS_GENERATION_SHA256_SIZE_VERIFIED' for r in rows))
        self.assertFalse(any('expediente' in key.lower() or 'empresa' in key.lower() for r in rows for key in r))

    def test_candidate_receipts_and_prohibitions(self):
        rows,metrics=launch.verify_rows_after_exact_gcs(self.items,
            lambda uri,gen,sha,size:self.data[uri])
        ctx=candidate_context()
        receipt=[{'uri':ctx['staging_prefix']+'parquet/part-00000.snappy.parquet',
                   'generation':'123','sha256':'b'*64,'rows':990,'size_bytes':1234}]
        m=launch.candidate_from_verified_rows(ctx,self.items,rows,metrics,receipt,
             claim_generation='123',context_sha='a'*64,manifest_ref={'uri':ctx['bronze_manifest_uri']})
        self.assertFalse(m['quality_approved'])
        self.assertFalse(m['gold_consumption_allowed'])
        self.assertFalse(m['official_publication_allowed'])
        self.assertEqual(m['source_count'],1979)
        self.assertEqual(len(m['source_inventory']),1979)
        self.assertEqual(m['document_rows'],990)

    def test_bad_receipts_rejected(self):
        rows,metrics=launch.verify_rows_after_exact_gcs(self.items,
            lambda uri,gen,sha,size:self.data[uri])
        c=candidate_context()
        r=[{'uri':c['staging_prefix']+'parquet/part-00000.snappy.parquet',
             'generation':'123','sha256':'b'*64,'rows':989,'size_bytes':1234}]
        with self.assertRaisesRegex(ValueError,'CONTEO_PARQUET'):
            launch.candidate_from_verified_rows(c,self.items,rows,metrics,r,
                 claim_generation='1',context_sha='a'*64,manifest_ref={})

    def test_create_only_upload_semantics_and_failure(self):
        class FakeBlob:
            generation='1234'
            def upload_from_file(self,stream,**kw):
                self.kw=kw
                assert kw['if_generation_match']==0
                assert kw['retry'] is None
                assert stream.read()==b'abc'
        blob=FakeBlob()
        class FakeBucket:
            def blob(self,path): return blob
        class FakeClient:
            def bucket(self,b):
                assert b==launch.SILVER_BUCKET
                return FakeBucket()
        uri=launch.expected_staging('bvl-bronze-inicial-20261125-001','attempt-abc123')+'parquet/part-00001.snappy.parquet'
        result=launch._create_only(FakeClient(),uri,b'abc',content_type='application/octet-stream')
        self.assertEqual(result['generation'],'1234')
        self.assertEqual(result['sha256'],sha256(b'abc').hexdigest())
        class FailedBlob(FakeBlob):
            def upload_from_file(self,stream,**kw):
                raise RuntimeError('CONFLICTO_IF_GENERATION_MATCH_0')
        class FailedClient(FakeClient):
            def bucket(self,b):
                class B:
                    def blob(self,path): return FailedBlob()
                return B()
        with self.assertRaisesRegex(RuntimeError,'CONFLICTO'):
            launch._create_only(FailedClient(),uri,b'abc',content_type='application/octet-stream')

    def test_permission_preflight_denies_missing(self):
        class Blob:
            def test_iam_permissions(self, perms): return []
        class Bucket:
            def __init__(self, b): self.name=b
            def test_iam_permissions(self, perms): return perms
            def blob(self, name): return Blob()
        class Client:
            def bucket(self, b): return Bucket(b)
        ctx=candidate_context()
        refs={'uri':f'gs://{launch.CONTROL_BUCKET}/authorizations/silver_documental/'
                    f"run_id={ctx['run_id']}/attempt_id={ctx['attempt_id']}/launch_context.json",
              'signature_uri':f'gs://{launch.CONTROL_BUCKET}/authorizations/silver_documental/'
                    f"run_id={ctx['run_id']}/attempt_id={ctx['attempt_id']}/launch_context.sig"}
        with self.assertRaisesRegex(ValueError,'IAM_INSUFICIENTE_PRECLAIM'):
            launch.validate_writer_permissions(Client(),ctx,self.items,refs)

    def test_post_claim_failure_receipt_no_retry(self):
        seen=[]
        orig=launch._create_only
        try:
            launch._create_only=lambda client,uri,payload,**kw: (
                seen.append((uri,json.loads(payload))),{'uri':uri,'generation':'1'})[1]
            pre=launch.expected_staging('bvl-bronze-inicial-20261125-001','attempt-abc123')
            rec=launch.record_failure_candidate(None,pre,claim_generation=12,
                              attempt_id='attempt-abc123',exc=ValueError('fake'))
            self.assertEqual(len(seen),1)
            self.assertTrue(seen[0][0].endswith('failure_candidate.json'))
            self.assertEqual(seen[0][1]['automatic_retry_allowed'],False)
            self.assertEqual(rec['generation'],'1')
        finally:
            launch._create_only=orig

    def test_preclaim_capacity_exceeded(self):
        arr=[{'txt':{'tamano_bytes':launch.MAX_PART_BYTES}} for _ in range(990)]
        with self.assertRaisesRegex(ValueError,'LIMITE_PARQUET_PRECLAIM'):
            launch.validate_part_capacity(arr)

    def test_zip_determinism(self):
        a=rel.dzip({'x.py':b'a', 'y.py':b'b'})
        b=rel.dzip({'y.py':b'b', 'x.py':b'a'})
        self.assertEqual(a,b)

    def test_zip_traversal(self):
        with self.assertRaisesRegex(ValueError,'INSEGURO'):
            rel.dzip({'../attack.py':b'a'})

    def test_no_authorization_fixture_can_unlock(self):
        with self.assertRaises(PermissionError):
            scope.require_quality_signed_contract(candidate_context())


if __name__=='__main__': unittest.main()
