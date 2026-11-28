"""No GCS, no tokens, no claims, no writes; mock IAM condicionado."""
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'scripts'/'processing'))
sys.path.insert(0, str(ROOT/'tests'))
import fase6_hechos_launcher_documental_v1 as launch
from test_fase6_hechos_launcher_documental_v1 import candidate_context, inventory_with_bytes

class FakeBlob:
    def __init__(self, owner, bucket, name):
        self.owner, self.bucket, self.name=owner,bucket,name
    def test_iam_permissions(self, permissions):
        self.owner.calls.append(('object',self.bucket,self.name,tuple(permissions)))
        if self.owner.failure and self.owner.failure(self.bucket,self.name):
            raise RuntimeError('IAM API unavailable')
        if self.owner.reject and self.owner.reject(self.bucket,self.name):
            return []
        return permissions
    def upload_from_file(self,*args,**kwargs):
        raise AssertionError('NUNCA ESCRIBIR')

class FakeBucket:
    def __init__(self,owner,name):
        self.owner,self.name=owner,name
    def test_iam_permissions(self,permissions):
        self.owner.calls.append(('bucket',self.name,tuple(permissions)))
        if self.name==launch.SILVER_BUCKET and not self.owner.allow_list:
            return []
        # Important: BUCKET GET/CREATE no concedido aun cuando OBJECT lo este.
        return [p for p in permissions if p == 'storage.objects.list']
    def blob(self,name):
        return FakeBlob(self.owner,self.name,name)

class FakeClient:
    def __init__(self,reject=None,failure=None,allow_list=True):
        self.reject,self.failure,self.allow_list=reject,failure,allow_list
        self.calls=[]
    def bucket(self,name):return FakeBucket(self,name)
    def list_blobs(self,*args,**kwargs):
        raise AssertionError('LISTADO REAL NO ES PARTE DE LA PRUEBA IAM')

class TestIAMCondicionado(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.entries,_=inventory_with_bytes()
    def setUp(self):
        self.ctx=candidate_context()
        prefix=f'gs://{launch.CONTROL_BUCKET}/authorizations/silver_documental/'
        prefix+=f"run_id={self.ctx['run_id']}/attempt_id={self.ctx['attempt_id']}/"
        self.refs={'uri':prefix+'launch_context.json',
                   'signature_uri':prefix+'launch_context.sig'}
    def call(self,c):
        return launch.validate_writer_permissions(c,self.ctx,self.entries,self.refs)

    def test_01_permisos_condicionados_en_objeto_autorizados(self):
        c=FakeClient()
        self.assertTrue(self.call(c))
        # No se concede `storage.objects.create` a nivel bucket.
        self.assertEqual([x for x in c.calls if x[0]=='bucket'],
                         [('bucket',launch.SILVER_BUCKET,('storage.objects.list',))])
        writes=[x for x in c.calls if x[0]=='object' and x[1]==launch.SILVER_BUCKET]
        self.assertEqual(len(writes),10)
        self.assertTrue(all(x[2].startswith(launch._gcs_uri_parts(
            self.ctx['staging_prefix'],launch.SILVER_BUCKET)) for x in writes))
        self.assertEqual(len(set(x[2] for x in writes)),10)
        self.assertEqual(sum('parquet/part-' in x[2] for x in writes),8)
        self.assertEqual(sum('/_quality/' in '/'+x[2] for x in writes),2)
        self.assertTrue(all(x[3]==('storage.objects.create',) for x in writes))

    def test_02_denegar_create_en_part_parquet(self):
        c=FakeClient(reject=lambda b,n: b==launch.SILVER_BUCKET and n.endswith('part-00007.snappy.parquet'))
        with self.assertRaisesRegex(ValueError,'IAM_INSUFICIENTE_PRECLAIM'):
            self.call(c)

    def test_03_denegar_manifest_candidate(self):
        c=FakeClient(reject=lambda b,n: b==launch.SILVER_BUCKET and n.endswith('manifest_candidate_documental.json'))
        with self.assertRaisesRegex(ValueError,'IAM_INSUFICIENTE_PRECLAIM'):
            self.call(c)

    def test_04_denegar_failure_candidate(self):
        c=FakeClient(reject=lambda b,n: b==launch.SILVER_BUCKET and n.endswith('failure_candidate.json'))
        with self.assertRaisesRegex(ValueError,'IAM_INSUFICIENTE_PRECLAIM'):
            self.call(c)

    def test_05_denegar_get_control_objeto(self):
        c=FakeClient(reject=lambda b,n: b==launch.CONTROL_BUCKET and n.endswith('launch_context.sig'))
        with self.assertRaisesRegex(ValueError,'IAM_INSUFICIENTE_PRECLAIM'):
            self.call(c)

    def test_06_denegar_list_silver(self):
        with self.assertRaisesRegex(ValueError,'IAM_INSUFICIENTE_PRECLAIM'):
            self.call(FakeClient(allow_list=False))

    def test_07_api_no_verificable_bloquea(self):
        c=FakeClient(failure=lambda b,n: b==launch.SILVER_BUCKET)
        with self.assertRaisesRegex(ValueError,'IAM_PRECLAIM_NO_VERIFICABLE'):
            self.call(c)

    def test_08_staging_fuera_de_intento_rechazado(self):
        self.ctx['staging_prefix']=self.ctx['staging_prefix'].replace('attempt-abc123','attempt-other')
        c=FakeClient()
        with self.assertRaisesRegex(ValueError,'IAM_CONTEXTO_NO_VINCULADO_A_STAGING'):
            self.call(c)
        self.assertEqual(c.calls,[])

    def test_09_referencias_faltantes_rechazadas(self):
        self.refs.pop('signature_uri')
        c=FakeClient()
        with self.assertRaisesRegex(ValueError,'IAM_REFERENCIAS_CONTEXT_INCOMPLETAS'):
            self.call(c)
        self.assertEqual(c.calls,[])

if __name__=='__main__':
    unittest.main()
