import hashlib
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE / 'scripts' / 'pipeline'))
import adaptador_gcs_silver_v1 as g

class FakeBlob:
    def __init__(self, bucket, name, generation=None):
        self.bucket=bucket; self.name=name; self.generation=generation; self.size=None
    def reload(self, retry=None):
        o=self.bucket.data[self.name]
        self.generation=o['generation']; self.size=len(o['bytes'])
    def download_as_bytes(self, if_generation_match=None, retry=None):
        o=self.bucket.data[self.name]
        if self.generation != o['generation'] or if_generation_match != o['generation']:
            raise ValueError('PreconditionFailed')
        return o['bytes']
    def upload_from_string(self, payload, content_type, if_generation_match=None, retry=None):
        assert if_generation_match==0 and retry is None and content_type=='application/json'
        if self.name in self.bucket.data: raise ValueError('PreconditionFailed')
        self.bucket.data[self.name]={'bytes':payload, 'generation':77}
        self.generation=77

class FakeBucket:
    def __init__(self): self.data={}
    def blob(self, name, generation=None): return FakeBlob(self,name,generation)

class FakeClient:
    def __init__(self): self.buckets={}
    def bucket(self, name):
        if name not in self.buckets:self.buckets[name]=FakeBucket()
        return self.buckets[name]

class TestGCSAdapter(unittest.TestCase):
    def setUp(self):
        self.client=FakeClient()
        self.uri=f'gs://{g.CONTROL_BUCKET}/manifests/bronze/smv/cotizaciones/run_id=x/manifest.json'
        self.data=b'{"ok":true}'
        self.digest=hashlib.sha256(self.data).hexdigest()
        b,n=g.split_gs_uri(self.uri)
        self.client.bucket(b).data[n]={'bytes':self.data,'generation':123}
    def test_01_lectura_version_exacta(self):
        self.assertEqual(g.read_exact_current_bytes(self.client,self.uri,123,self.digest,expected_size=len(self.data),allowed_bucket=g.CONTROL_BUCKET),self.data)
    def test_02_generation_erronea_rechazada(self):
        with self.assertRaises(g.GCSGateError):
            g.read_exact_current_bytes(self.client,self.uri,124,self.digest,allowed_bucket=g.CONTROL_BUCKET)
    def test_03_hash_erroneo_rechazado(self):
        with self.assertRaises(g.GCSGateError):
            g.read_exact_current_bytes(self.client,self.uri,123,'0'*64,allowed_bucket=g.CONTROL_BUCKET)
    def test_04_bucket_fuera_alcance(self):
        with self.assertRaises(g.GCSGateError):
            g.read_exact_current_bytes(self.client,self.uri,123,self.digest,allowed_bucket=g.BRONZE_BUCKET)
    def test_05_no_landing(self):
        uri=f'gs://{g.BRONZE_BUCKET}/landing/x.json'
        with self.assertRaises(g.GCSGateError):
            g.read_exact_current_bytes(self.client,uri,123,self.digest,allowed_bucket=g.BRONZE_BUCKET)
    def test_06_ruta_ambigua(self):
        with self.assertRaises(g.GCSGateError):g.split_gs_uri('gs://bucket/silver/../other')
    def test_07_claim_atomico_solo_fake(self):
        b=GCSBucket=g.CONTROL_BUCKET
        uri=f'gs://{b}/authorizations/consumed/silver/run_id=x/abc.json'
        store=g.GCSExclusiveClaimStore(self.client)
        self.assertEqual(store.create_exclusive(uri,b'{"ok":true}'),77)
        with self.assertRaises(ValueError):store.create_exclusive(uri,b'{"ok":true}')
    def test_08_rechazar_destino_de_claim_invalido(self):
        store=g.GCSExclusiveClaimStore(self.client)
        with self.assertRaises(g.GCSGateError):
            store.create_exclusive(f'gs://{g.BRONZE_BUCKET}/overwrite.json',b'{}')
    def test_09_manifest_offline_verificado(self):
        ds={'bronze_manifest_uri':self.uri,'bronze_manifest_generation':'123','bronze_manifest_sha256':self.digest}
        raw,rows=g.read_official_manifest(self.client,ds,lambda b,d:['verified'] if b==self.data else [])
        self.assertEqual(raw,self.data);self.assertEqual(rows,['verified'])
    def test_10_sin_efecto_automatico(self):
        self.assertEqual(len(self.client.bucket(g.CONTROL_BUCKET).data),1)

if __name__=='__main__':unittest.main()
