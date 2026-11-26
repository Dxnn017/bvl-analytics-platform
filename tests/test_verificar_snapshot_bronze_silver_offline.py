import hashlib
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

P=Path(__file__).resolve().parents[1]/'scripts/pipeline/verificar_snapshot_bronze_silver_offline.py'
spec=importlib.util.spec_from_file_location('snapcheck',P)
mod=importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
URI=f'gs://{mod.BUCKET}/smv/hechos_valores_consulta/run_id={mod.RUN}/json/archivo.json'

class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.t=tempfile.TemporaryDirectory();self.addCleanup(self.t.cleanup)
        self.p=Path(self.t.name)/'datos.json';self.p.write_bytes(b'{"prueba":true}')
        self.m={'run_id':mod.RUN,'dataset':'smv/hechos_valores_consulta','status':'BRONZE_COMPLETADO','quality_approved':True,'files':[{'uri':URI,'generation':'1795640338029794','size_bytes':self.p.stat().st_size,'sha256':hashlib.sha256(self.p.read_bytes()).hexdigest()}]}
        self.a=[{'uri':URI,'generation':'1795640338029794','local_path':str(self.p)}]
    def test_ok_sin_autorizar(self):
        r=mod.check(self.m,self.a);self.assertFalse(r['autorizacion_silver']);self.assertEqual(r['archivos'],1)
    def test_payload_alterado(self):
        self.p.write_bytes(b'fraude');self.assertRaises(ValueError,mod.check,self.m,self.a)
    def test_bytes_alterados(self):
        self.m['files'][0]['size_bytes']+=1;self.assertRaises(ValueError,mod.check,self.m,self.a)
    def test_hash_alterado(self):
        self.m['files'][0]['sha256']='0'*64;self.assertRaises(ValueError,mod.check,self.m,self.a)
    def test_generation_alterada(self):
        self.a[0]['generation']='9';self.assertRaises(ValueError,mod.check,self.m,self.a)
    def test_generation_nula(self):
        self.m['files'][0]['generation']=None;self.assertRaises(ValueError,mod.check,self.m,self.a)
    def test_landing_rechazado(self):
        self.m['files'][0]['uri']=URI.replace('/smv/','/landing/smv/');self.assertRaises(ValueError,mod.check,self.m,self.a)
    def test_sin_mapeo(self):
        self.assertRaises(ValueError,mod.check,self.m,[])
    def test_sin_fichero(self):
        self.p.unlink();self.assertRaises(ValueError,mod.check,self.m,self.a)
    def test_duplicado(self):
        self.m['files'].append(dict(self.m['files'][0]));self.a.append(dict(self.a[0]));self.assertRaises(ValueError,mod.check,self.m,self.a)
    def test_campo_incompleto(self):
        del self.m['files'][0]['sha256'];self.assertRaises(ValueError,mod.check,self.m,self.a)
    def test_bool_tamano_rechazado(self):
        self.m['files'][0]['size_bytes']=True;self.assertRaises(ValueError,mod.check,self.m,self.a)
    def test_run_id_distinto(self):
        self.m['run_id']='otro';self.assertRaises(ValueError,mod.check,self.m,self.a)
    def test_symlink_rechazado(self):
        other=Path(self.t.name)/'enlace';other.symlink_to(self.p);self.a[0]['local_path']=str(other);self.assertRaises(ValueError,mod.check,self.m,self.a)

if __name__=='__main__':unittest.main()
