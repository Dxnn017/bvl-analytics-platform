import copy
import hashlib
import json
import sys
import unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts/ingestion'))
from verificador_release_329c_offline import validar_preaprobacion_local, puerta_transferencia, ReleaseError
class Checks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a=(ROOT/'fixtures/QUALITY_APPROVED_BORRADOR_NO_PUBLICAR.json').read_bytes()
        cls.plan=(ROOT/'fixtures/plan_3599_CANDIDATO_LOCAL.json').read_bytes()
        cls.piloto=(ROOT/'fixtures/piloto_12_CANDIDATO_LOCAL.json').read_bytes()
    def alter(self, k, v):
        d=json.loads(self.a); d[k]=v
        return json.dumps(d).encode()
    def test_01_candidato_conforme(self):
        r=validar_preaprobacion_local(self.a,self.plan,self.piloto)
        self.assertEqual(r['documental'],'PASS'); self.assertFalse(r['transfer_allowed'])
    def test_02_run_falso(self):
        with self.assertRaises(ReleaseError): validar_preaprobacion_local(self.alter('run_id','OTRO'),self.plan,self.piloto)
    def test_03_plan_falso(self):
        with self.assertRaises(ReleaseError): validar_preaprobacion_local(self.a,self.plan+b' ',self.piloto)
    def test_04_piloto_falso(self):
        with self.assertRaises(ReleaseError): validar_preaprobacion_local(self.a,self.plan,self.piloto+b' ')
    def test_05_generacion_alterada(self):
        d=json.loads(self.a); d['reservation_ref']['generation']='0'
        with self.assertRaises(ReleaseError): validar_preaprobacion_local(json.dumps(d).encode(),self.plan,self.piloto)
    def test_06_intento_autorizar_transferencia(self):
        with self.assertRaises(ReleaseError): validar_preaprobacion_local(self.alter('transfer_allowed',True),self.plan,self.piloto)
    def test_07_intento_fingir_emisor(self):
        with self.assertRaises(ReleaseError): validar_preaprobacion_local(self.alter('verified_writer_identity',True),self.plan,self.piloto)
    def test_08_no_acepta_aprobacion_falsa(self):
        with self.assertRaises(ReleaseError): validar_preaprobacion_local(self.alter('draft_only',False),self.plan,self.piloto)
    def test_09_puerta_siempre_cerrada(self):
        with self.assertRaises(PermissionError): puerta_transferencia()
if __name__=='__main__':unittest.main()
