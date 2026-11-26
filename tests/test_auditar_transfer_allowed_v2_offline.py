import copy
import importlib.util
import json
import unittest
from pathlib import Path

SRC=Path(__file__).resolve().parents[1]/'scripts/ingestion/auditar_transfer_allowed_v2_offline.py'
spec=importlib.util.spec_from_file_location('auditar_transfer_v2',SRC)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class AuditorTransferV2(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        r=Path(__file__).resolve().parents[1]
        cls.raw=(r/'fixtures/TRANSFER_ALLOWED_V2_BORRADOR_NO_FIRMAR.json').read_bytes()
        cls.pilot=(r/'fixtures/piloto_12_CANDIDATO_LOCAL.json').read_bytes()
        cls.plan=(r/'fixtures/plan_3599_CANDIDATO_LOCAL.json').read_bytes()
        cls.quality=(r/'fixtures/QUALITY_APPROVED_PAYLOAD_PROPUESTO_NO_FIRMAR.json').read_bytes()
        cls.kq=(r/'config/quality329c/keys/bvl_quality_official_v1.pem').read_bytes()
        cls.kt=(r/'config/quality329c/keys/bvl_transfer_official_v1.pem').read_bytes()
        cls.decoded=m.parse_json_strict(cls.raw)
        cls.pilot_obj=m.parse_json_strict(cls.pilot)
        cls.plan_obj=m.parse_json_strict(cls.plan)
        cls.quality_obj=m.parse_json_strict(cls.quality)
    def inspect(self,d):
        return m.validar_contenido(d,self.pilot_obj,self.plan_obj,self.quality_obj,self.kq,self.kt)
    def test_01_happy_borrador_is_blocked(self):
        r=m.validar_raw(self.raw,self.pilot,self.plan,self.quality,self.kq,self.kt)
        self.assertFalse(r['transfer_allowed']);self.assertEqual(r['operaciones_gcs'],0)
    def test_02_modified_raw_hash(self):
        with self.assertRaises(m.Rechazo):m.validar_raw(self.raw+b' ',self.pilot,self.plan,self.quality,self.kq,self.kt)
    def test_03_modified_plan_hash(self):
        with self.assertRaises(m.Rechazo):m.validar_raw(self.raw,self.pilot,self.plan+b' ',self.quality,self.kq,self.kt)
    def test_04_modified_pilot_hash(self):
        with self.assertRaises(m.Rechazo):m.validar_raw(self.raw,self.pilot+b' ',self.plan,self.quality,self.kq,self.kt)
    def test_05_modified_quality_hash(self):
        with self.assertRaises(m.Rechazo):m.validar_raw(self.raw,self.pilot,self.plan,self.quality+b' ',self.kq,self.kt)
    def test_06_new_run_id(self):
        d=copy.deepcopy(self.decoded);d['run_id']='otro';self.assertRaises(m.Rechazo,self.inspect,d)
    def test_07_allowed_forged(self):
        d=copy.deepcopy(self.decoded);d['policy']['transfer_allowed']=True;self.assertRaises(m.Rechazo,self.inspect,d)
    def test_08_full_batch_forged(self):
        d=copy.deepcopy(self.decoded);d['policy']['full_batch_authorized']=True;self.assertRaises(m.Rechazo,self.inspect,d)
    def test_09_overwrite_forged(self):
        d=copy.deepcopy(self.decoded);d['policy']['overwrite_allowed']=True;self.assertRaises(m.Rechazo,self.inspect,d)
    def test_10_no_precondition(self):
        d=copy.deepcopy(self.decoded);d['policy']['required_create_precondition']='none';self.assertRaises(m.Rechazo,self.inspect,d)
    def test_11_extra_destination(self):
        d=copy.deepcopy(self.decoded);d['scope']['files'].append(dict(d['scope']['files'][0]));self.assertRaises(m.Rechazo,self.inspect,d)
    def test_12_altered_destination(self):
        d=copy.deepcopy(self.decoded);d['scope']['files'][0]['destino_gcs']='gs://evil/path';self.assertRaises(m.Rechazo,self.inspect,d)
    def test_13_changed_sha(self):
        d=copy.deepcopy(self.decoded);d['scope']['files'][0]['sha256_esperado']='0'*64;self.assertRaises(m.Rechazo,self.inspect,d)
    def test_14_changed_bytes(self):
        d=copy.deepcopy(self.decoded);d['scope']['files'][0]['bytes_esperados']+=1;self.assertRaises(m.Rechazo,self.inspect,d)
    def test_15_missing_file(self):
        d=copy.deepcopy(self.decoded);d['scope']['files'].pop();self.assertRaises(m.Rechazo,self.inspect,d)
    def test_16_official_quality_hash_injected(self):
        d=copy.deepcopy(self.decoded);d['quality']['official_signed_payload_sha256']='0'*64;self.assertRaises(m.Rechazo,self.inspect,d)
    def test_17_wrong_kms_spki(self):
        d=copy.deepcopy(self.decoded);d['signature_proposal']['public_key_spki_sha256']='0'*64;self.assertRaises(m.Rechazo,self.inspect,d)
    def test_18_wrong_public_key(self):
        self.assertRaises(m.Rechazo,m.validar_raw,self.raw,self.pilot,self.plan,self.quality,self.kq,self.kq)
    def test_19_missing_human_authorization(self):
        d=copy.deepcopy(self.decoded);d['policy']['require_independent_human_approval']=False;self.assertRaises(m.Rechazo,self.inspect,d)
    def test_20_missing_execution_authorization(self):
        d=copy.deepcopy(self.decoded);d['policy']['require_independent_execution_authorization']=False;self.assertRaises(m.Rechazo,self.inspect,d)
    def test_21_disallow_duplicates_json(self):
        self.assertRaises(m.Rechazo,m.parse_json_strict,b'{"x":1,"x":2}')
    def test_22_extra_field(self):
        d=copy.deepcopy(self.decoded);d['operational_authorized']=True;self.assertRaises(m.Rechazo,self.inspect,d)

if __name__=='__main__':unittest.main()
