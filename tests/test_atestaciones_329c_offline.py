import base64,copy,sys,unittest
from datetime import datetime, timezone
from pathlib import Path
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives import serialization
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts/ingestion'))
from verificador_atestaciones_329c_offline import *

class TestAtestaciones(unittest.TestCase):
 def setUp(self):
  self.qk=Ed25519PrivateKey.generate();self.rk=Ed25519PrivateKey.generate()
  self.qp=self.qk.public_key().public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw)
  self.rp=self.rk.public_key().public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw)
  self.ahora=datetime(2026,11,25,20,0,tzinfo=timezone.utc)
  base={'run_id':RUN_ID,'scope':'PILOT_12','batch_identity_sha256':BATCH_SHA,
        'reservation_generation':RESERVA_GENERATION,'reservation_sha256':RESERVA_SHA,
        'plan_sha256':PLAN_SHA,'pilot_sha256':PILOTO_SHA,'selected_destinations_sha256':DESTINOS_SHA,
        'selected_files':12,'selected_bytes':23052390,'issued_at':'2026-11-25T19:30:00Z',
        'expires_at':'2026-11-26T19:30:00Z'}
  def firmar(k,p,kid):return {'payload':p,'key_id':kid,'alg':'Ed25519','signature_b64':base64.b64encode(k.sign(canonico(p))).decode()}
  self.q=firmar(self.qk,dict(base,type='QUALITY_APPROVED'),'quality_offline_test')
  self.r=firmar(self.rk,dict(base,type='TRANSFER_ALLOWED'),'release_offline_test')
 def run_gate(self,q=None,r=None,**kw):
  opts=dict(quality_key_id='quality_offline_test',quality_public_key=self.qp,
            release_key_id='release_offline_test',release_public_key=self.rp,ahora=self.ahora)
  opts.update(kw)
  return validar_doble_atestacion(self.q if q is None else q,self.r if r is None else r,**opts)
 def test_01_firmas_validas_no_habilitan(self):
  result=self.run_gate();self.assertEqual(result['atestaciones_criptograficas'],'PASS');self.assertFalse(result['transfer_allowed'])
 def test_02_sin_calidad(self):
  with self.assertRaises((GateError,TypeError)):self.run_gate(q={})
 def test_03_sin_release(self):
  with self.assertRaises((GateError,TypeError)):self.run_gate(r={})
 def test_04_payload_manipulado(self):
  q=copy.deepcopy(self.q);q['payload']['selected_files']=13
  with self.assertRaises(GateError):self.run_gate(q=q)
 def test_05_firma_falsa(self):
  r=copy.deepcopy(self.r);r['signature_b64']='AA=='
  with self.assertRaises(GateError):self.run_gate(r=r)
 def test_06_identidad_run_incorrecta(self):
  r=copy.deepcopy(self.r);r['payload']['run_id']='OTRO'
  with self.assertRaises(GateError):self.run_gate(r=r)
 def test_07_expiracion(self):
  with self.assertRaises(GateError):self.run_gate(ahora=datetime(2026,11,27,tzinfo=timezone.utc))
 def test_08_claves_iguales(self):
  with self.assertRaises(GateError):self.run_gate(release_key_id='quality_offline_test',release_public_key=self.qp)
 def test_09_keys_swap(self):
  with self.assertRaises(GateError):self.run_gate(quality_public_key=self.rp)
 def test_10_falta_precondicion_gcs(self):
  with self.assertRaises(PermissionError):puerta_transferencia()
 def test_11_replay_alcance(self):
  r=copy.deepcopy(self.r);r['payload']['scope']='FULL_3599'
  with self.assertRaises(GateError):self.run_gate(r=r)
if __name__=='__main__': unittest.main()
