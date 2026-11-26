"""Sólo claves generadas temporalmente para las firmas de prueba."""
import copy
from datetime import datetime,timedelta,timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
import uuid

from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import ec

ROOT=Path(__file__).resolve().parents[1]
M=ROOT/'scripts/ingestion/auditar_contratos_humanos_101_7_e_offline.py'
sp=importlib.util.spec_from_file_location('audit_human', M)
a=importlib.util.module_from_spec(sp)
sp.loader.exec_module(a)
FIX=ROOT/'fixtures'

class ContratosHumanosTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.original={k:(FIX/n).read_bytes() for k,n in a.FILENAMES.items()}
  cls.pem=(FIX/'CLAVE_HUMANA_REFERENCIA_PUBLICA_LIZETH.pem').read_bytes()
  cls.key=ec.generate_private_key(ec.SECP256R1()) # Nunca usar claves reales Quality
  cls.pk=cls.key.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo)
  cls.spki=cls.key.public_key().public_bytes(serialization.Encoding.DER,serialization.PublicFormat.SubjectPublicKeyInfo)
  cls.pin=a.sha(cls.spki)
  cls.base_time=datetime(2026,11,25,22,30,0,tzinfo=timezone.utc)

 def draft(self,k): return json.loads(self.original[k])
 def check(self,k,doc): return a.validar_borrador(json.dumps(doc).encode(),k)
 def valid_draft(self,k): self.assertEqual(a.validar_borrador(self.original[k],k)['action'],a.ACTIONS[k])

 def build_lab(self,k,offset=0):
  doc=self.draft(k)
  doc['schema_version']=a.LAB_SCHEMA
  doc['status']=a.LAB_STATUS
  doc['authorization_id']=str(uuid.uuid4())
  dt=self.base_time+timedelta(minutes=offset)
  doc['issued_at_utc']=dt.strftime('%Y-%m-%dT%H:%M:%SZ')
  doc['expires_at_utc']=(dt+timedelta(minutes=40)).strftime('%Y-%m-%dT%H:%M:%SZ')
  for field,v in doc['target'].items():
   if v is None:
    if field=='executor_identity': doc['target'][field]='sa-bvl-ingestion@bvl-analytics-2026.iam.gserviceaccount.com'
    elif field=='executor_commit': doc['target'][field]='a'*40
    elif field.endswith('generation'): doc['target'][field]=1795640338029794
    else:doc['target'][field]='b'*64
  return doc
 @staticmethod
 def raw(d): return json.dumps(d,sort_keys=True,separators=(',',':')).encode()
 def sign(self,raw): return self.key.sign(raw,ec.ECDSA(hashes.SHA256()))
 def lab_chain(self):
  Q=self.build_lab('HUMAN_QUALITY_AUTHORIZATION',0)
  T=self.build_lab('HUMAN_TRANSFER_AUTHORIZATION',1)
  E=self.build_lab('HUMAN_EXECUTION_AUTHORIZATION',2)
  q=self.raw(Q);qs=self.sign(q)
  T['target']['quality_human_authorization_sha256']=a.sha(q)
  T['target']['quality_human_signature_sha256']=a.sha(qs)
  t=self.raw(T);ts=self.sign(t)
  E['target']['quality_human_authorization_sha256']=a.sha(q)
  E['target']['quality_human_signature_sha256']=a.sha(qs)
  E['target']['quality_signature_sha256']=T['target']['quality_signature_sha256']
  E['target']['quality_publication_generation']=T['target']['quality_publication_generation']
  E['target']['transfer_payload_sha256']=T['target']['transfer_payload_sha256']
  E['target']['transfer_human_authorization_sha256']=a.sha(t)
  E['target']['transfer_human_signature_sha256']=a.sha(ts)
  e=self.raw(E);es=self.sign(e)
  return [q,t,e,qs,ts,es]
 def valid_chain(self,c,confirmed=True):
  return a.validar_cadena_lab(*c,self.pk,self.pin,self.base_time+timedelta(minutes=4),identity_confirmed=confirmed)

 def test_01_sha_zip_oficial_pin(self):
  import zipfile
  z=ROOT/'fixtures'
  for n,h in a.EXPECTED_FILE_SHA.items(): self.assertEqual(a.sha((z/n).read_bytes()),h)
 def test_02_three_contracts(self):
  for k in a.FILENAMES: self.valid_draft(k)
 def test_03_pinned_identity(self): self.assertIsNotNone(a.verificar_pem_humano(self.pem))
 def test_04_old_key_refused(self):
  with self.assertRaises(a.Rechazo): a.verificar_pem_humano(b'old-invalid-key')
 def test_05_scope_12_exact(self):
  r=a.auditar_paquete(FIX,FIX/'CLAVE_HUMANA_REFERENCIA_PUBLICA_LIZETH.pem',
       (ROOT/'fixtures/piloto_12_CANDIDATO_LOCAL.json').read_bytes() if (ROOT/'fixtures/piloto_12_CANDIDATO_LOCAL.json').exists() else None)
  self.assertFalse(r['transfer_allowed'])
 def test_06_crossed_action(self):
  k='HUMAN_QUALITY_AUTHORIZATION';d=self.draft(k);d['action']='EXECUTE_PILOT_ONLY'
  with self.assertRaises(a.Rechazo): self.check(k,d)
 def test_07_crossed_type(self):
  k='HUMAN_TRANSFER_AUTHORIZATION';d=self.draft(k);d['document_type']='HUMAN_QUALITY_AUTHORIZATION'
  with self.assertRaises(a.Rechazo): self.check(k,d)
 def test_08_missing_signature_description(self):
  k='HUMAN_TRANSFER_AUTHORIZATION';d=self.draft(k);del d['signature']
  with self.assertRaises(a.Rechazo): self.check(k,d)
 def test_09_external_destination_prefix(self):
  k='HUMAN_EXECUTION_AUTHORIZATION';d=self.draft(k);d['scope']['destination_prefix']='gs://evil/landing/'
  with self.assertRaises(a.Rechazo): self.check(k,d)
 def test_10_extra_file_count(self):
  k='HUMAN_EXECUTION_AUTHORIZATION';d=self.draft(k);d['scope']['file_count']=3599
  with self.assertRaises(a.Rechazo): self.check(k,d)
 def test_11_extra_field(self):
  k='HUMAN_QUALITY_AUTHORIZATION';d=self.draft(k);d['allow_cloud']=True
  with self.assertRaises(a.Rechazo): self.check(k,d)
 def test_12_replay_unsigned_draft_with_signature_metadata(self):
  k='HUMAN_QUALITY_AUTHORIZATION';d=self.draft(k);d['status']='APPROVED'
  with self.assertRaises(a.Rechazo): self.check(k,d)
 def test_13_missing_quality_dependency(self):
  k='HUMAN_TRANSFER_AUTHORIZATION';d=self.draft(k);del d['target']['quality_human_signature_sha256']
  with self.assertRaises(a.Rechazo): self.check(k,d)
 def test_14_invalid_reservation(self):
  k='HUMAN_QUALITY_AUTHORIZATION';d=self.draft(k);d['reservation']['generation']='1'
  with self.assertRaises(a.Rechazo): self.check(k,d)
 def test_15_operational_flag_rejected(self):
  k='HUMAN_EXECUTION_AUTHORIZATION';d=self.draft(k);d['policy']['operational_authorization']=True
  with self.assertRaises(a.Rechazo): self.check(k,d)
 def test_16_reject_duplicate_json_keys(self):
  with self.assertRaises(a.Rechazo):a.parse_strict(b'{"status":"x","status":"y"}')
 def test_17_reject_nan(self):
  with self.assertRaises(a.Rechazo):a.parse_strict(b'{"x":NaN}')
 def test_18_synthetic_der_signature(self):
  raw=b'only testing key never official';sig=self.sign(raw)
  self.assertTrue(a.verificar_firma_detached(raw,sig,self.pk,self.pin))
 def test_19_modified_signature_rejected(self):
  raw=b'bytes';sig=self.sign(raw)
  with self.assertRaises(a.Rechazo):a.verificar_firma_detached(raw+b'!',sig,self.pk,self.pin)
 def test_20_wrong_public_key_rejected(self):
  raw=b'bytes';sig=self.sign(raw)
  other=ec.generate_private_key(ec.SECP256R1()).public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo)
  with self.assertRaises(a.Rechazo):a.verificar_firma_detached(raw,sig,other,self.pin)
 def test_21_signed_draft_refused(self):
  raw=self.original['HUMAN_QUALITY_AUTHORIZATION']; sig=self.sign(raw)
  self.assertTrue(a.verificar_firma_detached(raw,sig,self.pk,self.pin))
  with self.assertRaises(a.Rechazo):a.validar_lab(raw,'HUMAN_QUALITY_AUTHORIZATION',self.base_time)
 def test_22_lab_valid_chain(self): self.assertFalse(self.valid_chain(self.lab_chain())['transfer_allowed'])
 def test_23_lab_no_independent_identity(self):
  with self.assertRaises(a.Rechazo):self.valid_chain(self.lab_chain(),False)
 def test_24_lab_modified_document(self):
  c=self.lab_chain();c[1]=c[1]+b' '
  with self.assertRaises(a.Rechazo):self.valid_chain(c)
 def test_25_lab_modified_link(self):
  c=self.lab_chain();d=json.loads(c[2]);d['target']['transfer_human_signature_sha256']='0'*64
  c[2]=self.raw(d);c[5]=self.sign(c[2])
  with self.assertRaises(a.Rechazo):self.valid_chain(c)
 def test_26_lab_cross_action(self):
  c=self.lab_chain();d=json.loads(c[2]);d['action']='ISSUE_TRANSFER_ONLY'
  c[2]=self.raw(d);c[5]=self.sign(c[2])
  with self.assertRaises(a.Rechazo):self.valid_chain(c)
 def test_27_lab_expired(self):
  raw=self.raw(self.build_lab('HUMAN_QUALITY_AUTHORIZATION',0))
  with self.assertRaises(a.Rechazo):a.validar_lab(raw,'HUMAN_QUALITY_AUTHORIZATION',self.base_time+timedelta(days=1))
 def test_28_lab_issued_future(self):
  raw=self.raw(self.build_lab('HUMAN_QUALITY_AUTHORIZATION',8))
  with self.assertRaises(a.Rechazo):a.validar_lab(raw,'HUMAN_QUALITY_AUTHORIZATION',self.base_time)
 def test_29_lab_max_validity(self):
  d=self.build_lab('HUMAN_QUALITY_AUTHORIZATION');d['expires_at_utc']=(self.base_time+timedelta(hours=25)).strftime('%Y-%m-%dT%H:%M:%SZ')
  with self.assertRaises(a.Rechazo):a.validar_lab(self.raw(d),'HUMAN_QUALITY_AUTHORIZATION',self.base_time+timedelta(minutes=1))
 def test_30_lab_bad_executor(self):
  d=self.build_lab('HUMAN_EXECUTION_AUTHORIZATION');d['target']['executor_identity']='other@example.com'
  with self.assertRaises(a.Rechazo):a.validar_lab(self.raw(d),'HUMAN_EXECUTION_AUTHORIZATION',self.base_time+timedelta(minutes=1))
 def test_31_lab_bad_commit(self):
  d=self.build_lab('HUMAN_EXECUTION_AUTHORIZATION');d['target']['executor_commit']='262ff92'
  with self.assertRaises(a.Rechazo):a.validar_lab(self.raw(d),'HUMAN_EXECUTION_AUTHORIZATION',self.base_time+timedelta(minutes=1))
 def test_32_lab_duplicate_uuid(self):
  c=self.lab_chain();d=json.loads(c[1]);d['authorization_id']=json.loads(c[0])['authorization_id']
  c[1]=self.raw(d);c[4]=self.sign(c[1]); e=json.loads(c[2]);e['target']['transfer_human_authorization_sha256']=a.sha(c[1]);e['target']['transfer_human_signature_sha256']=a.sha(c[4]);c[2]=self.raw(e);c[5]=self.sign(c[2])
  with self.assertRaises(a.Rechazo):self.valid_chain(c)
 def test_33_local_unique_consumption(self):
  with tempfile.TemporaryDirectory() as td:
   ledger=a.LibroConsumoLocal(Path(td)/'ledger.sqlite'); u=str(uuid.uuid4())
   self.assertTrue(ledger.reclamar(u,'ISSUE_QUALITY_ONLY','0'*64))
   with self.assertRaises(a.Rechazo):ledger.reclamar(u,'ISSUE_QUALITY_ONLY','0'*64)
 def test_34_local_race_single_winner(self):
  with tempfile.TemporaryDirectory() as td:
   ledger=a.LibroConsumoLocal(Path(td)/'ledger.sqlite');u=str(uuid.uuid4());result=[]
   def run():
    try:ledger.reclamar(u,'ISSUE_TRANSFER_ONLY','c'*64);result.append(True)
    except a.Rechazo:result.append(False)
   ts=[threading.Thread(target=run) for _ in range(5)]
   for t in ts:t.start()
   for t in ts:t.join()
   self.assertEqual(result.count(True),1)
 def test_35_registry_anchor_success(self):
  raw=b'{"identity":"Lizeth - Quality 329C"}'
  self.assertTrue(a.verificar_registro_ancorado(raw,a.sha(raw)))
 def test_36_registry_tampered(self):
  raw=b'{"identity":"Lizeth - Quality 329C"}'
  with self.assertRaises(a.Rechazo):a.verificar_registro_ancorado(raw+b' ',a.sha(raw))
 def test_37_signature_missing(self):
  with self.assertRaises((a.Rechazo,ValueError,TypeError)):
   a.verificar_firma_detached(b'test',b'',self.pk,self.pin)
 def test_38_crossed_quality_hash(self):
  c=self.lab_chain();d=json.loads(c[1]);d['target']['quality_human_authorization_sha256']='3'*64
  c[1]=self.raw(d);c[4]=self.sign(c[1]);e=json.loads(c[2]);e['target']['transfer_human_authorization_sha256']=a.sha(c[1]);e['target']['transfer_human_signature_sha256']=a.sha(c[4]);c[2]=self.raw(e);c[5]=self.sign(c[2])
  with self.assertRaises(a.Rechazo):self.valid_chain(c)
 def test_39_replay_global_id_other_action(self):
  with tempfile.TemporaryDirectory() as td:
   ledger=a.LibroConsumoLocal(Path(td)/'ledger.sqlite');u=str(uuid.uuid4())
   ledger.reclamar(u,'ISSUE_QUALITY_ONLY','1'*64)
   with self.assertRaises(a.Rechazo):ledger.reclamar(u,'EXECUTE_PILOT_ONLY','2'*64)
 def test_40_future_absurd_expiry(self):
  d=self.build_lab('HUMAN_TRANSFER_AUTHORIZATION')
  d['expires_at_utc']='2026-11-27T22:30:00Z'
  with self.assertRaises(a.Rechazo):a.validar_lab(self.raw(d),'HUMAN_TRANSFER_AUTHORIZATION',self.base_time+timedelta(minutes=3))
 def test_35_null_fields_not_completed(self):
  for k in a.FILENAMES:
   d=self.draft(k);self.assertIsNone(d['authorization_id']);self.assertIsNone(d['issued_at_utc']);self.assertIsNone(d['expires_at_utc'])

if __name__=='__main__':unittest.main()
