"""Pruebas 100% offline; P256 en clave EFIMERA solo para estas pruebas."""
import copy
import hashlib
import json
import os
import sys
import unittest
from datetime import datetime,timezone
from pathlib import Path
from unittest.mock import patch, Mock

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'/'pipeline'))
import puerta_silver_operativa_v1_candidata as gate
import puerta_silver_v1_candidata as core
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import ec

ruta_contrato = os.environ.get('BVL_CONTRACT_V1_PATH')
if not ruta_contrato:
    raise RuntimeError('Falta BVL_CONTRACT_V1_PATH: contrato oficial requerido')
FIX = Path(ruta_contrato).expanduser().resolve()
if not FIX.is_file():
    raise RuntimeError(f'Contrato no encontrado: {FIX}')

class FakeClaimStore:
    def __init__(self):self.objects={}
    def create_exclusive(self, uri,payload):
        if uri in self.objects:raise ValueError('PreconditionFailed 412')
        self.objects[uri]=payload
        return 999

class Phase5GateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw_contract=FIX.read_bytes()
        cls.contract=core.validate_contract(cls.raw_contract,gate.CONTRACT_SHA256)
        cls.private=ec.generate_private_key(ec.SECP256R1())
        cls.pem=cls.private.public_key().public_bytes(serialization.Encoding.PEM,
                       serialization.PublicFormat.SubjectPublicKeyInfo)
        pub_der=cls.private.public_key().public_bytes(serialization.Encoding.DER,
                       serialization.PublicFormat.SubjectPublicKeyInfo)
        cls.test_pin=hashlib.sha256(pub_der).hexdigest()
        cls.now=datetime(2026,11,26,18,0,tzinfo=timezone.utc)

    def setUp(self):
        self.pinpatch=patch.object(gate,'QUALITY_PIN',self.test_pin)
        self.pinpatch.start()
        self.addCleanup(self.pinpatch.stop)
        self.annex={
            'schema_version':gate.ANNEX_SCHEMA,'status':'RATIFICADO','quality_approved':True,
            'base_contract_sha256':gate.CONTRACT_SHA256,
            'project_id':self.contract['project_id'], 'region':self.contract['region'],
            'run_id':self.contract['run_id'], 'automatic_retry_allowed':False,
            'overwrite_allowed':False, 'direct_processes':[],
            'derived_processes':[{'silver_product':'hechos_importancia/identidad_empresarial',
                'bronze_dependencies':[], 'silver_dependencies':[
                  'hechos_importancia/eventos','finanzas_empresariales/principales_cuentas',
                  'referencias_smv/puente_ruc_nemonico','mercado_diario/cotizaciones',
                  'referencias_smv/hechos_valores_consulta']}]
        }
        for d in self.contract['datasets']:
            n=d.get('proposed_silver_product')
            if not n:
                n={'smv/hechos_importancia':'hechos_importancia/eventos',
                   'smv/hechos_valores_consulta':'referencias_smv/hechos_valores_consulta'}[d['dataset']]
            products=[n]
            if d['dataset']=='smv/valores_inscritos': products+=['referencias_smv/puente_ruc_nemonico']
            self.annex['direct_processes'].append({'bronze_dataset':d['dataset'],'silver_products':products})
        self.closure={
            'schema_version':gate.CLOSURE_SCHEMA,'status':'PHASE5_CLOSED',
            'phase_5_closed':True,'quality_approved':True,
            'base_contract_sha256':gate.CONTRACT_SHA256,
            'project_id':self.contract['project_id'],'region':self.contract['region'],
            'run_id':self.contract['run_id'], 'closed_at_utc':'2026-11-26T17:30:00+00:00'}
        self.auth=self.single('smv/valores_inscritos')
        self.seal_links()

    def sign(self, obj):
        raw=gate.canonical(obj)
        sig=self.private.sign(raw,ec.ECDSA(hashes.SHA256()))
        return raw,sig

    def seal_links(self):
        self.closure['operational_annex_sha256']=gate.digest(gate.canonical(self.annex))
        self.auth['operational_annex_sha256']=gate.digest(gate.canonical(self.annex))
        self.auth['phase5_closure_sha256']=gate.digest(gate.canonical(self.closure))

    def single(self,dataset):
        row=next(d for d in self.contract['datasets'] if d['dataset']==dataset)
        products=next(p['silver_products'] for p in self.annex['direct_processes'] if p['bronze_dataset']==dataset)
        return {'schema_version':gate.AUTH_SCHEMA,'status':'SILVER_AUTORIZADO',
           'mode':'SINGLE_SOURCE','dataset':dataset,'run_id':self.contract['run_id'],
           'project_id':self.contract['project_id'],'region':self.contract['region'],
           'base_contract_sha256':gate.CONTRACT_SHA256,'operation':'BUILD_SILVER_STAGING',
           'service_account':self.contract['consumer']['service_account'],
           'attempt_id':'a001','not_before_utc':'2026-11-26T17:00:00+00:00',
           'expires_at_utc':'2026-11-26T19:00:00+00:00',
           'overwrite_allowed':False,'official_publication_allowed':False,
           'automatic_retry_allowed':False,
           'bronze_dependencies':[{'dataset':dataset,'manifest_uri':row['bronze_manifest_uri'],
               'manifest_generation':row['bronze_manifest_generation'],
               'manifest_sha256':row['bronze_manifest_sha256']}],
           'silver_dependencies':[],
           'silver_outputs':[{'name':p,'staging_prefix':gate.stage(p,self.contract['run_id'],'a001')} for p in products]}

    def multi(self):
        self.auth={
            **self.auth, 'mode':'MULTI_SOURCE','silver_product':'hechos_importancia/identidad_empresarial',
            'bronze_dependencies':[],'silver_dependencies':[],
            'silver_outputs':[{'name':'hechos_importancia/identidad_empresarial',
                'staging_prefix':gate.stage('hechos_importancia/identidad_empresarial',self.contract['run_id'],'a001')}]}
        del self.auth['dataset']
        for p in self.annex['derived_processes'][0]['silver_dependencies']:
            self.auth['silver_dependencies'].append({'product':p,'manifest_uri':
                f'gs://{gate.CONTROL}/manifests/silver/{p}/version_id=v001/manifest.json',
                'manifest_generation':'100','manifest_sha256':'a'*64,'version_id':'v001'})
        self.seal_links()

    def plan(self):
        ann,asig=self.sign(self.annex)
        close,csig=self.sign(self.closure)
        auth,usig=self.sign(self.auth)
        return gate.validate_signed_bundle(self.raw_contract,ann,asig,close,csig,
                                           auth,usig,self.pem,now=self.now)

    def test_01_real_contract_integrity_and_phase_open(self):
        self.assertFalse(self.contract['phase_5_closed'])
        self.assertEqual(gate.digest(self.raw_contract),gate.CONTRACT_SHA256)

    def test_02_signed_single_two_outputs_approved_offline(self):
        x=self.plan()
        self.assertEqual(x.authorization['mode'],'SINGLE_SOURCE')
        self.assertEqual(len(x.authorization['silver_outputs']),2)
        self.assertIn('/attempt_id=a001/',x.authorization['silver_outputs'][0]['staging_prefix'])

    def test_03_annex_signature_tampering_rejected(self):
        ann,asig=self.sign(self.annex)
        with self.assertRaises(Exception):gate.signed(ann+b' ',asig,self.pem)

    def test_04_wrong_trust_public_key_rejected(self):
        ann,asig=self.sign(self.annex)
        with patch.object(gate,'QUALITY_PIN','0'*64):
            with self.assertRaises(Exception):gate.signed(ann,asig,self.pem)

    def test_05_closure_missing_approval_rejected(self):
        self.closure['quality_approved']=False
        self.seal_links()
        with self.assertRaisesRegex(gate.OperationalError,'Fase 5'):
            self.plan()

    def test_06_missing_second_output_rejected(self):
        self.auth['silver_outputs']=self.auth['silver_outputs'][:1]
        with self.assertRaisesRegex(gate.OperationalError,'salidas'):
            self.plan()

    def test_07_wrong_output_staging_rejected(self):
        self.auth['silver_outputs'][0]['staging_prefix']='gs://bvl26-silver-7f3a9c/official/hack/'
        with self.assertRaisesRegex(gate.OperationalError,'staging'):
            self.plan()

    def test_08_future_or_expired_authorization_rejected(self):
        self.auth['expires_at_utc']='2026-11-26T17:30:00+00:00'
        with self.assertRaisesRegex(gate.OperationalError,'Vigencia'):
            self.plan()

    def test_09_long_validity_rejected(self):
        self.auth['expires_at_utc']='2026-11-27T00:00:00+00:00'
        with self.assertRaisesRegex(gate.OperationalError,'Vigencia'):
            self.plan()

    def test_10_manifests_generation_tampered(self):
        self.auth['bronze_dependencies'][0]['manifest_generation']='12345'
        with self.assertRaisesRegex(gate.OperationalError,'Manifest Bronze'):
            self.plan()

    def test_11_multiple_source_all_five_versions(self):
        self.multi()
        p=self.plan()
        self.assertEqual(len(p.authorization['silver_dependencies']),5)

    def test_12_multiple_source_missing_one_dependency_rejected(self):
        self.multi()
        self.auth['silver_dependencies'].pop()
        with self.assertRaisesRegex(gate.OperationalError,'multiples'):
            self.plan()

    def test_13_multiple_source_unversioned_rejected(self):
        self.multi()
        self.auth['silver_dependencies'][0].pop('version_id')
        with self.assertRaisesRegex(gate.OperationalError,'Version'):
            self.plan()

    def test_14_claim_atomic_and_no_reuse(self):
        p=self.plan()
        store=FakeClaimStore()
        principal=self.contract['consumer']['service_account']
        self.assertEqual(gate.consume_once(p,store,effective_principal=principal,now=self.now),999)
        with self.assertRaises(ValueError):gate.consume_once(p,store,effective_principal=principal,now=self.now)
        self.assertEqual(len(store.objects),1)

    def test_15_wrong_runtime_identity_denied(self):
        p=self.plan()
        with self.assertRaisesRegex(gate.OperationalError,'Identidad'):
            gate.consume_once(p,FakeClaimStore(),effective_principal='attacker',now=self.now)

    def test_16_new_expiry_same_attempt_same_fingerprint(self):
        old=gate.fingerprint(self.auth)
        self.auth['expires_at_utc']='2026-11-26T20:00:00+00:00'
        self.auth['silver_outputs'].reverse()
        self.assertEqual(gate.fingerprint(self.auth),old)

    def test_17_new_attempt_new_fingerprint_and_staging(self):
        before=gate.fingerprint(self.auth)
        self.auth['attempt_id']='a002'
        for d in self.auth['silver_outputs']:
            d['staging_prefix']=gate.stage(d['name'],self.contract['run_id'],'a002')
        self.assertNotEqual(gate.fingerprint(self.auth),before)
        self.assertEqual(self.plan().authorization['attempt_id'],'a002')

    def test_18_recovery_same_attempt_denied(self):
        self.auth.update({'recovery_of_fingerprint':'a'*64,
                          'quality_reconciliation_id':'rec-001',
                          'previous_attempt_id':'a001'})
        with self.assertRaisesRegex(gate.OperationalError,'reutiliza'):
            self.plan()

    def test_19_recovery_new_attempt_with_quality_reference(self):
        self.auth.update({'recovery_of_fingerprint':'a'*64,
                          'quality_reconciliation_id':'rec-001',
                          'previous_attempt_id':'a001','attempt_id':'a002'})
        for d in self.auth['silver_outputs']:
            d['staging_prefix']=gate.stage(d['name'],self.contract['run_id'],'a002')
        self.assertEqual(self.plan().authorization['attempt_id'],'a002')

    def test_20_cannot_claim_unverified_object(self):
        with self.assertRaisesRegex(gate.OperationalError,'no verificado'):
            gate.consume_once({'fake':True},FakeClaimStore(),effective_principal='x',now=self.now)

    def test_21_exact_auth_paths_both_modes(self):
        c=self.contract
        s,_=gate.authorization_paths('SINGLE_SOURCE','smv/valores_inscritos',None,c['run_id'],'a001')
        m,_=gate.authorization_paths('MULTI_SOURCE',None,'hechos_importancia/identidad_empresarial',c['run_id'],'a001')
        self.assertIn('/smv/valores_inscritos/run_id=',s)
        self.assertIn('/derived/hechos_importancia/identidad_empresarial/run_id=',m)
        self.assertIn('/attempt_id=a001/authorization.json',s)

    def test_22_claim_store_zero_before_explicit_claim(self):
        store=FakeClaimStore(); self.plan();self.assertEqual(store.objects,{})

    def test_23_annex_duplicate_output_denied(self):
        for p in self.annex['direct_processes']:
            if p['bronze_dataset']=='smv/cotizaciones':p['silver_products'].append(p['silver_products'][0])
        self.seal_links()
        with self.assertRaisesRegex(gate.OperationalError,'duplicados'):
            self.plan()

    def test_24_missing_auth_signature_rejected(self):
        ann,asig=self.sign(self.annex);close,csig=self.sign(self.closure)
        auth,_=self.sign(self.auth)
        with self.assertRaises(Exception):
            gate.validate_signed_bundle(self.raw_contract,ann,asig,close,csig,auth,b'broken',self.pem,now=self.now)


    def test_25_two_outputs_joint_success_requires_both(self):
        plan=self.plan()
        outs=plan.authorization['silver_outputs']
        one=[{'name':outs[0]['name'],'staging_prefix':outs[0]['staging_prefix'],
              'status':'STAGING_VALIDADO','quality_check_passed':True,'files_count':1}]
        with self.assertRaisesRegex(gate.OperationalError,'Falta'):
            gate.validate_joint_staging_completion(plan,one)
        two=one+[{'name':outs[1]['name'],'staging_prefix':outs[1]['staging_prefix'],
                  'status':'STAGING_VALIDADO','quality_check_passed':True,'files_count':3}]
        result=gate.validate_joint_staging_completion(plan,two)
        self.assertEqual(result['outputs_checked'],2)
        self.assertFalse(result['official_publication_allowed'])

    def test_26_unvalidated_output_rejected(self):
        plan=self.plan()
        a=[{'name':o['name'],'staging_prefix':o['staging_prefix'],
            'status':'STAGING_VALIDADO','quality_check_passed':True,'files_count':1}
           for o in plan.authorization['silver_outputs']]
        a[1]['quality_check_passed']=False
        with self.assertRaisesRegex(gate.OperationalError,'sin validacion'):
            gate.validate_joint_staging_completion(plan,a)


    def _trusted_refs_and_contents(self):
        ann,asig=self.sign(self.annex)
        clo,csig=self.sign(self.closure)
        aut,usig=self.sign(self.auth)
        c=self.contract['run_id']; ds=self.auth.get('dataset'); product=self.auth.get('silver_product')
        au,au_sig=gate.authorization_paths(self.auth['mode'],ds,product,c,self.auth['attempt_id'])
        pairs={
          'annex':(f'gs://{gate.CONTROL}/contracts/bronze_silver/v1/operational_annex.json',ann,asig),
          'closure':(f'gs://{gate.CONTROL}/authorizations/phase5/run_id={c}/closure.json',clo,csig),
          'authorization':(au,aut,usig)
        }
        refs={};contents={}
        for name,(uri,raw,sig) in pairs.items():
            sig_uri=au_sig if name=='authorization' else uri.replace('.json','.sig')
            refs[name]={'uri':uri,'generation':'101','sha256':gate.digest(raw),
                        'signature_uri':sig_uri,'signature_generation':'102',
                        'signature_sha256':gate.digest(sig)}
            contents[uri]=raw;contents[sig_uri]=sig
        return refs,contents

    def test_27_gcs_readonly_preflight_no_claim(self):
        refs,contents=self._trusted_refs_and_contents()
        with patch.object(gate.gcs,'read_exact_current_bytes',side_effect=lambda _client,uri,*a,**k:contents[uri]) as read, \
             patch.object(gate.gcs,'read_official_manifest',return_value=(b'{}',[{'uri':'fake'}])) as manifests, \
             patch.object(gate.gcs,'verify_official_inputs',return_value=1) as inputs, \
             patch.object(gate.gcs.GCSExclusiveClaimStore,'create_exclusive') as claim:
            result=gate.check_gcs_signed_bundle(Mock(),refs,self.pem,self.raw_contract,now=self.now)
            self.assertEqual(result.authorization['mode'],'SINGLE_SOURCE')
            self.assertEqual(read.call_count,6)
            manifests.assert_called_once(); inputs.assert_called_once();claim.assert_not_called()

    def test_28_gcs_readonly_rejects_wrong_auth_path(self):
        refs,contents=self._trusted_refs_and_contents()
        refs['authorization']['uri']=refs['authorization']['uri'].replace('attempt_id=a001','attempt_id=evil')
        refs['authorization']['signature_uri']=refs['authorization']['signature_uri'].replace('attempt_id=a001','attempt_id=evil')
        evil_uri=refs['authorization']['uri']
        contents[evil_uri]=gate.canonical(self.auth)
        contents[refs['authorization']['signature_uri']]=self.sign(self.auth)[1]
        with patch.object(gate.gcs,'read_exact_current_bytes',side_effect=lambda _client,uri,*a,**k:contents[uri]), \
             patch.object(gate.gcs,'read_official_manifest') as manifests:
            with self.assertRaisesRegex(gate.OperationalError,'Ruta de auth'):
                gate.check_gcs_signed_bundle(Mock(),refs,self.pem,self.raw_contract,now=self.now)
            manifests.assert_not_called()

    def test_29_incomplete_quality_context_rejected_without_gcs_reads(self):
        with patch.object(gate.gcs,'read_exact_current_bytes') as rd:
            with self.assertRaisesRegex(gate.OperationalError,'incompleto'):
                gate.check_gcs_signed_bundle(Mock(),{'authorization':{}},self.pem,self.raw_contract,now=self.now)
            rd.assert_not_called()

if __name__=='__main__':unittest.main()
