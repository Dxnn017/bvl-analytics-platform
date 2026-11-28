"""Pruebas contractuales F6 totalmente offline; la clave P256 es EFIMERA.

El contrato es un stub sintético para aislar la puerta; NO prueba la firma
ni los bytes del contrato oficial de Quality. Esos siguen validados por
check_base en la ejecucion real.
"""
import copy
import hashlib
import importlib.util
import json
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch, Mock

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'scripts'/'pipeline'))
import puerta_silver_operativa_v1_candidata as gate

DATASETS=[
 ('bvl/cotizaciones','mercado_diario/cotizaciones'),
 ('bvl/montos_sab','mercado_diario/montos_sab'),
 ('smv/valores_inscritos','referencias_smv/valores_inscritos'),
 ('smv/hechos_importancia','hechos_importancia/eventos'),
 ('smv/hechos_valores_consulta','referencias_smv/hechos_valores_consulta'),
 ('smv/cambios_patrimonio','finanzas_empresariales/cambios_patrimonio'),
 ('smv/estado_resultados','finanzas_empresariales/estado_resultados'),
 ('smv/flujo_efectivo','finanzas_empresariales/flujo_efectivo'),
 ('smv/principales_cuentas','finanzas_empresariales/principales_cuentas'),
 ('smv/situacion_financiera','finanzas_empresariales/situacion_financiera'),
 ('bcrp/embig','macroeconomia/embig'),
 ('bcrp/tipo_cambio','macroeconomia/tipo_cambio'),
]

class ExtensionDocumentalTests(unittest.TestCase):
    def setUp(self):
        self.now=datetime(2026,11,28,12,0,tzinfo=timezone.utc)
        self.key=ec.generate_private_key(ec.SECP256R1())
        self.pem=self.key.public_key().public_bytes(
            serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo)
        der=self.key.public_key().public_bytes(
            serialization.Encoding.DER,serialization.PublicFormat.SubjectPublicKeyInfo)
        self.pin=hashlib.sha256(der).hexdigest()
        p=patch.object(gate,'QUALITY_PIN',self.pin);p.start();self.addCleanup(p.stop)
        self.contract={
          'project_id':'bvl-analytics-2026', 'region':'southamerica-west1',
          'run_id':'bvl-bronze-inicial-20261125-001',
          'consumer':{'service_account':'sa-bvl-silver@bvl-analytics-2026.iam.gserviceaccount.com'},
          'datasets':[{'dataset':name,
            'proposed_silver_product': (product if name != gate.DOC_DATASET else None),
            'bronze_manifest_uri':f'gs://{gate.CONTROL}/manifests/bronze/{name}/m.json',
            'bronze_manifest_generation':'101','bronze_manifest_sha256':'a'*64}
            for name,product in DATASETS]
        }
        p=patch.object(gate,'check_base',return_value=self.contract)
        p.start();self.addCleanup(p.stop)
        self.contract_raw=b'SYNTHETIC_CONTRACT_STUB_NEVER_ACCEPT_AS_OFFICIAL'
        base={k:self.contract[k] for k in ('project_id','region','run_id')}
        base['base_contract_sha256']=gate.CONTRACT_SHA256
        self.hist_annex={**base,'schema_version':gate.ANNEX_SCHEMA,
          'status':'RATIFICADO','quality_approved':True,
          'automatic_retry_allowed':False,'overwrite_allowed':False,
          'direct_processes':[
            {'bronze_dataset':name,'silver_products':[
                product, 'referencias_smv/puente_ruc_nemonico']
                if name=='smv/valores_inscritos' else [product]}
            for name,product in DATASETS],
          'derived_processes':[{'silver_product':'hechos_importancia/identidad_empresarial',
            'bronze_dependencies':[], 'silver_dependencies':[
            'hechos_importancia/eventos','finanzas_empresariales/principales_cuentas',
            'referencias_smv/puente_ruc_nemonico','mercado_diario/cotizaciones',
            'referencias_smv/hechos_valores_consulta']}]}
        self.hist_closure={**base,'schema_version':gate.CLOSURE_SCHEMA,
          'status':'PHASE5_CLOSED','phase_5_closed':True,
          'quality_approved':True, 'closed_at_utc':'2026-11-26T17:30:00+00:00',
          'operational_annex_sha256':gate.digest(gate.canonical(self.hist_annex))}
        self.hist_ann_raw=gate.canonical(self.hist_annex)
        self.hist_clo_raw=gate.canonical(self.hist_closure)
        self.annex=gate.expected_documental_annex(self.hist_annex,
            gate.digest(self.hist_ann_raw),gate.digest(self.hist_clo_raw))
        self.closure=gate.expected_documental_closure(self.hist_closure,
            gate.canonical(self.annex),gate.digest(self.hist_ann_raw),
            gate.digest(self.hist_clo_raw),'2026-11-28T11:00:00+00:00')
        d=next(d for d in self.contract['datasets'] if d['dataset']==gate.DOC_DATASET)
        self.auth={**base,
            'schema_version':gate.AUTH_SCHEMA,'status':'SILVER_AUTORIZADO',
            'mode':'SINGLE_SOURCE','dataset':gate.DOC_DATASET,
            'silver_product':None,'documental_scope':gate.DOC_SCOPE,
            'operation':'BUILD_SILVER_STAGING',
            'service_account':self.contract['consumer']['service_account'],
            'attempt_id':'doc001','not_before_utc':'2026-11-28T11:30:00+00:00',
            'expires_at_utc':'2026-11-28T12:30:00+00:00',
            'overwrite_allowed':False,'official_publication_allowed':False,
            'automatic_retry_allowed':False,
            'bronze_dependencies':[{'dataset':gate.DOC_DATASET,
                'manifest_uri':d['bronze_manifest_uri'],
                'manifest_generation':d['bronze_manifest_generation'],
                'manifest_sha256':d['bronze_manifest_sha256']}],
            'silver_dependencies':[],
            'silver_outputs':[{'name':gate.DOC_PRODUCT,
               'staging_prefix':gate.stage(gate.DOC_PRODUCT,base['run_id'],'doc001')}],
            'operational_annex_sha256':gate.digest(gate.canonical(self.annex)),
            'phase5_closure_sha256':gate.digest(gate.canonical(self.closure))}
    def signed(self,obj):
        raw=gate.canonical(obj)
        return raw,self.key.sign(raw,ec.ECDSA(hashes.SHA256()))
    def plan(self, *, old=True, history=True):
        a,c,u=(self.hist_annex,self.hist_closure,copy.deepcopy(self.auth)) if old else (self.annex,self.closure,self.auth)
        if old:
            u.pop('documental_scope',None)
            u['silver_outputs']=[{'name':gate.HIST_PRODUCT,
               'staging_prefix':gate.stage(gate.HIST_PRODUCT,self.contract['run_id'],'doc001')}]
            u['operational_annex_sha256']=gate.digest(gate.canonical(a))
            u['phase5_closure_sha256']=gate.digest(gate.canonical(c))
        ar,asig=self.signed(a)
        cr,csig=self.signed(c)
        ur,usig=self.signed(u)
        extra={}
        if history:
            hr,hs=self.signed(self.hist_annex)
            pr,ps=self.signed(self.hist_closure)
            extra={'historical_annex_raw':hr,'historical_annex_sig':hs,
                   'historical_closure_raw':pr,'historical_closure_sig':ps}
        return gate.validate_signed_bundle(self.contract_raw,ar,asig,cr,csig,
                   ur,usig,self.pem,now=self.now,**(extra if not old else {}))
    def test_01_legacy_historico_firmado_sigue_funcionando(self):
        self.assertEqual(self.plan(old=True).authorization['silver_outputs'][0]['name'],gate.HIST_PRODUCT)
    def test_02_documental_firmado_acepta_solo_nuevo_producto(self):
        self.assertEqual(self.plan(old=False).authorization['silver_outputs'][0]['name'],gate.DOC_PRODUCT)
    def test_03_contrato_sha_fijo_sin_cambios(self):
        self.assertEqual(gate.CONTRACT_SHA256,'6f33f3a88e04b38a5e75863d3ccc67005231f05d474b9916d34fd7030edef20a')
    def test_04_anexo_historico_immutable(self):
        old=self.hist_ann_raw
        gate.expected_documental_annex(self.hist_annex,gate.digest(old),gate.digest(self.hist_clo_raw))
        self.assertEqual(gate.canonical(self.hist_annex),old)
    def test_05_extension_sin_padres_firmados_rechazada(self):
        with self.assertRaisesRegex(gate.OperationalError,'historicos FIRMADOS'):self.plan(old=False,history=False)
    def test_06_huella_padre_incorrecta_rechazada(self):
        self.annex['historical_annex_sha256']='0'*64
        self.relink()
        with self.assertRaisesRegex(gate.OperationalError,'Acta de extension documental'):self.plan(old=False)
    def test_07_inyeccion_salida_extra_rechazada(self):
        self.auth['silver_outputs'].append(copy.deepcopy(self.auth['silver_outputs'][0]))
        with self.assertRaisesRegex(gate.OperationalError,'salidas'):self.plan(old=False)
    def test_08_eventos_no_autorizado_en_extension(self):
        self.auth['silver_outputs'][0]['name']=gate.HIST_PRODUCT
        self.auth['silver_outputs'][0]['staging_prefix']=gate.stage(gate.HIST_PRODUCT,self.contract['run_id'],'doc001')
        with self.assertRaisesRegex(gate.OperationalError,'Productos no ratificados'):self.plan(old=False)
    def test_09_documental_no_autorizado_en_anexo_historico(self):
        new=self.plan(old=True)
        self.assertNotEqual(new.authorization['silver_outputs'][0]['name'],gate.DOC_PRODUCT)
    def test_10_otro_intento_staging_rechazado(self):
        self.auth['silver_outputs'][0]['staging_prefix']=self.auth['silver_outputs'][0]['staging_prefix'].replace('doc001','otro001')
        with self.assertRaisesRegex(gate.OperationalError,'staging'):self.plan(old=False)
    def test_11_reintento_automatico_rechazado(self):
        self.auth['automatic_retry_allowed']=True
        with self.assertRaisesRegex(gate.OperationalError,'inseguro'):self.plan(old=False)
    def test_12_multi_source_no_valido_en_extension(self):
        self.auth['mode']='MULTI_SOURCE'
        with self.assertRaisesRegex(gate.OperationalError,'no exclusiva'):self.plan(old=False)
    def test_13_autorizacion_sin_marcador_documental_rechazada(self):
        self.auth.pop('documental_scope')
        with self.assertRaisesRegex(gate.OperationalError,'no exclusiva'):self.plan(old=False)
    def test_14_otra_extension_no_ratificada_rechazada(self):
        self.annex['documental_extension']['silver_product']='hechos_importancia/otro'
        self.relink()
        with self.assertRaisesRegex(gate.OperationalError,'lista blanca'):self.plan(old=False)
    def test_15_extension_modifica_otras_fuentes_rechazada(self):
        self.annex['direct_processes'][0]['silver_products']=['finanzas_empresariales/inventado']
        self.relink()
        with self.assertRaisesRegex(gate.OperationalError,'Mapeo distinto al contrato'):self.plan(old=False)
    def test_16_acta_cambia_procedencia_historica_rechazada(self):
        self.closure['historical_closure_sha256']='f'*64
        self.auth['phase5_closure_sha256']=gate.digest(gate.canonical(self.closure))
        with self.assertRaisesRegex(gate.OperationalError,'Acta de extension'):self.plan(old=False)
    def test_17_firma_alterada_rechazada(self):
        ar,asig=self.signed(self.annex)
        cr,csig=self.signed(self.closure)
        ur,usig=self.signed(self.auth)
        pr,ps=self.signed(self.hist_annex)
        qr,qs=self.signed(self.hist_closure)
        with self.assertRaises(Exception):
            gate.validate_signed_bundle(self.contract_raw,ar,asig,cr,csig,ur,usig,
                self.pem,now=self.now,historical_annex_raw=pr,
                historical_annex_sig=ps[:-2]+b'zz',
                historical_closure_raw=qr,historical_closure_sig=qs)
    def test_18_documental_no_acepta_referencias_historicas_incompletas(self):
        with self.assertRaisesRegex(gate.OperationalError,'historicos FIRMADOS'):
            self.plan(old=False,history=False)
    def test_19_contexto_legacy_rechaza_extension_de_inyeccion(self):
        self.hist_annex['documental_extension']=gate.documental_extension()
        with self.assertRaisesRegex(gate.OperationalError,'anexo historico'):self.plan(old=True)
    def test_20_autorizacion_historica_con_marcador_rechazada(self):
        ann=copy.deepcopy(self.hist_annex)
        self.assertEqual(gate.validate_annex(ann,self.contract),ann)
        legacy=copy.deepcopy(self.auth)
        legacy['operational_annex_sha256']=gate.digest(gate.canonical(ann))
        legacy['phase5_closure_sha256']=gate.digest(gate.canonical(self.hist_closure))
        with self.assertRaisesRegex(gate.OperationalError,'Marcador documental'):
            gate.validate_auth(legacy,self.contract,ann,gate.canonical(ann),gate.canonical(self.hist_closure),now=self.now)
    def test_21_plan_documental_no_consumible_offline(self):
        offline=self.plan(old=False)
        with self.assertRaisesRegex(gate.OperationalError,'offline no puede consumir'):
            gate.consume_once(offline,object(),now=self.now)
    def test_22_ruta_auth_se_mantiene_unica_y_exacta(self):
        u,_=gate.authorization_paths('SINGLE_SOURCE',gate.DOC_DATASET,None,
                                     self.contract['run_id'],'doc001')
        self.assertEqual(u,('gs://'+gate.CONTROL+'/authorizations/silver/smv/hechos_importancia/'
            'run_id='+self.contract['run_id']+'/attempt_id=doc001/authorization.json'))
    def test_23_fingerprint_diferente_para_producto_documental(self):
        a=self.plan(old=False); b=self.plan(old=True)
        self.assertNotEqual(a.fingerprint,b.fingerprint)
    def test_24_firma_anexo_historico_no_es_firma_documental(self):
        h,sig=self.signed(self.hist_annex)
        with self.assertRaises(Exception):gate.signed(gate.canonical(self.annex),sig,self.pem)
    def _fake_trusted_bundle(self):
        objs = {}
        sources = {
          'annex':('contracts/bronze_silver/v1/operational_annex_documental.json',self.annex),
          'closure':('authorizations/phase5/closure_documental.json',self.closure),
          'historical_annex':('contracts/bronze_silver/v1/operational_annex.json',self.hist_annex),
          'historical_closure':('authorizations/phase5/closure.json',self.hist_closure),
        }
        aurl, sigurl = gate.authorization_paths('SINGLE_SOURCE',gate.DOC_DATASET,None,
                                                 self.contract['run_id'],'doc001')
        refs={}
        for key,(relative,obj) in sources.items():
            uri='gs://'+gate.CONTROL+'/'+relative
            suri=uri.removesuffix('.json')+'.sig'
            raw,sig=self.signed(obj)
            objs[uri]=raw;objs[suri]=sig
            refs[key]={'uri':uri,'signature_uri':suri,'generation':'101',
                       'sha256':gate.digest(raw),'signature_generation':'102',
                       'signature_sha256':gate.digest(sig)}
        ar,sign=self.signed(self.auth)
        objs[aurl]=ar;objs[sigurl]=sign
        refs['authorization']={'uri':aurl,'signature_uri':sigurl,
          'generation':'101','sha256':gate.digest(ar),'signature_generation':'102',
          'signature_sha256':gate.digest(sign)}
        return refs,objs

    def _gcs_plan(self, refs=None, objects=None):
        if refs is None:refs,objects=self._fake_trusted_bundle()
        client=Mock()
        client._credentials=SimpleNamespace(
            service_account_email=self.contract['consumer']['service_account'])
        with patch.object(gate.gcs,'read_exact_current_bytes',
                          side_effect=lambda _client,uri,*args,**kwargs:objects[uri]), \
             patch.object(gate.gcs,'read_official_manifest',return_value=(b'{}',[{'uri':'fake'}])), \
             patch.object(gate.gcs,'verify_official_inputs',return_value=1):
            plan=gate.check_gcs_signed_bundle(client,refs,self.pem,self.contract_raw,now=self.now)
        return plan,client

    def test_25_preclaim_gcs_firmas_historicas_y_extension_validas(self):
        p,client=self._gcs_plan()
        self.assertIsInstance(p,gate.GCSVerifiedPlan)
        self.assertEqual(p.authorization['silver_outputs'][0]['name'],gate.DOC_PRODUCT)
        self.assertEqual(p.fingerprint,gate.fingerprint(p.authorization))

    def test_26_launcher_acepta_plan_documental_con_intento_exacto(self):
        p,_=self._gcs_plan()
        import importlib.util
        sys.path.insert(0,str(ROOT/'scripts'/'processing'))
        if 'fase6_hechos_launch_scope_candidato' not in sys.modules:
            sys.modules['fase6_hechos_launch_scope_candidato']=types.ModuleType(
                'fase6_hechos_launch_scope_candidato')
        spec=importlib.util.spec_from_file_location('test_local_launcher',
            ROOT/'scripts'/'processing'/'fase6_hechos_launcher_documental_v1.py')
        launcher=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(launcher)
        ctx={'run_id':self.auth['run_id'],'attempt_id':self.auth['attempt_id'],
             'authorization_fingerprint':p.fingerprint,
             'staging_prefix':self.auth['silver_outputs'][0]['staging_prefix'],
             'service_account':self.auth['service_account']}
        self.assertTrue(launcher._verify_f5_new_product(p,ctx))
        ctx['authorization_fingerprint']='0'*64
        with self.assertRaisesRegex(ValueError,'FINGERPRINT_DISTINTO'):
            launcher._verify_f5_new_product(p,ctx)

    def test_27_preflight_documental_sin_cadena_historica_no_pasa(self):
        refs,items=self._fake_trusted_bundle()
        del refs['historical_annex'];del refs['historical_closure']
        with self.assertRaisesRegex(gate.OperationalError,'historicos FIRMADOS'):
            self._gcs_plan(refs,items)

    def test_28_preflight_rechaza_referencia_externa_extra(self):
        refs,items=self._fake_trusted_bundle()
        refs['injection']=refs['annex']
        with self.assertRaisesRegex(gate.OperationalError,'incompleto'):
            self._gcs_plan(refs,items)

    def test_29_preflight_rechaza_sustitucion_acta_historica(self):
        refs,items=self._fake_trusted_bundle()
        uri=refs['historical_closure']['uri']
        raw,sig=self.signed(self.closure)
        items[uri]=raw
        items[refs['historical_closure']['signature_uri']]=sig
        with self.assertRaises(gate.OperationalError):self._gcs_plan(refs,items)

    def test_30_gcs_preclaim_no_consume_claim(self):
        refs,items=self._fake_trusted_bundle()
        with patch.object(gate,'consume_once',side_effect=AssertionError('claim indebido')):
            p,_=self._gcs_plan(refs,items)
            self.assertIsInstance(p,gate.GCSVerifiedPlan)

    def test_31_plantillas_exige_firmas_historicas_y_solo_generan_json(self):
        import importlib.util
        target=ROOT/'scripts'/'quality'/'generar_plantillas_f6_documental.py'
        spec=importlib.util.spec_from_file_location('generator_doc_extension',target)
        gen=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gen)
        hr,hs=self.signed(self.hist_annex)
        cr,cs=self.signed(self.hist_closure)
        result=gen.build_templates(self.contract_raw,hr,hs,cr,cs,self.pem,
            attempt_id='doc001',not_before_utc='2026-11-28T11:30:00+00:00',
            expires_at_utc='2026-11-28T12:30:00+00:00',
            closed_at_utc='2026-11-28T11:00:00+00:00')
        self.assertEqual(set(result),{'operational_annex_documental.json',
            'closure_documental.json','authorization.json'})
        self.assertEqual(result['authorization.json'],gate.canonical(self.auth))
        with self.assertRaises(Exception):
            gen.build_templates(self.contract_raw,hr,b'firma mala',cr,cs,self.pem,
             attempt_id='doc001',not_before_utc='2026-11-28T11:30:00+00:00',
             expires_at_utc='2026-11-28T12:30:00+00:00',
             closed_at_utc='2026-11-28T11:00:00+00:00')

    def test_32_acta_documental_no_puede_adelantar_cierre_historico(self):
        self.closure['closed_at_utc']='2026-11-25T01:00:00+00:00'
        self.auth['phase5_closure_sha256']=gate.digest(gate.canonical(self.closure))
        with self.assertRaisesRegex(gate.OperationalError,'anterior al cierre historico'):
            self.plan(old=False)

    def relink(self):
        self.closure['operational_annex_sha256']=gate.digest(gate.canonical(self.annex))
        self.auth['operational_annex_sha256']=gate.digest(gate.canonical(self.annex))
        self.auth['phase5_closure_sha256']=gate.digest(gate.canonical(self.closure))

if __name__=='__main__':unittest.main()
