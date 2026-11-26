"""Pruebas SOLO LOCALES; no credenciales ni GCP. Los datos del release son sinteticos."""
import concurrent.futures
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch

MODULE=Path(__file__).resolve().parents[1]/'scripts/ingestion/simular_consumo_101_7_e7_offline.py'
spec=importlib.util.spec_from_file_location('bvl_e7_offline', MODULE)
m=importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

def make_data():
    docs={}
    q={'document_type':m.KIND[0], 'target':{'quality_payload_sha256':'a'*64}}
    qbytes=json.dumps(q,sort_keys=True).encode()
    t={'document_type':m.KIND[1], 'target':{'quality_human_authorization_sha256':m.sha(qbytes)}}
    tbytes=json.dumps(t,sort_keys=True).encode()
    e={'document_type':m.KIND[2], 'target':{'quality_human_authorization_sha256':m.sha(qbytes),
                                            'transfer_human_authorization_sha256':m.sha(tbytes)}}
    docs=dict(zip(m.KIND,[qbytes,tbytes,json.dumps(e,sort_keys=True).encode()]))
    hashes={k:m.sha(v) for k,v in docs.items()}
    firmas={k:('firma_de_laboratorio_'+k).encode() for k in m.KIND}
    hashes_firmas={k:m.sha(v) for k,v in firmas.items()}
    refs={'run_id':m.RUN_ID, 'project_id':m.PROJECT_ID,
        'reservation':{'generation':m.RESERVATION_GENERATION,'sha256':m.RESERVATION_SHA256},
        'scope':{'mode':'PILOTO_12_ARCHIVOS','file_count':12,'total_bytes':23052390,
            'pilot_sha256':m.PILOT_SHA256,'destination_sha256':m.CANONICAL_DEST_SHA256,
            'full_plan_sha256':m.FULL_PLAN_SHA256,'full_batch_authorized':False,
            'overwrite_allowed':False}, 'documents':hashes.copy(),
        'signatures':hashes_firmas.copy()}
    return refs,docs,hashes,firmas,hashes_firmas

def trust():
    data={'schema':'BVL-HUMAN-TRUST-RECORD-329C-v1',
        'public_key_file_sha256':m.PEM_SHA256,'public_key_spki_sha256':m.SPKI_SHA256,
        'revoked_previous_fingerprint':m.REVOKED_PEM_SHA256,
        'trust_for':'HUMAN_IDENTITY_SIGNATURE_CHECKS_ONLY',
        'cloud_transfer_allowed':False,'operational_authorization':False}
    return json.dumps(data).encode()

class SimulacionE7(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.ref,self.docs,self.hashes,self.firmas,self.hashes_firmas=make_data()
        self.auth=str(uuid.uuid4())

    def test_01_enlace_tres_docs(self):
        self.assertFalse(m.verificar_vinculos(self.ref,self.docs,firmas=self.firmas,anclajes_confiables=self.hashes,anclajes_firmas=self.hashes_firmas)['transfer_allowed'])
    def test_02_sustitucion_documento(self):
        d=self.docs.copy();d[m.KIND[0]]+=b' '
        with self.assertRaises(m.Rechazo):m.verificar_vinculos(self.ref,d,firmas=self.firmas,anclajes_confiables=self.hashes,anclajes_firmas=self.hashes_firmas)
    def test_03_referencia_y_hash_autosuministrados_no_valen(self):
        d=self.docs.copy();d[m.KIND[0]]+=b' '
        ref=copy.deepcopy(self.ref);ref['documents'][m.KIND[0]]=m.sha(d[m.KIND[0]])
        with self.assertRaises(m.Rechazo):m.verificar_vinculos(ref,d,firmas=self.firmas,anclajes_confiables=self.hashes,anclajes_firmas=self.hashes_firmas)
    def test_04_calidad_cruzada(self):
        d=self.docs.copy();x=json.loads(d[m.KIND[1]]);x['target']['quality_human_authorization_sha256']='b'*64
        d[m.KIND[1]]=json.dumps(x).encode();h={k:m.sha(v) for k,v in d.items()}
        ref=copy.deepcopy(self.ref);ref['documents']=h.copy()
        with self.assertRaises(m.Rechazo):m.verificar_vinculos(ref,d,firmas=self.firmas,anclajes_confiables=h,anclajes_firmas=self.hashes_firmas)
    def test_05_transfer_cruzada(self):
        d=self.docs.copy();x=json.loads(d[m.KIND[2]]);x['target']['transfer_human_authorization_sha256']='b'*64
        d[m.KIND[2]]=json.dumps(x).encode();h={k:m.sha(v) for k,v in d.items()}
        ref=copy.deepcopy(self.ref);ref['documents']=h.copy()
        with self.assertRaises(m.Rechazo):m.verificar_vinculos(ref,d,firmas=self.firmas,anclajes_confiables=h,anclajes_firmas=self.hashes_firmas)
    def test_31_firma_ausente_bloquea_enlace(self):
        firmas=self.firmas.copy();firmas.pop(m.KIND[1])
        with self.assertRaises(m.Rechazo):m.verificar_vinculos(self.ref,self.docs,
             firmas=firmas,anclajes_confiables=self.hashes,anclajes_firmas=self.hashes_firmas)
    def test_32_firma_modificada_bloquea_enlace(self):
        firmas=self.firmas.copy();firmas[m.KIND[0]]+=b'X'
        with self.assertRaises(m.Rechazo):m.verificar_vinculos(self.ref,self.docs,
             firmas=firmas,anclajes_confiables=self.hashes,anclajes_firmas=self.hashes_firmas)
    def test_33_firma_y_hash_falsos_no_pasan_pin_externo(self):
        firmas=self.firmas.copy();firmas[m.KIND[0]]+=b'X'
        ref=copy.deepcopy(self.ref);ref['signatures'][m.KIND[0]]=m.sha(firmas[m.KIND[0]])
        with self.assertRaises(m.Rechazo):m.verificar_vinculos(ref,self.docs,
             firmas=firmas,anclajes_confiables=self.hashes,anclajes_firmas=self.hashes_firmas)
    def test_06_tipo_cruzado(self):
        d=self.docs.copy();d[m.KIND[0]]=d[m.KIND[1]]
        with self.assertRaises(m.Rechazo):m.verificar_vinculos(self.ref,d,firmas=self.firmas,anclajes_confiables=self.hashes,anclajes_firmas=self.hashes_firmas)
    def test_07_falta_documento(self):
        d=self.docs.copy();d.pop(m.KIND[2])
        with self.assertRaises(m.Rechazo):m.verificar_vinculos(self.ref,d,firmas=self.firmas,anclajes_confiables=self.hashes,anclajes_firmas=self.hashes_firmas)
    def test_08_proyecto_alterado(self):
        ref=copy.deepcopy(self.ref);ref['project_id']='otro'
        with self.assertRaises(m.Rechazo):m.verificar_vinculos(ref,self.docs,firmas=self.firmas,anclajes_confiables=self.hashes,anclajes_firmas=self.hashes_firmas)
    def test_09_run_id_alterado(self):
        ref=copy.deepcopy(self.ref);ref['run_id']='otra-corrida'
        with self.assertRaises(m.Rechazo):m.verificar_vinculos(ref,self.docs,firmas=self.firmas,anclajes_confiables=self.hashes,anclajes_firmas=self.hashes_firmas)
    def test_10_reserva_alterada(self):
        ref=copy.deepcopy(self.ref);ref['reservation']['generation']='99'
        with self.assertRaises(m.Rechazo):m.verificar_vinculos(ref,self.docs,firmas=self.firmas,anclajes_confiables=self.hashes,anclajes_firmas=self.hashes_firmas)
    def test_11_intento_3599(self):
        ref=copy.deepcopy(self.ref);ref['scope']['file_count']=3599
        with self.assertRaises(m.Rechazo):m.verificar_vinculos(ref,self.docs,firmas=self.firmas,anclajes_confiables=self.hashes,anclajes_firmas=self.hashes_firmas)
    def test_12_destino_extra(self):
        ref=copy.deepcopy(self.ref);ref['scope']['destination_sha256']='f'*64
        with self.assertRaises(m.Rechazo):m.verificar_vinculos(ref,self.docs,firmas=self.firmas,anclajes_confiables=self.hashes,anclajes_firmas=self.hashes_firmas)
    def test_13_sobrescritura(self):
        ref=copy.deepcopy(self.ref);ref['scope']['overwrite_allowed']=True
        with self.assertRaises(m.Rechazo):m.verificar_vinculos(ref,self.docs,firmas=self.firmas,anclajes_confiables=self.hashes,anclajes_firmas=self.hashes_firmas)
    def test_14_registro_pin(self):
        r=trust()
        # PEM publico que Quality ya versiono en fixtures.
        pem=Path(__file__).resolve().parents[1]/'fixtures/CLAVE_HUMANA_REFERENCIA_PUBLICA_LIZETH.pem'
        self.assertEqual(m.verificar_confianza(r,pin_registro_independiente=m.sha(r),
              pem_raw=pem.read_bytes(),lista_revocacion={m.REVOKED_PEM_SHA256})['confianza_local'],'PIN_VALIDO')
    def test_15_registro_sustituido(self):
        r=trust();pem=(Path(__file__).resolve().parents[1]/'fixtures/CLAVE_HUMANA_REFERENCIA_PUBLICA_LIZETH.pem').read_bytes()
        with self.assertRaises(m.Rechazo):m.verificar_confianza(r+b' ',pin_registro_independiente=m.sha(r),
             pem_raw=pem,lista_revocacion={m.REVOKED_PEM_SHA256})
    def test_16_registro_sin_ancla(self):
        with self.assertRaises(m.Rechazo):m.verificar_confianza(trust(),pin_registro_independiente='',
             pem_raw=b'dummy',lista_revocacion={m.REVOKED_PEM_SHA256})
    def test_17_revocacion_actual(self):
        pem=(Path(__file__).resolve().parents[1]/'fixtures/CLAVE_HUMANA_REFERENCIA_PUBLICA_LIZETH.pem').read_bytes()
        r=trust()
        with self.assertRaises(m.Rechazo):m.verificar_confianza(r,pin_registro_independiente=m.sha(r),
             pem_raw=pem,lista_revocacion={m.REVOKED_PEM_SHA256,m.PEM_SHA256})
    def test_18_registro_operativo_falso(self):
        r=json.loads(trust());r['cloud_transfer_allowed']=True
        b=json.dumps(r).encode();pem=(Path(__file__).resolve().parents[1]/'fixtures/CLAVE_HUMANA_REFERENCIA_PUBLICA_LIZETH.pem').read_bytes()
        with self.assertRaises(m.Rechazo):m.verificar_confianza(b,pin_registro_independiente=m.sha(b),
             pem_raw=pem,lista_revocacion={m.REVOKED_PEM_SHA256})
    def test_19_consumo_local_y_replay(self):
        res=m.consumir_simulado(self.tmp.name,self.auth,self.hashes)
        self.assertTrue(res.startswith('RECLAMO_SIMULADO'))
        self.assertEqual(m.consumir_simulado(self.tmp.name,self.auth,self.hashes),
                         'CONFLICTO_412_REUTILIZACION_BLOQUEADA')
    def test_20_dos_ejecutores_concurrentes(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:m.consumir_simulado(self.tmp.name,self.auth,self.hashes),range(2)))
        self.assertEqual(sum(x.startswith('RECLAMO_SIMULADO') for x in results),1)
        self.assertEqual(results.count('CONFLICTO_412_REUTILIZACION_BLOQUEADA'),1)
    def test_21_ocho_ejecutores_concurrentes(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results=list(pool.map(lambda _:m.consumir_simulado(self.tmp.name,self.auth,self.hashes),range(8)))
        self.assertEqual(sum(x.startswith('RECLAMO_SIMULADO') for x in results),1)
        self.assertEqual(results.count('CONFLICTO_412_REUTILIZACION_BLOQUEADA'),7)
    def test_22_dos_procesos_independientes(self):
        code='''import importlib.util,sys,json; p=sys.argv[1];s=importlib.util.spec_from_file_location("m",p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);print(m.consumir_simulado(sys.argv[2],sys.argv[3],json.loads(sys.argv[4])))'''
        args=[sys.executable,'-c',code,str(MODULE),self.tmp.name,self.auth,json.dumps(self.hashes)]
        p1=subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        p2=subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        a,e1=p1.communicate(timeout=10);b,e2=p2.communicate(timeout=10)
        self.assertEqual((p1.returncode,p2.returncode),(0,0),e1+e2)
        self.assertEqual(sum(x.startswith('RECLAMO_SIMULADO') for x in (a.strip(),b.strip())),1)
        self.assertEqual((a+b).count('CONFLICTO_412_REUTILIZACION_BLOQUEADA'),1)
    def test_23_actor_que_no_es_consumidor(self):
        with self.assertRaises(m.Rechazo):m.consumir_simulado(self.tmp.name,self.auth,self.hashes,actor=m.INGESTOR_EXISTING)
    def test_24_firma_faltante(self):
        h=self.hashes.copy();h.pop(m.KIND[2]);
        with self.assertRaises(m.Rechazo):m.consumir_simulado(self.tmp.name,self.auth,h)
    def test_25_uuid_invalido(self):
        with self.assertRaises(m.Rechazo):m.consumir_simulado(self.tmp.name,'../../secret',self.hashes)
    def test_26_error_fs_bloquea(self):
        with patch.object(m.os,'open',side_effect=PermissionError('denegado')):
            self.assertEqual(m.consumir_simulado(self.tmp.name,self.auth,self.hashes),'ERROR_REGISTRO_FAIL_CLOSED')
    def test_27_archivo_solo_se_crea_una_vez(self):
        m.consumir_simulado(self.tmp.name,self.auth,self.hashes)
        p=m.ruta_simulada(self.tmp.name,self.auth)
        first=p.read_bytes();m.consumir_simulado(self.tmp.name,self.auth,self.hashes)
        self.assertEqual(first,p.read_bytes());self.assertFalse(json.loads(first)['transfer_allowed'])
    def test_28_dos_uuid_son_registros_distintos_no_autorizan(self):
        a=m.consumir_simulado(self.tmp.name,self.auth,self.hashes)
        b=m.consumir_simulado(self.tmp.name,str(uuid.uuid4()),self.hashes)
        self.assertTrue(a.startswith('RECLAMO_SIMULADO') and b.startswith('RECLAMO_SIMULADO'))
        self.assertEqual(len(list(Path(self.tmp.name).rglob('*.json'))),2)
        # Necesario en Cloud: impedir nueva autorizacion UUID con docs ya consumidos.
    def test_29_sin_credenciales_cloud(self):
        raw=MODULE.read_text()
        self.assertNotIn('google.cloud',raw)
        self.assertNotIn('upload_from_filename',raw)
    def test_30_auditor_no_puede_cambiar_gate(self):
        m.consumir_simulado(self.tmp.name,self.auth,self.hashes)
        payload=json.loads(m.ruta_simulada(self.tmp.name,self.auth).read_bytes())
        self.assertEqual(payload['gcs_operations'],0)
        self.assertEqual(payload['transfer_allowed'],False)

if __name__=='__main__': unittest.main()
