import hashlib
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'/'ingestion'))
import verificar_quality_payload_329c_v1 as m

class CompatibilidadQuality329C(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture=ROOT/'fixtures'
        cls.payload=(fixture/'QUALITY_APPROVED_PAYLOAD_PROPUESTO_NO_FIRMAR.json').read_bytes()
        cls.plan=(fixture/'plan_3599_CANDIDATO_LOCAL.json').read_bytes()
        cls.pilot=(fixture/'piloto_12_CANDIDATO_LOCAL.json').read_bytes()
        cls.template=(fixture/'AUTORIZACION_HUMANA_101_7_PLANTILLA_NO_VALIDA.json').read_bytes()

    def test_01_payload_original_referencias_y_modo_bloqueado(self):
        result=m.validar_payload_329c(self.payload,self.plan,self.pilot)
        self.assertEqual(result['schema_compatible'],'PASS')
        self.assertEqual(result['alcance']['archivos'],12)
        self.assertFalse(result['transfer_allowed'])
        self.assertEqual(result['operaciones_gcs'],0)

    def test_02_sha_exactamente_sin_normalizar(self):
        self.assertEqual(m.sha(self.payload),m.PAYLOAD_SHA)
        with self.assertRaisesRegex(m.AuditoriaError,'PAYLOAD_SHA'):
            m.validar_payload_329c(self.payload+b'\n',self.plan,self.pilot)

    def test_03_plan_manipulado(self):
        with self.assertRaisesRegex(m.AuditoriaError,'PLAN_SHA'):
            m.validar_payload_329c(self.payload,self.plan+b' ',self.pilot)

    def test_04_piloto_manipulado(self):
        with self.assertRaisesRegex(m.AuditoriaError,'PILOTO_SHA'):
            m.validar_payload_329c(self.payload,self.plan,self.pilot+b' ')

    def test_05_rechazar_claves_json_duplicadas(self):
        with self.assertRaisesRegex(m.AuditoriaError,'JSON_CLAVE_DUPLICADA'):
            m.cargar_json_exacto(b'{"run_id":"a","run_id":"b"}')

    def test_06_rechazar_utf8_invalido(self):
        with self.assertRaisesRegex(m.AuditoriaError,'JSON_INVALIDO'):
            m.cargar_json_exacto(b'\xff')

    def test_07_rechazar_bool_por_int(self):
        with self.assertRaises(m.AuditoriaError):
            m.validar_campos({'total_bytes':True},{'total_bytes':23052390},'TEST')

    def test_08_rechazar_estado_y_esquema_incorrecto(self):
        p=json.loads(self.payload)
        p['schema_version']='OTRO'
        with self.assertRaises(m.AuditoriaError):
            m.validar_payload_329c(json.dumps(p).encode(),self.plan,self.pilot)
        p=json.loads(self.payload)
        p['status']='TRANSFER_ALLOWED'
        with self.assertRaises(m.AuditoriaError):
            m.validar_payload_329c(json.dumps(p).encode(),self.plan,self.pilot)

    def test_09_destinos_fuera_del_perimetro(self):
        for bad in ['a/../b','a//b','/absoluto','x\\y']:
            with self.subTest(bad=bad),self.assertRaises(m.AuditoriaError):
                m.validar_ruta(bad)

    def test_10_hash_destinos_oficial_reproducible(self):
        d=json.loads(self.pilot)
        s=sorted(item['destino_gcs'] for item in d['archivos_piloto'])
        canon=json.dumps(s,ensure_ascii=False,separators=(',',':')).encode()
        self.assertEqual(hashlib.sha256(canon).hexdigest(),m.DEST_SHA)

    def test_11_documento_humano_no_autoriza(self):
        with self.assertRaisesRegex(m.AuditoriaError,'HUMANA_NO_AUTENTICADA'):
            m.verificar_humana_pendiente(self.template,m.PAYLOAD_SHA)

    def test_12_evidencia_humana_falsa_no_autoriza(self):
        p=json.loads(self.template)
        p.update(decision='APPROVED', approver_principal='alice@example.com',approval_id='X')
        with self.assertRaisesRegex(m.AuditoriaError,'HUMANA_NO_AUTENTICADA'):
            m.verificar_humana_pendiente(json.dumps(p).encode(),m.PAYLOAD_SHA)

    def test_13_sin_firma_rechazo(self):
        with self.assertRaisesRegex(m.AuditoriaError,'FIRMA_DER_FALTANTE'):
            m.verificar_firma_quality(self.payload,b'',b'dummy')

    def test_14_firma_p256_efimera_positiva_solo_criptografia(self):
        key=ec.generate_private_key(ec.SECP256R1())
        pem=key.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo)
        spki=key.public_key().public_bytes(serialization.Encoding.DER,serialization.PublicFormat.SubjectPublicKeyInfo)
        signature=key.sign(self.payload,ec.ECDSA(hashes.SHA256()))
        with patch.object(m,'QUALITY_SPKI',hashlib.sha256(spki).hexdigest()):
            out=m.verificar_firma_quality(self.payload,signature,pem)
            self.assertFalse(out['transfer_allowed'])
            self.assertEqual(out['firma_criptografica'],'PASS')

    def test_15_firma_alterada_rechazo(self):
        key=ec.generate_private_key(ec.SECP256R1())
        pem=key.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo)
        spki=key.public_key().public_bytes(serialization.Encoding.DER,serialization.PublicFormat.SubjectPublicKeyInfo)
        signature=key.sign(self.payload,ec.ECDSA(hashes.SHA256()))
        with patch.object(m,'QUALITY_SPKI',hashlib.sha256(spki).hexdigest()):
            with self.assertRaisesRegex(m.AuditoriaError,'FIRMA_INVALIDA'):
                m.verificar_firma_quality(self.payload+b' ',signature,pem)

    def test_16_clave_quality_no_fijada_rechazo(self):
        key=ec.generate_private_key(ec.SECP256R1())
        pem=key.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo)
        signature=key.sign(self.payload,ec.ECDSA(hashes.SHA256()))
        with self.assertRaisesRegex(m.AuditoriaError,'QUALITY_CLAVE_NO_OFICIAL'):
            m.verificar_firma_quality(self.payload,signature,pem)

    def test_17_confusion_de_curvas_rechazo(self):
        key=ec.generate_private_key(ec.SECP384R1())
        pem=key.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo)
        signature=key.sign(self.payload,ec.ECDSA(hashes.SHA384()))
        with self.assertRaisesRegex(m.AuditoriaError,'CURVA_INCORRECTA'):
            m.verificar_firma_quality(self.payload,signature,pem)

    def test_18_destino_extra_de_piloto_rechazado(self):
        p=json.loads(self.pilot)
        p['archivos_piloto'].append(dict(p['archivos_piloto'][0]))
        changed=json.dumps(p,sort_keys=True).encode()
        with patch.object(m,'PILOT_SHA',hashlib.sha256(changed).hexdigest()):
            with self.assertRaisesRegex(m.AuditoriaError,'PILOTO_CANTIDAD'):
                m.auditar_referencias(self.plan,changed)

    def test_19_destino_no_autorizado_rechazado(self):
        p=json.loads(self.pilot)
        p['archivos_piloto'][0]['destino_gcs']='gs://bvl26-bronze-7f3a9c/landing/NO_PERMITIDO'
        changed=json.dumps(p,sort_keys=True).encode()
        with patch.object(m,'PILOT_SHA',hashlib.sha256(changed).hexdigest()):
            with self.assertRaisesRegex(m.AuditoriaError,'DESTINO_PILOTO_NO_AUTORIZADO'):
                m.auditar_referencias(self.plan,changed)

    def test_20_hash_de_archivo_distinto_rechazado(self):
        p=json.loads(self.pilot)
        p['archivos_piloto'][0]['sha256_esperado']='0'*64
        changed=json.dumps(p,sort_keys=True).encode()
        with patch.object(m,'PILOT_SHA',hashlib.sha256(changed).hexdigest()):
            with self.assertRaisesRegex(m.AuditoriaError,'PILOTO_SHA_ARCHIVO'):
                m.auditar_referencias(self.plan,changed)

    def test_21_replay_de_ruta_duplicada_rechazado(self):
        p=json.loads(self.pilot)
        p['archivos_piloto'][1]=dict(p['archivos_piloto'][0])
        changed=json.dumps(p,sort_keys=True).encode()
        with patch.object(m,'PILOT_SHA',hashlib.sha256(changed).hexdigest()):
            with self.assertRaisesRegex(m.AuditoriaError,'PILOTO_DESTINOS_DATASETS'):
                m.auditar_referencias(self.plan,changed)

    def test_22_swap_quality_orchestrator_rechazado(self):
        with self.assertRaisesRegex(m.AuditoriaError,'QUALITY_CLAVE_NO_OFICIAL'):
            official=ROOT/'config/quality329c/keys/bvl_transfer_official_v1.pem'
            if not official.is_file():
                self.skipTest('PEM oficial Orchestrator no incluido en fixture local')
            pem=official.read_bytes()
            key=ec.generate_private_key(ec.SECP256R1())
            sig=key.sign(self.payload,ec.ECDSA(hashes.SHA256()))
            m.verificar_firma_quality(self.payload,sig,pem)

    def test_23_gate_operativo_cerrado(self):
        with self.assertRaises(PermissionError):
            m.puerta_transferencia({'quality_approved':True, 'transfer_allowed':True})

if __name__ == '__main__':
    unittest.main()
