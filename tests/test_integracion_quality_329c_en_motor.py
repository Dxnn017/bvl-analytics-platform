import copy
import json
import pathlib
import sys
import unittest
from unittest.mock import patch

ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'/'ingestion'))
from orquestar_transferencias_bronze_v01 import revisar_quality_329c_desde_coordinador_local, ejecutar_operativo
from puerta_quality_bronze_v01 import transferir_operativamente, auditar_integracion_quality_329c_local
import transferir_hdfs_gcs as nucleo

class Integracion329C(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fx=ROOT/'fixtures'
        cls.plan=(fx/'plan_3599_CANDIDATO_LOCAL.json').read_bytes()
        cls.pilot=(fx/'piloto_12_CANDIDATO_LOCAL.json').read_bytes()
        cls.draft=(fx/'QUALITY_APPROVED_BORRADOR_NO_PUBLICAR.json').read_bytes()

    def comprobar(self, plan=None, pilot=None, draft=None):
        return revisar_quality_329c_desde_coordinador_local(
            self.plan if plan is None else plan,
            self.pilot if pilot is None else pilot,
            self.draft if draft is None else draft)

    def cambiado(self, name, value):
        d=json.loads(self.draft); d[name]=value
        return json.dumps(d).encode()

    def test_01_lote_y_piloto_correctos_siguen_bloqueados(self):
        r=self.comprobar()
        self.assertEqual((r['archivos_lote'],r['archivos_piloto'],r['bytes_piloto']),
                         (3599,12,23052390))
        self.assertFalse(r['transfer_allowed']);self.assertEqual(r['operaciones_gcs'],0)
        self.assertFalse(r['autor_quality_autenticado'])
    def test_02_aprobacion_ausente_rechaza(self):
        with self.assertRaises((ValueError, TypeError)):self.comprobar(draft=b'')
    def test_03_release_independiente_ausente_bloquea(self):
        r=self.comprobar()
        self.assertFalse(r['release_101_7_autenticado'])
        with self.assertRaises(PermissionError):ejecutar_operativo()
        with self.assertRaises(PermissionError):transferir_operativamente()
    def test_04_generacion_reserva_alterada_rechaza(self):
        d=json.loads(self.draft);d['reservation_ref']['generation']='0'
        with self.assertRaises(ValueError):self.comprobar(draft=json.dumps(d).encode())
    def test_05_hash_reserva_alterado_rechaza(self):
        d=json.loads(self.draft);d['reservation_ref']['sha256']='0'*64
        with self.assertRaises(ValueError):self.comprobar(draft=json.dumps(d).encode())
    def test_06_run_id_ajeno_rechaza(self):
        with self.assertRaises(ValueError):self.comprobar(draft=self.cambiado('run_id','OTRO_RUN'))
    def test_07_scope_full_no_es_piloto(self):
        with self.assertRaises(ValueError):self.comprobar(draft=self.cambiado('scope','FULL_3599'))
    def test_08_piloto_con_archivo_13_rechaza(self):
        p=json.loads(self.pilot);p['archivos_piloto'].append(copy.deepcopy(p['archivos_piloto'][0]))
        with self.assertRaises(ValueError):self.comprobar(pilot=json.dumps(p).encode())
    def test_09_transfer_allowed_inyectado_rechaza(self):
        with self.assertRaises(ValueError):self.comprobar(draft=self.cambiado('transfer_allowed',True))
    def test_10_falso_gate_de_paso_1017_rechaza(self):
        with self.assertRaises(ValueError):self.comprobar(draft=self.cambiado('step_101_7_authorized',True))
    def test_11_falso_operativo_rechaza(self):
        with self.assertRaises(ValueError):self.comprobar(draft=self.cambiado('is_operational',True))
    def test_12_plan_corrupto_rechaza(self):
        with self.assertRaises(ValueError):self.comprobar(plan=self.plan+b' ')
    def test_13_piloto_corrupto_rechaza(self):
        with self.assertRaises(ValueError):self.comprobar(pilot=self.pilot+b' ')
    def test_14_nucleo_booleano_manual_no_autoriza(self):
        with patch.object(nucleo,'TRANSFERENCIAS_HABILITADAS',True):
            with self.assertRaises(PermissionError):nucleo.transferir_archivo({},object(),contexto={})
            with self.assertRaises(ValueError):self.comprobar()
    def test_15_modo_local_no_accede_a_gcs(self):
        with patch('subprocess.run',side_effect=AssertionError('SUBPROCESS_NO_DEBE_USARSE')):
            r=self.comprobar()
            self.assertEqual(r['integracion_local'],'PASS')
    def test_16_escrituras_no_tienen_entrada_expuesta(self):
        with self.assertRaises(PermissionError):ejecutar_operativo({'approved':True})
        with self.assertRaises(PermissionError):transferir_operativamente({'approved':True})

if __name__=='__main__':unittest.main()
