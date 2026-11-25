import copy, json, sys, unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'/'ingestion'))
from puerta_quality_bronze_v01 import auditar_contrato_329a_preflight_local, transferir_operativamente
from adaptador_329a_reserva_local import Incompatible
F=ROOT/'fixtures'
class Integracion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.r=json.loads((F/'reserva_329a_FICTICIA_LOCAL.json').read_bytes())
        cls.p=(F/'plan_3599_CANDIDATO_LOCAL.json').read_bytes()
        cls.i=json.loads((F/'piloto_12_CANDIDATO_LOCAL.json').read_bytes())
    def test_integration_ok_and_blocked(self):
        r=auditar_contrato_329a_preflight_local(self.r,self.p,self.i)
        self.assertEqual(r['integracion_motor'],'PUERTA_329C_CONTRATO_329A_OFFLINE_PASS')
        self.assertFalse(r['cloud_habilitado'])
        self.assertFalse(r['transfer_allowed'])
        self.assertEqual(r['operaciones_gcp'],0)
    def test_reserva_no_reservada(self):
        x=copy.deepcopy(self.r);x['reservation_status']='PENDIENTE'
        with self.assertRaises(Incompatible):auditar_contrato_329a_preflight_local(x,self.p,self.i)
    def test_false_quality_approval(self):
        x=copy.deepcopy(self.r);x['validation_approved']=True
        with self.assertRaises(Incompatible):auditar_contrato_329a_preflight_local(x,self.p,self.i)
    def test_run_id_falso(self):
        x=copy.deepcopy(self.r);x['run_id']='bvl-bronze-inicial-20261124-001'
        with self.assertRaises(Incompatible):auditar_contrato_329a_preflight_local(x,self.p,self.i)
    def test_plan_mutado(self):
        x=self.p.replace(b'PLANIFICADO_NO_EJECUTADO',b'PLANIFICADO_YA_EJECUTADO',1)
        with self.assertRaises(Incompatible):auditar_contrato_329a_preflight_local(self.r,x,self.i)
    def test_piloto_alterado(self):
        x=copy.deepcopy(self.i);x['archivos_piloto'][0]['sha256_esperado']='0'*64
        with self.assertRaises(Incompatible):auditar_contrato_329a_preflight_local(self.r,self.p,x)
    def test_bloqueo_operativo_conservado(self):
        with self.assertRaises(PermissionError):transferir_operativamente()
