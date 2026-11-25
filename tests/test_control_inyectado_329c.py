import sys, pathlib, unittest, hashlib, json, copy
root=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(root/'scripts/ingestion'))
from puerta_control_gcs_inyectable_329c import *
from puerta_quality_bronze_v01 import verificar_contenido_quality_con_lector_inyectado

class Gate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fx=root/'fixtures'
        cls.plan=(fx/'plan_3599_CANDIDATO_LOCAL.json').read_bytes()
        cls.pilot=(fx/'piloto_12_CANDIDATO_LOCAL.json').read_bytes()
        cls.refs={
            'reservation':{'uri':RESERVA_URI,'generation':int(RESERVA_GENERATION),'sha256':RESERVA_SHA},
            'approval':{'uri':APPROVAL_URI,'generation':999,'sha256':'0'*64},
        }
    def test_no_aprobacion_rechaza(self):
        with self.assertRaises(ControlError):
            self.verify(lambda uri,g: b'{}')
    def test_gen_reserva_falsa_rechaza_antes_de_red(self):
        refs=copy.deepcopy(self.refs); refs['reservation']['generation']+=1
        self.reject_without_read(refs)
    def test_sha_reserva_falso_rechaza_antes_de_red(self):
        refs=copy.deepcopy(self.refs); refs['reservation']['sha256']='0'*64
        self.reject_without_read(refs)
    def test_uri_aprobacion_incorrecta_rechaza(self):
        refs=copy.deepcopy(self.refs); refs['approval']['uri']='gs://otro-bucket/a.json'
        self.reject_without_read(refs)
    def test_plan_modificado_rechaza_antes_de_red(self):
        with self.assertRaises(ControlError): self.verify(lambda u,g: self.fail('READ'),plan=self.plan+b' ')
    def test_piloto_modificado_rechaza_antes_de_red(self):
        with self.assertRaises(ControlError): self.verify(lambda u,g: self.fail('READ'),pilot=self.pilot+b' ')
    def test_escritura_bloqueada(self):
        with self.assertRaises(PermissionError): despacho()
    def test_ref_sin_generacion_rechaza(self):
        refs=copy.deepcopy(self.refs); refs['approval']['generation']=None
        self.reject_without_read(refs)
    def reject_without_read(self,refs):
        with self.assertRaises(ControlError): self.verify(lambda u,g: self.fail('LECTURA_NO_AUTORIZADA'),refs=refs)
    def verify(self,lector,refs=None,plan=None,pilot=None):
        return verificar_contenido_quality_con_lector_inyectado(lector,refs or self.refs,
            self.plan if plan is None else plan,self.pilot if pilot is None else pilot)
