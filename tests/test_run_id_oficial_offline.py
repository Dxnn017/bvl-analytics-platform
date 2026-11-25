"""Verifica compatibilidad de esquema run_id oficial (solo datos sintéticos)."""
import copy, json, sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'/'ingestion'))
from preparar_piloto_bronze_local import validar_plan
from orquestar_transferencias_bronze_v01 import seleccionar_lote
from transferir_hdfs_gcs import validar_operacion, transferir_archivo
from test_control_bronze_local import ControlLocalTests

RUN='bvl-bronze-inicial-20261125-001'
OLD='bvl-bronze-inicial-20261124-001'
class RunOficialOfflineTest(unittest.TestCase):
    def test_formato_nuevo_run_con_3599_destinos(self):
        ControlLocalTests.setUpClass()
        plan=copy.deepcopy(ControlLocalTests.plan)
        plan['run_id']=RUN
        for op in plan['operaciones']:
            op['run_id']=RUN
            op['destino']=op['destino'].replace('run_id='+OLD+'/', 'run_id='+RUN+'/',1)
        validar_plan(plan)
        lote=seleccionar_lote(json.dumps(plan).encode(),'completo')
        self.assertEqual(lote['run_id'],RUN)
        self.assertEqual(lote['archivos'],3599)
        self.assertFalse(lote['transfer_allowed'])
        self.assertEqual(len({o['destino'] for o in lote['operaciones']}),3599)
        with self.assertRaises(PermissionError):
            transferir_archivo(lote['operaciones'][0],None)

if __name__=='__main__':unittest.main()
