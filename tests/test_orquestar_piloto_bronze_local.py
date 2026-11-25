"""Pruebas de rechazo del orquestador; sin red, IAM, HDFS ni GCS."""
import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts' / 'ingestion'))
from preparar_piloto_bronze_local import preparar
from orquestar_piloto_bronze_local import integrar, guardar, OrquestacionError

# Datos sintéticos; pruebas de contratos, nunca sustituyen SHA reales.
from test_preparar_piloto_bronze_local import plan_valido


class OrquestadorPilotoTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plan = plan_valido()
        cls.raw = json.dumps(cls.plan, sort_keys=True).encode()
        cls.piloto = preparar(cls.plan, hashlib.sha256(cls.raw).hexdigest())
        cls.evidencia = {
            'run_id': cls.plan['run_id'], 'tipo': 'EVIDENCIA_LOCAL_HDFS',
            'archivos_esperados': 12, 'archivos_verificados': 12,
            'archivos_con_error': 0, 'bytes_esperados': cls.piloto['bytes_esperados'],
            'quality_approved': False, 'transfer_allowed': False,
            'transferencias_gcs': 0,
            'archivos': [
                dict(x, estado='VERIFICADO_HDFS', error=None,
                     bytes_observados=x['bytes_esperados'],
                     sha256_hdfs_observado=x['sha256_esperado'])
                for x in cls.piloto['archivos_piloto']
            ],
        }

    def correr(self, piloto=None, evidencia=None, raw=None):
        return integrar(self.raw if raw is None else raw,
                        self.piloto if piloto is None else piloto,
                        self.evidencia if evidencia is None else evidencia)

    def test_flujo_valido_pero_bloqueado(self):
        r = self.correr()
        self.assertEqual(r['hdfs_verificados'], 12)
        self.assertEqual(r['gcs_transferidos'], 0)
        self.assertFalse(r['transfer_allowed'])
        self.assertFalse(r['liberacion_101_7_verificada'])

    def test_hdfs_sin_verificar(self):
        h = copy.deepcopy(self.evidencia)
        h['archivos'][0]['estado'] = 'PENDIENTE'
        with self.assertRaisesRegex(OrquestacionError, 'HDFS_NO_VERIFICADO'):
            self.correr(evidencia=h)

    def test_sha_hdfs_mutado(self):
        h = copy.deepcopy(self.evidencia)
        h['archivos'][0]['sha256_hdfs_observado'] = 'f'*64
        with self.assertRaisesRegex(OrquestacionError, 'HDFS_SHA'):
            self.correr(evidencia=h)

    def test_generacion_gcs_falsa(self):
        p = copy.deepcopy(self.piloto)
        p['archivos_piloto'][0]['generacion_gcs'] = 1
        with self.assertRaisesRegex(OrquestacionError, 'PILOTO_YA_TRANSFERIDO'):
            self.correr(piloto=p)

    def test_intento_aprobacion_local(self):
        p = copy.deepcopy(self.piloto)
        p['quality_approved'] = True
        with self.assertRaisesRegex(OrquestacionError, 'PILOTO_APROBACION_INVALIDA'):
            self.correr(piloto=p)

    def test_plan_diferente_rechazado(self):
        plan = copy.deepcopy(self.plan)
        plan['operaciones'][0]['sha256'] = 'f'*64
        with self.assertRaisesRegex(OrquestacionError, 'PLAN_SHA_NO_COINCIDE'):
            self.correr(raw=json.dumps(plan, sort_keys=True).encode())

    def test_destino_cambiado(self):
        p = copy.deepcopy(self.piloto)
        p['archivos_piloto'][0]['destino_gcs'] = 'gs://mal/otra-ruta'
        with self.assertRaisesRegex(OrquestacionError, 'ARCHIVO_NO_AUTORIZADO'):
            self.correr(piloto=p)

    def test_archivo_duplicado(self):
        p = copy.deepcopy(self.piloto)
        p['archivos_piloto'][1] = copy.deepcopy(p['archivos_piloto'][0])
        with self.assertRaisesRegex(OrquestacionError, 'DUPLICADO'):
            self.correr(piloto=p)

    def test_permutacion_evidencia_no_autorizada(self):
        h = copy.deepcopy(self.evidencia)
        h['archivos'][0], h['archivos'][1] = h['archivos'][1], h['archivos'][0]
        with self.assertRaisesRegex(OrquestacionError, 'HDFS_IDENTIDAD'):
            self.correr(evidencia=h)

    def test_guardado_idempotente_y_bloqueado(self):
        r = self.correr()
        with tempfile.TemporaryDirectory() as d:
            a = guardar(r, d)
            b = guardar(r, d)
            self.assertEqual(a, b)
            self.assertEqual(len(a), 3)
            data = json.loads(Path(a[0]).read_text())
            self.assertFalse(data['transfer_allowed'])
            self.assertEqual(data['hdfs_verificados'], 12)
            Path(a[1]).write_text('MODIFICADO')
            with self.assertRaisesRegex(OrquestacionError, 'EVIDENCIA_PREEXISTENTE_DISTINTA'):
                guardar(r, d)


if __name__ == '__main__':
    unittest.main()
