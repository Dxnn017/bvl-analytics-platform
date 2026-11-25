"""Prueba coordinador Bronze sin GCS ni HDFS. Las cargas son ficticias."""
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts' / 'ingestion'))
from orquestar_transferencias_bronze_v01 import (
    OrquestadorError, seleccionar_lote, validar_recibo,
    _procesar_ensayo_con_inyeccion, ejecutar_operativo,
)
from preparar_piloto_bronze_local import preparar
import test_control_bronze_local as fixtures


class WorkerBronzeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixtures.ControlLocalTests.setUpClass()
        cls.plan = fixtures.ControlLocalTests.plan
        cls.plan_bytes = json.dumps(cls.plan).encode()
        cls.piloto = preparar(cls.plan, hashlib.sha256(cls.plan_bytes).hexdigest())
        cls.evidencia = {
            'tipo': 'EVIDENCIA_LOCAL_HDFS',
            'run_id': cls.plan['run_id'],
            'archivos_esperados': 12, 'archivos_verificados': 12,
            'archivos_con_error': 0,
            'bytes_esperados': cls.piloto['bytes_esperados'],
            'quality_approved': False, 'transfer_allowed': False,
            'transferencias_gcs': 0,
            'archivos': [dict(item, estado='VERIFICADO_HDFS', error=None,
                             bytes_observados=item['bytes_esperados'],
                             sha256_hdfs_observado=item['sha256_esperado'])
                         for item in cls.piloto['archivos_piloto']],
        }

    def batch_piloto(self):
        return seleccionar_lote(self.plan_bytes, 'piloto', self.piloto, self.evidencia)

    def test_full_3599(self):
        lote = seleccionar_lote(self.plan_bytes, 'completo')
        self.assertEqual(lote['archivos'], 3599)
        self.assertEqual(lote['datasets'], 12)
        self.assertFalse(lote['transfer_allowed'])

    def test_pilot_12(self):
        lote = self.batch_piloto()
        self.assertEqual(lote['archivos'], 12)
        self.assertEqual(lote['datasets'], 12)
        self.assertEqual(lote['bytes'], self.piloto['bytes_esperados'])

    def test_piloto_evidencia_obligatoria(self):
        with self.assertRaisesRegex(OrquestadorError, 'PILOTO_EVIDENCIA_REQUERIDA'):
            seleccionar_lote(self.plan_bytes, 'piloto')

    def test_completo_no_acepta_piloto(self):
        with self.assertRaisesRegex(OrquestadorError, 'COMPLETO_NO_ACEPTA_PILOTO'):
            seleccionar_lote(self.plan_bytes, 'completo', self.piloto, self.evidencia)

    def test_piloto_rechaza_sha_hdfs_invalido(self):
        e = json.loads(json.dumps(self.evidencia))
        e['archivos'][0]['sha256_hdfs_observado'] = '0' * 64
        with self.assertRaisesRegex(Exception, 'HDFS_SHA'):
            seleccionar_lote(self.plan_bytes, 'piloto', self.piloto, e)

    @staticmethod
    def fake(op, estado='TRANSFERIDO'):
        return {
            'run_id': op['run_id'], 'dataset': op['dataset'],
            'origen_hdfs': op['origen'], 'destino_gcs': op['destino'],
            'bytes_esperados': op['size_bytes'],
            'bytes_observados': op['size_bytes'],
            'sha256_esperado': op['sha256'], 'sha256_gcs': op['sha256'],
            'generacion_gcs': 42, 'estado': estado,
            'validacion': 'INTEGRIDAD_VERIFICADA',
            'identidad_esperada': 'sa-bvl-ingestion@bvl-analytics-2026.iam.gserviceaccount.com',
        }

    def test_validar_recibo(self):
        op = self.batch_piloto()['operaciones'][0]
        self.assertEqual(validar_recibo(op, self.fake(op))['generacion_gcs'], 42)

    def test_recibo_sha_invalido(self):
        op = self.batch_piloto()['operaciones'][0]
        recibo = self.fake(op)
        recibo['sha256_gcs'] = '0' * 64
        with self.assertRaisesRegex(OrquestadorError, 'RECIBO_INCONSISTENTE:sha256_gcs'):
            validar_recibo(op, recibo)

    def test_recibo_generacion_invalida(self):
        op = self.batch_piloto()['operaciones'][0]
        r = self.fake(op); r['generacion_gcs'] = 0
        with self.assertRaisesRegex(OrquestadorError, 'RECIBO_GENERACION'):
            validar_recibo(op, r)

    def test_ensayo_journal_append_and_reconcile(self):
        lote = self.batch_piloto()
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'eventos.jsonl'
            rep = _procesar_ensayo_con_inyeccion(lote, self.fake, p)
            self.assertEqual(rep['archivos_verificados_simulados'], 12)
            self.assertEqual(rep['operaciones_gcs'], 0)
            self.assertFalse(rep['transfer_allowed'])
            _procesar_ensayo_con_inyeccion(
                lote, lambda op: self.fake(op, 'REINTENTO_EXISTENTE'), p)
            lines = [json.loads(x) for x in p.read_text().splitlines()]
            self.assertEqual(len(lines), 24)
            self.assertTrue(all(x['tipo'] == 'ENSAYO_LOCAL_SIN_CLOUD' for x in lines))

    def test_reintento_transitorio_recuperado(self):
        lote = self.batch_piloto()
        contador = 0
        def fake(op):
            nonlocal contador
            contador += 1
            if contador == 1:
                raise TimeoutError('temporal')
            return self.fake(op)
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'eventos.jsonl'
            r = _procesar_ensayo_con_inyeccion(lote, fake, p)
            self.assertEqual(r['archivos_verificados_simulados'], 12)
            self.assertEqual(len(p.read_text().splitlines()), 13)

    def test_reintento_agotado_no_continua(self):
        lote = self.batch_piloto()
        contador = 0
        def fake(op):
            nonlocal contador
            contador += 1
            raise TimeoutError('temporal')
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'eventos.jsonl'
            with self.assertRaisesRegex(OrquestadorError, 'ERROR_TRANSITORIO_AGOTADO'):
                _procesar_ensayo_con_inyeccion(lote, fake, p, reintentos=1)
            self.assertEqual(contador, 2)
            self.assertEqual(len(p.read_text().splitlines()), 2)

    def test_recibo_invalido_no_prosigue(self):
        lote = self.batch_piloto()
        contador = 0
        def fake(op):
            nonlocal contador
            contador += 1
            r = self.fake(op)
            r['bytes_observados'] = 0
            return r
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'eventos.jsonl'
            with self.assertRaisesRegex(OrquestadorError, 'RECIBO_INCONSISTENTE'):
                _procesar_ensayo_con_inyeccion(lote, fake, p)
            self.assertEqual(contador, 1)
            self.assertEqual(len(p.read_text().splitlines()), 1)

    def test_gateway_cloud_bloqueado(self):
        client = MagicMock()
        with self.assertRaisesRegex(PermissionError, 'PASO_101_7'):
            ejecutar_operativo(self.batch_piloto(), client, {'approved': True})
        client.bucket.assert_not_called()


if __name__ == '__main__':
    unittest.main()
