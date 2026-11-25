"""Pruebas unitarias OFFLINE: sin permisos ni llamadas GCS/HDFS reales."""
import copy
import hashlib
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts' / 'ingestion'))
import transferir_hdfs_gcs as motor
from orquestar_transferencias_bronze_v01 import (
    OrquestadorError, transferir_mediante_nucleo, validar_recibo,
)

RUN = 'bvl-bronze-inicial-20261124-001'
DATOS = b'%PDF-1.7\narchivo realista de prueba\n%%EOF\n'


def operacion(datos=DATOS):
    relativo = 'pdf/ingestion_date=2026-10-04/test.pdf'
    return dict(run_id=RUN, dataset='smv/hechos_importancia',
                origen=f'/datalake/bvl/bronze/smv/hechos_importancia/{relativo}',
                destino=f'gs://{motor.BUCKET}/landing/smv/hechos_importancia/run_id={RUN}/{relativo}',
                size_bytes=len(datos), sha256=hashlib.sha256(datos).hexdigest(),
                estado='PLANIFICADO_NO_EJECUTADO')


def contexto():
    d = dict(run_id=RUN, plan_sha256='a'*64, release_id='QUALITY-20261125-0001', alcance='piloto')
    for key, pref in (('reservation', 'reservations'), ('approval', 'approvals')):
        d[key] = dict(uri=f'gs://{motor.CONTROL_BUCKET}/{pref}/bronze/{RUN}.json',
                      generation=123 if key == 'reservation' else 124,
                      sha256='b'*64 if key == 'reservation' else 'c'*64)
    return d


class PreconditionFailed(Exception):
    pass


class NotFound(Exception):
    pass


class FakeBlob:
    def __init__(self, bucket, objeto, generation=None):
        self.bucket = bucket
        self.objeto = objeto
        self.generation = generation
        self.metadata = None

    def upload_from_filename(self, filename, *, if_generation_match, **kwargs):
        assert if_generation_match == 0
        if self.objeto in self.bucket.objetos:
            raise PreconditionFailed('412')
        self.bucket.objetos[self.objeto] = dict(
            bytes=Path(filename).read_bytes(), metadata=dict(self.metadata),
            generation=7001)
        self.generation = 7001

    def reload(self, **kwargs):
        record = self.bucket.objetos.get(self.objeto)
        if record is None:
            raise NotFound('404')
        self.generation = record['generation']
        self.metadata = dict(record['metadata'])

    def download_to_filename(self, ruta, *, if_generation_match, **kwargs):
        record = self.bucket.objetos[self.objeto]
        assert if_generation_match == record['generation']
        assert self.generation == record['generation']
        Path(ruta).write_bytes(record['bytes'])


class FakeBucket:
    def __init__(self):
        self.objetos = {}

    def blob(self, objeto, generation=None):
        return FakeBlob(self, objeto, generation)


class FakeClient:
    def __init__(self):
        self.b = FakeBucket()
        self.consultas = 0

    def bucket(self, nombre):
        assert nombre == motor.BUCKET
        self.consultas += 1
        return self.b


def fake_hdfs(datos):
    def run(comando, **kwargs):
        assert comando[:3] == ['hdfs', 'dfs', '-get']
        Path(comando[4]).write_bytes(datos)
        return types.SimpleNamespace(returncode=0, stderr='')
    return run


class MotorBronzeCierreTests(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()
        self.op = operacion()
        self.ctx = contexto()
        api = types.ModuleType('google.api_core')
        exc = types.ModuleType('google.api_core.exceptions')
        exc.PreconditionFailed = PreconditionFailed
        exc.NotFound = NotFound
        self.modules = patch.dict(sys.modules, {
            'google': types.ModuleType('google'),
            'google.api_core': api,
            'google.api_core.exceptions': exc,
        })
        self.modules.start()
        self.addCleanup(self.modules.stop)

    def test_bloqueo_publico_no_usa_red_incluso_con_contexto(self):
        with self.assertRaisesRegex(PermissionError, 'PASO_101_7'):
            motor.transferir_archivo(self.op, self.client, self.ctx)
        self.assertEqual(self.client.consultas, 0)

    def test_flip_booleano_no_levanta_release(self):
        with patch.object(motor, 'TRANSFERENCIAS_HABILITADAS', True):
            with self.assertRaisesRegex(PermissionError, 'RELEASE_QUALITY_AUTENTICADO_NO_IMPLEMENTADO'):
                motor.transferir_archivo(self.op, self.client, dict(approved=True))
        self.assertEqual(self.client.consultas, 0)

    def test_conexion_coordinador_tambien_bloqueada(self):
        with self.assertRaisesRegex(PermissionError, 'PASO_101_7'):
            transferir_mediante_nucleo(self.op, self.client, self.ctx)
        self.assertEqual(self.client.consultas, 0)

    def test_controles_con_ref_fuera_control_rechazan_sin_hdfs(self):
        ctx = contexto()
        ctx['approval']['uri'] = 'gs://bucket-externo/approvals/bronze/x.json'
        with patch.object(motor, 'TRANSFERENCIAS_HABILITADAS', True), \
             patch.object(motor.subprocess, 'run') as run:
            with self.assertRaisesRegex(ValueError, 'CONTEXTO_URI_approval'):
                motor._transferir_motor_interno(self.op, self.client, ctx)
            run.assert_not_called()
        self.assertEqual(self.client.consultas, 0)

    def test_controles_generacion_boolean_rechazan(self):
        ctx = contexto()
        ctx['reservation']['generation'] = True
        with patch.object(motor, 'TRANSFERENCIAS_HABILITADAS', True):
            with self.assertRaisesRegex(ValueError, 'CONTEXTO_GENERACION_reservation'):
                motor._transferir_motor_interno(self.op, self.client, ctx)

    def test_motor_simula_transferencia_y_metadata(self):
        with patch.object(motor, 'TRANSFERENCIAS_HABILITADAS', True), \
             patch.object(motor.subprocess, 'run', side_effect=fake_hdfs(DATOS)):
            recibo = motor._transferir_motor_interno(self.op, self.client, self.ctx)
        self.assertEqual(recibo['estado'], 'TRANSFERIDO')
        self.assertEqual(recibo['sha256_gcs'], self.op['sha256'])
        self.assertEqual(recibo['generacion_gcs'], 7001)
        self.assertEqual(recibo['reserva']['generation'], 123)
        self.assertEqual(recibo['quality_release_id'], self.ctx['release_id'])
        self.assertFalse(recibo['autorizacion_operativa_validada_por_motor'])
        almacenado = next(iter(self.client.b.objetos.values()))
        self.assertEqual(almacenado['metadata']['bvl_approval_generation'], '124')
        self.assertEqual(almacenado['metadata']['bvl_plan_sha256'], 'a'*64)
        self.assertEqual(validar_recibo(self.op, recibo, self.ctx)['estado'], 'TRANSFERIDO')

    def test_motor_simula_colision_412_idempotente(self):
        with patch.object(motor, 'TRANSFERENCIAS_HABILITADAS', True), \
             patch.object(motor.subprocess, 'run', side_effect=fake_hdfs(DATOS)):
            a = motor._transferir_motor_interno(self.op, self.client, self.ctx)
            b = motor._transferir_motor_interno(self.op, self.client, self.ctx)
        self.assertEqual(a['estado'], 'TRANSFERIDO')
        self.assertEqual(b['estado'], 'REINTENTO_EXISTENTE')
        self.assertEqual(len(self.client.b.objetos), 1)

    def test_motor_rechaza_metadata_412_distinta(self):
        with patch.object(motor, 'TRANSFERENCIAS_HABILITADAS', True), \
             patch.object(motor.subprocess, 'run', side_effect=fake_hdfs(DATOS)):
            motor._transferir_motor_interno(self.op, self.client, self.ctx)
            otro = contexto()
            otro['release_id'] = 'QUALITY-20261125-9999'
            with self.assertRaisesRegex(RuntimeError, 'CONFLICTO_IDENTIDAD_GCS'):
                motor._transferir_motor_interno(self.op, self.client, otro)

    def test_motor_rechaza_contenido_remoto_distinto_tras_412(self):
        with patch.object(motor, 'TRANSFERENCIAS_HABILITADAS', True), \
             patch.object(motor.subprocess, 'run', side_effect=fake_hdfs(DATOS)):
            motor._transferir_motor_interno(self.op, self.client, self.ctx)
            record = next(iter(self.client.b.objetos.values()))
            record['bytes'] = b'OTRO CONTENIDO'
            with self.assertRaisesRegex(RuntimeError, 'INTEGRIDAD_GCS_NO_COINCIDE'):
                motor._transferir_motor_interno(self.op, self.client, self.ctx)

    def test_motor_rechaza_hash_hdfs_distinto_antes_de_subir(self):
        with patch.object(motor, 'TRANSFERENCIAS_HABILITADAS', True), \
             patch.object(motor.subprocess, 'run', side_effect=fake_hdfs(b'basura')):
            with self.assertRaisesRegex(RuntimeError, 'INTEGRIDAD_HDFS_NO_COINCIDE'):
                motor._transferir_motor_interno(self.op, self.client, self.ctx)
        self.assertEqual(self.client.consultas, 0)

    def test_recibo_sin_tipos_control_rechazado(self):
        with patch.object(motor, 'TRANSFERENCIAS_HABILITADAS', True), \
             patch.object(motor.subprocess, 'run', side_effect=fake_hdfs(DATOS)):
            r = motor._transferir_motor_interno(self.op, self.client, self.ctx)
        r['aprobacion']['sha256'] = '0'*64
        with self.assertRaisesRegex(OrquestadorError, 'RECIBO_TRAZABILIDAD:aprobacion'):
            validar_recibo(self.op, r, self.ctx)


if __name__ == '__main__':
    unittest.main()
