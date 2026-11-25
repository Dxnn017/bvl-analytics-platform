"""Pruebas de preflight y lectura GCS simulada; JAMAS usan red."""
import hashlib
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts' / 'ingestion'))
from ejecutor_bronze_v01 import (CONTROL_BUCKET, IntegracionError,
                                ejecutar_piloto, leer_control_lectura_unica,
                                verificar_cadena_control_solo_lectura,
                                validar_referencia)
from validar_control_bronze_local import TEMPLATE

RUN = 'bvl-bronze-inicial-20261124-001'


def datos_objeto(tipo='reservations'):
    contenido = json.dumps({'run_id': RUN, 'status': 'RESERVED' if tipo == 'reservations' else 'QUALITY_APPROVED'}).encode()
    ruta = f'{tipo}/bronze/{RUN}.json'
    ref = {'uri': f'gs://{CONTROL_BUCKET}/{ruta}', 'generation': 123,
           'sha256': hashlib.sha256(contenido).hexdigest()}
    blob = MagicMock()
    blob.generation = 123
    blob.download_as_bytes.return_value = contenido
    client = MagicMock()
    client.bucket.return_value.blob.return_value = blob
    return client, blob, ref


class EjecutorBronzeTests(unittest.TestCase):
    def test_template_compatible_carpetas_intermedias(self):
        self.assertTrue(TEMPLATE.endswith('/{manifest_relative_path}'))

    def test_reserva_lectura_protegida(self):
        client, blob, ref = datos_objeto()
        content, obj = leer_control_lectura_unica(client, RUN, 'reservations', ref)
        self.assertEqual(obj['run_id'], RUN)
        self.assertEqual(len(content), len(blob.download_as_bytes.return_value))
        client.bucket.assert_called_once_with(CONTROL_BUCKET)
        blob.reload.assert_called_once_with(if_generation_match=123)
        blob.download_as_bytes.assert_called_once_with(if_generation_match=123)

    def test_aprobacion_lectura_protegida(self):
        client, blob, ref = datos_objeto('approvals')
        _, obj = leer_control_lectura_unica(client, RUN, 'approvals', ref)
        self.assertEqual(obj['status'], 'QUALITY_APPROVED')

    def test_rechaza_uri_no_canonica(self):
        _, _, ref = datos_objeto()
        ref['uri'] = 'gs://mal/reservations/bronze/' + RUN + '.json'
        with self.assertRaisesRegex(IntegracionError, 'CONTROL_URI_NO_CANONICA'):
            validar_referencia(ref, RUN, 'reservations')

    def test_rechaza_sha_falso(self):
        client, _, ref = datos_objeto()
        ref['sha256'] = '0' * 64
        with self.assertRaisesRegex(IntegracionError, 'CONTROL_SHA_NO_COINCIDE'):
            leer_control_lectura_unica(client, RUN, 'reservations', ref)

    def test_rechaza_generacion_cambiada(self):
        client, blob, ref = datos_objeto()
        blob.generation = 124
        with self.assertRaisesRegex(IntegracionError, 'CONTROL_GENERACION_NO_COINCIDE'):
            leer_control_lectura_unica(client, RUN, 'reservations', ref)

    def test_rechaza_run_ajeno(self):
        client, blob, ref = datos_objeto()
        blob.download_as_bytes.return_value = b'{"run_id":"otro"}'
        ref['sha256'] = hashlib.sha256(blob.download_as_bytes.return_value).hexdigest()
        with self.assertRaisesRegex(IntegracionError, 'CONTROL_RUN_NO_COINCIDE'):
            leer_control_lectura_unica(client, RUN, 'reservations', ref)

    def test_rechaza_sin_referencia_confiable(self):
        client, _, _ = datos_objeto()
        with self.assertRaisesRegex(IntegracionError, 'REFERENCIA_TIPO'):
            leer_control_lectura_unica(client, RUN, 'reservations', None)
        client.bucket.assert_not_called()

    def test_cadena_completa_reserva_y_aprobacion_en_memoria(self):
        # Plan 3599 / manifests laboratorio, mismos contratos que usa el repo.
        from test_control_bronze_local import ControlLocalTests
        ControlLocalTests.setUpClass()
        fixture = ControlLocalTests
        reserva_bytes = json.dumps(fixture.reserva, separators=(',', ':'),
                                   sort_keys=True).encode()
        approval = json.loads(json.dumps(fixture.aprobacion))
        approval['reservation_binding'] = {
            'reservation_uri': f'gs://{CONTROL_BUCKET}/reservations/bronze/{RUN}.json',
            'reservation_generation': 123,
            'reservation_sha256': hashlib.sha256(reserva_bytes).hexdigest(),
        }
        approval_bytes = json.dumps(approval).encode()
        refs = {
            'reservation': {
                'uri': f'gs://{CONTROL_BUCKET}/reservations/bronze/{RUN}.json',
                'generation': 123,
                'sha256': hashlib.sha256(reserva_bytes).hexdigest(),
            },
            'approval': {
                'uri': f'gs://{CONTROL_BUCKET}/approvals/bronze/{RUN}.json',
                'generation': 456,
                'sha256': hashlib.sha256(approval_bytes).hexdigest(),
            },
        }
        objetos = {}
        for path, content, gen in (
            (f'reservations/bronze/{RUN}.json', reserva_bytes, 123),
            (f'approvals/bronze/{RUN}.json', approval_bytes, 456),
        ):
            blob = MagicMock()
            blob.generation = gen
            blob.download_as_bytes.return_value = content
            objetos[path] = blob
        client = MagicMock()
        client.bucket.return_value.blob.side_effect = lambda name: objetos[name]
        result = verificar_cadena_control_solo_lectura(
            client, fixture.plan, fixture.control, refs)
        self.assertEqual(result['archivos_plan'], 3599)
        self.assertFalse(result['transfer_allowed'])
        self.assertFalse(result['emisor_autenticado'])
        self.assertFalse(result['release_autenticado'])
        self.assertEqual(client.bucket.return_value.blob.call_count, 2)

    def test_transferencia_fisicamente_bloqueada(self):
        cliente = MagicMock()
        with self.assertRaisesRegex(PermissionError, 'PASO_101_7'):
            ejecutar_piloto(cliente, {'operational_gate_authorized': True})
        cliente.bucket.assert_not_called()


if __name__ == '__main__':
    unittest.main()
