"""Pruebas de integración offline, cliente Control falso y cero GCS."""
import copy
import hashlib
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts' / 'ingestion'))
import test_control_bronze_local as fixture
from preparar_piloto_bronze_local import preparar
from puerta_quality_bronze_v01 import (
    PuertaError, preflight_unificado, verificar_reserva_y_aprobacion_solo_lectura,
    transferir_operativamente,
)


class ObjetoFalso:
    def __init__(self, datos, generacion):
        self.contenido = datos
        self.generation = generacion
        self.leidas = 0

    def reload(self, if_generation_match=None):
        if if_generation_match != self.generation:
            raise ValueError('generation mismatch')
        self.leidas += 1

    def download_as_bytes(self, if_generation_match=None):
        if if_generation_match != self.generation:
            raise ValueError('generation mismatch')
        return self.contenido


class BucketFalso:
    def __init__(self, objetos):
        self.objetos = objetos
        self.leidos = []

    def blob(self, ruta):
        self.leidos.append(ruta)
        return self.objetos[ruta]


class ClienteFalso:
    def __init__(self, objetos):
        self.b = BucketFalso(objetos)
        self.buckets = []

    def bucket(self, nombre):
        assert nombre == 'bvl26-control-7f3a9c'
        self.buckets.append(nombre)
        return self.b


class PuertaQualityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture.ControlLocalTests.setUpClass()
        cls.plan = fixture.ControlLocalTests.plan
        cls.control = fixture.ControlLocalTests.control
        cls.base_reserva = fixture.ControlLocalTests.reserva
        cls.base_aprobacion = fixture.ControlLocalTests.aprobacion
        cls.bytes_plan = json.dumps(cls.plan).encode()
        cls.run_id = cls.plan['run_id']
        cls.piloto = preparar(cls.plan, hashlib.sha256(cls.bytes_plan).hexdigest())
        cls.evidencia = {
            'tipo':'EVIDENCIA_LOCAL_HDFS', 'run_id': cls.run_id,
            'archivos_esperados':12, 'archivos_verificados':12,
            'archivos_con_error':0, 'bytes_esperados':cls.piloto['bytes_esperados'],
            'quality_approved':False, 'transfer_allowed':False,
            'transferencias_gcs':0,
            'archivos':[dict(x, estado='VERIFICADO_HDFS', error=None,
                             bytes_observados=x['bytes_esperados'],
                             sha256_hdfs_observado=x['sha256_esperado'])
                        for x in cls.piloto['archivos_piloto']],
        }

    def ambiente(self, alterar_reserva=None, alterar_aprobacion=None):
        reserva = copy.deepcopy(self.base_reserva)
        if alterar_reserva:
            alterar_reserva(reserva)
        rb = json.dumps(reserva, separators=(',', ':'), sort_keys=True).encode()
        aprobacion = copy.deepcopy(self.base_aprobacion)
        aprobacion['reservation_binding'] = {
            'reservation_uri':f'gs://bvl26-control-7f3a9c/reservations/bronze/{self.run_id}.json',
            'reservation_generation':77,
            'reservation_sha256':hashlib.sha256(rb).hexdigest(),
        }
        if alterar_aprobacion:
            alterar_aprobacion(aprobacion)
        ab = json.dumps(aprobacion).encode()
        obj = {
            f'reservations/bronze/{self.run_id}.json': ObjetoFalso(rb, 77),
            f'approvals/bronze/{self.run_id}.json': ObjetoFalso(ab, 88),
        }
        refs = {
            'reservation':{
                'uri':f'gs://bvl26-control-7f3a9c/reservations/bronze/{self.run_id}.json',
                'generation':77, 'sha256':hashlib.sha256(rb).hexdigest(),
            },
            'approval':{
                'uri':f'gs://bvl26-control-7f3a9c/approvals/bronze/{self.run_id}.json',
                'generation':88, 'sha256':hashlib.sha256(ab).hexdigest(),
            },
        }
        return ClienteFalso(obj), refs

    def test_lote_completo(self):
        result = preflight_unificado(self.bytes_plan, 'completo')
        self.assertEqual(result['archivos'], 3599)
        self.assertFalse(result['transfer_allowed'])

    def test_piloto_con_evidencia(self):
        result = preflight_unificado(self.bytes_plan, 'piloto', self.piloto, self.evidencia)
        self.assertEqual(result['archivos'], 12)
        self.assertFalse(result['transfer_allowed'])

    def test_lectura_control_no_habilita_transferencias(self):
        client, refs = self.ambiente()
        lote = preflight_unificado(self.bytes_plan, 'completo')
        res = verificar_reserva_y_aprobacion_solo_lectura(
            lote, self.plan, self.control, refs, client)
        self.assertEqual(res['manifests_contrastados_estructuralmente'], 12)
        self.assertEqual(res['archivos_plan'], 3599)
        self.assertFalse(res['transfer_allowed'])
        self.assertFalse(res['release_101_7_autenticado'])
        self.assertEqual(len(client.b.leidos), 2)
        self.assertEqual(client.buckets, ['bvl26-control-7f3a9c'] * 2)

    def test_cero_transferencias_aun_con_aprobacion(self):
        lote = preflight_unificado(self.bytes_plan, 'piloto', self.piloto, self.evidencia)
        with self.assertRaisesRegex(PermissionError, 'PASO_101_7'):
            transferir_operativamente(lote, {'quality_approved':True})

    def test_hash_aprobacion_invalido(self):
        client, refs = self.ambiente()
        refs['approval']['sha256'] = '0' * 64
        lote = preflight_unificado(self.bytes_plan, 'completo')
        with self.assertRaisesRegex(Exception, 'CONTROL_SHA_NO_COINCIDE'):
            verificar_reserva_y_aprobacion_solo_lectura(
                lote, self.plan, self.control, refs, client)

    def test_generacion_reserva_invalida(self):
        client, refs = self.ambiente()
        refs['reservation']['generation'] = 99
        lote = preflight_unificado(self.bytes_plan, 'completo')
        with self.assertRaises(Exception):
            verificar_reserva_y_aprobacion_solo_lectura(
                lote, self.plan, self.control, refs, client)

    def test_reserva_sin_estado_valido(self):
        client, refs = self.ambiente(alterar_reserva=lambda r:r.update(status='OTHER'))
        lote = preflight_unificado(self.bytes_plan, 'completo')
        with self.assertRaisesRegex(Exception, 'RESERVA_NO_RESERVED'):
            verificar_reserva_y_aprobacion_solo_lectura(
                lote, self.plan, self.control, refs, client)

    def test_referencias_no_confiables_no_deben_habilitar(self):
        lote = preflight_unificado(self.bytes_plan, 'completo')
        client, refs = self.ambiente()
        refs['approval']['uri'] = 'gs://bucket-malicioso/approvals/bronze/x.json'
        with self.assertRaisesRegex(Exception, 'CONTROL_URI_NO_CANONICA'):
            verificar_reserva_y_aprobacion_solo_lectura(
                lote, self.plan, self.control, refs, client)


if __name__ == '__main__':
    unittest.main()
