import copy
import hashlib
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts' / 'ingestion'))
from validar_control_bronze_local import (BUCKET, TEMPLATE, QUALITY, ContratoError, validar_local)
from transferir_hdfs_gcs import TRANSFERENCIAS_HABILITADAS, transferir_archivo

RUN = 'bvl-bronze-inicial-20261124-001'
DATASETS = [
    'bcrp/embig', 'bcrp/tipo_cambio', 'smv/cambios_patrimonio',
    'smv/cotizaciones', 'smv/estado_resultados', 'smv/flujo_efectivo',
    'smv/hechos_importancia', 'smv/hechos_valores_consulta',
    'smv/montos_sab', 'smv/principales_cuentas',
    'smv/situacion_financiera', 'smv/valores_inscritos',
]


class ControlLocalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ops = []
        for i in range(3599):
            d = DATASETS[i % 12]
            relative = f'ingestion_date=2026-11-24/archivo_{i:04d}.json'
            ops.append({
                'run_id': RUN, 'dataset': d,
                'origen': f'/datalake/bvl/bronze/{d}/{relative}',
                'destino': f'{BUCKET}{d}/run_id={RUN}/{relative}',
                'size_bytes': 362240 if i == 0 else 109300,
                'sha256': 'b' * 64,
                'estado': 'PLANIFICADO_NO_EJECUTADO',
            })
        cls.plan = dict(run_id=RUN, tipo='PLAN_LOCAL_BRONZE',
                        publicacion='NO_PUBLICADO', transferencias=0,
                        datasets=12, archivos=3599, bytes=393623640, operaciones=ops)
        defs = []
        manifestos = []
        prefijos = []
        for i, d in enumerate(DATASETS):
            prefijo = f'{BUCKET}{d}/run_id={RUN}/'
            prefijos.append(prefijo)
            sha = f'{i+1:064x}'
            defs.append({'dataset': d, 'manifest_sha256': sha,
                         'run_id_operativo': RUN, 'landing_prefix_operativo': prefijo})
            manifestos.append({'manifest_id': f'm{i}', 'source': d.split('/')[0],
                               'dataset': d, 'official_uri': f'gs://bvl26-control-7f3a9c/manifests/{i}.json',
                               'manifest_sha256': sha, 'target_prefix': prefijo})
        cls.control = {'run_id': RUN, 'status': 'CANDIDATO', 'validation_approved': False, 'datasets': defs}
        cls.reserva = {
            'contract_version': 'v0.1', 'run_id': RUN, 'layer': 'bronze', 'status': 'RESERVED',
            'owner_service_account': QUALITY,
            'target_base_prefix': BUCKET, 'target_path_template': TEMPLATE,
            'target_prefixes': prefijos, 'approved_manifests': manifestos,
        }
        cls.aprobacion = {
            'contract_version': 'v0.1', 'signal_type': 'QUALITY_APPROVED', 'status': 'QUALITY_APPROVED',
            'run_id': RUN, 'layer': 'bronze', 'approved_by_sa': QUALITY,
            'target_base_prefix': BUCKET, 'target_path_template': TEMPLATE,
            'approved_manifests': manifestos,
        }

    def ejecutar(self, plan=None, reserva=None, aprobacion=None, control=None, gen=123):
        r = copy.deepcopy(reserva if reserva is not None else self.reserva)
        a = copy.deepcopy(aprobacion if aprobacion is not None else self.aprobacion)
        contenido = json.dumps(r, separators=(',', ':'), sort_keys=True).encode()
        a['reservation_binding'] = {
            'reservation_uri': f'gs://bvl26-control-7f3a9c/reservations/bronze/{RUN}.json',
            'reservation_generation': gen,
            'reservation_sha256': hashlib.sha256(contenido).hexdigest(),
        }
        return validar_local(plan if plan is not None else self.plan,
                            control if control is not None else self.control,
                            contenido, json.dumps(a).encode(), gen)

    def test_nucleo_transferencias_bloqueado(self):
        self.assertIs(TRANSFERENCIAS_HABILITADAS, False)
        with self.assertRaises(PermissionError):
            transferir_archivo({}, None)

    def test_valida_pero_nunca_habilita_cloud(self):
        result = self.ejecutar()
        self.assertEqual(result['archivos'], 3599)
        self.assertFalse(result['transfer_allowed'])
        # Incluso un flag local malicioso no levanta el Paso 101.7.
        aprobacion = copy.deepcopy(self.aprobacion)
        aprobacion['is_operational'] = True
        self.assertFalse(self.ejecutar(aprobacion=aprobacion)['transfer_allowed'])

    def test_rechaza_binding_sha_falso(self):
        reserva_bytes = json.dumps(self.reserva, separators=(',', ':'), sort_keys=True).encode()
        aprobacion = copy.deepcopy(self.aprobacion)
        aprobacion['reservation_binding'] = {
            'reservation_uri': f'gs://bvl26-control-7f3a9c/reservations/bronze/{RUN}.json',
            'reservation_generation': 123,
            'reservation_sha256': '0' * 64,
        }
        with self.assertRaisesRegex(ContratoError, 'APROBACION_RESERVA_HASH'):
            validar_local(self.plan, self.control, reserva_bytes, json.dumps(aprobacion).encode(), 123)

    def test_rechaza_reserva_no_reserved(self):
        r = copy.deepcopy(self.reserva)
        r['status'] = 'APPROVED_FOR_INGESTION'
        with self.assertRaisesRegex(ContratoError, 'RESERVA_ESTADO'):
            self.ejecutar(reserva=r)

    def test_rechaza_aprobacion_incorrecta(self):
        a = copy.deepcopy(self.aprobacion)
        a['status'] = 'REJECTED'
        with self.assertRaisesRegex(ContratoError, 'APROBACION_ESTADO'):
            self.ejecutar(aprobacion=a)

    def test_rechaza_manifests_mutados(self):
        a = copy.deepcopy(self.aprobacion)
        a['approved_manifests'][0]['manifest_sha256'] = 'f' * 64
        with self.assertRaisesRegex(ContratoError, 'APROBACION_MANIFEST'):
            self.ejecutar(aprobacion=a)

    def test_rechaza_prefijos_extra(self):
        r = copy.deepcopy(self.reserva)
        r['target_prefixes'][0] = 'gs://otro-bucket/landing/'
        with self.assertRaisesRegex(ContratoError, 'RESERVA_PREFIJOS'):
            self.ejecutar(reserva=r)

    def test_rechaza_destino_traversal(self):
        plan = copy.deepcopy(self.plan)
        plan['operaciones'][0]['origen'] = '/datalake/bvl/bronze/bcrp/embig/../fuga'
        plan['operaciones'][0]['destino'] = f'{BUCKET}bcrp/embig/run_id={RUN}/../fuga'
        with self.assertRaisesRegex(ContratoError, 'OP_TRAVERSAL'):
            self.ejecutar(plan=plan)


if __name__ == '__main__':
    unittest.main()
