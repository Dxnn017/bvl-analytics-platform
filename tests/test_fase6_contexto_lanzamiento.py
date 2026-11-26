"""Pruebas F6 contexto firmado, exclusivamente offline."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import copy
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts' / 'processing'))
sys.path.insert(0, str(ROOT / 'scripts' / 'pipeline'))
import fase6_contexto_lanzamiento as module


def example():
    now = datetime.now(timezone.utc)
    return {
        'schema_version': module.SCHEMA,
        'status': 'QUALITY_APPROVED_SUPERVISED_LAUNCH',
        'quality_approved': True,
        'project_id': module.PROJECT,
        'region': module.REGION,
        'dataset': module.DATASET,
        'product': module.PRODUCT,
        'operation': 'BUILD_SILVER_STAGING',
        'run_id': 'bvl-bronze-inicial-20261125-001',
        'attempt_id': 'tipo-cambio-nuevo-12345',
        'authorization_fingerprint': 'a' * 64,
        'processor_commit': 'a' * 40,
        'entrypoint_sha256': 'b' * 64,
        'dependencies_sha256': 'c' * 64,
        'entrypoint_uri': 'gs://bvl26-dataproc-staging-7f3a9c/bvl-phase6/releases/a/fase6_tipo_cambio_batch.py',
        'entrypoint_generation': '123',
        'dependencies_uri': 'gs://bvl26-dataproc-staging-7f3a9c/bvl-phase6/releases/a/fase6_dependencias.zip',
        'dependencies_generation': '124',
        'runtime_version': '2.2',
        'service_account': module.SERVICE_ACCOUNT,
        'batch_id': 'bvl-f6-tipo-cambio-123',
        'subnet': 'projects/bvl-analytics-2026/regions/southamerica-west1/subnetworks/bvl-private',
        'driver_cores': 4, 'executor_cores': 4, 'executor_instances': 2,
        'ttl_seconds': 1800,
        'not_before_utc': (now - timedelta(minutes=1)).isoformat(),
        'expires_at_utc': (now + timedelta(minutes=5)).isoformat(),
        'overwrite_allowed': False,
        'automatic_retry_allowed': False,
        'official_publication_allowed': False,
    }


class TestContext(unittest.TestCase):
    def test_contexto_valido(self):
        self.assertTrue(module.verify_payload(example()))

    def test_no_acepta_commit_mutado(self):
        doc = example()
        doc['processor_commit'] = 'f' * 40
        plan = SimpleNamespace(fingerprint='a' * 64,
                               authorization={'run_id': doc['run_id'], 'attempt_id': doc['attempt_id'],
                                              'operation': doc['operation'], 'dataset': doc['dataset'],
                                              'service_account': doc['service_account']})
        with self.assertRaisesRegex(module.LaunchError, 'Commit'):
            module.bind_launch_to_plan(doc, plan, processor_commit='a' * 40,
                                       batch_id=doc['batch_id'])

    def test_fingerprint_intento_mismatch(self):
        doc = example()
        plan = SimpleNamespace(fingerprint='0' * 64,
                               authorization={'run_id': doc['run_id'], 'attempt_id': doc['attempt_id'],
                                              'operation': doc['operation'], 'dataset': doc['dataset'],
                                              'service_account': doc['service_account']})
        with self.assertRaisesRegex(module.LaunchError, 'Fingerprint'):
            module.bind_launch_to_plan(doc, plan, processor_commit=doc['processor_commit'],
                                       batch_id=doc['batch_id'])
        plan.fingerprint = doc['authorization_fingerprint']
        self.assertTrue(module.bind_launch_to_plan(doc, plan,
            processor_commit=doc['processor_commit'], batch_id=doc['batch_id']))
        plan.authorization['attempt_id'] = 'otro-intento'
        with self.assertRaisesRegex(module.LaunchError, 'autorizacion GCS'):
            module.bind_launch_to_plan(doc, plan, processor_commit=doc['processor_commit'],
                                       batch_id=doc['batch_id'])

    def test_expires_invalid(self):
        doc = example()
        doc['expires_at_utc'] = '2020-01-01T00:00:00+00:00'
        with self.assertRaisesRegex(module.LaunchError, 'vigente'):
            module.verify_payload(doc)

    def test_contexto_no_autorizado(self):
        for changed in ({'quality_approved': False}, {'status': 'PROPUESTA'},
                        {'official_publication_allowed': True}, {'overwrite_allowed': True},
                        {'runtime_version': '3.0'}, {'executor_instances': 3}):
            with self.subTest(changed=changed):
                doc = dict(example(), **changed)
                with self.assertRaises(module.LaunchError):
                    module.verify_payload(doc)

    def test_refs_rutas_y_hashes(self):
        doc = example()
        prefix = (f'gs://{module.CONTROL_BUCKET}/authorizations/silver_launch/'
                  f'run_id={doc["run_id"]}/attempt_id={doc["attempt_id"]}/')
        refs = {'uri': prefix + 'launch_context.json', 'generation': '123', 'sha256': 'a' * 64,
                'signature_uri': prefix + 'launch_context.sig',
                'signature_generation': '124', 'signature_sha256': 'b' * 64}
        self.assertTrue(module.validate_refs(refs, doc['run_id'], doc['attempt_id']))
        refs['uri'] = refs['uri'].replace(doc['attempt_id'], 'otro')
        with self.assertRaisesRegex(module.LaunchError, 'Rutas'):
            module.validate_refs(refs, doc['run_id'], doc['attempt_id'])

    def test_no_acepta_modulos_fuera_del_zip(self):
        with self.assertRaisesRegex(module.LaunchError, 'fuera del ZIP'):
            module.archive_path(SimpleNamespace(__file__='/tmp/foo.py', __name__='foo'))

    def test_no_acepta_campos_adicionales(self):
        doc = example()
        doc['extra'] = True
        with self.assertRaisesRegex(module.LaunchError, 'Campos'):
            module.verify_payload(doc)


if __name__ == '__main__':
    unittest.main()
