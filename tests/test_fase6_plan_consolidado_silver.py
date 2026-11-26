"""Pruebas offline para plan de 13 productos pendientes."""
import importlib.util
import sys
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'processing' / 'fase6_plan_consolidado_silver.py'
spec = importlib.util.spec_from_file_location('bvl_plan_consolidado', SCRIPT)
plan = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = plan
spec.loader.exec_module(plan)


class TestPlan(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        for path in {p[3] for p in plan.DIRECT} | {plan.DERIVED_CODE}:
            f = self.root / path
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text('# fuente historica sin autorizar\n', encoding='utf-8')

    def tearDown(self):
        self.tmp.cleanup()

    def test_11_procesos_12_salidas_y_derivado(self):
        self.assertTrue(plan.validate_mapping())
        data = plan.compose_plan(self.root)
        self.assertEqual(len(data['steps']), 12)
        self.assertEqual(sum(len(p['products']) for p in data['steps']), 13)

    def test_sin_reintentos_sin_publishing_ni_auto_execution(self):
        data = plan.compose_plan(self.root)
        self.assertFalse(data['execution_enabled'])
        self.assertFalse(data['writes_gcs'])
        self.assertTrue(all(not p['ready_to_execute'] for p in data['steps']))
        self.assertTrue(all(p['no_auto_retry'] and p['no_publish_from_staging'] for p in data['steps']))

    def test_dos_salidas_valores_atómicas(self):
        data = plan.compose_plan(self.root)
        item = next(p for p in data['steps'] if p['bronze_dataset'] == 'smv/valores_inscritos')
        self.assertEqual(item['mode'], 'SINGLE_SOURCE')
        self.assertEqual(len(item['products']), 2)

    def test_derived_is_last_and_five_dependencies(self):
        data = plan.compose_plan(self.root)
        item = data['steps'][-1]
        self.assertEqual(item['mode'], 'MULTI_SOURCE')
        self.assertEqual(set(item['silver_dependencies']), set(plan.DEPENDS))
        self.assertTrue(all(d in {p for x in data['steps'][:-1] for p in x['products']}
                            for d in item['silver_dependencies']))

    def test_quota_one_batch(self):
        resources = plan.compose_plan(self.root)['resource_policy']
        self.assertEqual(resources['driver_vcpus'] + resources['executors'] * resources['executor_vcpus_each'], 12)
        self.assertEqual(resources['max_concurrent_dataproc_batches'], 1)

    def test_missing_source_fails_closed(self):
        (self.root / plan.DERIVED_CODE).unlink()
        with self.assertRaises(plan.PlanError):
            plan.compose_plan(self.root)

    def test_file_hash_is_from_actual_bytes(self):
        data = plan.compose_plan(self.root)
        entry = data['steps'][0]['source_code']
        self.assertEqual(entry['sha256'], plan.sha256((self.root / entry['path']).read_bytes()).hexdigest())

    def test_fails_when_unknown_mapping(self):
        self.assertEqual(sum(x[2] for x in plan.DIRECT), 3598)
        self.assertEqual(plan.DERIVED, 'hechos_importancia/identidad_empresarial')


if __name__ == '__main__':
    unittest.main()
