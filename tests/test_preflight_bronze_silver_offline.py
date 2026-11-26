"""Pruebas offline de contrato sin acceso a Spark, HDFS ni GCS."""
import copy
import json
import os
import sys
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'/'pipeline'))
import preflight_bronze_silver_offline as mod

class TestPreflight(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract=json.loads(Path(os.environ.get('BVL_BRONZE_SILVER_DRAFT', str(Path.home() / 'Descargas' / 'bvl_silver_preflight_draft.json'))).read_text(encoding='utf-8'))
    def bad(self, mutate):
        x=copy.deepcopy(self.contract)
        mutate(x)
        with self.assertRaises(ValueError): mod.check(x)
    def test_draft_ok_and_blocked(self):
        r=mod.check(copy.deepcopy(self.contract))
        self.assertEqual((r['datasets'],r['archivos'],r['bytes']),(12,3599,393623640))
        self.assertFalse(r['autoriza_silver'])
        self.assertFalse(r['opera_gcs'])
    def test_fake_approval_rejected(self): self.bad(lambda x:x.update(quality_approved=True))
    def test_fake_publication_rejected(self): self.bad(lambda x:x.update(published=True))
    def test_fake_silver_auth_rejected(self): self.bad(lambda x:x.update(silver_consumption_allowed=True))
    def test_wrong_run_id_rejected(self): self.bad(lambda x:x.update(run_id='otro-lote'))
    def test_wrong_dataset_count_rejected(self): self.bad(lambda x:x['datasets'][0].update(expected_files=2))
    def test_wrong_bytes_rejected(self): self.bad(lambda x:x['datasets'][2].update(expected_bytes=0))
    def test_duplicate_dataset_rejected(self): self.bad(lambda x:x['datasets'][1].update(dataset=x['datasets'][0]['dataset']))
    def test_landing_consumption_rejected(self): self.bad(lambda x:x.update(consume_landing_directly=True))
    def test_wrong_official_prefix_rejected(self): self.bad(lambda x:x['datasets'][0].update(proposed_official_prefix='gs://wrong/'))
    def test_manifest_reference_fake_rejected(self): self.bad(lambda x:x['datasets'][0].update(bronze_manifest_uri='gs://fake/manifest.json'))
    def test_fake_generation_rejected(self): self.bad(lambda x:x['datasets'][0].update(bronze_manifest_generation='123'))
    def test_fake_input_rejected(self): self.bad(lambda x:x['datasets'][0].update(bronze_input_uris=['gs://fake']))
    def test_version_rejected(self): self.bad(lambda x:x.update(schema_version='BVL-BRONZE-SILVER-CONTRACT-v1'))
    def test_input_fields_rejected(self): self.bad(lambda x:x.update(required_file_fields=['uri','sha256']))
    def test_trigger_candidate_rejected(self): self.bad(lambda x:x.update(trigger_on_candidate_manifest=True))
    def test_overwrite_rejected(self): self.bad(lambda x:x.update(overwrite_original_data=True))
    def test_missing_dataset_rejected(self): self.bad(lambda x:x['datasets'].pop())
    def test_null_account_rejected(self): self.bad(lambda x:x.update(consumer_service_account=None))
    def test_wrong_manifest_path_rejected(self): self.bad(lambda x:x['datasets'][0].update(proposed_manifest_uri='gs://fake'))

if __name__=='__main__': unittest.main()
