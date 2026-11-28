import copy
from hashlib import sha256
from pathlib import Path
import sys
import unittest
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts' / 'processing'))
import fase6_hechos_launch_scope_candidato as scope


def fixture():
    arr = []
    for i in range(990):
        guid = str(UUID(int=i + 1)).upper()
        name = f'20250109_{guid}.pdf'
        digest = sha256(('PDF0' if i == 989 else f'PDF{i}').encode()).hexdigest()
        arr.append({'uri': scope.PREFIX + f'pdf/ingestion_date=2026-10-04/{name}',
                    'source_hdfs': f'/datalake/bvl/bronze/smv/hechos_importancia/pdf/ingestion_date=2026-10-04/{name}',
                    'generation': '1795680000000000', 'size_bytes': 1, 'sha256': digest})
        if i != 989:
            txt = name[:-4] + '.txt'
            arr.append({'uri': scope.PREFIX + f'texto_extraido/ingestion_date=2026-10-04/{txt}',
                        'source_hdfs': f'/datalake/bvl/bronze/smv/hechos_importancia/texto_extraido/ingestion_date=2026-10-04/{txt}',
                        'generation': '1795680000000000', 'size_bytes': 1,
                        'sha256': sha256(f'TXT{i}'.encode()).hexdigest()})
    return arr


class CandidateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.items = fixture()

    def test_valid_synthetic_inventory_no_auth(self):
        result = scope.validate_inventory(self.items)
        self.assertEqual((result['pdf_count'], result['txt_count']), (990, 989))
        self.assertFalse(result['claim_allowed'])
        self.assertFalse(result['publish_allowed'])
        self.assertFalse(result['bytes_read_from_gcs'])

    def test_incomplete(self):
        with self.assertRaisesRegex(ValueError, '1979'):
            scope.validate_inventory(self.items[:-1])

    def test_wrong_bucket(self):
        a = copy.deepcopy(self.items); a[0]['uri'] = a[0]['uri'].replace(scope.BUCKET, 'fake-bucket')
        with self.assertRaisesRegex(ValueError, 'BRONZE_FUERA'):
            scope.validate_inventory(a)

    def test_tampered_sha(self):
        a = copy.deepcopy(self.items); a[0]['sha256'] = 'nothash'
        with self.assertRaisesRegex(ValueError, 'SHA_INVALIDO'):
            scope.validate_inventory(a)

    def test_missing_generation(self):
        a = copy.deepcopy(self.items); a[0]['generation'] = None
        with self.assertRaisesRegex(ValueError, 'GENERACION_INVALIDA'):
            scope.validate_inventory(a)

    def test_duplicate_source(self):
        a = copy.deepcopy(self.items); a[1]['source_hdfs'] = a[0]['source_hdfs']
        with self.assertRaisesRegex(ValueError, 'OBJETO_REPETIDO'):
            scope.validate_inventory(a)

    def test_bad_pdf_sha_breaks_text_lineage(self):
        a = copy.deepcopy(self.items); a[0]['sha256'] = a[2]['sha256']
        with self.assertRaisesRegex(ValueError, 'TEXTOS_NO_CONCILIADOS'):
            scope.validate_inventory(a)

    def test_cannot_unlock(self):
        with self.assertRaises(PermissionError):
            scope.require_quality_signed_contract({'quality_approved': True})

    def test_no_historical_product(self):
        self.assertNotEqual(scope.PRODUCT_PROPOSED, scope.OLD_PRODUCT_BLOCKED)


if __name__ == '__main__':
    unittest.main()
