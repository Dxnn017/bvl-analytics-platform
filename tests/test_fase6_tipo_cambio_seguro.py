"""Pruebas puras y dobles GCS. No usa claves, Spark ni la red."""
import io
import sys
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts' / 'processing'))
import fase6_tipo_cambio_seguro as p


class FakeBlob:
    def __init__(self):
        self.generation = None
        self.uploads = []

    def upload_from_file(self, stream, **kw):
        self.uploads.append(kw)
        self.data = stream.read()
        self.generation = 9811


class FakeBucket:
    def __init__(self):
        self.created = {}

    def blob(self, name):
        item = FakeBlob()
        self.created[name] = item
        return item


class FakeClient:
    def __init__(self):
        self.buckets = {}

    def bucket(self, name):
        return self.buckets.setdefault(name, FakeBucket())


class TestPilotoPuro(unittest.TestCase):
    def test_csv_fiel_con_comillas_nuevas_lineas(self):
        raw = (b'"_c0","PD04638PD","descripcion"\r\n'
               b'"01Ene98","3,47","texto\nmultilinea"\r\n')
        header, rows = p.parse_bcrp_csv_pinned(raw)
        self.assertEqual(header, ['_c0', 'PD04638PD', 'descripcion'])
        self.assertEqual(rows[0][2], 'texto\nmultilinea')

    def test_csv_bcrp_primera_cabecera_vacia_como_spark(self):
        raw = b',PD04638PD\r\n02Ene97,"3,35"\r\n'
        header, rows = p.parse_bcrp_csv_pinned(raw)
        self.assertEqual(header, ['_c0', 'PD04638PD'])
        self.assertEqual(rows, [['02Ene97', '3,35']])

    def test_rechaza_csv_malformado(self):
        for raw in [b'_c0,PD04638PD\n03Ene97\n',
                    b'_c0,PD04638PD\n"sin cerrar,3.7\n',
                    b'_c0,PD04638PD\n',
                    b'_c0,PD04638PD,PD04638PD\nA,B,C']:
            with self.subTest(raw=raw), self.assertRaises(p.PilotoError):
                p.parse_bcrp_csv_pinned(raw)

    def test_rechaza_csv_excesivo(self):
        with self.assertRaises(p.PilotoError):
            p.parse_bcrp_csv_pinned(b'x' * (p.MAX_CSV_BYTES + 1))

    def test_prefijo_exactamente_autorizado(self):
        auth = {'mode': 'SINGLE_SOURCE', 'dataset': p.BRONZE_DATASET,
                'operation': 'BUILD_SILVER_STAGING', 'run_id': 'run01',
                'attempt_id': 'attempt01', 'silver_outputs': [{'name': p.SILVER_PRODUCT,
                'staging_prefix': f'gs://{p.SILVER_BUCKET}/staging/{p.SILVER_PRODUCT}/run_id=run01/attempt_id=attempt01/'}]}
        self.assertEqual(p.stage_prefix_from_authorization(auth), auth['silver_outputs'][0]['staging_prefix'])
        auth['silver_outputs'][0]['staging_prefix'] += 'otro/'
        with self.assertRaises(p.PilotoError):
            p.stage_prefix_from_authorization(auth)

    def test_particionado_determinista(self):
        rows = [{'fecha': date(2026, 11, 26), 'anio': 2026, 'mes': 11, 'tipo_cambio_venta': 3.55},
                {'fecha': date(2026, 10, 1), 'anio': 2026, 'mes': 10, 'tipo_cambio_venta': None}]
        groups = p.partition_records(rows)
        self.assertEqual(list(groups), [(2026, 10), (2026, 11)])
        self.assertEqual(groups[(2026, 10)][0]['tipo_cambio_venta'], None)

    def test_rechazo_particion_y_valores_no_finitos(self):
        base = {'fecha': date(2026, 11, 26), 'anio': 2026, 'mes': 11, 'tipo_cambio_venta': 3.55}
        for changes in ({'mes': 12}, {'tipo_cambio_venta': float('nan')},
                        {'tipo_cambio_venta': float('inf')}, {'anio': 2027}):
            with self.subTest(changes=changes), self.assertRaises(p.PilotoError):
                p.partition_records([dict(base, **changes)])

    def test_upload_exclusivo_create_only(self):
        client = FakeClient()
        uri = f'gs://{p.SILVER_BUCKET}/staging/macroeconomia/tipo_cambio/run_id=r/attempt_id=a/part.parquet'
        item = p.upload_create_only(client, uri, b'PARQUET', 'application/octet-stream')
        self.assertEqual(item['generation'], '9811')
        blob = client.bucket(p.SILVER_BUCKET).created[uri[len(f'gs://{p.SILVER_BUCKET}/'):]]
        self.assertEqual(blob.uploads[0]['if_generation_match'], 0)
        self.assertIsNone(blob.uploads[0]['retry'])
        self.assertEqual(blob.uploads[0]['checksum'], 'crc32c')
        self.assertEqual(blob.data, b'PARQUET')

    def test_rechaza_publicacion_fuera_staging(self):
        for uri in (f'gs://{p.SILVER_BUCKET}/versions/a.parquet',
                    'gs://bvl26-gold-7f3a9c/staging/a.parquet'):
            with self.subTest(uri=uri), self.assertRaises(p.PilotoError):
                p.upload_create_only(FakeClient(), uri, b'data', 'application/octet-stream')

    def test_candidate_no_publica(self):
        files = [{'uri': 'gs://bvl26-silver-7f3a9c/staging/foo', 'size_bytes': 5, 'generation':'1', 'sha256':'a'*64, 'rows':1}]
        raw = p.encode_candidate_manifest('r','a','f'*64, {'uri':'bronze'},files,
                                          {'bronze_rows':1,'silver_rows':1},'4'*40)
        import json
        doc = json.loads(raw)
        self.assertFalse(doc['quality_approved'])
        self.assertFalse(doc['official_publication_allowed'])
        self.assertEqual(doc['files_count'], 1)

    def test_direct_batch_bloqueado_por_defecto(self):
        import subprocess
        tool = Path(__file__).resolve().parents[1] / 'scripts/processing/fase6_tipo_cambio_batch.py'
        r = subprocess.run([sys.executable, str(tool)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn('BLOQUEADO_POR_DEFECTO', r.stdout)
        self.assertIn('SIN_GCS_SIN_SPARK', r.stdout)

    def test_puerta_originales_no_modificadas(self):
        import hashlib
        source = Path(__file__).resolve().parents[1]
        hashes = {
            'scripts/pipeline/adaptador_gcs_silver_v1.py':
                'a87268952e29b308b113a660a94c806f20fe882730306d3472a93ca66635c4f0',
            'scripts/pipeline/puerta_silver_operativa_v1_candidata.py':
                'd01b48d234ff74668aa22cdf96d1d288d9f0e7e25f2dc375a95befb0a7e71af3',
            'scripts/pipeline/puerta_silver_v1_candidata.py':
                'c9bfca406a293906b67db6203468ef64cc7eb49d6985d6b17b66458840c5a281',
        }
        for name, expected in hashes.items():
            self.assertEqual(hashlib.sha256((source / name).read_bytes()).hexdigest(), expected)



if __name__ == '__main__':
    unittest.main()
