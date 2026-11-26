"""Validacion local del recorrido lector CP1252 -> transform_embig Spark ANSI.

Ejecutar con spark-submit --master local[1], nunca Dataproc productivo.
"""
import pathlib
import sys
import unittest
from pyspark.sql import SparkSession

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts' / 'processing'))
from fase6_restantes_preflight import build_source_dataframe
from fase6_restantes_transform import transform_embig

class SparkEMBIG(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[1]')
                     .appName('BVL-EMBIG-CP1252-TEST')
                     .config('spark.ui.enabled', 'false')
                     .config('spark.sql.ansi.enabled', 'true')
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    def test_cp1252_bronze_to_silver_local(self):
        raw = b'"","PD04709XD"\n"","Tasas de inter\xe9s: EMBIG Per\xfa"\n01Ene98,421\n02Ene98,411\n'
        df = build_source_dataframe(self.spark, 'bcrp/embig', [raw])
        output, metrics = transform_embig(df)
        rows = sorted(output.collect(), key=lambda r: r['fecha'])
        self.assertEqual(len(rows), 2)
        self.assertEqual(metrics['bronze_rows'], 3)
        self.assertEqual(metrics['silver_rows'], 2)
        self.assertEqual(metrics['descriptive_rows'], 1)
        self.assertEqual([r['embig_peru'] for r in rows], [421.0, 411.0])
        self.assertEqual(rows[0]['anio'], 1998)
        self.assertEqual(rows[0]['mes'], 1)

if __name__ == '__main__':
    unittest.main()
