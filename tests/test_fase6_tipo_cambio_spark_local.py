"""Pruebas de transformacion ejecutables en Debian con PySpark instalado.

Si no existe pyspark se omiten (nunca se afirma que Spark paso en ese caso).
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts/processing'))
try:
    from pyspark.sql import SparkSession
    import bronze_to_silver_dataproc_candidato as core
except ImportError:
    SparkSession = None


@unittest.skipIf(SparkSession is None, "PySpark no instalado: ejecutar en Debian")
class TestSparkTipoCambio(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master('local[1]').appName('BVL-F6-Spark-Test').getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_tipos_metricas_fecha_nulos_duplicados(self):
        rows = [(None, 'descripcion'),
                ('02Ene97', '3,35'),
                ('02Ene97', '3.35'),
                ('03Ene97', 'n.d.'),
                ('xxEne97', '3.42')]
        df = self.spark.createDataFrame(rows, ['_c0','PD04638PD'])
        silver, metrics = core.transform_tipo_cambio_bcrp(df)
        self.assertEqual(metrics['bronze_rows'], 5)
        self.assertEqual(metrics['silver_rows'], 2)
        self.assertEqual(metrics['duplicates_removed'], 1)
        self.assertEqual(metrics['invalid_or_filtered_rows'], 2)
        self.assertEqual(metrics['descriptive_rows'], 1)
        self.assertEqual(metrics['invalid_date_rows'], 1)
        self.assertEqual(metrics['null_value_rows_before_dedup'], 1)
        self.assertEqual({(r['anio'],r['mes']) for r in silver.collect()}, {(1997,1)})

    def test_conflicto_fecha_bloquea(self):
        df = self.spark.createDataFrame([
            ('02Ene97','3.4'),('02Ene97','3.5')], ['_c0','PD04638PD'])
        with self.assertRaisesRegex(ValueError, 'contradictorios'):
            core.transform_tipo_cambio_bcrp(df)

    def test_numerico_malformado_bloquea(self):
        df = self.spark.createDataFrame([
            ('02Ene97','3.4xyz')], ['_c0','PD04638PD'])
        with self.assertRaisesRegex(ValueError, 'no reconocidos'):
            core.transform_tipo_cambio_bcrp(df)


    def test_ansi_invalid_dates_and_csv_blank_null_equivalence(self):
        """ANSI TRUE: 31Feb y anio malformado no causan excepcion."""
        old_ansi = self.spark.conf.get('spark.sql.ansi.enabled')
        try:
            self.spark.conf.set('spark.sql.ansi.enabled', 'true')
            cases = [
                (None, 'encabezado'),
                ('', 'desc vacia'),
                ('   ', 'desc espacios'),
                ('02Ene97', '3,41'),
                ('03Ene97', ''),
                ('04Ene97', None),
                ('05Ene97', 'n.d.'),
                ('31Feb97', '3,45'),
                ('02EneAB', '3,50'),
            ]
            df = self.spark.createDataFrame(cases, ['_c0', 'PD04638PD'])
            silver, metrics = core.transform_tipo_cambio_bcrp(df)
            self.assertEqual(metrics['bronze_rows'], 9)
            self.assertEqual(metrics['descriptive_rows'], 3)
            self.assertEqual(metrics['invalid_date_rows'], 2)
            self.assertEqual(metrics['null_value_rows_before_dedup'], 3)
            self.assertEqual(metrics['silver_rows'], 4)
            self.assertEqual(metrics['invalid_or_filtered_rows'], 5)
            self.assertEqual(sorted([r['fecha'].day for r in silver.collect()]), [2, 3, 4, 5])
        finally:
            self.spark.conf.set('spark.sql.ansi.enabled', old_ansi)

    def test_csv_raw_header_and_empty_cells(self):
        """El lector fijo trata vacios, n.d. y primera cabecera vacia."""
        import fase6_tipo_cambio_seguro as safe
        raw = (b',PD04638PD\n'
               b',descripcion\n'
               b'02Ene97,"3,44"\n'
               b'03Ene97,\n'
               b'04Ene97,""\n'
               b'05Ene97,n.d.\n')
        headers, rows = safe.parse_bcrp_csv_pinned(raw)
        self.assertEqual(headers, ['_c0', 'PD04638PD'])
        df = self.spark.createDataFrame(rows, schema=headers)
        silver, metrics = core.transform_tipo_cambio_bcrp(df)
        self.assertEqual(metrics['descriptive_rows'], 1)
        self.assertEqual(metrics['silver_rows'], 4)
        self.assertEqual(metrics['null_value_rows_before_dedup'], 3)



if __name__ == '__main__':
    unittest.main()
