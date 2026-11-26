#!/usr/bin/env python3
"""Test de Spark REAL para Debian: spark-submit --master local[1] tests/...py."""
import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'scripts/processing'))

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None

@unittest.skipIf(SparkSession is None, 'PySpark no instalado; ejecutar con spark-submit en Debian')
class TestTransformacionesSpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[1]')
                     .appName('BVL-F6-Adaptadores-Local')
                     .config('spark.ui.enabled','false')
                     .config('spark.sql.ansi.enabled','true')
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls): cls.spark.stop()

    def test_embig_fechas_null_y_duplicado(self):
        from fase6_restantes_transform import transform_embig
        df = self.spark.createDataFrame([
          ('', 'nota'), ('01Ene97','600'), ('01Ene97','600'),
          ('02Ene97','n.d.'), ('31Feb97','700')], ['_c0','PD04709XD'])
        out,m=transform_embig(df)
        self.assertEqual(m['bronze_rows'], 5)
        self.assertEqual(m['silver_rows'], 2)
        self.assertEqual(m['duplicates_removed'], 1)
        self.assertEqual(m['invalid_or_filtered_rows'], 2)
        self.assertEqual(out.filter('embig_peru is null').count(), 1)

    def test_embig_conflicto_bloquea(self):
        from fase6_restantes_transform import transform_embig
        df = self.spark.createDataFrame([('01Ene97','600'),('01Ene97','601')],
                                        ['_c0','PD04709XD'])
        with self.assertRaises(ValueError): transform_embig(df)

    def test_financiero_ansi_invalid_year(self):
        from fase6_restantes_transform import transform_financial
        df=self.spark.createDataFrame([('201X','12345678901')], ['Ejercicio','RUC'])
        with self.assertRaises(ValueError):
            transform_financial(df, 'smv/estado_resultados')

    def test_financiero_dedup_exacta(self):
        from fase6_restantes_transform import transform_financial
        df=self.spark.createDataFrame([('2025','12345678901','30/03/2025'),
                                       ('2025','12345678901','30/03/2025')],
                                      ['Ejercicio','RUC','FechaRegistro'])
        out,m=transform_financial(df,'smv/flujo_efectivo')
        self.assertEqual(m['duplicates_removed'],1)
        self.assertEqual(out.count(),1)

    def test_cotizaciones_duplicacion_conflictiva(self):
        from fase6_restantes_transform import transform_cotizaciones
        df=self.spark.createDataFrame([
           ('28/03/2025','A','X'),('28/03/2025','A','Y')],
           ['FechaCotizacion','Valor','Descripcion'])
        with self.assertRaises(ValueError):transform_cotizaciones(df)

    def test_montos_unicidad(self):
        from fase6_restantes_transform import transform_montos_sab
        df=self.spark.createDataFrame([
          ('01/01/2025','01'),('01/01/2025','02')],
          ['_FechaNegociacionConsulta','CodigoSAB'])
        out,m=transform_montos_sab(df)
        self.assertEqual(out.count(),2)
        self.assertEqual(m['business_key_unique'],2)

    def test_principales_conserva_colisiones(self):
        from fase6_restantes_transform import transform_principales
        names=['RPJ','RUC','NombreEmpresa','TipoEmpresa','TipoSector','CIIU',
               'Ejercicio','Trimestre','TipoInformacion','MetodoFlujoEfectivo',
               'Moneda','ActivoTotal','PatrimonioTotal','TotalIngreso',
               'UtilidadNeta','PasivoTotal']
        x=('A','12345678901','Empresa','X','S','C','2025','1T','REG','D',
           'PEN','100','60','20','10','40')
        y=list(x);y[11]='105'
        df=self.spark.createDataFrame([x,tuple(y)],names)
        out,m=transform_principales(df)
        self.assertEqual(out.count(),2)
        self.assertEqual(m['business_key_collisions'],1)
        self.assertEqual(m['exact_duplicates'],0)

if __name__ == '__main__': unittest.main(verbosity=2)
