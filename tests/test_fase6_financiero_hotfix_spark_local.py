#!/usr/bin/env python3
"""INC-F6-SMV-002: casos Spark ANSI con datos sintéticos, sin GCS."""
from pathlib import Path
import sys, unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts/processing'))
from fase6_restantes_transform import transform_financial, spark_data_frame
from fase6_restantes_core import ScopeError
from pyspark.sql import SparkSession

class FinancialSpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark=(SparkSession.builder.master('local[1]').appName('BVL-F6-financial-hotfix-test')
            .config('spark.sql.ansi.enabled','true')
            .config('spark.ui.enabled','false').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
    @classmethod
    def tearDownClass(cls):cls.spark.stop()
    def test_cambios_no_ruc_preserva_rpj_y_conteos(self):
        row={'Cuenta':'100','DescripcionColumna':'Capital','DescripcionCuenta':'Cuenta de capital',
             'Ejercicio':'2024','FechaRegistro':'31/03/2024','Monto1':'1000',
             'OrdenColumna':'1','RPJ':'  00RPJ001  ','TipoInformacion':'I','Trimestre':'1er Trimestre'}
        df=spark_data_frame(self.spark,[row,row])
        result,m=transform_financial(df,'smv/cambios_patrimonio')
        self.assertEqual(result.count(),1)
        self.assertNotIn('ruc',result.columns)
        self.assertEqual(result.first()['rpj'],'00RPJ001')
        self.assertEqual((m['bronze_rows'],m['silver_rows'],m['duplicates_removed']), (2,1,1))
        self.assertIn('descripcion_columna',result.columns)
        self.assertEqual(result.first()['ejercicio'],2024)
    def test_cambios_rpj_vacio_rechazado(self):
        for rpj in ('', '   '):
            df=self.spark.createDataFrame([{'RPJ':rpj,'Ejercicio':'2024','Cuenta':'1'}])
            with self.subTest(rpj=rpj),self.assertRaises(ScopeError):
                transform_financial(df,'smv/cambios_patrimonio')
    def test_cambios_sin_rpj_rechazado(self):
        df=self.spark.createDataFrame([{'Ejercicio':'2024','Cuenta':'1'}])
        with self.assertRaises(ScopeError):transform_financial(df,'smv/cambios_patrimonio')
    def test_otras_tres_requieren_ruc(self):
        for ds in ('smv/estado_resultados','smv/flujo_efectivo','smv/situacion_financiera'):
            df=self.spark.createDataFrame([{'RPJ':'1','Ejercicio':'2024','Cuenta':'1'}])
            with self.subTest(ds=ds),self.assertRaises(ScopeError):transform_financial(df,ds)
    def test_otras_tres_ruc_valido(self):
        for ds in ('smv/estado_resultados','smv/flujo_efectivo','smv/situacion_financiera'):
            df=self.spark.createDataFrame([{'RUC':' 20123456789 ','RPJ':'x','Ejercicio':'2024','Cuenta':'1'}])
            output,m=transform_financial(df,ds)
            with self.subTest(ds=ds):
                self.assertEqual(output.first()['ruc'],'20123456789')
                self.assertEqual(m['silver_rows'],1)
if __name__=='__main__':unittest.main()
