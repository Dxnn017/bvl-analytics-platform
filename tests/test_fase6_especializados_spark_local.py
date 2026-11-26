"""Solo Spark local: spark-submit --master local[1] tests/test_fase6_especializados_spark_local.py"""
import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'/'processing'))
try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession=None
from fase6_especializados_valores import transform_valores_inscritos,ValoresQualityError,REQUIRED

@unittest.skipIf(SparkSession is None,'Requiere spark-submit en Debian')
class TestValoresSpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark=(SparkSession.builder.master('local[1]').appName('BVL-ValoresF6-Local')
                   .config('spark.ui.enabled','false').config('spark.sql.ansi.enabled','true')
                   .getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
    @classmethod
    def tearDownClass(cls):cls.spark.stop()
    @staticmethod
    def row(ruc='12345678901',nemo='AAA',monto='1,234',razon='Empresa'):
        return {'RUC':ruc,'RazonSocial':razon,'DenominacionValor':'Acciones',
                'NemonicoValor':nemo,'CodigoISIN':'PE123','TipoValor':'AC',
                'FechaInscripcion':'01/01/2025','ResolucionInscripcion':'RES',
                'Moneda':'PEN','MontoInscrito':monto,'Cotizacion':'3.50',
                'FechaUltCot':'02/01/2025','_empresa_origen':'A',
                '_fuente':'SMV','_fecha_extraccion':'2026-10-05 10:00:00'}
    def make(self, rows):
        return self.spark.createDataFrame([tuple(row.get(f) for f in REQUIRED) for row in rows],list(REQUIRED))
    def test_dos_salidas_puente_determinista(self):
        v,p,m=transform_valores_inscritos(self.make([self.row(),self.row()]))
        self.assertEqual(v.count(),2)
        self.assertEqual(p.count(),1)
        self.assertTrue(m['all_outputs_required_for_quality'])
    def test_nemonico_dos_ruc_bloquea(self):
        with self.assertRaises(ValoresQualityError):
            transform_valores_inscritos(self.make([self.row(),self.row(ruc='98765432109')]))
    def test_par_atributos_discrepantes_bloquea(self):
        with self.assertRaises(ValoresQualityError):
            transform_valores_inscritos(self.make([self.row(),self.row(razon='Otra empresa')]))
    def test_numero_ansi_invalido_bloquea(self):
        with self.assertRaises(ValoresQualityError):
            transform_valores_inscritos(self.make([self.row(monto='mal-numero')]))
    def test_fecha_ansi_invalida_bloquea(self):
        row=self.row();row['FechaInscripcion']='31/02/2025'
        with self.assertRaises(ValoresQualityError):
            transform_valores_inscritos(self.make([row]))

if __name__=='__main__':unittest.main(verbosity=2)
