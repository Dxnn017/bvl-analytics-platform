"""Pruebas reales Spark local (ejecutar con spark-submit en Debian)."""
import hashlib
import json
import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'/'processing'))
from fase6_integracion_10_local import local_transform_from_snapshot
from fase6_restantes_core import RUN_REFERENCE,BRONZE_BUCKET


def wrap(ds, fname, data):
    return dict(uri=f'gs://{BRONZE_BUCKET}/{ds}/run_id={RUN_REFERENCE}/{fname}',
        generation='1795680140972893',size_bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),data=data)


def xml(*records):
    from xml.sax.saxutils import escape
    return ('<root>' + ''.join('<ValorObtenidoBE>'+''.join(
        f'<{k}>{escape(v)}</{k}>' for k,v in row.items())+'</ValorObtenidoBE>' for row in records) + '</root>').encode()


class TestSparkIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark=(SparkSession.builder.master('local[1]').appName('BVL-F6-Integracion-Local')
                   .config('spark.sql.ansi.enabled','true').config('spark.ui.enabled','false')
                   .config('spark.sql.shuffle.partitions','1').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
    @classmethod
    def tearDownClass(cls): cls.spark.stop()

    def test_embig_entra_por_catalogo(self):
        ds='bcrp/embig'
        csv=b',PD04709XD\n02Ene97,4.5\n,\n'
        result=local_transform_from_snapshot(self.spark,ds,[wrap(ds,'embig.csv',csv)])
        self.assertEqual(set(result.products), {'macroeconomia/embig'})
        self.assertEqual(result.evidence['transformation']['silver_rows'],1)
        self.assertEqual(result.products['macroeconomia/embig'].count(),1)
        self.assertFalse(result.evidence['publish_allowed'])

    def test_valores_dos_salidas_mismo_lote(self):
        ds='smv/valores_inscritos'
        ruc='20100000001'
        attrs={'RazonSocial':'Empresa Uno','NemonicoValor':'ABC',
               'CodigoISIN':'PE1111111111','DenominacionValor':'Valor ABC'}
        sources=[]
        for k in range(289):
            name=f'{20100000001+k:011}.xml'
            sources.append(wrap(ds,name,xml(attrs) if k==0 else xml()))
        row={'RUC':ruc,**attrs,'TipoValor':'ACC','FechaInscripcion':'01/02/2025',
             'ResolucionInscripcion':'R123','Moneda':'PEN','MontoInscrito':'10',
             'Cotizacion':'5','FechaUltCot':'01/02/2025','_empresa_origen':'Empresa Uno',
             '_fuente':'SMV','_fecha_extraccion':'2026-10-05T11:00:00'}
        sources.append(wrap(ds,'valores_inscritos_smv.json',json.dumps({'Resultado':[row]}).encode()))
        result=local_transform_from_snapshot(self.spark,ds,sources)
        self.assertEqual(len(result.products),2)
        self.assertEqual(result.products['referencias_smv/valores_inscritos'].count(),1)
        self.assertEqual(result.products['referencias_smv/puente_ruc_nemonico'].count(),1)

    def test_hechos_valores_conserva_lineage(self):
        ds='smv/hechos_valores_consulta'
        xmlrows=xml({'NemonicoValor':'ABC','RazonSocial':'Empresa Uno'})
        sources=[]
        for i in range(22):
            sources.append(wrap(ds,f'consulta_{i:02}.xml', xmlrows if i==0 else xml()))
        row={'NemonicoValor':'ABC','RazonSocial':'Empresa Uno','empresa_consultada':'Empresa Uno'}
        sources.append(wrap(ds,'consolidado.json',json.dumps([row]).encode()))
        result=local_transform_from_snapshot(self.spark,ds,sources)
        df=result.products['referencias_smv/hechos_valores_consulta']
        self.assertIn('xml_sources_json',df.columns)
        self.assertEqual(df.count(),1)
        self.assertEqual(len(json.loads(df.first()['xml_sources_json'])),1)

if __name__=='__main__':unittest.main(verbosity=2)
