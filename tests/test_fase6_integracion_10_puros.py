import hashlib
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts' / 'processing'))
from fase6_integracion_10_local import (
    roadmap, checked_snapshot, READY_LOCAL, BLOCKED_DATASET,
    local_transform_from_snapshot, ScopeError,
)
from fase6_valores_conciliacion import reconcile_valores
from fase6_restantes_core import RUN_REFERENCE, BRONZE_BUCKET, SOURCES
from fase6_especializados_hechos_metadata import MetadataApprovalRequired


def obj(ds, filename, raw, gen='1795680140972893'):
    return dict(uri=f'gs://{BRONZE_BUCKET}/{ds}/run_id={RUN_REFERENCE}/{filename}',
                generation=gen, sha256=hashlib.sha256(raw).hexdigest(),
                size_bytes=len(raw), data=raw)


def xml(*rows, code=None):
    from xml.sax.saxutils import escape
    values = ''.join('<ValorObtenidoBE>' + ''.join(
        '<%s>%s</%s>' % (k, escape(v), k) for k, v in row.items()
    ) + '</ValorObtenidoBE>' for row in rows)
    err = '<CodigoError>%s</CodigoError>' % code if code else ''
    return ('<root>' + values + err + '</root>').encode()


RUC = '20100000001'
RUC2 = '20100000002'
XML_VALUES = dict(RazonSocial='Empresa Uno', NemonicoValor='ABC',
    CodigoISIN='PE1111111111', DenominacionValor='Valor ABC')


def fixture_valores():
    xmls = [(f'gs://bvl26-bronze-7f3a9c/smv/valores_inscritos/run_id={RUN_REFERENCE}/{i:011}.xml',
             xml(XML_VALUES) if i == int(RUC) else xml()) for i in range(20100000001,20100000001+289)]
    return xmls, json.dumps({'Resultado': [
        dict(RUC=RUC, **XML_VALUES, TipoValor='ACC', FechaInscripcion='01/02/2025',
             ResolucionInscripcion='R123', Moneda='PEN', MontoInscrito='10',
             Cotizacion='5', FechaUltCot='01/02/2025',
             _empresa_origen='Empresa Uno', _fuente='SMV',
             _fecha_extraccion='2026-10-05T11:00:00')
    ]}).encode()


class TestRoadmap(unittest.TestCase):
    def test_numero_procesos(self):
        p=roadmap()
        self.assertEqual(p['processing_count'],11)
        self.assertEqual(p['direct_output_count'],12)
        self.assertEqual(p['remaining_output_count'],13)
        self.assertEqual(p['local_candidate_processes'],10)
        self.assertEqual(p['blocked_processes'],1)
        self.assertFalse(p['execution_enabled'])
        self.assertFalse(p['quality_approved'])
    def test_valores_dos_salidas_un_solo_intento(self):
        p=roadmap()
        v=next(b for b in p['batches'] if b['dataset']=='smv/valores_inscritos')
        self.assertEqual(len(v['products']),2)
        self.assertEqual(v['mode'],'SINGLE_SOURCE')
    def test_derived_ultimo_cinco(self):
        p=roadmap()
        self.assertEqual(p['batches'][-1]['mode'],'MULTI_SOURCE')
        self.assertEqual(len(p['batches'][-1]['dependencies']),5)
        self.assertFalse(p['batches'][-1]['cloud_execution_enabled'])
    def test_hechos_bloqueado(self):
        self.assertNotIn(BLOCKED_DATASET,READY_LOCAL)
        with self.assertRaises(MetadataApprovalRequired):
            local_transform_from_snapshot(None,BLOCKED_DATASET,[])


class TestManifest(unittest.TestCase):
    def test_bytes_validos_snapshot_local(self):
        record=obj('bcrp/embig','embig.csv',b',PD04709XD\n02Ene97,5.0\n')
        rows, audit=checked_snapshot('bcrp/embig',[record])
        self.assertEqual(len(rows),1)
        self.assertEqual(audit['object_count'],1)
        self.assertIn('NO_ES_FIRMA', audit['status'])
    def test_tampered_payload_rechazado(self):
        record=obj('bcrp/embig','embig.csv',b',PD04709XD\n02Ene97,5.0\n')
        record['data'] += b'alterado'
        with self.assertRaises(ScopeError): checked_snapshot('bcrp/embig',[record])
    def test_rechaza_landing(self):
        record=obj('bcrp/embig','embig.csv',b'A')
        record['uri'] = record['uri'].replace('/bcrp/embig/', '/landing/bcrp/embig/')
        with self.assertRaises(ScopeError): checked_snapshot('bcrp/embig',[record])
    def test_rechaza_generacion(self):
        record=obj('bcrp/embig','embig.csv',b'A',gen='0')
        with self.assertRaises(ScopeError): checked_snapshot('bcrp/embig',[record])
    def test_rechaza_fuente_incompleta(self):
        record=obj('smv/cotizaciones','cot.json',b'{}')
        with self.assertRaises(ScopeError): checked_snapshot('smv/cotizaciones',[record])
    def test_no_acepta_campos_extra(self):
        record=obj('bcrp/embig','embig.csv',b'A')
        record['trusted'] = True
        with self.assertRaises(ScopeError): checked_snapshot('bcrp/embig',[record])


class TestValores(unittest.TestCase):
    def test_289_xml_1_json_conciliados(self):
        xmls, js = fixture_valores()
        rows, metrics=reconcile_valores(xmls,js)
        self.assertEqual(len(rows),1)
        self.assertEqual(metrics['xml_records'],1)
        self.assertTrue(metrics['xml_json_exact_multiset'])
    def test_mismatch_prohibe(self):
        xmls,js=fixture_valores()
        objj=json.loads(js)
        objj['Resultado'][0]['CodigoISIN']='PE0000000000'
        with self.assertRaises(ScopeError): reconcile_valores(xmls,json.dumps(objj).encode())
    def test_duplicados_no_silenciosos(self):
        xmls,js=fixture_valores()
        xmls[0]=(xmls[0][0],xml(XML_VALUES, XML_VALUES))
        with self.assertRaises(ScopeError): reconcile_valores(xmls,js)
    def test_xml_106_registrado(self):
        xmls,js=fixture_valores()
        xmls[-1]=(xmls[-1][0],xml(code=106))
        rows,met=reconcile_valores(xmls,js)
        self.assertEqual(len(rows),1)
        self.assertEqual(met['xml_errors_106_107'][0]['code'],'106')
    def test_xml_ruc_incoherente(self):
        xmls,js=fixture_valores()
        xmls[0]=(xmls[0][0],xml(dict(XML_VALUES,RUC=RUC2)))
        with self.assertRaises(ScopeError): reconcile_valores(xmls,js)


class TestSecurity(unittest.TestCase):
    def test_sin_primitivas_cloud(self):
        text=(ROOT/'scripts/processing/fase6_integracion_10_local.py').read_text()
        self.assertNotIn('consume_once(',text)
        self.assertNotIn('upload_from_',text)
        self.assertNotIn('google.cloud',text)
        self.assertNotIn('SparkSession.builder',text)
    def test_ninguna_salida_fuera_mapeo(self):
        from fase6_integracion_10_local import _make_candidate
        with self.assertRaises(ScopeError):
            _make_candidate('bcrp/embig',{'otro/producto': object()},{},{})

if __name__=='__main__': unittest.main(verbosity=2)
