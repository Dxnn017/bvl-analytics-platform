import json
import sys
import unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'/'processing'))
from fase6_especializados_hechos_valores import (parse_json,parse_xml,reconcile,HechosValoresQualityError)
from fase6_especializados_hechos_metadata import require_official_metadata,MetadataApprovalRequired,execute_hechos_events


def xml(*records,code=None):
    from xml.sax.saxutils import escape
    vals=''.join('<ValorObtenidoBE><NemonicoValor>'+escape(n)+'</NemonicoValor><RazonSocial>'+escape(r)+'</RazonSocial></ValorObtenidoBE>' for n,r in records)
    err=('<CodigoError>'+str(code)+'</CodigoError>') if code else ''
    return ('<root>'+vals+err+'</root>').encode()


def fixture(n=1):
    files=[(f'gs://bvl26-bronze-7f3a9c/a/{i:02}.xml',xml(('ABC','Empresa X')) if i==0 else xml()) for i in range(22)]
    js=[{'empresa_consultada':'Empresa X','NemonicoValor':'ABC','RazonSocial':'Empresa X'}]
    return files,json.dumps(js).encode()

class TestHechosValores(unittest.TestCase):
    def test_xml_json_concordancia(self):
        files,raw=fixture();rows,meta=reconcile(files,raw)
        self.assertEqual(meta['xml_count'],22)
        self.assertEqual(meta['json_row_count'],1)
        self.assertEqual(rows[0]['nemonico_smv'],'ABC')
        self.assertEqual(len(rows[0]['_xml_sources']),1)
    def test_xml_106_se_reporta(self):
        files,raw=fixture();files[-1]=(files[-1][0],xml(code=106))
        rows,meta=reconcile(files,raw)
        self.assertTrue(meta['requires_quality_review_of_errors'])
        self.assertEqual(meta['xml_error_106_107'][0]['codigo_smv'],'106')
    def test_rechaza_fila_json_no_xml(self):
        files,raw=fixture()
        with self.assertRaises(HechosValoresQualityError):
            reconcile(files,json.dumps([{'empresa_consultada':'X','NemonicoValor':'OTRO','RazonSocial':'X'}]).encode())
    def test_rechaza_xml_dtd(self):
        with self.assertRaises(HechosValoresQualityError):parse_xml(b'<!DOCTYPE foo><root/>')
    def test_rechaza_error_desconocido(self):
        with self.assertRaises(HechosValoresQualityError):parse_xml(xml(code=500))
    def test_rechaza_codigos_con_registros(self):
        with self.assertRaises(HechosValoresQualityError):parse_xml(xml(('ABC','X'),code=106))
    def test_rechaza_json_clave_duplicada(self):
        with self.assertRaises(HechosValoresQualityError):parse_json(b'[ {"NemonicoValor":"A", "NemonicoValor":"B"} ]')
    def test_rechaza_xml_extra(self):
        files,raw=fixture();with_extra=files+[(files[0][0]+'x',xml())]
        with self.assertRaises(HechosValoresQualityError):reconcile(with_extra,raw)
    def test_no_dedup_silenciosa(self):
        files,raw=fixture();files[0]=(files[0][0],xml(('ABC','Empresa X'),('ABC','Empresa X')))
        one=json.loads(raw);one.append(one[0]);rows,m=reconcile(files,json.dumps(one).encode())
        self.assertEqual(len(rows),2)
        self.assertEqual(m['unique_nemonico_razon'],1)

class TestMetadata(unittest.TestCase):
    def test_bloquea_sin_fuente(self):
        with self.assertRaises(MetadataApprovalRequired):require_official_metadata({})
    def test_no_ejecuta_sin_contrato(self):
        with self.assertRaises(MetadataApprovalRequired):execute_hechos_events()

if __name__=='__main__':unittest.main(verbosity=2)
