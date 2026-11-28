#!/usr/bin/env python3
"""Pruebas del hotfix 003-005, sin GCS/Dataproc/claims."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'/'processing'))
import hashlib, json, unittest
from fase6_restantes_core import ScopeError
from fase6_montos_sab_quarantine import (partition_montos_sab, OFFICIAL_EXPECTED, ERR_LOG,
                                          PREFIX)
from fase6_especializados_hechos_valores import parse_xml, reconcile, HechosValoresQualityError
from fase6_valores_conciliacion import reconcile_valores

def xml_error(code, message, rows=''):
    return ("<Envelope><Body><consultaValoresRUCResponse>"
            f"<Resultado><CodigoError>{code}</CodigoError>"
            f"<MensajeError>{message}</MensajeError>{rows}</Resultado>"
            "</consultaValoresRUCResponse></Body></Envelope>").encode()

def xml_record(ticker='AAA',name='Empresa S.A.'):
    return ('<Envelope><Body><Resultado><ValorObtenidoBE><NemonicoValor>'
            +ticker+'</NemonicoValor><RazonSocial>'+name+
            '</RazonSocial></ValorObtenidoBE></Resultado></Body></Envelope>').encode()

def mkpin(raw, name='montos_sab_20210104.json', generation='1795679268694882'):
    return {'uri':PREFIX+'ingestion_date=2026-10-04/'+name,
            'generation':generation,'sha256':hashlib.sha256(raw).hexdigest(),
            'size_bytes':len(raw)}

def mkobj(code='0',message='Consulta generada',n=2):
    return json.dumps({'ResponseCode':code,'Message':message,
                       'Resultado':[{'CodigoSAB':'X','_FechaNegociacionConsulta':'2021-01-04'}]*n},
                      separators=(',',':')).encode()

class Xml110(unittest.TestCase):
    def test_110_exact_no_records(self):
        self.assertEqual(parse_xml(xml_error('110','Empresa no tiene valores inscritos en SMV')),
                         ([],['110']))
    def test_106_107_preserved(self):
        for code in ('106','107'):
            with self.subTest(code=code):
                self.assertEqual(parse_xml(xml_error(code,'Otra respuesta sin registros')),( [],[code]))
    def test_unknown_code_rejected(self):
        with self.assertRaises(HechosValoresQualityError):
            parse_xml(xml_error('500','Error'))
    def test_110_wrong_message_rejected(self):
        with self.assertRaises(HechosValoresQualityError):
            parse_xml(xml_error('110','Respuesta exitosa'))
    def test_110_missing_message_rejected(self):
        with self.assertRaises(HechosValoresQualityError):
            parse_xml(b'<Envelope><CodigoError>110</CodigoError></Envelope>')
    def test_110_mixed_records_rejected(self):
        with self.assertRaises(HechosValoresQualityError):
            parse_xml(xml_error('110','Empresa no tiene valores inscritos en SMV',
                         '<ValorObtenidoBE><NemonicoValor>X</NemonicoValor></ValorObtenidoBE>'))
    def test_110_other_code_rejected(self):
        raw=xml_error('110','Empresa no tiene valores inscritos en SMV').replace(
            b'</Resultado>',b'<CodigoError>107</CodigoError></Resultado>')
        with self.assertRaises(HechosValoresQualityError): parse_xml(raw)
    def test_valid_record(self):
        self.assertEqual(len(parse_xml(xml_record())[0]),1)
    def test_duplicate_xml_field_rejected(self):
        raw=xml_record().replace(b'</ValorObtenidoBE>',
            b'<NemonicoValor>DUP</NemonicoValor></ValorObtenidoBE>')
        with self.assertRaises(HechosValoresQualityError): parse_xml(raw)
    def test_dtd_forbidden(self):
        with self.assertRaises(HechosValoresQualityError):
            parse_xml(b'<!DOCTYPE foo><Envelope></Envelope>')
    def test_hechos_reconcile_110_logged(self):
        xs=[('gs://test/%02d.xml'%i,xml_error('106','Empresa no registrada')) for i in range(20)]
        xs.append(('gs://test/20.xml',xml_error('110','Empresa no tiene valores inscritos en SMV')))
        xs.append(('gs://test/21.xml',xml_record()))
        js=[{'NemonicoValor':'AAA','RazonSocial':'Empresa S.A.',
             'empresa_consultada':'Empresa S.A.'}]
        out,metrics=reconcile(xs,json.dumps(js).encode())
        self.assertEqual(len(out),1)
        self.assertEqual(len(metrics['xml_sin_valores_110']),1)
        self.assertEqual(len(metrics['xml_error_106_107']),20)
    def test_valores_reconcile_110_logged(self):
        x=[('gs://test/20100000001.xml',xml_error('110','Empresa no tiene valores inscritos en SMV')),
           ('gs://test/20100000002.xml',xml_error('107','Empresa NO Inscrita en SMV')),
           ('gs://test/20100000003.xml',xml_record())]
        js={'Resultado':[{'RUC':'20100000003','NemonicoValor':'AAA','RazonSocial':'Empresa S.A.'}]}
        out,m=reconcile_valores(x,json.dumps(js).encode(),expected_xml=3)
        self.assertEqual(len(out),1)
        self.assertEqual(len(m['xml_sin_valores_110']),1)
        self.assertEqual(len(m['xml_errors_106_107']),1)

class MontosQuarantine(unittest.TestCase):
    def test_normal_and_quarantine(self):
        a=mkobj(n=3);b=mkobj('-2147024864',ERR_LOG,n=2)
        normals,quarantined,m=partition_montos_sab([
            (mkpin(a),a), (mkpin(b,'montos_sab_20231122.json'),b)],
            expected={'bronze_rows':5,'rows_normal':3,'rows_quarantined':2,'files_quarantined':1})
        self.assertEqual(len(normals),3)
        self.assertEqual(quarantined[0]['registro_cuarentena'][0]['CodigoSAB'],'X')
        self.assertFalse(m['coverage_full'])
        self.assertEqual(m['rows_silently_discarded'],0)
    def test_error_cannot_be_treated_as_normal(self):
        b=mkobj('-2147024864',ERR_LOG,n=2)
        _,q,m=partition_montos_sab([(mkpin(b),b)])
        self.assertEqual((len(q),m['rows_normal'],m['rows_quarantined']), (1,0,2))
    def test_unrecognized_error_rejected(self):
        b=mkobj('-9999','Unknown')
        with self.assertRaises(ScopeError): partition_montos_sab([(mkpin(b),b)])
    def test_error_code_mismatched_message_rejected(self):
        b=mkobj('-2147024864','Consulta generada')
        with self.assertRaises(ScopeError):partition_montos_sab([(mkpin(b),b)])
    def test_fake_success_message_rejected(self):
        b=mkobj('0','Error')
        with self.assertRaises(ScopeError):partition_montos_sab([(mkpin(b),b)])
    def test_sha_mismatch_rejected(self):
        b=mkobj();pin=mkpin(b);pin['sha256']='0'*64
        with self.assertRaises(ScopeError):partition_montos_sab([(pin,b)])
    def test_generation_missing_rejected(self):
        b=mkobj();pin=mkpin(b);del pin['generation']
        with self.assertRaises(ScopeError):partition_montos_sab([(pin,b)])
    def test_ambiguous_wrappers_rejected(self):
        b=b'{"ResponseCode":"0","Message":"Consulta generada","Resultado":[],"data":[]}'
        with self.assertRaises(ScopeError):partition_montos_sab([(mkpin(b),b)])
    def test_duplicate_keys_rejected(self):
        b=b'{"ResponseCode":"0","ResponseCode":"0","Message":"Consulta generada","Resultado":[{}]}'
        with self.assertRaises(ScopeError):partition_montos_sab([(mkpin(b),b)])
    def test_nan_rejected(self):
        b=b'{"ResponseCode":"0","Message":"Consulta generada","Resultado":[{"x":NaN}]}'
        with self.assertRaises(ScopeError):partition_montos_sab([(mkpin(b),b)])
    def test_partial_counts_cannot_claim_official_complete(self):
        b=mkobj()
        with self.assertRaises(ScopeError):
            partition_montos_sab([(mkpin(b),b)],expected=OFFICIAL_EXPECTED)
    def test_repeat_uri_rejected(self):
        b=mkobj()
        with self.assertRaises(ScopeError):
            partition_montos_sab([(mkpin(b),b),(mkpin(b),b)])

if __name__=='__main__':unittest.main()
