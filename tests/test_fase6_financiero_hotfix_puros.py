#!/usr/bin/env python3
"""INC-F6-SMV-001: pruebas positivas y negativas, sin servicios Cloud."""
from pathlib import Path
import sys, json, unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts/processing'))
from fase6_restantes_transform import load_json_records
from fase6_restantes_core import ScopeError

def b(o): return json.dumps(o,ensure_ascii=False,separators=(',',':')).encode()
class FinancialReader(unittest.TestCase):
    def test_ok_contrato_real(self):
        for row in ({'RPJ':'1234','Ejercicio':'2024'}, {'RUC':'12345678901','Ejercicio':'2021'}):
            o={'ResponseCode':'OK','Message':'ConsultaGenerada','data':[row]}
            self.assertEqual(load_json_records([b(o)]),[row])
    def test_legacy_numero_sin_cambiar(self):
        self.assertEqual(load_json_records([b({'ResponseCode':'200','Resultado':[{'x':1}]})]),[{'x':1}])
    def test_error_rechazado(self):
        for code in ('ERROR','500','403',500,'OK ','ok'):
            with self.subTest(code=code), self.assertRaises(ScopeError):
                load_json_records([b({'ResponseCode':code,'Message':'ConsultaGenerada','data':[{'x':1}]})])
    def test_mensaje_obligatorio(self):
        for msg in (None,'Error','ConsultaGenerada ','ConsultaFallida',''):
            o={'ResponseCode':'OK','data':[{'x':1}]}
            if msg is not None:o['Message']=msg
            with self.subTest(msg=msg),self.assertRaises(ScopeError):load_json_records([b(o)])
    def test_wrapper_ambiguo(self):
        with self.assertRaises(ScopeError):
            load_json_records([b({'ResponseCode':'OK','Message':'ConsultaGenerada','data':[{'x':1}], 'Resultado':[{'x':1}]})])
    def test_campo_error_extra(self):
        for field in ('Error','CodigoError','Warning'):
            o={'ResponseCode':'OK','Message':'ConsultaGenerada','data':[{'x':1}],field:'500'}
            with self.subTest(field=field),self.assertRaises(ScopeError):load_json_records([b(o)])
    def test_wrapper_no_lista(self):
        with self.assertRaises(ScopeError):
            load_json_records([b({'ResponseCode':'OK','Message':'ConsultaGenerada','data':{'x':1}})])
    def test_campo_duplicado_en_raiz(self):
        with self.assertRaises(ScopeError):
            load_json_records([b'{"ResponseCode":"OK","ResponseCode":"OK","Message":"ConsultaGenerada","data":[{"x":1}]}'])
    def test_campo_duplicado_en_registro(self):
        with self.assertRaises(ScopeError):
            load_json_records([b'{"ResponseCode":"OK","Message":"ConsultaGenerada","data":[{"x":1,"x":2}]}'])
    def test_errores_de_estructura(self):
        for o in ({'ResponseCode':'OK','Message':'ConsultaGenerada','data':[1]},
                  {'ResponseCode':'OK','Message':'ConsultaGenerada','data':[]},
                  {'ResponseCode':'OK','Message':'ConsultaGenerada','Resultado':[{'x':1}]},
                  {'data':[],'Resultado':[{'x':1}]}):
            with self.subTest(o=o), self.assertRaises(ScopeError):load_json_records([b(o)])
    def test_json_nan_no_aceptado(self):
        with self.assertRaises(ScopeError):
            load_json_records([b'{"ResponseCode":"OK","Message":"ConsultaGenerada","data":[{"monto":NaN}]}'])
if __name__=='__main__':unittest.main()
