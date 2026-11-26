"""Pruebas focalizadas del fallo reproducible EMBIG CP1252; sin Cloud."""
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts' / 'processing'))
from fase6_restantes_transform import load_bcrp_csv
from fase6_restantes_core import ScopeError

class EncodingEMBIG(unittest.TestCase):
    def test_serie_cp1252_con_acentos(self):
        raw = b'"","PD04709XD"\n"","Tasas de inter\xe9s: EMBIG Per\xfa"\n01Ene98,421\n02Ene98,411\n'
        rows = load_bcrp_csv(raw, 'PD04709XD')
        self.assertEqual(len(rows), 3)
        self.assertIn('interés', rows[0]['PD04709XD'])
        self.assertIn('Perú', rows[0]['PD04709XD'])
        self.assertEqual(rows[1], {'_c0': '01Ene98', 'PD04709XD': '421'})
    def test_ascii_es_valido_en_cp1252(self):
        rows = load_bcrp_csv(b',PD04709XD\n01Ene98,421\n', 'PD04709XD')
        self.assertEqual(rows[0]['PD04709XD'], '421')
    def test_cp1252_tiene_tabla_explicita_no_latin1(self):
        rows = load_bcrp_csv(b',PD04709XD\n,Precio 10\x80\n', 'PD04709XD')
        self.assertEqual(rows[0]['PD04709XD'], 'Precio 10€')
    def test_byte_indefinido_falla_sin_fallback(self):
        with self.assertRaises(ScopeError):
            load_bcrp_csv(b',PD04709XD\n,Error\x81\n', 'PD04709XD')
    def test_otra_serie_sin_codificacion_ratificada_falla(self):
        with self.assertRaises(ScopeError):
            load_bcrp_csv(b',OTRA\n01Ene98,10\n', 'OTRA')
    def test_fila_tercera_columna_falla(self):
        with self.assertRaises(ScopeError):
            load_bcrp_csv(b',PD04709XD\n01Ene98,421,EXTRA\n', 'PD04709XD')
    def test_header_incorrecto_falla(self):
        with self.assertRaises(ScopeError):
            load_bcrp_csv(b',PD123\n01Ene98,421\n', 'PD04709XD')
    def test_vacio_falla(self):
        with self.assertRaises(ScopeError):
            load_bcrp_csv(b'', 'PD04709XD')
    def test_preflight_conserva_generaciones_y_hashes(self):
        src = (ROOT / 'scripts/processing/fase6_restantes_preflight.py').read_text()
        self.assertIn('read_exact_current_bytes(', src)
        self.assertIn("item['generation']", src)
        self.assertIn("item['sha256']", src)
    def test_no_cambia_lector_de_json(self):
        src = (ROOT / 'scripts/processing/fase6_restantes_transform.py').read_text()
        self.assertIn("raw.decode('utf-8-sig')", src)
        self.assertIn("text = raw.decode('cp1252', errors='strict')", src)

if __name__ == '__main__':
    unittest.main()
