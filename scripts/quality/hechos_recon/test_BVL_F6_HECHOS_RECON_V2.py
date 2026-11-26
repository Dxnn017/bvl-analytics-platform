import unittest
from BVL_F6_HECHOS_RECON_V2 import reconcile_unlinked_events,reconcile_counts,EVENT_FIELDS,csv_bytes

class V2Tests(unittest.TestCase):
    def setup_case(self):
        linked=[{'numero_expediente':'e1','cantidad_documentos_pdf':'1','empresa':'X'},
                {'numero_expediente':'e2','cantidad_documentos_pdf':'1','empresa':'Y'}]
        reference=[{'numero_expediente':'e1','empresa':'X'},
                   {'numero_expediente':'e2','empresa':'Y'},
                   {'numero_expediente':'e3','empresa':'MAL_COPIAR'}]
        return linked,reference
    def test_aisla_referencia_no_bronze(self):
        a,b=self.setup_case()
        result,orphan=reconcile_unlinked_events(a,b,expected_missing=1)
        self.assertEqual(len(result),3)
        self.assertEqual(len(orphan),1)
        self.assertEqual(orphan[0]['numero_expediente'],'e3')
        self.assertEqual(orphan[0]['empresa'],'')
        self.assertIn('CUARENTENA',orphan[0]['estado_trazabilidad'])
        self.assertEqual(orphan[0]['cantidad_documentos_pdf'],'0')
    def test_rechaza_contar_mal(self):
        a,b=self.setup_case()
        with self.assertRaisesRegex(ValueError,'CANTIDAD_SIN_PDF'):
            reconcile_unlinked_events(a,b,expected_missing=2)
    def test_rechaza_hist_duplicado(self):
        a,b=self.setup_case()
        with self.assertRaisesRegex(ValueError,'REFERENCIA_HISTORICA_CLAVES'):
            reconcile_unlinked_events(a,b+b[-1:],expected_missing=1)
    def test_rechaza_expediente_inventado(self):
        a,b=self.setup_case();a[0]['numero_expediente']='fake'
        with self.assertRaisesRegex(ValueError,'AJENOS_A_REFERENCIA'):
            reconcile_unlinked_events(a,b,expected_missing=2)
    def test_no_muta_original(self):
        a,b=self.setup_case();original=dict(a[0])
        reconcile_unlinked_events(a,b,expected_missing=1)
        self.assertEqual(a[0],original)
    def test_csv_solo_columnas_esperadas(self):
        a,b=self.setup_case()
        result,orphan=reconcile_unlinked_events(a,b,expected_missing=1)
        text=csv_bytes(orphan,EVENT_FIELDS).decode()
        self.assertNotIn('MAL_COPIAR',text)
        self.assertIn('CUARENTENA',text)
        self.assertEqual(text,csv_bytes(orphan,EVENT_FIELDS).decode())
    def test_rechaza_referencia_vacia(self):
        a,b=self.setup_case();b[-1]['numero_expediente']=''
        with self.assertRaisesRegex(ValueError,'CLAVES'):
            reconcile_unlinked_events(a,b,expected_missing=1)
    def test_orden_determinista(self):
        a,b=self.setup_case()
        one,_=reconcile_unlinked_events(a,b,expected_missing=1)
        two,_=reconcile_unlinked_events(a[::-1],b[::-1],expected_missing=1)
        self.assertEqual(one,two)

if __name__=='__main__':unittest.main()
