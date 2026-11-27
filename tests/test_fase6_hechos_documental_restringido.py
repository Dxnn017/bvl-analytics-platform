import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'/'processing'))
import fase6_hechos_documental_restringido as m

A='20250123_000A9594-0000-C51F-8A99-6083F98F32F5'
B='20250123_000B9594-0000-C91A-A3CE-5E38FB4082D3'
C='20250109_00305E8F-0000-C315-B8EF-87F926BA0608'

def entry(stem, ext, digest=None, date='2026-10-04', n=100, gen='123'):
    typ='pdf' if ext=='pdf' else 'texto_extraido'
    name=f'{stem}.{ext}'
    origin=f'{m.PREFIX_HDFS}{typ}/ingestion_date={date}/{name}'
    uri=f'{m.PREFIX_GCS}{typ}/ingestion_date={date}/{name}'
    return {'uri':uri,'source_hdfs':origin,'sha256':digest or 'a'*64,'size_bytes':n,'generation':gen}

class Tests(unittest.TestCase):
    def test_nombre_guia(self):
        self.assertEqual(m.parse_name(A+'.pdf')[0],'2025-01-23')
        self.assertEqual(m.parse_name(A+'.pdf')[1],A.split('_')[1])

    def test_nombre_invalido(self):
        for s in ('../'+A+'.pdf','20250230_'+A.split('_')[1]+'.pdf','x.txt','123.pdf'):
            with self.subTest(s=s),self.assertRaises(ValueError): m.parse_name(s)

    def test_official_index_dos_pdf_igual_sha(self):
        files=[entry(A,'pdf','a'*64),entry(B,'pdf','a'*64),entry(C,'pdf','b'*64),
               entry(A,'txt','c'*64),entry(C,'txt','d'*64)]
        pdf,txt=m.classify_official(files,expect_counts=False)
        idx=m.prepare_document_index(pdf,txt,require_complete=False)
        self.assertEqual(len(idx),3)
        dup=[x for x in idx if x['pdf']['archivo']==B+'.pdf'][0]
        self.assertEqual(dup['origen_texto'],'CANONICO_POR_SHA_DOCUMENTAL')
        self.assertTrue(dup['duplicado_pdf_sha256'])
        self.assertEqual(dup['txt']['archivo'], A+'.txt')

    def test_no_inventa_texto_si_no_pdf_sha_igual(self):
        files=[entry(A,'pdf','a'*64),entry(B,'pdf','b'*64),entry(A,'txt','c'*64)]
        pdf,txt=m.classify_official(files,False)
        idx=m.prepare_document_index(pdf,txt,require_complete=False)
        self.assertEqual([x['origen_texto'] for x in idx if x['pdf']['archivo']==B+'.pdf'],['SIN_TXT'])

    def test_origen_invalido(self):
        bad=entry(A,'pdf');bad['source_hdfs']='/tmp/forjado.pdf'
        with self.assertRaisesRegex(ValueError,'ORIGEN_HDFS'):m.classify_official([bad],False)

    def test_uri_invalida(self):
        bad=entry(A,'pdf');bad['uri']='gs://otro-bucket/file.pdf'
        with self.assertRaisesRegex(ValueError,'URI_GCS'):m.classify_official([bad],False)

    def test_tamanyo_invalid(self):
        for size in (None,-1,0,'123',m.MAX_BYTES+1):
            bad=entry(A,'pdf');bad['size_bytes']=size
            with self.subTest(size=size),self.assertRaises(ValueError):m.classify_official([bad],False)

    def test_clave_repetida(self):
        x=entry(A,'pdf')
        with self.assertRaisesRegex(ValueError,'OBJETO_O_ORIGEN_REPETIDO'):
            m.classify_official([x,x],False)

    def test_hash_invalido(self):
        bad=entry(A,'pdf',digest='no-sha')
        with self.assertRaisesRegex(ValueError,'SHA256_INVALIDO'):
            m.classify_official([bad],False)

    def test_generacion_invalida(self):
        bad=entry(A,'pdf');bad['generation']=''
        with self.assertRaisesRegex(ValueError,'GENERACION_FALTANTE'):
            m.classify_official([bad],False)

    def test_ruta_traversal(self):
        bad=entry(A,'pdf');bad['source_hdfs']=m.PREFIX_HDFS+'pdf/../pdf/ingestion_date=2026-10-04/'+A+'.pdf'
        with self.assertRaisesRegex(ValueError,'RUTA_HDFS_INSEGURA'):
            m.classify_official([bad],False)

    def test_txt_calidad_computada(self):
        self.assertEqual(m.quality_text(b'  ')[2],'SIN_TEXTO')
        self.assertEqual(m.quality_text(b'hola')[2],'CORTO_REVISAR')
        self.assertEqual(m.quality_text(('ñ'*100).encode('utf-8'))[2],'CON_TEXTO')

    def test_txt_utf8_invalido(self):
        with self.assertRaisesRegex(ValueError,'TXT_NO_UTF8'):
            m.quality_text(b'\xff\xfe')

    def test_salida_excluye_empresa_y_expediente(self):
        # Simular lectura de bytes de la fuente oficial, verificando semántica del resultado
        from hashlib import sha256
        pa=b'%PDF-original';ta=b'Texto ' * 20
        p=entry(A,'pdf',digest=sha256(pa).hexdigest(),n=len(pa))
        t=entry(A,'txt',digest=sha256(ta).hexdigest(),n=len(ta))
        pdf,txt=m.classify_official([p,t],False)
        idx=m.prepare_document_index(pdf,txt,False)
        def mocked_read(x):
            return pa if x['archivo'].endswith('.pdf') else ta
        docs,stats=m.verify_and_build(idx,mocked_read)
        self.assertEqual(len(docs),1)
        self.assertEqual(stats['txt_unicos_verificados'],1)
        self.assertEqual(docs[0]['estado_calidad_txt_propuesto'],'CON_TEXTO')
        self.assertNotIn('numero_expediente',docs[0]);self.assertNotIn('empresa',docs[0])
        self.assertNotIn('fecha_presentacion',docs[0]);self.assertNotIn('sector',docs[0])
        self.assertTrue(docs[0]['fecha_nominal_no_es_fecha_oficial_hecho'])

    def test_no_contar_duplicados_como_descartados(self):
        from hashlib import sha256
        pdfdata=b'abc';txtdata=b'hello world'*20
        pa=sha256(pdfdata).hexdigest();ta=sha256(txtdata).hexdigest()
        f=[entry(A,'pdf',pa,n=3),entry(B,'pdf',pa,n=3),entry(A,'txt',ta,n=len(txtdata))]
        p,t=m.classify_official(f,False)
        idx=m.prepare_document_index(p,t,False)
        docs,s=m.verify_and_build(idx,lambda item: pdfdata if item['archivo'].endswith('.pdf') else txtdata)
        self.assertEqual(len(docs),2)
        self.assertEqual(s['duplicados_documentales_marcados'],1)
        self.assertEqual(s['txt_unicos_verificados'],1)

if __name__=='__main__':unittest.main()
