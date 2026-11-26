import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent))
from BVL_F6_HECHOS_RECON_CANDIDATA import reconstruct, compare_reference, csv_bytes

SHA1='a'*64; SHA2='b'*64
BASE='/datalake/bvl/bronze/smv/hechos_importancia/'
PDF=BASE+'pdf/ingestion_date=2026-10-04/'
TXT=BASE+'texto_extraido/ingestion_date=2026-10-04/'
URI='gs://bvl26-bronze-7f3a9c/smv/hechos_importancia/run_id=bvl-bronze-inicial-20261125-001/'

def fixtures():
    m=[{'archivo_pdf':'p1.pdf','tamano_bytes':'12','sha256':SHA1,'archivo_canonico':'p1.pdf','duplicado':'false'},
       {'archivo_pdf':'p2.pdf','tamano_bytes':'12','sha256':SHA1,'archivo_canonico':'p1.pdf','duplicado':'true'}]
    a=[{'guid_documento':'g1','numero_expediente':'e1','archivo_pdf':'p1.pdf','empresa':'ABC','fecha_presentacion':'02/01/2025','coherencia_temporal':'COHERENTE'},
       {'guid_documento':'g2','numero_expediente':'e1','archivo_pdf':'p2.pdf','empresa':'ABC','fecha_presentacion':'02/01/2025','coherencia_temporal':'COHERENTE'}]
    paths=[PDF+'p1.pdf',PDF+'p2.pdf',TXT+'p1.txt']
    b=[{'source_hdfs':paths[0],'uri':URI+'p1.pdf','sha256':SHA1,'size_bytes':12,'generation':1},
       {'source_hdfs':paths[1],'uri':URI+'p2.pdf','sha256':SHA1,'size_bytes':12,'generation':2},
       {'source_hdfs':paths[2],'uri':URI+'p1.txt','sha256':SHA2,'size_bytes':9,'generation':3}]
    return m,a,b,paths[:2],paths[2:]

class Tests(unittest.TestCase):
    def test_reconstruction_reuses_canonical_txt(self):
        e,d,issues=reconstruct(*fixtures(),strict_counts=False)
        self.assertEqual(len(e),1)
        self.assertEqual(e[0]['cantidad_documentos_pdf'],'2')
        self.assertEqual(len(d),2)
        self.assertEqual([x['fuente_texto'] for x in d],['DIRECTO','CANONICO_DUPLICADO'])
        self.assertEqual(issues,[])
    def test_original_hdfs_guid_not_derived_from_pdf_hash(self):
        e,d,_=reconstruct(*fixtures(),strict_counts=False)
        self.assertIn('PENDIENTE_RATIFICACION',d[0]['origen_campos_portal'])
        self.assertEqual(d[0]['bronze_pdf_generation'],'1')
    def test_rejects_conflicting_sha(self):
        items=list(fixtures()); items[2][0]['sha256']=SHA2
        with self.assertRaisesRegex(ValueError,'SHA_TAMANO'):
            reconstruct(*items,strict_counts=False)
    def test_rejects_unmapped_pdf(self):
        items=list(fixtures()); items[1]=items[1][:1]
        with self.assertRaisesRegex(ValueError,'MAPEO_NO_CUBRE'):
            reconstruct(*items,strict_counts=False)
    def test_rejects_duplicate_guid(self):
        items=list(fixtures()); items[1][1]['guid_documento']='g1'
        with self.assertRaisesRegex(ValueError,'GUID_DUPLICADO'):
            reconstruct(*items,strict_counts=False)
    def test_rejects_unknown_pdf(self):
        items=list(fixtures()); items[1][1]['archivo_pdf']='unknown.pdf'
        with self.assertRaisesRegex(ValueError,'MAPEO_PDF_SIN_BRONZE'):
            reconstruct(*items,strict_counts=False)
    def test_marks_conflicting_company_instead_of_guessing(self):
        items=list(fixtures()); items[1][1]['empresa']='OTRA'
        e,_,issues=reconstruct(*items,strict_counts=False)
        self.assertEqual(e[0]['empresa'],'')
        self.assertTrue(any(x['campo']=='empresa' for x in issues))
    def test_reference_compare_does_not_copy_historical_rows(self):
        e,d,_=reconstruct(*fixtures(),strict_counts=False)
        comp=compare_reference(e,d,[{'numero_expediente':'e1','empresa':'DIFERENTE'}],
                            [{'guid_documento':'g1','archivo_pdf':'p1.pdf'},
                             {'guid_documento':'g2','archivo_pdf':'p2.pdf'}])
        self.assertEqual(comp['diferencias_campos_comparables']['empresa'],1)
    def test_csv_output_deterministic(self):
        _,d,_=reconstruct(*fixtures(),strict_counts=False)
        fields=['guid_documento','bronze_pdf_sha256']
        self.assertEqual(csv_bytes(d,fields),csv_bytes(d,fields))
        self.assertIn(b'g1',csv_bytes(d,fields))

if __name__=='__main__':unittest.main()
