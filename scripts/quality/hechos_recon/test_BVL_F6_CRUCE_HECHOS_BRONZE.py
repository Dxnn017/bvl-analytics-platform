import unittest
from BVL_F6_CRUCE_HECHOS_BRONZE import historical_evidence,cross_match,canonical_path

class AuditTests(unittest.TestCase):
 def setUp(self):
  self.pdfs=[f'/pdf/doc{i}.pdf' for i in range(990)]
  self.txts=[f'/txt/doc{i}.txt' for i in range(989)]
  self.sha='a'*64
  self.hist=[dict(archivo_pdf=f'doc{i}.pdf',tamano_bytes='10',sha256=self.sha) for i in range(990)]
  self.docs=[dict(archivo_pdf=f'doc{i}.pdf') for i in range(990)]
 def test_historic_valid(self):
  self.assertEqual(len(historical_evidence(self.hist,self.docs,self.pdfs,self.txts)),990)
 def test_reject_duplicate(self):
  self.hist[1]['archivo_pdf']='doc0.pdf'
  with self.assertRaises(ValueError):historical_evidence(self.hist,self.docs,self.pdfs,self.txts)
 def test_reject_wrong_metadata(self):
  self.hist[20]['sha256']='x'
  with self.assertRaises(ValueError):historical_evidence(self.hist,self.docs,self.pdfs,self.txts)
 def test_cross_full(self):
  bronze=[dict(uri=f'gs://bvl26-bronze-7f3a9c/smv/hechos_importancia/run_id=bvl-bronze-inicial-20261125-001/pdf/doc{i}.pdf',source_hdfs=x,size_bytes=10,sha256=self.sha,generation=1) for i,x in enumerate(self.pdfs)]
  bronze += [dict(uri=f'gs://bvl26-bronze-7f3a9c/smv/hechos_importancia/run_id=bvl-bronze-inicial-20261125-001/txt/doc{i}.txt',source_hdfs=x,size_bytes=20,sha256=self.sha,generation=1) for i,x in enumerate(self.txts)]
  self.assertTrue(cross_match(historical_evidence(self.hist,self.docs,self.pdfs,self.txts),self.pdfs,self.txts,bronze)['recorded_hashes_match'])
 def test_cross_mismatch(self):
  bronze=[dict(uri='gs://x',source_hdfs=x,size_bytes=20,sha256=self.sha,generation=1) for x in self.pdfs+self.txts]
  self.assertFalse(cross_match(historical_evidence(self.hist,self.docs,self.pdfs,self.txts),self.pdfs,self.txts,bronze)['recorded_hashes_match'])
 def test_normalize_hdfs_url(self):
  self.assertEqual(canonical_path('hdfs://namenode:9000/pdf/a.pdf'),'/pdf/a.pdf')

if __name__=='__main__':unittest.main()
