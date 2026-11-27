#!/usr/bin/env python3
"""Puros, NO GCS, NO claim, NO Spark, NO red."""
import hashlib,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'/'processing'))
from fase6_montos_sab_scope_quality import (
    validate_scope,check_actual_quarantine,coverage_manifest,QUALITY_EXPECTED,SCHEMA,STATUS,PREFIX
)
from fase6_hechos_documental_cloud_candidato import validate_input,build_verified_document_rows


def fixtures():
    pins=[]
    for i in range(1221):
        raw=f'obj{i}'.encode()
        pins.append({'uri':PREFIX+f'ingestion_date=2026-10-04/montos_sab_{20210101+i:08d}.json',
                     'generation':str(1000000000000000+i),
                     'sha256':hashlib.sha256(raw).hexdigest(), 'size_bytes':len(raw)})
    ex=pins[:14]
    doc={'schema_version':SCHEMA,'status':STATUS,'quality_approved':True,
         'dataset':'smv/montos_sab','run_id':'bvl-bronze-inicial-20261125-001',
         'attempt_id':'attempt-sab-12345','authorization_fingerprint':'a'*64,
         'signed_launch_sha256':'b'*64, 'allow_partial_staging':True,
         'allow_publication':False,'allow_quarantine_as_valid_rows':False,
         'source_files':1221,'normal_files':1207,'quarantine_files':14,
         'source_rows':22156,'normal_rows':21911,'quarantine_rows':245,
         'excluded_objects':ex,'policy':'QUARANTINE_14_ERROR_FILES_NO_SILENT_DROP',
         'quality_decision_id':'QUALITY-SMV-SAB-PARTIAL-001'}
    return pins,ex,doc

class Scope(unittest.TestCase):
    def validate(self,doc,pins):
        return validate_scope(doc,pins,attempt_id='attempt-sab-12345',
                              fingerprint='a'*64,launch_sha256='b'*64)
    def test_strict_success(self):
        pins,ex,doc=fixtures(); self.assertEqual(len(self.validate(doc,pins)),14)
    def test_no_unapproved_partial(self):
        pins,ex,doc=fixtures();doc['quality_approved']=False
        with self.assertRaises(ValueError):self.validate(doc,pins)
    def test_no_unapproved_publication(self):
        pins,ex,doc=fixtures();doc['allow_publication']=True
        with self.assertRaises(ValueError):self.validate(doc,pins)
    def test_bound_attempt(self):
        pins,ex,doc=fixtures();doc['attempt_id']='attempt-otro-123456'
        with self.assertRaises(ValueError):self.validate(doc,pins)
    def test_no_extra_fields(self):
        pins,ex,doc=fixtures();doc['anything']=True
        with self.assertRaises(ValueError):self.validate(doc,pins)
    def test_no_missing_pin(self):
        pins,ex,doc=fixtures();doc['excluded_objects']=ex[:-1]
        with self.assertRaises(ValueError):self.validate(doc,pins)
    def test_no_duplicate_excluded(self):
        pins,ex,doc=fixtures();doc['excluded_objects']=[ex[0]]+ex[:13]
        with self.assertRaises(ValueError):self.validate(doc,pins)
    def test_no_changed_generation(self):
        pins,ex,doc=fixtures(); doc['excluded_objects'][0]=dict(ex[0],generation='123')
        with self.assertRaises(ValueError):self.validate(doc,pins)
    def test_no_changed_sha(self):
        pins,ex,doc=fixtures();doc['excluded_objects'][0]=dict(ex[0],sha256='f'*64)
        with self.assertRaises(ValueError):self.validate(doc,pins)
    def test_no_incomplete_inventory(self):
        pins,ex,doc=fixtures()
        with self.assertRaises(ValueError):self.validate(doc,pins[:-1])
    def test_actual_quarantine_must_match(self):
        pins,ex,doc=fixtures(); selected=self.validate(doc,pins)
        metrics={'files_bronze':1221,'files_normal':1207,'files_quarantined':14,
                 'bronze_rows':22156,'rows_normal':21911,'rows_quarantined':245,
                 'rows_silently_discarded':0,'coverage_full':False}
        quarantined=[{'uri':p['uri']} for p in ex]
        self.assertTrue(check_actual_quarantine(selected,quarantined,metrics))
        with self.assertRaises(ValueError):
            check_actual_quarantine(selected,quarantined[:-1],metrics)
    def test_coverage_cannot_claim_full(self):
        pins,ex,doc=fixtures(); metrics={'files_normal':1207,'files_quarantined':14,
                 'bronze_rows':22156,'rows_normal':21911,'rows_quarantined':245}
        report=coverage_manifest(metrics,quality_scope_sha256='a'*64,
                      quarantine_receipt={'sha256':'b'*64})
        self.assertFalse(report['full_coverage'])
        self.assertFalse(report['allows_official_publication'])
        self.assertGreater(report['coverage_pct'],98)
        self.assertLess(report['coverage_pct'],99)
    def test_document_requires_official_1979(self):
        with self.assertRaises(ValueError):validate_input([])
    def test_document_rejects_fake_reader(self):
        with self.assertRaises(ValueError):build_verified_document_rows([],read_exact=None)

if __name__=='__main__':unittest.main()
