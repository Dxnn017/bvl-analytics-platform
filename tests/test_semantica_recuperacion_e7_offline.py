import importlib.util
import hashlib
import json
import pathlib
import sys
import threading
import unittest

path=pathlib.Path(__file__).resolve().parents[1]/'scripts/ingestion/validar_semantica_recuperacion_e7_offline.py'
spec=importlib.util.spec_from_file_location('e7_semantics',path)
m=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=m
spec.loader.exec_module(m)

class TestE7Semantics(unittest.TestCase):
    def setUp(self):
        self.intent=m.pilot_intent()
        self.marker=m.claim_name(m.semantic_sha256(self.intent))
        self.files=[f'archivo_{i:02}' for i in range(12)]
    def test_core_scope_12(self):
        self.assertEqual(self.intent['operation_type'],'BRONZE_PILOT_TRANSFER')
    def test_canonical_exact_494_bytes_and_quality_fingerprint(self):
        raw=m.semantic_canonical_bytes(self.intent)
        self.assertEqual(len(self.intent),8)
        self.assertEqual(set(self.intent),{
            'schema_version','project_id','run_id','operation_type',
            'reservation_generation','reservation_sha256','pilot_json_sha256',
            'canonical_destination_list_sha256'})
        self.assertEqual(len(raw),494)
        self.assertNotIn(b'\n',raw)
        self.assertEqual(raw,json.dumps(self.intent,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode('utf-8'))
        self.assertTrue(all(type(v) is str for v in self.intent.values()))
        self.assertEqual(self.intent['reservation_generation'],'1795640338029794')
        # La huella esperada solo existe como asercion de prueba, NO en el calculador.
        self.assertEqual(hashlib.sha256(raw).hexdigest(),
            '87f93d0d1d84cdc4b346dee64e4c9a3a69f24e53ed94006d2f2fc0b7c6b56382')
        self.assertEqual(m.semantic_sha256(self.intent),hashlib.sha256(raw).hexdigest())

    def test_real_pilot_fixture_bytes_match_approved_reference(self):
        pilot=pathlib.Path(__file__).resolve().parents[1]/'fixtures/piloto_12_CANDIDATO_LOCAL.json'
        self.assertTrue(pilot.is_file(),f'Falta evidencia local del piloto: {pilot}')
        raw=pilot.read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(),self.intent['pilot_json_sha256'])
        content=json.loads(raw)
        self.assertEqual(content['run_id'],m.RUN_ID)
        self.assertEqual(content['archivos'],12)
        self.assertEqual(content['bytes_esperados'],23052390)
        self.assertFalse(content['quality_approved'])
        self.assertFalse(content['transfer_allowed'])

    def test_exact_official_claim_path(self):
        self.assertEqual(self.marker,
            'authorizations/consumed/bronze/run_id=bvl-bronze-inicial-20261125-001/'
            '87f93d0d1d84cdc4b346dee64e4c9a3a69f24e53ed94006d2f2fc0b7c6b56382.json')
        self.assertNotIn('/sem-',self.marker)

    def test_non_string_reservation_generation_rejected(self):
        with self.assertRaises(m.Blocked):m.pilot_intent(reservation_generation=1795640338029794)

    def test_wrong_layer_rejected_outside_fingerprint(self):
        with self.assertRaises(m.Blocked):m.pilot_intent(layer='gold')

    def test_wrong_reservation_uri_rejected_outside_fingerprint(self):
        with self.assertRaises(m.Blocked):m.pilot_intent(reservation_uri='gs://otro/registro')

    def test_modified_operation_and_schema_rejected(self):
        for k,v in [('schema_version','INVALID'),('operation_type','FULL_TRANSFER')]:
            intent=self.intent.copy();intent[k]=v
            with self.subTest(k=k), self.assertRaises(m.Blocked):m.semantic_sha256(intent)

    def test_newline_payload_and_nonstring_rejected(self):
        intent=self.intent.copy();intent['reservation_generation']=int(intent['reservation_generation'])
        with self.assertRaises(m.Blocked):m.semantic_sha256(intent)
        intent=self.intent.copy();intent['extra']='x'
        with self.assertRaises(m.Blocked):m.semantic_sha256(intent)

    def test_changed_uuid_timestamp_signature_stable_and_claim_collides(self):
        c=m.OfflineClaims()
        for uuid,timestamp,signature in [('aaa','2026-11-25','s1'),('bbb','2026-11-26','s2')]:
            outer={'intent':self.intent,'uuid':uuid,'issued_at':timestamp,'signature':signature}
            fingerprint=m.semantic_sha256(outer['intent'])
            marker=m.claim_name(fingerprint)
            self.assertEqual(marker,self.marker)
            if uuid=='aaa':c.try_claim(marker)
            else:
                with self.assertRaises(m.Blocked):c.try_claim(marker)

    def test_wrong_fingerprint_cannot_claim(self):
        with self.assertRaises(m.Blocked):m.claim_name('0'*64)

    def test_stable_semantic_hash_independent_of_metadata(self):
        # UUID, timestamps and signature aren't keys of the frozen operation intent.
        for uuid in ['aaa','bbb']:
            for timestamp in ['2026-11-25','2026-11-26']:
                outer={'intent':self.intent,'uuid':uuid,'issued_at':timestamp,'signature':'other'}
                self.assertEqual(m.semantic_sha256(outer['intent']),m.semantic_sha256(self.intent))
    def test_reissue_claim_blocked(self):
        c=m.OfflineClaims(); c.try_claim(self.marker)
        with self.assertRaises(m.Blocked): c.try_claim(self.marker)
    def test_real_concurrency_one_winner(self):
        c=m.OfflineClaims(); outputs=[]; lock=threading.Lock()
        def worker():
            try: out=c.try_claim(self.marker)
            except m.Blocked as e: out=str(e)
            with lock: outputs.append(out)
        ts=[threading.Thread(target=worker) for _ in range(20)]
        for t in ts:t.start()
        for t in ts:t.join()
        self.assertEqual(sum('201_SIMULADO' in x for x in outputs),1)
        self.assertEqual(outputs.count('CONFLICTO_412_SIMULADO'),19)
    def test_changed_reservation_generation_rejected(self):
        with self.assertRaises(m.Blocked):m.pilot_intent(reservation_generation='999')
    def test_modified_pilot_hash_rejected(self):
        with self.assertRaises(m.Blocked):m.pilot_intent(pilot_sha256='0'*64)
    def test_changed_destinations_rejected(self):
        with self.assertRaises(m.Blocked):m.pilot_intent(destinations_sha256='1'*64)
    def test_scope_injection_rejected(self):
        x={**self.intent,'extra_file':'gs://example/a'}
        with self.assertRaises(m.Blocked):m.semantic_sha256(x)
    def test_invalid_claim_path_rejected(self):
        with self.assertRaises(m.Blocked):m.OfflineClaims().try_claim('fake/prefix')
    def test_failure_retains_consumed_claim(self):
        c=m.OfflineClaims();c.try_claim(self.marker)
        with self.assertRaises(m.Blocked):c.try_claim(self.marker)
    def test_recovery_requires_stopped_executor(self):
        with self.assertRaises(m.Blocked):m.pending_after_failure(self.files,self.files[:4],prior_claim_consumed=True,old_executor_stopped=False,reconciled=True,recovery_human_authorized=True)
    def test_recovery_requires_human_auth(self):
        with self.assertRaises(m.Blocked):m.pending_after_failure(self.files,self.files[:4],prior_claim_consumed=True,old_executor_stopped=True,reconciled=True,recovery_human_authorized=False)
    def test_recovery_requires_reconciliation(self):
        with self.assertRaises(m.Blocked):m.pending_after_failure(self.files,self.files[:4],prior_claim_consumed=True,old_executor_stopped=True,reconciled=False,recovery_human_authorized=True)
    def test_recovery_pending_only(self):
        p=m.pending_after_failure(self.files,self.files[:4],prior_claim_consumed=True,old_executor_stopped=True,reconciled=True,recovery_human_authorized=True)
        self.assertEqual(p, self.files[4:])
        self.assertEqual(set(p)&set(self.files[:4]),set())
    def test_recovery_prevents_extra_file(self):
        with self.assertRaises(m.Blocked):m.pending_after_failure(self.files,['extra'],prior_claim_consumed=True,old_executor_stopped=True,reconciled=True,recovery_human_authorized=True)
    def test_recovery_new_marker_and_idempotent(self):
        fp=m.semantic_sha256(self.intent)
        a=m.recovery_claim_name(fp,self.files[4:])
        b=m.recovery_claim_name(fp,list(reversed(self.files[4:])))
        self.assertEqual(a,b)
        self.assertNotEqual(a,self.marker)
        c=m.OfflineClaims();c.try_claim(a)
        with self.assertRaises(m.Blocked):c.try_claim(b)
    def test_no_recovery_if_all_complete(self):
        with self.assertRaises(m.Blocked):m.pending_after_failure(self.files,self.files,prior_claim_consumed=True,old_executor_stopped=True,reconciled=True,recovery_human_authorized=True)

if __name__=='__main__':unittest.main()
