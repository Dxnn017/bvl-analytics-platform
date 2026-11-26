import importlib.util
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
        self.assertEqual(self.intent['action'],'INGEST_PILOT_12')
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
