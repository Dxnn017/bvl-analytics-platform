"""Pruebas offline de preclaim IAM: sin GCS, KMS, claims ni Dataproc."""
from __future__ import annotations
import copy
from datetime import datetime, timezone, timedelta
from hashlib import sha256
from pathlib import Path
from unittest import TestCase, mock
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts' / 'processing'))
sys.path.insert(0, str(ROOT / 'tests'))
import fase6_hechos_launcher_documental_v1 as launch
from test_fase6_hechos_launcher_documental_v1 import candidate_context, inventory_with_bytes


def sample_attestation(ctx, *, managed_folder=False):
    now = datetime.now(timezone.utc)
    targets = launch.expected_iam_create_targets(ctx)
    pref = f'gs://{launch.CONTROL_BUCKET}/authorizations/silver_documental/run_id={ctx["run_id"]}/attempt_id={ctx["attempt_id"]}/iam_evidence/'
    prefix = f'projects/_/buckets/{launch.SILVER_BUCKET}/objects/staging/{launch.PRODUCT}/'
    role = f'projects/{launch.PROJECT}/roles/bvlStorageReservationWriter'
    binding = {'role':role,'members':['serviceAccount:'+ctx['service_account']],
               'condition':{'title':'silver-staging','expression':f'resource.name.startsWith("{prefix}")'}}
    resource_name = f'projects/_/buckets/{launch.SILVER_BUCKET}'
    if managed_folder:
        resource_name += f'/managedFolders/staging/{launch.PRODUCT}/'
    return {
        'schema_version':launch.IAM_SCOPE_SCHEMA,
        'status':'QUALITY_ATTESTED_SCOPE_CANDIDATE',
        'project_id':launch.PROJECT,
        'product':launch.PRODUCT,
        'service_account':ctx['service_account'],
        'run_id':ctx['run_id'], 'attempt_id':ctx['attempt_id'],
        'batch_id':ctx['batch_id'], 'processor_commit':ctx['processor_commit'],
        'staging_prefix':ctx['staging_prefix'],
        'authorization_fingerprint':ctx['authorization_fingerprint'],
        'launch_context_sha256':'a'*64,
        'issued_at_utc':(now-timedelta(seconds=30)).isoformat(),
        'expires_at_utc':(now+timedelta(minutes=3)).isoformat(),
        'diagnostic':{
            'report_sha256':launch.IAM_PROBE_REPORT_SHA256,
            'object_uri':launch.IAM_PROBE_OBJECT,
            'generation':launch.IAM_PROBE_GENERATION,
            'creation_http':200, 'read_integrity':'PASS', 'cleanup_http':204,
            'claims_consumed':0, 'dataproc_executed':False},
        'targets':targets,
        'allow_policy':{
            'source_uri':pref+('managed_folder_policy_snapshot.json' if managed_folder else 'bucket_policy_snapshot.json'),
            'source_generation':'1234','source_sha256':'b'*64,
            'version':3, 'etag':'CAAAA23', 'bindings':[binding],
            'observed_at_utc':(now-timedelta(seconds=20)).isoformat(),
            'resource_type':'MANAGED_FOLDER' if managed_folder else 'BUCKET',
            'resource_name':resource_name,
        },
        'role_definition':{
            'source_uri':pref+'custom_role_definition.json',
            'source_generation':'1234','source_sha256':'c'*64,
            'name':role, 'includedPermissions':['storage.objects.create'],
            'observed_at_utc':(now-timedelta(seconds=20)).isoformat(),
        },
        'independent_deny_review':{
            'source_uri':pref+'deny_review.json',
            'source_generation':'1234','source_sha256':'d'*64,
            'reviewer':'quality-review@example.org',
            'principal':'serviceAccount:'+ctx['service_account'],
            'permission':'storage.objects.create',
            'objects_sha256':sha256(launch.canonical(targets)).hexdigest(),
            'outcome':'NO_APPLICABLE_DENY_VERIFIED',
            'evaluated_at_utc':(now-timedelta(seconds=10)).isoformat(),
        }
    }


class FakeClient:
    def __init__(self, deny_list=False):
        self.object_calls = []
        self.deny_list=deny_list
    def bucket(self,bucket):
        parent=self
        class Bucket:
            def test_iam_permissions(self,perms):
                return [] if parent.deny_list else list(perms)
            def blob(self,name):
                class Blob:
                    def test_iam_permissions(self,perms):
                        parent.object_calls.append((bucket,name,tuple(perms)))
                        if 'storage.objects.create' in perms:
                            raise RuntimeError('HTTP400: create para objeto inexistente')
                        return list(perms)
                    def upload_from_file(self,*args,**kwargs):
                        raise AssertionError('WRITE PROHIBIDO EN PRUEBA')
                return Blob()
        return Bucket()


class TestSignedIAMScope(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.entries,_=inventory_with_bytes()

    def setUp(self):
        self.ctx=candidate_context()
        self.doc=sample_attestation(self.ctx)
        prefix=f'gs://{launch.CONTROL_BUCKET}/authorizations/silver_documental/run_id={self.ctx["run_id"]}/attempt_id={self.ctx["attempt_id"]}/'
        self.launch_refs={'uri':prefix+'launch_context.json','signature_uri':prefix+'launch_context.sig'}

    def check(self,doc=None):
        return launch.validate_iam_scope_attestation(doc or self.doc,self.ctx,'a'*64)

    def test_01_policy_bucket_scope_valid(self):
        self.assertTrue(self.check())

    def test_02_managed_folder_valid(self):
        self.assertTrue(self.check(sample_attestation(self.ctx,managed_folder=True)))

    def test_03_target_missing_fails(self):
        self.doc['targets'].pop()
        with self.assertRaisesRegex(ValueError,'OBJETIVOS_EXACTOS'):
            self.check()

    def test_04_target_extra_fails(self):
        self.doc['targets'].append(launch.IAM_PROBE_OBJECT)
        with self.assertRaisesRegex(ValueError,'OBJETIVOS_EXACTOS'):
            self.check()

    def test_05_other_attempt_fails(self):
        self.doc['attempt_id']='otro-attempt'
        with self.assertRaisesRegex(ValueError,'ALCANCE_DISTINTO'):
            self.check()

    def test_06_other_context_sha_fails(self):
        self.doc['launch_context_sha256']='1'*64
        with self.assertRaisesRegex(ValueError,'ALCANCE_DISTINTO'):
            self.check()

    def test_07_diagnostic_probe_is_not_scope(self):
        self.doc['allow_policy']['bindings'][0]['condition']['expression']=(
            f'resource.name.startsWith("projects/_/buckets/{launch.SILVER_BUCKET}/objects/staging/{launch.PRODUCT}/_iam_diagnostic_only/")')
        with self.assertRaisesRegex(ValueError,'POLICY_NO_ACREDITA'):
            self.check()

    def test_08_role_without_create_fails(self):
        self.doc['role_definition']['includedPermissions']=['storage.objects.get']
        with self.assertRaisesRegex(ValueError,'ROLE_NO_ACREDITA_CREATE'):
            self.check()

    def test_09_no_deny_review_fails(self):
        self.doc['independent_deny_review']['outcome']='UNKNOWN'
        with self.assertRaisesRegex(ValueError,'DENY_REVIEW_NO_ACREDITADA'):
            self.check()

    def test_10_policy_stale_fails(self):
        self.doc['allow_policy']['observed_at_utc']=(
            datetime.now(timezone.utc)-timedelta(hours=1)).isoformat()
        with self.assertRaisesRegex(ValueError,'SNAPSHOT_OBSOLETA'):
            self.check()

    def test_11_signed_attestation_expired_fails(self):
        self.doc['expires_at_utc']=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()
        with self.assertRaisesRegex(ValueError,'ATESTACION_VENCIDA'):
            self.check()

    def test_12_policy_OR_expression_unknown_fails(self):
        self.doc['allow_policy']['bindings'][0]['condition']['expression'] += ' || true'
        with self.assertRaisesRegex(ValueError,'POLICY_NO_ACREDITA'):
            self.check()

    def test_13_other_principal_fails(self):
        self.doc['allow_policy']['bindings'][0]['members']=['serviceAccount:unauthorized@example.org']
        with self.assertRaisesRegex(ValueError,'POLICY_NO_ACREDITA'):
            self.check()

    def test_14_wrong_diagnostic_report_fails(self):
        self.doc['diagnostic']['report_sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'DIAGNOSTICO_NO_ACREDITADO'):
            self.check()

    def test_15_attestation_skips_HTTP400_but_preserves_read_and_list(self):
        client=FakeClient()
        self.assertTrue(launch.validate_writer_permissions(
            client,self.ctx,self.entries,self.launch_refs,
            iam_attestation=self.doc,context_sha='a'*64))
        self.assertGreaterEqual(len(client.object_calls),7)
        self.assertTrue(all('storage.objects.create' not in calls[2] for calls in client.object_calls))

    def test_16_without_attestation_HTTP400_blocks(self):
        with self.assertRaisesRegex(ValueError,'IAM_PRECLAIM_NO_VERIFICABLE'):
            launch.validate_writer_permissions(FakeClient(),self.ctx,self.entries,self.launch_refs)

    def test_17_signed_scope_cannot_override_denied_read(self):
        with self.assertRaisesRegex(ValueError,'IAM_INSUFICIENTE_PRECLAIM'):
            launch.validate_writer_permissions(FakeClient(deny_list=True),self.ctx,self.entries,
                self.launch_refs,iam_attestation=self.doc,context_sha='a'*64)

    def test_18_signed_reference_required_in_execute(self):
        args=launch.parser().parse_args(['--execute','--ack-supervised'])
        with self.assertRaisesRegex(ValueError,'REFERENCIAS_QUALITY_FALTANTES'):
            launch.execute(args)

    def test_19_untrusted_signature_fails_closed(self):
        class FakeGate:
            @staticmethod
            def signed(raw,signature,pem):
                raise ValueError('FIRMA_INVALIDA')
        refs={
            'uri':f'gs://{launch.CONTROL_BUCKET}/authorizations/silver_documental/run_id={self.ctx["run_id"]}/attempt_id={self.ctx["attempt_id"]}/iam_scope_attestation.json',
            'signature_uri':f'gs://{launch.CONTROL_BUCKET}/authorizations/silver_documental/run_id={self.ctx["run_id"]}/attempt_id={self.ctx["attempt_id"]}/iam_scope_attestation.sig',
            'generation':'123','sha256':'a'*64,'signature_generation':'124','signature_sha256':'b'*64,
        }
        with mock.patch.dict(sys.modules,{'puerta_silver_operativa_v1_candidata':FakeGate()}):
            with mock.patch.object(launch,'_read_pinned',return_value=b'fake'):
                with self.assertRaisesRegex(ValueError,'FIRMA_INVALIDA'):
                    launch._verified_iam_scope_attestation(None,refs,b'pub',self.ctx,'a'*64)

    def test_20_deny_review_must_target_all_objects(self):
        self.doc['independent_deny_review']['objects_sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'DENY_REVIEW_NO_ACREDITADA'):
            self.check()


if __name__ == '__main__':
    import unittest
    unittest.main()
