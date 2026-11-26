import sys
from pathlib import Path
import unittest
from datetime import datetime, timedelta, timezone

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'scripts'/'processing'))

import fase6_launch_restantes as launch
import fase6_restantes_batch as batch
from fase6_restantes_core import SOURCES, FILE_COUNTS, expected_prefix


class LaunchTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026,11,26,20,0,tzinfo=timezone.utc)
        self.commit = 'f'*40
        self.dataset = 'smv/valores_inscritos'
        self.attempt = 'valores-inscritos-20261126-0001'

    def doc(self):
        root = ('gs://bvl26-dataproc-staging-7f3a9c/'
                f'bvl-phase6/releases/{self.commit}/')
        return {
            'schema_version':launch.SCHEMA,
            'status':'QUALITY_APPROVED_SUPERVISED_LAUNCH',
            'quality_approved': True,
            'project_id':launch.PROJECT,
            'region':launch.REGION,
            'mode':'SINGLE_SOURCE',
            'dataset':self.dataset,
            'silver_outputs':[{'name':p,'staging_prefix':expected_prefix(p,
                    'bvl-bronze-inicial-20261125-001',self.attempt)}
                    for p in SOURCES[self.dataset]],
            'operation':'BUILD_SILVER_STAGING',
            'run_id':'bvl-bronze-inicial-20261125-001',
            'attempt_id':self.attempt,
            'authorization_fingerprint':'a'*64,
            'processor_commit':self.commit,
            'entrypoint_sha256':'b'*64,
            'dependencies_sha256':'c'*64,
            'entrypoint_uri':root+'fase6_restantes_batch.py',
            'entrypoint_generation':123456789,
            'dependencies_uri':root+'fase6_restantes_dependencias.zip',
            'dependencies_generation':123456790,
            'runtime_version':'2.2',
            'service_account':launch.QUALITY_SERVICE_ACCOUNT,
            'batch_id':'bvl-f6-valores-20261126',
            'subnet':'projects/bvl-analytics-2026/regions/southamerica-west1/subnetworks/bvl-net',
            'driver_cores':4, 'executor_cores':4, 'executor_instances':2,
            'ttl_seconds':3600,
            'not_before_utc':(self.now-timedelta(minutes=3)).isoformat(),
            'expires_at_utc':(self.now+timedelta(minutes=60)).isoformat(),
            'overwrite_allowed':False, 'automatic_retry_allowed':False,
            'official_publication_allowed':False,
        }

    def test_valores_two_outputs(self):
        d=self.doc()
        self.assertEqual(launch.validate_payload(d,now=self.now)["silver_outputs"],d["silver_outputs"])

    def test_valid_payload(self):
        d=self.doc()
        self.assertEqual(launch.validate_payload(d,now=self.now),d)

    def test_missing_second_output_fails(self):
        d=self.doc(); d['silver_outputs']=d['silver_outputs'][:1]
        with self.assertRaises(ValueError): launch.validate_payload(d,now=self.now)

    def test_extra_output_fails(self):
        d=self.doc(); d['silver_outputs'].append(d['silver_outputs'][0])
        with self.assertRaises(ValueError): launch.validate_payload(d,now=self.now)

    def test_different_attempt_fails(self):
        d=self.doc(); d['silver_outputs'][0]['staging_prefix']+='other'
        with self.assertRaises(ValueError): launch.validate_payload(d,now=self.now)

    def test_expired_fails(self):
        d=self.doc()
        with self.assertRaises(ValueError):
            launch.validate_payload(d,now=self.now+timedelta(hours=3))

    def test_no_auto_retry(self):
        d=self.doc(); d['automatic_retry_allowed']=True
        with self.assertRaises(ValueError): launch.validate_payload(d,now=self.now)

    def test_bad_signature_reference_uri(self):
        d=self.doc()
        ref={'uri':'gs://bvl26-control-7f3a9c/authorizations/other/launch_context.json',
             'signature_uri':'gs://bvl26-control-7f3a9c/authorizations/other/launch_context.sig',
             'generation':1,'signature_generation':2,'sha256':'a'*64,'signature_sha256':'b'*64}
        with self.assertRaises(ValueError):
            launch.validate_refs(ref,d['run_id'],d['attempt_id'])

    def test_hechos_blocked(self):
        d=self.doc(); d['dataset']='smv/hechos_importancia'
        with self.assertRaises(ValueError): launch.validate_payload(d,now=self.now)

    def test_derived_blocked(self):
        d=self.doc(); d['dataset']='hechos_importancia/identidad_empresarial'
        with self.assertRaises(ValueError): launch.validate_payload(d,now=self.now)

    def test_wrong_commit_artifact_path(self):
        d=self.doc(); d['entrypoint_uri']=d['entrypoint_uri'].replace('f'*40,'a'*40)
        with self.assertRaises(ValueError): launch.validate_payload(d,now=self.now)

    def test_identity_and_mode_required(self):
        d=self.doc(); d['mode']='MULTI_SOURCE'
        with self.assertRaises(ValueError): launch.validate_payload(d,now=self.now)

    def test_read_only_default(self):
        self.assertEqual(batch.main([]),0)
        self.assertFalse(batch.validate_cli(batch.parser().parse_args([])))

    def test_no_unauthorized_execute(self):
        args=batch.parser().parse_args(['--execute','--dataset','bcrp/embig'])
        with self.assertRaises(ValueError):batch.validate_cli(args)

    def test_all_10_partitions_mapped(self):
        self.assertEqual(len(batch.PARTITIONS),10)
        self.assertEqual(set(batch.PARTITIONS),launch.SUPPORTED)

    def test_source_size_rejects_unexpected(self):
        f=[{'uri': 'gs://bvl26-bronze-7f3a9c/bcrp/embig/run_id=bvl-bronze-inicial-20261125-001/embig.csv',
            'generation':'1','size_bytes':32*1024*1024+1,'sha256':'a'*64}]
        with self.assertRaises(ValueError):batch.check_manifest_capacity('bcrp/embig',f)

if __name__ == '__main__':unittest.main()
