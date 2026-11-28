"""Integracion offline launcher.execute -> antes de claim.
Solo fixtures criptograficas efimeras y GCS en memoria. NO es una autorizacion real.
Nunca escribir en GCP. Depende de pruebas existentes del repositorio.
"""
from __future__ import annotations

import copy
import json
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
for rel in ('scripts/pipeline', 'scripts/processing', 'tests'):
    sys.path.insert(0, str(ROOT / rel))

import adaptador_gcs_silver_v1 as gcs
import puerta_silver_v1_candidata as core
import puerta_silver_operativa_v1_candidata as gate
import fase6_hechos_launcher_documental_v1 as launch
import test_f6_extension_documental_focal as fixture_module
from test_fase6_hechos_launcher_documental_v1 import inventory_with_bytes, candidate_context
from test_fase6_iam_atestacion_firmada_v1 import sample_attestation


class LimiteAnteriorAlClaim(BaseException):
    pass


class AlmacenGCSOffline:
    """Imita solo los metodos externos GCS; nunca sube objetos."""
    def __init__(self, identidad):
        self._credentials = SimpleNamespace(service_account_email=identidad)
        self.objects = {}
        self.reads = 0
        self.writes = 0
        self.lists = 0

    def add(self, uri, raw, generation):
        assert isinstance(raw, bytes)
        self.objects[uri] = (raw, str(generation))

    def bucket(self, name):
        parent = self

        class Bucket:
            def blob(self, key, generation=None):
                uri = f'gs://{name}/{key}'

                class Blob:
                    def __init__(self):
                        self.generation = None
                        self.size = None

                    def reload(self, retry=None):
                        raw, gen = parent.objects[uri]
                        self.generation, self.size = int(gen), len(raw)

                    def download_as_bytes(self, if_generation_match=None, retry=None):
                        raw, gen = parent.objects[uri]
                        assert int(gen) == int(if_generation_match)
                        if generation is not None:
                            assert int(gen) == int(generation)
                        parent.reads += 1
                        return raw

                    def test_iam_permissions(self, perms):
                        return list(perms)

                    def test_iam_permissions(self, perms):
                        assert perms == ['storage.objects.get']
                        return list(perms)

                    def upload_from_string(self, *a, **kw):
                        parent.writes += 1
                        raise AssertionError('PROHIBIDO_CONSUMIR_CLAIM')

                    def upload_from_file(self, *a, **kw):
                        parent.writes += 1
                        raise AssertionError('PROHIBIDO_ESCRIBIR_STAGING')

                return Blob()

            def test_iam_permissions(self, perms):
                return list(perms)

        return Bucket()

    def list_blobs(self, bucket, *, prefix, max_results):
        assert bucket == launch.SILVER_BUCKET and max_results == 1
        self.lists += 1
        return []


class TestPreclaimIntegralSimulado(unittest.TestCase):
    def test_execute_real_llega_justo_antes_del_claim(self):
        now = datetime.now(timezone.utc)
        fixtures = fixture_module.ExtensionDocumentalTests('test_01_legacy_historico_firmado_sigue_funcionando')
        fixtures.setUp()
        # El fixture preexistente parchea check_base; desactivar ese mock para
        # utilizar el validador real con un contrato sintetico completo.
        fixtures.doCleanups()

        entries, payloads = inventory_with_bytes()
        total = sum(x['size_bytes'] for x in entries)
        manifest = {'dataset': launch.DATASET, 'run_id': launch.RUN,
                    'status': 'BRONZE_COMPLETADO', 'quality_approved': True,
                    'silver_consumption_allowed': False,
                    'total_bytes': total, 'files': entries}
        manifest_raw = core.canonical_bytes(manifest)
        manifest_sha = sha256(manifest_raw).hexdigest()
        manifest_gen = '101'

        contract = copy.deepcopy(fixtures.contract)
        contract.update({
            'schema_version': core.CONTRACT_SCHEMA,
            'phase': 5, 'phase_5_closed': False,
            'bronze': {'status': 'BRONZE_PUBLICADO'},
            'silver_publication_allowed': False,
            'authorization_protocol': {
                'schema_version': core.AUTH_SCHEMA,
                'claim_create_if_generation_match': 0,
                'single_use_required': True, 'max_validity_seconds': 14400,
            },
            'cryptographic_protocol': {'public_key_spki_sha256': fixtures.pin},
        })
        contract['consumer'].update({
            'allowed_operation': 'BUILD_SILVER_STAGING',
            'official_publication_allowed': False,
            'silver_bucket': launch.SILVER_BUCKET,
        })
        for ds in contract['datasets']:
            ds.update({'run_id': launch.RUN, 'files_count': 0,
                       'total_bytes': 0, 'bronze_input_uris': []})
        target = next(d for d in contract['datasets'] if d['dataset'] == launch.DATASET)
        target.update({'files_count': len(entries), 'total_bytes': total,
                       'bronze_input_uris': [e['uri'] for e in entries],
                       'bronze_manifest_generation': manifest_gen,
                       'bronze_manifest_sha256': manifest_sha})
        # Completar totales generales en otra fuente (solo para contract fixture).
        other = next(d for d in contract['datasets'] if d['dataset'] != launch.DATASET)
        other.update({'files_count': 3599 - len(entries),
                      'total_bytes': 393623640 - total,
                      'bronze_input_uris': [f'gs://synthetic/other-{i}'
                                            for i in range(3599 - len(entries))]})
        contract_raw = core.canonical_bytes(contract)
        contract_sha = sha256(contract_raw).hexdigest()

        # Reconstruir la cadena historica/extension con hashes del contrato
        # sintetico. Se mantiene la logica REAL de validacion y firma P256.
        fixtures.contract = contract
        fixtures.contract_raw = contract_raw
        fixtures.now = now
        fixtures.hist_annex['base_contract_sha256'] = contract_sha
        fixtures.hist_closure['base_contract_sha256'] = contract_sha
        fixtures.hist_closure['operational_annex_sha256'] = gate.digest(
            gate.canonical(fixtures.hist_annex))
        fixtures.annex = gate.expected_documental_annex(
            fixtures.hist_annex, gate.digest(gate.canonical(fixtures.hist_annex)),
            gate.digest(gate.canonical(fixtures.hist_closure)))
        fixtures.closure = gate.expected_documental_closure(
            fixtures.hist_closure, gate.canonical(fixtures.annex),
            gate.digest(gate.canonical(fixtures.hist_annex)),
            gate.digest(gate.canonical(fixtures.hist_closure)),
            (now - timedelta(minutes=10)).isoformat())
        fixtures.auth.update({
            'base_contract_sha256': contract_sha,
            'attempt_id': 'attempt-abc123',
            'not_before_utc': (now-timedelta(minutes=5)).isoformat(),
            'expires_at_utc': (now+timedelta(minutes=20)).isoformat(),
            'silver_outputs': [{'name': launch.PRODUCT,
                'staging_prefix': launch.expected_staging(launch.RUN, 'attempt-abc123')}],
            'bronze_dependencies': [{'dataset': launch.DATASET,
                'manifest_uri': target['bronze_manifest_uri'],
                'manifest_generation': manifest_gen,
                'manifest_sha256': manifest_sha}],
            'operational_annex_sha256': gate.digest(gate.canonical(fixtures.annex)),
            'phase5_closure_sha256': gate.digest(gate.canonical(fixtures.closure)),
        })

        client = AlmacenGCSOffline(contract['consumer']['service_account'])
        for uri, data in payloads.items():
            client.add(uri, data, '1795680000000000')
        client.add(target['bronze_manifest_uri'], manifest_raw, manifest_gen)
        refs, documents = fixtures._fake_trusted_bundle()
        auth_uri, sig_uri = gate.authorization_paths(
            fixtures.auth['mode'], fixtures.auth.get('dataset'),
            fixtures.auth.get('silver_product'), fixtures.auth['run_id'],
            fixtures.auth['attempt_id'])
        auth_raw, auth_sig = fixtures.signed(fixtures.auth)
        documents[auth_uri] = auth_raw
        documents[sig_uri] = auth_sig
        refs['authorization'] = {
            'uri': auth_uri,
            'signature_uri': sig_uri,
            'generation': '101',
            'sha256': sha256(auth_raw).hexdigest(),
            'signature_generation': '102',
            'signature_sha256': sha256(auth_sig).hexdigest(),
        }
        for info in refs.values():
            for uri_key, gen_key in (
                ('uri', 'generation'), ('signature_uri', 'signature_generation')):
                uri = info[uri_key]
                client.add(uri, documents[uri], info[gen_key])

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            def save(filename, raw):
                x = p / filename
                x.write_bytes(raw)
                return x

            deps = save('dependencies.zip', b'ZIP_SYNTHETIC_OFFLINE_ONLY')
            context = candidate_context()
            context.update({
                'service_account': contract['consumer']['service_account'],
                'bronze_manifest_uri': target['bronze_manifest_uri'],
                'bronze_manifest_generation': manifest_gen,
                'bronze_manifest_sha256': manifest_sha,
                'authorization_fingerprint': gate.fingerprint(fixtures.auth),
                'entrypoint_sha256': sha256(Path(launch.__file__).read_bytes()).hexdigest(),
                'dependencies_sha256': sha256(deps.read_bytes()).hexdigest(),
            })
            root = (f'gs://{launch.ARTIFACT_BUCKET}/bvl-phase6/releases/'
                    + context['processor_commit'] + '/')
            client.add(root + 'fase6_hechos_launcher_documental_v1.py',
                       Path(launch.__file__).read_bytes(),
                       context['entrypoint_generation'])
            client.add(root + 'fase6_hechos_documental_dependencias.zip',
                       deps.read_bytes(), context['dependencies_generation'])

            base = (f'gs://{launch.CONTROL_BUCKET}/authorizations/silver_documental/'
                    f"run_id={context['run_id']}/attempt_id={context['attempt_id']}/")
            def signed_refs(prefix, name, obj):
                raw, sig = fixtures.signed(obj)
                uri, siguri = prefix+name+'.json', prefix+name+'.sig'
                client.add(uri, raw, '101')
                client.add(siguri, sig, '102')
                return {'uri': uri, 'generation': '101',
                        'sha256': sha256(raw).hexdigest(),
                        'signature_uri': siguri, 'signature_generation': '102',
                        'signature_sha256': sha256(sig).hexdigest()}

            context_refs = signed_refs(base, 'launch_context', context)
            context_sha = context_refs['sha256']
            iam = sample_attestation(context)
            iam['launch_context_sha256'] = context_sha
            for field, raw_doc in (
                ('allow_policy', lambda x: {'version': x['version'],
                    'etag': x['etag'], 'bindings': x['bindings']}),
                ('role_definition', lambda x: {'name': x['name'],
                    'includedPermissions': x['includedPermissions']}),
                ('independent_deny_review', lambda x: {'principal': x['principal'],
                    'permission': x['permission'],
                    'objects_sha256': x['objects_sha256'],
                    'outcome': x['outcome'],
                    'evaluated_at_utc': x['evaluated_at_utc']}),
            ):
                item = iam[field]
                raw = core.canonical_bytes(raw_doc(item))
                item['source_sha256'] = sha256(raw).hexdigest()
                client.add(item['source_uri'], raw, item['source_generation'])
            iam_refs = signed_refs(base, 'iam_scope_attestation', iam)

            def json_file(name, obj):
                return save(name, core.canonical_bytes(obj))
            args = SimpleNamespace(
                ack_supervised=True,
                contract=save('contract.json', contract_raw),
                quality_public_pem=save('quality.pem', fixtures.pem),
                trusted_refs=json_file('trusted.json', refs),
                launch_refs=json_file('launch.json', context_refs),
                iam_attestation_refs=json_file('iam.json', iam_refs),
                dependencies_zip=deps,
                attempt_id=context['attempt_id'],
                processor_commit=context['processor_commit'],
                batch_id=context['batch_id'],
            )

            # Impedir completamente que el primer call-site de consumo se ejecute.
            # No se sustituye ningun validador ni funcion de preflight.
            module_google = types.ModuleType('google')
            module_cloud = types.ModuleType('google.cloud')
            module_storage = types.ModuleType('google.cloud.storage')
            module_storage.Client = lambda **kw: client
            module_google.cloud = module_cloud
            module_cloud.storage = module_storage
            modules = {'google': module_google,
                       'google.cloud': module_cloud,
                       'google.cloud.storage': module_storage}
            with patch.object(core, 'QUALITY_SPKI_PIN', fixtures.pin), \
                 patch.object(gate, 'QUALITY_PIN', fixtures.pin), \
                 patch.object(gate, 'CONTRACT_SHA256', contract_sha), \
                 patch.object(launch, 'BRONZE_MANIFEST_SHA', manifest_sha), \
                 patch.object(launch, 'BRONZE_MANIFEST_GENERATION', manifest_gen), \
                 patch.dict(sys.modules, modules), \
                 patch.object(gate, 'consume_once', side_effect=LimiteAnteriorAlClaim) as claim:
                with self.assertRaises(LimiteAnteriorAlClaim):
                    launch.execute(args)
                claim.assert_called_once()
                self.assertEqual(client.writes, 0)
                self.assertGreater(client.reads, 1979)
                self.assertEqual(client.lists, 1)

        print('PREFLIGHT_REAL_MODULES: OK')
        print('CLAIM_INTERCEPTADO_ANTES_CONSUMO: SI')
        print('GCS_REAL: NO | CLAIMS_CONSUMIDOS: 0 | WRITES: 0')


if __name__ == '__main__':
    unittest.main()
