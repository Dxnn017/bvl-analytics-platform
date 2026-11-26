#!/usr/bin/env python3
"""Quality launch context RESTANTES v1 (PROPOSAL, not ratified).

Only verifies; no GCS writes, signature generation, IAM, or claim creation.
Quality must explicitly ratify this schema and sign each SINGLE_SOURCE attempt.
The 41266f0 gate is imported, never patched.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import re

from fase6_restantes_core import (
    SOURCES, RUN_REFERENCE, CANDIDATE_TRANSFORMS,
    expected_prefix, require, QUALITY_SERVICE_ACCOUNT,
)

SCHEMA = 'BVL-SILVER-LAUNCH-CONTEXT-REST-v1'
CONTROL_BUCKET = 'bvl26-control-7f3a9c'
TEMP_BUCKET = 'bvl26-dataproc-staging-7f3a9c'
PROJECT = 'bvl-analytics-2026'
REGION = 'southamerica-west1'
SUPPORTED = frozenset(CANDIDATE_TRANSFORMS | {
    'smv/valores_inscritos', 'smv/hechos_valores_consulta'
})
HEX64 = re.compile(r'[0-9a-f]{64}\Z')
HEX40 = re.compile(r'[0-9a-f]{40}\Z')
BATCH = re.compile(r'[a-z][a-z0-9-]{3,62}\Z')
SEGMENT = re.compile(r'[a-z0-9][a-z0-9-]{5,100}\Z')
FIELDS = frozenset({
    'schema_version', 'status', 'quality_approved', 'project_id', 'region',
    'mode', 'dataset', 'silver_outputs', 'operation', 'run_id', 'attempt_id',
    'authorization_fingerprint', 'processor_commit',
    'entrypoint_sha256', 'dependencies_sha256',
    'entrypoint_uri', 'entrypoint_generation',
    'dependencies_uri', 'dependencies_generation',
    'runtime_version', 'service_account', 'batch_id', 'subnet',
    'driver_cores', 'executor_cores', 'executor_instances', 'ttl_seconds',
    'not_before_utc', 'expires_at_utc', 'overwrite_allowed',
    'automatic_retry_allowed', 'official_publication_allowed',
})


def digest(raw):
    return sha256(raw).hexdigest()


def _utc(value):
    require(isinstance(value, str), 'Tiempo no es string UTC')
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as ex:
        raise ValueError('Fecha UTC invalida') from ex
    require(dt.tzinfo is not None and dt.utcoffset().total_seconds() == 0,
            'Fecha debe llevar zona UTC')
    return dt.astimezone(timezone.utc)


def positive_gen(value):
    require(type(value) in (str, int) and str(value).isascii() and
            str(value).isdecimal() and int(value) > 0, 'Generacion invalida')
    return int(value)


def validate_payload(doc, *, now=None):
    """Strict shape + authorization scope, but NOT a signature check."""
    require(isinstance(doc, dict) and set(doc) == FIELDS,
            'Contexto de lanzamiento REST con campos extra/faltantes')
    fixed = {
        'schema_version': SCHEMA,
        'status': 'QUALITY_APPROVED_SUPERVISED_LAUNCH',
        'quality_approved': True,
        'project_id': PROJECT,
        'region': REGION,
        'mode': 'SINGLE_SOURCE',
        'operation': 'BUILD_SILVER_STAGING',
        'run_id': RUN_REFERENCE,
        'runtime_version': '2.2',
        'service_account': QUALITY_SERVICE_ACCOUNT,
        'driver_cores': 4,
        'executor_cores': 4,
        'executor_instances': 2,
        'overwrite_allowed': False,
        'automatic_retry_allowed': False,
        'official_publication_allowed': False,
    }
    for key, value in fixed.items():
        require(doc[key] == value and type(doc[key]) is type(value),
                f'Campo firmado incompatible: {key}')
    ds = doc['dataset']
    require(isinstance(ds, str) and ds in SUPPORTED,
            'Dataset no disponible o Hechos bloqueado')
    attempt = doc['attempt_id']
    require(isinstance(attempt, str) and SEGMENT.fullmatch(attempt),
            'Attempt ID invalido')
    outputs = doc['silver_outputs']
    require(isinstance(outputs, list) and len(outputs) == len(SOURCES[ds]) and
            all(isinstance(o, dict) and set(o) == {'name', 'staging_prefix'} for o in outputs),
            'Salidas del contexto no ratificadas')
    actual = {o['name']: o['staging_prefix'] for o in outputs}
    expected = {product: expected_prefix(product, RUN_REFERENCE, attempt)
                for product in SOURCES[ds]}
    require(actual == expected and len(actual) == len(outputs),
            'Salidas fuera de scope o duplicadas')
    for key in ('authorization_fingerprint','entrypoint_sha256','dependencies_sha256'):
        require(isinstance(doc[key], str) and HEX64.fullmatch(doc[key]),
                f'SHA invalido: {key}')
    commit = doc['processor_commit']
    require(isinstance(commit, str) and HEX40.fullmatch(commit),
            'Processor commit no es SHA-1 de Git completo')
    require(commit != '41266f0efba4477c30266e19ca3280838ff3d802',
            'Commit de puerta F5 no representa el nuevo procesador')
    require(isinstance(doc['batch_id'], str) and BATCH.fullmatch(doc['batch_id']),
            'Batch ID invalido')
    require(isinstance(doc['subnet'], str) and doc['subnet'].startswith('projects/') and
            '/regions/southamerica-west1/subnetworks/' in doc['subnet'],
            'Subred regional no fijada')
    require(type(doc['ttl_seconds']) is int and 60 <= doc['ttl_seconds'] <= 3600,
            'TTL fuera de rango')
    require(doc['driver_cores'] + doc['executor_cores'] * doc['executor_instances'] <= 12,
            'Cuota Dataproc excedida')
    root = f'gs://{TEMP_BUCKET}/bvl-phase6/releases/{commit}/'
    require(doc['entrypoint_uri'] == root+'fase6_restantes_batch.py' and
            doc['dependencies_uri'] == root+'fase6_restantes_dependencias.zip',
            'Artefacto GCS fuera de release fijado')
    for key in ('entrypoint_generation', 'dependencies_generation'):
        positive_gen(doc[key])
    t0, t1 = _utc(doc['not_before_utc']), _utc(doc['expires_at_utc'])
    instant = now or datetime.now(timezone.utc)
    require(t0 <= instant <= t1 and 0 < (t1 - t0).total_seconds() <= 14400,
            'Contexto no vigente')
    return doc


def validate_refs(refs, run_id, attempt_id):
    require(isinstance(refs, dict) and set(refs) == {
        'uri','generation','sha256','signature_uri',
        'signature_generation','signature_sha256'},
        'Referencias independientes Quality incompletas')
    prefix = (f'gs://{CONTROL_BUCKET}/authorizations/silver_launch/'
              f'run_id={run_id}/attempt_id={attempt_id}/')
    require(refs['uri'] == prefix+'launch_context.json' and
            refs['signature_uri'] == prefix+'launch_context.sig',
            'Referencia launch no vinculada al intento')
    for key in ('sha256', 'signature_sha256'):
        require(isinstance(refs[key], str) and HEX64.fullmatch(refs[key]),
                'Hash de referencia Quality invalido')
    for key in ('generation','signature_generation'):
        positive_gen(refs[key])
    return True


def verify_signed_launch(client, refs, pem, *, expected_run, expected_attempt):
    """Only external Quality refs, exact generations and KMS-pinned F5 verifier."""
    import adaptador_gcs_silver_v1 as gcs
    import puerta_silver_operativa_v1_candidata as gate
    validate_refs(refs, expected_run, expected_attempt)
    data = gcs.read_exact_current_bytes(client,refs['uri'],refs['generation'],
            refs['sha256'],allowed_bucket=CONTROL_BUCKET)
    sig = gcs.read_exact_current_bytes(client,refs['signature_uri'],
            refs['signature_generation'],refs['signature_sha256'],
            allowed_bucket=CONTROL_BUCKET)
    doc = gate.signed(data, sig, pem)
    validate_payload(doc)
    require(doc['run_id'] == expected_run and doc['attempt_id'] == expected_attempt,
            'Contexto firmado para otro intento')
    return doc, digest(data)


def bind_launch_to_plan(doc, plan, *, dataset, processor_commit, batch_id):
    """Call ONLY after gate.check_gcs_signed_bundle produced a GCSVerifiedPlan."""
    import puerta_silver_operativa_v1_candidata as gate
    from fase6_restantes_core import validate_authorized_scope
    require(type(plan) is gate.GCSVerifiedPlan,
            'Plan no obtenido de preflight GCS real')
    auth = plan.authorization
    expected = validate_authorized_scope(auth, dataset)
    require(doc['dataset'] == dataset and doc['processor_commit'] == processor_commit and
            doc['batch_id'] == batch_id, 'Contexto no vinculado al batch')
    require(doc['authorization_fingerprint'] == plan.fingerprint and
            doc['run_id'] == auth['run_id'] and doc['attempt_id'] == auth['attempt_id'] and
            doc['operation'] == auth['operation'] and
            doc['service_account'] == auth['service_account'],
            'Contexto y autorizacion F5 no coinciden')
    require({o['name']:o['staging_prefix'] for o in doc['silver_outputs']} == expected,
            'Dos salidas o staging no coinciden con firma Quality')
    return True


def verify_runtime_artifacts(doc, main_path, modules):
    """Check module provenance from one exact py-files archive; no GCS."""
    import fase6_contexto_lanzamiento as old_launch
    return old_launch.verify_runtime_artifacts(doc, main_path, modules)


def verify_runtime_libs():
    import fase6_contexto_lanzamiento as old_launch
    return old_launch.verify_runtime_libs()
