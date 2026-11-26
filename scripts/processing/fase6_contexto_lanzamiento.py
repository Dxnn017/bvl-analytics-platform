#!/usr/bin/env python3
"""BVL F6: enlace al contexto de lanzamiento firmado por Quality.

No emite autorizaciones ni consume claims. La puerta de Fase 5 es externa e
inmutable; este control adicional fija commit, artefactos y entorno del batch.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import re
import sys

SCHEMA = 'BVL-SILVER-LAUNCH-CONTEXT-v1'
CONTROL_BUCKET = 'bvl26-control-7f3a9c'
PROJECT = 'bvl-analytics-2026'
REGION = 'southamerica-west1'
DATASET = 'bcrp/tipo_cambio'
PRODUCT = 'macroeconomia/tipo_cambio'
SERVICE_ACCOUNT = 'sa-bvl-silver@bvl-analytics-2026.iam.gserviceaccount.com'
COMMIT_RE = re.compile(r'^[0-9a-f]{40}$')
HEX_RE = re.compile(r'^[0-9a-f]{64}$')
BATCH_RE = re.compile(r'^[a-z][a-z0-9-]{3,62}$')
KEYS = {
    'schema_version', 'status', 'quality_approved', 'project_id', 'region',
    'dataset', 'product', 'operation', 'run_id', 'attempt_id',
    'authorization_fingerprint', 'processor_commit', 'entrypoint_sha256',
    'dependencies_sha256', 'entrypoint_uri', 'entrypoint_generation',
    'dependencies_uri', 'dependencies_generation', 'runtime_version',
    'service_account', 'batch_id', 'subnet', 'driver_cores',
    'executor_cores', 'executor_instances', 'ttl_seconds',
    'not_before_utc', 'expires_at_utc', 'overwrite_allowed',
    'automatic_retry_allowed', 'official_publication_allowed',
}

class LaunchError(ValueError):
    pass


def must(value, why):
    if not value:
        raise LaunchError(why)


def sha(raw):
    return sha256(raw).hexdigest()


def verify_payload(doc, *, now=None):
    """Valida contenido firmado, sin efectos laterales ni accesos cloud."""
    from puerta_silver_v1_candidata import utc
    must(isinstance(doc, dict) and set(doc) == KEYS, 'Campos del contexto Quality incompletos/extra')
    expected = {
        'schema_version': SCHEMA,
        'status': 'QUALITY_APPROVED_SUPERVISED_LAUNCH',
        'quality_approved': True,
        'project_id': PROJECT,
        'region': REGION,
        'dataset': DATASET,
        'product': PRODUCT,
        'operation': 'BUILD_SILVER_STAGING',
        'runtime_version': '2.2',
        'service_account': SERVICE_ACCOUNT,
        'overwrite_allowed': False,
        'automatic_retry_allowed': False,
        'official_publication_allowed': False,
        'driver_cores': 4,
        'executor_cores': 4,
        'executor_instances': 2,
    }
    for key, value in expected.items():
        must(doc.get(key) == value and type(doc.get(key)) is type(value),
             'Campo Quality no permitido: ' + key)
    must(type(doc['ttl_seconds']) is int and 60 <= doc['ttl_seconds'] <= 3600,
         'TTL no autorizado')
    must(doc['driver_cores'] + doc['executor_cores'] * doc['executor_instances'] <= 12,
         'Cuota de 12 vCPU excedida')
    for name in ('entrypoint_sha256', 'dependencies_sha256', 'authorization_fingerprint'):
        must(isinstance(doc[name], str) and HEX_RE.fullmatch(doc[name]), 'SHA invalido: ' + name)
    must(isinstance(doc['processor_commit'], str) and COMMIT_RE.fullmatch(doc['processor_commit']),
         'Commit no es identificador Git completo')
    must(isinstance(doc['run_id'], str) and doc['run_id'] == 'bvl-bronze-inicial-20261125-001',
         'Run fuera de lote oficial')
    must(isinstance(doc['attempt_id'], str) and re.fullmatch(r'[a-z0-9][a-z0-9-]{5,100}', doc['attempt_id']),
         'Intento invalido')
    must(isinstance(doc['batch_id'], str) and BATCH_RE.fullmatch(doc['batch_id']),
         'Batch ID no cumple Dataproc')
    must(isinstance(doc['subnet'], str) and doc['subnet'].startswith(
        'projects/') and '/regions/southamerica-west1/subnetworks/' in doc['subnet'],
        'Subred regional no fijada')
    for key, suffix in (('entrypoint_uri', '/fase6_tipo_cambio_batch.py'),
                        ('dependencies_uri', '/fase6_dependencias.zip')):
        must(isinstance(doc[key], str) and doc[key].startswith(
            'gs://bvl26-dataproc-staging-7f3a9c/bvl-phase6/releases/') and doc[key].endswith(suffix),
            'Artefacto fuera de prefijo aprobado: ' + key)
    for key in ('entrypoint_generation', 'dependencies_generation'):
        must(isinstance(doc[key], (int, str)) and not isinstance(doc[key], bool) and
             str(doc[key]).isascii() and str(doc[key]).isdecimal() and int(doc[key]) > 0,
             'Generacion de release invalida')
    nb, exp = utc(doc['not_before_utc']), utc(doc['expires_at_utc'])
    now = now or datetime.now(timezone.utc)
    must(nb <= now <= exp and 0 < (exp - nb).total_seconds() <= 14400,
         'Contexto de Quality no vigente')
    return doc


def validate_refs(refs, run_id, attempt_id):
    """Referencias independientes provistas por Quality (no autoafirmadas)."""
    must(isinstance(refs, dict) and set(refs) == {
        'uri', 'generation', 'sha256', 'signature_uri',
        'signature_generation', 'signature_sha256'}, 'Referencias Quality incompletas')
    prefix = (f'gs://{CONTROL_BUCKET}/authorizations/silver_launch/'
              f'run_id={run_id}/attempt_id={attempt_id}/')
    must(refs['uri'] == prefix + 'launch_context.json' and
         refs['signature_uri'] == prefix + 'launch_context.sig',
         'Rutas Quality no coinciden con intento')
    for k in ('sha256', 'signature_sha256'):
        must(isinstance(refs[k], str) and HEX_RE.fullmatch(refs[k]), 'SHA referencia Quality invalido')
    for k in ('generation', 'signature_generation'):
        must(str(refs[k]).isascii() and str(refs[k]).isdecimal() and int(refs[k]) > 0,
             'Generacion referencia Quality invalida')
    return refs


def verify_signed_launch(client, refs, pem, expected_run, expected_attempt, now=None):
    """Lee generaciones exactas en Control y verifica la firma P256 fijada por F5."""
    import adaptador_gcs_silver_v1 as gcs
    import puerta_silver_operativa_v1_candidata as gate
    validate_refs(refs, expected_run, expected_attempt)
    raw = gcs.read_exact_current_bytes(client, refs['uri'], refs['generation'],
                                       refs['sha256'], allowed_bucket=CONTROL_BUCKET)
    sig = gcs.read_exact_current_bytes(client, refs['signature_uri'],
                                       refs['signature_generation'], refs['signature_sha256'],
                                       allowed_bucket=CONTROL_BUCKET)
    doc = gate.signed(raw, sig, pem)  # Fase 5: canonical, pin P256 y firma DER
    verify_payload(doc, now=now)
    must(doc['run_id'] == expected_run and doc['attempt_id'] == expected_attempt,
         'Lanzamiento firmado corresponde a otro intento')
    return doc, sha(raw)


def archive_path(module):
    """Exige módulos importados exclusivamente desde un ZIP de --py-files."""
    full = str(getattr(module, '__file__', '')).replace('\\', '/')
    marker = '.zip/'
    must(marker in full, 'Modulo ejecutado fuera del ZIP aprobado: ' + getattr(module, '__name__', '?'))
    path = Path(full.split(marker, 1)[0] + '.zip')
    must(path.is_file(), 'No se encontro ZIP local del runtime')
    return path


def verify_runtime_artifacts(doc, main_path, modules):
    entry = Path(main_path)
    must(entry.is_file() and sha(entry.read_bytes()) == doc['entrypoint_sha256'],
         'Entrypoint no coincide con hash firmado por Quality')
    archives = {archive_path(module) for module in modules}
    must(len(archives) == 1, 'Modulos provenientes de archivos ZIP diferentes')
    arc = next(iter(archives))
    must(sha(arc.read_bytes()) == doc['dependencies_sha256'],
         'Dependencias no coinciden con hash firmado por Quality')
    return str(arc)


def verify_runtime_libs():
    """No crea SparkSession ni consume claim; falla antes del primer uso."""
    import pyarrow as pa
    import google.cloud.storage as storage
    import pyspark
    must(sys.version_info[:2] == (3, 12), 'Runtime Python distinto a 3.12')
    must(pyspark.__version__.startswith('3.5.'), 'Spark distinto de 3.5.x')
    must(pa.__version__.startswith('15.'), 'PyArrow distinto de 15.x')
    must(storage.__version__.startswith('2.16.'), 'google-cloud-storage distinto de 2.16.x')
    must(pa.Codec.is_available('snappy'), 'Snappy no disponible en PyArrow')
    return {'python': '.'.join(map(str, sys.version_info[:3])),
            'pyspark': pyspark.__version__, 'pyarrow': pa.__version__,
            'google_cloud_storage': storage.__version__, 'snappy': 'OK'}


def bind_launch_to_plan(doc, plan, *, processor_commit, batch_id):
    auth = plan.authorization
    must(doc['processor_commit'] == processor_commit,
         'Commit del argumento no corresponde a firma Quality')
    must(doc['batch_id'] == batch_id, 'Batch ID no coincide con firma Quality')
    must(doc['authorization_fingerprint'] == plan.fingerprint,
         'Fingerprint de autorizacion distinto al contexto de lanzamiento')
    must(doc['run_id'] == auth['run_id'] and doc['attempt_id'] == auth['attempt_id'],
         'Contexto no corresponde a autorizacion GCS verificada')
    must(auth['operation'] == doc['operation'] and auth['dataset'] == doc['dataset'] and
         auth['service_account'] == doc['service_account'],
         'Scope firmado Quality distinto al de autorizacion')
    return True
