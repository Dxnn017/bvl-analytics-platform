#!/usr/bin/env python3
"""Launcher CANDIDATO para Hechos Documental Restringido.

PRODUCCION BLOQUEADA hasta que Quality emita NUEVA autorizacion F5 y
contexto de lanzamiento firmados especificamente para este producto.
Ninguna firma o claim de hechos_importancia/eventos se admite.

F5 permanece inmutable: este modulo llama la puerta existente. El paquete
incluye preflight, lectura GCS fijada, salida staging create-only y reporte
candidato; las llamadas a GCS estan guardadas tras --execute y firmas validas.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
for f in (ROOT / 'scripts' / 'pipeline', ROOT / 'scripts' / 'processing'):
    if f.is_dir():
        sys.path.insert(0, str(f))

import fase6_hechos_launch_scope_candidato as scope

PROJECT = 'bvl-analytics-2026'
REGION = 'southamerica-west1'
PRODUCT = 'hechos_importancia/documental_restringido'
DATASET = 'smv/hechos_importancia'
BLOCKED_PRODUCT = 'hechos_importancia/eventos'
BRONZE_BUCKET = 'bvl26-bronze-7f3a9c'
SILVER_BUCKET = 'bvl26-silver-7f3a9c'
CONTROL_BUCKET = 'bvl26-control-7f3a9c'
ARTIFACT_BUCKET = 'bvl26-dataproc-staging-7f3a9c'
RUN = 'bvl-bronze-inicial-20261125-001'
BRONZE_MANIFEST_SHA = 'd2c5f5d5a363e271a770497b4af6feedde454f999bc2fe7696a5c0ab0784eab0'
BRONZE_MANIFEST_GENERATION = '1795680153321464'
SCHEMA = 'BVL-SILVER-DOCUMENTAL-LAUNCH-CONTEXT-CANDIDATE-v1'
CANDIDATE_SCHEMA = 'BVL-SILVER-DOCUMENTAL-STAGING-CANDIDATE-v1'
HEX = re.compile(r'[0-9a-f]{64}\Z')
COMMIT = re.compile(r'[0-9a-f]{40}\Z')
IDENT = re.compile(r'[a-zA-Z0-9][a-zA-Z0-9_-]{5,100}\Z')
MAX_TOTAL_SOURCE_BYTES = 1024 * 1024 * 1024
MAX_PART_BYTES = 64 * 1024 * 1024
MAX_PARQUET_ROWS = 125
SCHEMA_FIELDS = (
    'guid_documento_archivo','nombre_pdf','ruta_pdf_bronze','generacion_pdf_bronze',
    'sha256_pdf','tamano_pdf_bytes','fecha_nominal_archivo','fecha_nominal_no_es_fecha_oficial_hecho',
    'duplicado_por_sha256','nombre_pdf_canonico','nombre_txt','ruta_txt_bronze',
    'generacion_txt_bronze','sha256_txt','tamano_txt_bytes','origen_texto',
    'texto_extraido','caracteres_txt_verificados','estado_calidad_txt_propuesto','verificacion'
)
BOOL_FIELDS=frozenset({'fecha_nominal_no_es_fecha_oficial_hecho','duplicado_por_sha256'})
LONG_FIELDS=frozenset({'tamano_pdf_bytes','tamano_txt_bytes','caracteres_txt_verificados'})


def schema_spec():
    """Tipo de las 20 columnas, independiente de que PyArrow este instalado."""
    return [{'name':k, 'type': ('boolean' if k in BOOL_FIELDS else
                                 'int64' if k in LONG_FIELDS else 'string')}
            for k in SCHEMA_FIELDS]


def validate_part_capacity(index):
    """Preclaim: limite conservador por particion antes de gastar un intento."""
    require(type(index) is list and len(index)==990, 'INDEX_INCOMPLETO')
    for offset in range(0,990,MAX_PARQUET_ROWS):
        part=index[offset:offset+MAX_PARQUET_ROWS]
        text_bytes=sum(x['txt']['tamano_bytes'] for x in part if x['txt'] is not None)
        # Margen conservador para metadatos y expansion del contenido.
        require(text_bytes*3//2 + 2*1024*1024 <= MAX_PART_BYTES,
                'LIMITE_PARQUET_PRECLAIM_EXCEDIDO')
    return True

# Contexto CANDIDATO: los nombres y serializacion final requieren ratificacion.
CONTEXT_KEYS = frozenset({
    'schema_version', 'status', 'quality_approved', 'project_id', 'region',
    'source_dataset', 'product', 'bronze_run_id', 'bronze_manifest_uri',
    'bronze_manifest_generation', 'bronze_manifest_sha256',
    'run_id', 'attempt_id', 'batch_id', 'processor_commit',
    'authorization_fingerprint', 'entrypoint_uri', 'entrypoint_generation',
    'entrypoint_sha256', 'dependencies_uri', 'dependencies_generation',
    'dependencies_sha256', 'service_account', 'staging_prefix',
    'not_before_utc', 'expires_at_utc', 'schema_id', 'runtime_version',
    'overwrite_allowed', 'delete_allowed', 'automatic_retry_allowed',
    'official_publication_allowed', 'gold_consumption_allowed',
    'operation', 'mode',
})


def require(condition, msg):
    if not condition:
        raise ValueError(msg)


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf-8')


def _generation(value):
    require(type(value) in (str, int) and str(value).isascii() and
            str(value).isdecimal() and int(value) > 0, 'GENERACION_INVALIDA')
    return str(value)


def _utc(v):
    require(type(v) is str, 'FECHA_UTC_INVALIDA')
    try:
        val = datetime.fromisoformat(v.replace('Z', '+00:00'))
    except ValueError as exc:
        raise ValueError('FECHA_UTC_INVALIDA') from exc
    require(val.tzinfo is not None and val.utcoffset().total_seconds() == 0,
            'FECHA_NO_UTC')
    return val.astimezone(timezone.utc)


def expected_staging(run, attempt):
    require(all(type(v) is str and IDENT.fullmatch(v) for v in (run, attempt)),
            'RUN_O_ATTEMPT_INVALIDO')
    return (f'gs://{SILVER_BUCKET}/staging/{PRODUCT}/'
            f'run_id={run}/attempt_id={attempt}/')


def validate_context(doc, *, now=None):
    """Valida SHAPE, nunca convierte JSON local en autorizacion firmada."""
    require(type(doc) is dict and set(doc) == CONTEXT_KEYS,
            'CONTEXTO_CAMPOS_DISTINTOS_DEL_CONTRATO')
    fixed = {
        'schema_version': SCHEMA,
        'status': 'QUALITY_APPROVED_DOCUMENTAL_SUPERVISED_LAUNCH',
        'quality_approved': True, 'project_id': PROJECT, 'region': REGION,
        'source_dataset': DATASET, 'product': PRODUCT,
        'bronze_run_id': RUN, 'bronze_manifest_generation': BRONZE_MANIFEST_GENERATION,
        'bronze_manifest_sha256': BRONZE_MANIFEST_SHA,
        'mode': 'SINGLE_SOURCE', 'operation': 'BUILD_SILVER_STAGING',
        'schema_id': 'BVL-HECHOS-DOCUMENTAL-RESTRINGIDO-CANDIDATO-v1',
        'runtime_version': '2.2',
        'overwrite_allowed': False, 'delete_allowed': False,
        'automatic_retry_allowed': False, 'official_publication_allowed': False,
        'gold_consumption_allowed': False,
    }
    for key, val in fixed.items():
        require(type(doc[key]) is type(val) and doc[key] == val,
                'CONTEXT_SCOPE_INVALIDO:' + key)
    require(type(doc['bronze_manifest_uri']) is str and
            doc['bronze_manifest_uri'].startswith(f'gs://{CONTROL_BUCKET}/') and
            '/staging/' not in doc['bronze_manifest_uri'] and
            '/current.json' not in doc['bronze_manifest_uri'],
            'REFERENCIA_MANIFEST_NO_OFICIAL')
    require(type(doc['service_account']) is str and
            doc['service_account'].split('@')[0] == 'sa-bvl-silver',
            'CUENTA_SILVER_NO_AUTORIZADA')
    for key in ('authorization_fingerprint', 'entrypoint_sha256', 'dependencies_sha256'):
        require(type(doc[key]) is str and HEX.fullmatch(doc[key]),
                'HASH_CONTEXT_INVALIDO:' + key)
    require(type(doc['processor_commit']) is str and COMMIT.fullmatch(doc['processor_commit']),
            'COMMIT_INVALIDO')
    for key in ('entrypoint_generation','dependencies_generation'):
        _generation(doc[key])
    require(all(type(doc[k]) is str and IDENT.fullmatch(doc[k])
                for k in ('run_id','attempt_id','batch_id')), 'ID_CONTEXT_INVALIDO')
    require(doc['staging_prefix'] == expected_staging(doc['run_id'],doc['attempt_id']),
            'STAGING_FUERA_DE_ALCANCE')
    root = (f'gs://{ARTIFACT_BUCKET}/bvl-phase6/releases/'
            f"{doc['processor_commit']}/")
    require(doc['entrypoint_uri'] == root + 'fase6_hechos_launcher_documental_v1.py' and
            doc['dependencies_uri'] == root + 'fase6_hechos_documental_dependencias.zip',
            'ARTEFACTOS_FUERA_DE_RELEASE')
    start, end = _utc(doc['not_before_utc']), _utc(doc['expires_at_utc'])
    instant = now or datetime.now(timezone.utc)
    require(start <= instant <= end and 0 < (end - start).total_seconds() <= 14400,
            'CONTEXTO_EXPIRADO_O_DURACION_INVALIDA')
    return doc


def validate_official_manifest(manifest, entries):
    """Exige objeto oficial completo; no acepta CSV historicos ni un manifest local como firma."""
    require(type(manifest) is dict and manifest.get('dataset') == DATASET and
            manifest.get('run_id') == RUN and
            manifest.get('quality_approved') is True and
            manifest.get('status') == 'BRONZE_COMPLETADO',
            'MANIFEST_BRONZE_NO_OFICIAL')
    require(manifest.get('files') == entries, 'OBJETOS_DISTINTOS_DEL_MANIFEST')
    summary = scope.validate_inventory(entries)
    require(summary['source_object_count'] == 1979 and
            summary['pdf_count'] == 990 and summary['txt_count'] == 989,
            'INVENTARIO_INCOMPLETO')
    require(sum(e['size_bytes'] for e in entries) <= MAX_TOTAL_SOURCE_BYTES,
            'CAPACIDAD_SOURCE_EXCEDIDA')
    return summary


def candidate_from_verified_rows(context, entries, rows, metrics, receipts, *,
                                 claim_generation, context_sha, manifest_ref):
    """Genera manifest SOLO candidato tras lectura exacta y escritura create-only."""
    require(type(rows) is list and len(rows) == 990, 'NO_HAY_990_DOCUMENTOS')
    require(len({r['guid_documento_archivo'] for r in rows}) == 990,
            'GUID_DOCUMENTAL_DUPLICADO')
    require(metrics.get('pdf_verificados') == 990 and
            metrics.get('txt_unicos_verificados') == 989 and
            metrics.get('duplicados_documentales_marcados') == 1,
            'METRICAS_VERIFICACION_INVALIDAS')
    require(all(r.get('verificacion') == 'BYTES_GCS_GENERATION_SHA256_SIZE_VERIFIED'
                for r in rows), 'ETIQUETA_GCS_NO_VERIFICADA')
    require(type(receipts) is list and receipts and all(type(r) is dict for r in receipts),
            'SIN_PARQUET')
    prefix = context['staging_prefix']
    require(all(type(r.get('uri')) is str and
                r['uri'].startswith(prefix + 'parquet/part-') and
                r['uri'].endswith('.snappy.parquet') and
                str(r.get('generation','')).isdigit() and int(r['generation']) > 0 and
                type(r.get('size_bytes')) is int and r['size_bytes'] > 0 and
                type(r.get('rows')) is int and r['rows'] > 0 and
                type(r.get('sha256')) is str and HEX.fullmatch(r['sha256'])
                for r in receipts), 'RECIBO_PARQUET_NO_VERIFICABLE')
    require(sum(r['rows'] for r in receipts) == 990, 'CONTEO_PARQUET_INCOMPLETO')
    require(len({r['uri'] for r in receipts}) == len(receipts), 'PARQUET_REPETIDO')
    inventory = [{k: e[k] for k in ('uri','generation','sha256','size_bytes')}
                 for e in sorted(entries, key=lambda x:x['uri'])]
    return {
        'schema_version': CANDIDATE_SCHEMA,
        'status': 'STAGING_CANDIDATO_PENDIENTE_QUALITY',
        'quality_approved': False,
        'official_publication_allowed': False,
        'gold_consumption_allowed': False,
        'source_dataset': DATASET,
        'product': PRODUCT,
        'bronze_run_id': RUN,
        'bronze_manifest_reference': manifest_ref,
        'source_count':1979, 'pdf_count':990, 'txt_count':989,
        'document_rows':990, 'source_inventory':inventory,
        'schema_id': context['schema_id'],
        'schema_fields':schema_spec(),
        'schema_sha256':sha256(canonical(schema_spec())).hexdigest(),
        'stage_prefix':prefix,
        'parquet_receipts':receipts,
        'quality_metrics':metrics,
        'provenance': {
            'verification':'BYTES_GCS_GENERATION_SHA256_SIZE_VERIFIED',
            'nominal_date_is_official_event_date':False,
            'historical_company_expedient_link_accredited':False,
            'quarantined_unpaired_historical_events':27,
            'historical_event_product_blocked':BLOCKED_PRODUCT,
            'no_company_ruc_nemonico_or_expedient_from_historical_csv':True,
        },
        'signed_context_sha256':context_sha,
        'authorization_fingerprint':context['authorization_fingerprint'],
        'claim_generation':str(claim_generation),
        'processor_commit':context['processor_commit'],
        'batch_id':context['batch_id'],
        'run_id':context['run_id'],
        'attempt_id':context['attempt_id'],
    }


def verify_rows_after_exact_gcs(files, read_exact):
    """Adaptador existente + cambio etiqueta UNICAMENTE tras comprobar bytes reales."""
    from fase6_hechos_documental_cloud_candidato import build_verified_document_rows
    rows, metrics = build_verified_document_rows(files,read_exact=read_exact)
    for r in rows:
        require(r['verificacion'] == 'BYTES_HDFS_SHA256_IGUAL_MANIFEST_BRONZE_PINNED',
                'ETIQUETA_PREVIA_INESPERADA')
        r['verificacion'] = 'BYTES_GCS_GENERATION_SHA256_SIZE_VERIFIED'
    require(len(rows) == 990 and metrics['txt_unicos_verificados'] == 989,
            'DOCUMENTOS_VERIFICADOS_INCOMPLETOS')
    require(all(set(r)==set(SCHEMA_FIELDS) for r in rows),
            'ESQUEMA_DOCUMENTAL_INCOMPATIBLE')
    return rows, metrics


def _read_json(path):
    """JSON de referencias externas, no JSON de permiso ni de firma."""
    return json.loads(Path(path).read_text(encoding='utf-8'))


def _gcs_uri_parts(uri, bucket):
    pref = f'gs://{bucket}/'
    require(type(uri) is str and uri.startswith(pref) and '..' not in uri and
            '\\' not in uri and '//' not in uri[len(pref):], 'URI_GCS_INVALIDA')
    return uri[len(pref):]


def _read_pinned(client, uri, generation, digest, *, bucket, size=None):
    import adaptador_gcs_silver_v1 as gcs
    data = gcs.read_exact_current_bytes(client, uri, generation, digest,
            expected_size=size, allowed_bucket=bucket)
    require(type(data) is bytes and sha256(data).hexdigest() == digest and
            (size is None or len(data)==size), 'LECTURA_GCS_NO_FIJADA')
    return data


def _create_only(client, uri, payload, *, content_type):
    _gcs_uri_parts(uri, SILVER_BUCKET)
    require(type(payload) is bytes and payload, 'CONTENIDO_STAGING_INVALIDO')
    blob = client.bucket(SILVER_BUCKET).blob(_gcs_uri_parts(uri,SILVER_BUCKET))
    blob.upload_from_file(BytesIO(payload), size=len(payload), rewind=True,
                          content_type=content_type, checksum='crc32c',
                          if_generation_match=0, retry=None)
    require(blob.generation is not None, 'GENERACION_STAGING_NO_DEVUELTA')
    return {'uri':uri,'generation':str(blob.generation),
            'sha256':sha256(payload).hexdigest(),'size_bytes':len(payload)}


def _stage_parquet(client, rows, prefix):
    import pyarrow as pa
    import pyarrow.parquet as pq
    fixed_schema=pa.schema([pa.field(f['name'],(pa.bool_() if f['type']=='boolean' else
                                 pa.int64() if f['type']=='int64' else pa.string()))
                            for f in schema_spec()])
    receipts = []
    for i, start in enumerate(range(0, len(rows), MAX_PARQUET_ROWS)):
        batch = rows[start:start + MAX_PARQUET_ROWS]
        table = pa.Table.from_pylist(batch,schema=fixed_schema)
        with BytesIO() as buffer:
            pq.write_table(table, buffer, compression='snappy')
            raw = buffer.getvalue()
        require(0 < len(raw) <= MAX_PART_BYTES, 'PARQUET_FUERA_DE_LIMITE')
        # Validate serialization before any stage write.
        restored = pq.read_table(BytesIO(raw))
        require(restored.num_rows == len(batch) and
                restored.schema.equals(fixed_schema) and
                restored.column_names == list(SCHEMA_FIELDS),
                'PARQUET_ESQUEMA_O_CONTEO_DISTINTO')
        receipt = _create_only(client,
            prefix + f'parquet/part-{i:05d}.snappy.parquet',raw,
            content_type='application/octet-stream')
        receipts.append({**receipt,'rows':len(batch),'format':'parquet',
                         'compression':'snappy'})
    require(sum(x['rows'] for x in receipts)==990, 'PARQUET_TOTAL_INCORRECTO')
    return receipts


# La atestacion es un documento firmado por Quality, NUNCA un permiso local.
IAM_SCOPE_SCHEMA = 'BVL-F6-DOCUMENTAL-IAM-SCOPE-ATTESTATION-v1'
IAM_PROBE_REPORT_SHA256 = 'f4ff57f304b5ec381b156220279238aad0fa6e55e6da9ed6e596a762278151d8'
IAM_PROBE_GENERATION = '1795841107238750'
IAM_PROBE_OBJECT = (
    f'gs://{SILVER_BUCKET}/staging/{PRODUCT}/_iam_diagnostic_only/'
    'probe-7dc9827c171d4fe28424e91343f6bbe4.json'
)
IAM_SCOPE_MAX_AGE_SECONDS = 600


def expected_iam_create_targets(context):
    """Fija todos los nombres que el launcher puede crear; no escribe objetos."""
    prefix = expected_staging(context['run_id'], context['attempt_id'])
    require(context['staging_prefix'] == prefix, 'IAM_SCOPE_STAGING_DISTINTO')
    targets = [prefix + f'parquet/part-{i:05d}.snappy.parquet'
               for i in range(len(range(0, 990, MAX_PARQUET_ROWS)))]
    targets += [prefix + '_quality/manifest_candidate_documental.json',
                prefix + '_quality/failure_candidate.json']
    require(len(targets) == 10 and len(set(targets)) == 10,
            'IAM_SCOPE_NOMBRES_INCONSISTENTES')
    return targets


def validate_iam_scope_attestation(doc, context, context_sha, *, now=None):
    """Evalua evidencia de IAM firmada Quality para los 10 objetos exactos.

    Este verificador NO firma ni presume permisos. Solo se permite llamar en
    produccion tras `gate.signed` y lectura GCS fijada por generacion+SHA.
    La revision de denegaciones/herencia es una declaracion firmada y auditada
    por Quality; no equivale a una prueba GCS de cada objeto inexistente.
    """
    required = {'schema_version', 'status', 'project_id', 'product', 'service_account',
        'run_id', 'attempt_id', 'batch_id', 'processor_commit', 'staging_prefix',
        'authorization_fingerprint', 'launch_context_sha256', 'issued_at_utc',
        'expires_at_utc', 'diagnostic', 'targets', 'allow_policy', 'role_definition',
        'independent_deny_review'}
    require(type(doc) is dict and set(doc) == required,
            'IAM_ATESTACION_ESTRUCTURA_INVALIDA')
    exact = {
        'schema_version': IAM_SCOPE_SCHEMA,
        'status': 'QUALITY_ATTESTED_SCOPE_CANDIDATE',
        'project_id': PROJECT, 'product': PRODUCT,
        'service_account': context['service_account'],
        'run_id': context['run_id'], 'attempt_id': context['attempt_id'],
        'batch_id': context['batch_id'], 'processor_commit': context['processor_commit'],
        'staging_prefix': context['staging_prefix'],
        'authorization_fingerprint': context['authorization_fingerprint'],
        'launch_context_sha256': context_sha,
    }
    for key, value in exact.items():
        require(doc.get(key) == value and type(doc.get(key)) is type(value),
                'IAM_ATESTACION_ALCANCE_DISTINTO:' + key)
    require(type(context_sha) is str and HEX.fullmatch(context_sha) is not None,
            'IAM_CONTEXT_SHA_INVALIDO')
    now = now or datetime.now(timezone.utc)
    issued, expires = _utc(doc['issued_at_utc']), _utc(doc['expires_at_utc'])
    ctx_end = _utc(context['expires_at_utc'])
    require(issued <= now <= expires <= ctx_end and
            0 < (expires-issued).total_seconds() <= IAM_SCOPE_MAX_AGE_SECONDS and
            0 <= (now-issued).total_seconds() <= IAM_SCOPE_MAX_AGE_SECONDS,
            'IAM_ATESTACION_VENCIDA_O_NO_VIGENTE')

    diagnostic = doc['diagnostic']
    require(type(diagnostic) is dict and set(diagnostic) == {
        'report_sha256', 'object_uri', 'generation', 'creation_http',
        'read_integrity', 'cleanup_http', 'claims_consumed', 'dataproc_executed'},
        'IAM_DIAGNOSTICO_ESTRUCTURA_INVALIDA')
    require(diagnostic == {
        'report_sha256': IAM_PROBE_REPORT_SHA256,
        'object_uri': IAM_PROBE_OBJECT,
        'generation': IAM_PROBE_GENERATION,
        'creation_http': 200,
        'read_integrity': 'PASS',
        'cleanup_http': 204,
        'claims_consumed': 0,
        'dataproc_executed': False,
    }, 'IAM_DIAGNOSTICO_NO_ACREDITADO')
    # El probe es una evidencia secundaria; por si solo NO habilita ninguno de
    # los objetos bajo /run_id=.../attempt_id=...

    targets = expected_iam_create_targets(context)
    require(type(doc['targets']) is list and doc['targets'] == targets,
            'IAM_OBJETIVOS_EXACTOS_NO_COINCIDEN')

    evidence_prefix = (f'gs://{CONTROL_BUCKET}/authorizations/silver_documental/'
        f"run_id={context['run_id']}/attempt_id={context['attempt_id']}/iam_evidence/")
    policy = doc['allow_policy']
    require(type(policy) is dict and set(policy) == {
        'source_uri', 'source_generation', 'source_sha256', 'version', 'etag',
        'bindings', 'observed_at_utc', 'resource_type', 'resource_name'},
            'IAM_POLICY_SNAPSHOT_INCOMPLETA')
    source_kind = policy['resource_type']
    require(source_kind in ('BUCKET', 'MANAGED_FOLDER'),
            'IAM_POLICY_RESOURCE_TYPE_INVALIDO')
    expected_policy_file = ('bucket_policy_snapshot.json' if source_kind == 'BUCKET'
                            else 'managed_folder_policy_snapshot.json')
    if source_kind == 'BUCKET':
        require(policy['resource_name'] == f'projects/_/buckets/{SILVER_BUCKET}',
                'IAM_BUCKET_RESOURCE_DISTINTO')
    else:
        managed_root = f'projects/_/buckets/{SILVER_BUCKET}/managedFolders/'
        require(type(policy['resource_name']) is str and
                policy['resource_name'].startswith(managed_root),
                'IAM_MANAGED_FOLDER_FUERA_DE_SCOPE')
        managed_relative = policy['resource_name'][len(managed_root):]
        require(managed_relative.startswith('staging/') and
                all(_gcs_uri_parts(uri, SILVER_BUCKET).startswith(managed_relative)
                    for uri in targets), 'IAM_MANAGED_FOLDER_NO_CUBRE_TARGETS')
    require(type(policy['source_uri']) is str and
            policy['source_uri'] == evidence_prefix+expected_policy_file and
            type(policy['source_sha256']) is str and HEX.fullmatch(policy['source_sha256']) and
            type(policy['etag']) is str and bool(policy['etag']) and
            policy['version'] == 3 and type(policy['bindings']) is list,
            'IAM_POLICY_SNAPSHOT_INVALIDA')
    _generation(policy['source_generation'])
    require(0 <= (now-_utc(policy['observed_at_utc'])).total_seconds() <= IAM_SCOPE_MAX_AGE_SECONDS,
            'IAM_POLICY_SNAPSHOT_OBSOLETA')

    role = doc['role_definition']
    require(type(role) is dict and set(role) == {
        'source_uri', 'source_generation', 'source_sha256', 'name',
        'includedPermissions', 'observed_at_utc'},
        'IAM_ROLE_EVIDENCIA_INCOMPLETA')
    require(type(role['source_uri']) is str and
            role['source_uri'] == evidence_prefix+'custom_role_definition.json' and
            type(role['source_sha256']) is str and HEX.fullmatch(role['source_sha256']) and
            type(role['name']) is str and role['name'].startswith('projects/'+PROJECT+'/roles/') and
            type(role['includedPermissions']) is list and
            'storage.objects.create' in role['includedPermissions'],
            'IAM_ROLE_NO_ACREDITA_CREATE')
    _generation(role['source_generation'])
    require(0 <= (now-_utc(role['observed_at_utc'])).total_seconds() <= IAM_SCOPE_MAX_AGE_SECONDS,
            'IAM_ROLE_EVIDENCIA_OBSOLETA')

    # Lista cerrada de expresiones CEL: prefijo simple heredado o la condicion
    # IAM real de Quality, con comprobacion literal de Object y del prefijo.
    # No se evalua CEL arbitrario ni se aceptan operadores adicionales.
    expected_member = 'serviceAccount:' + context['service_account']
    simple_condition = re.compile(
        r'''resource\.name\.startsWith\((["'])(projects/_/buckets/[^"']+/objects/[^"']+)\1\)\Z''')
    object_condition = re.compile(
        r'''resource\.type *== *(["'])storage\.googleapis\.com/Object\1 *&& *resource\.name\.startsWith\((["'])(projects/_/buckets/[^"']+/objects/[^"']+)\2\)\Z''')
    matching = []
    for binding in policy['bindings']:
        if type(binding) is not dict or binding.get('role') != role['name']:
            continue
        if type(binding.get('members')) is not list or expected_member not in binding['members']:
            continue
        cond = binding.get('condition')
        if type(cond) is not dict or type(cond.get('expression')) is not str:
            continue
        expression = cond['expression']
        match = simple_condition.fullmatch(expression)
        if match is not None:
            policy_prefix = match.group(2)
        else:
            match = object_condition.fullmatch(expression)
            if match is None:
                continue
            policy_prefix = match.group(3)
        required_root = f'projects/_/buckets/{SILVER_BUCKET}/objects/staging/'
        if not policy_prefix.startswith(required_root):
            continue
        if all(('projects/_/buckets/'+SILVER_BUCKET+'/objects/'+
                _gcs_uri_parts(uri, SILVER_BUCKET)).startswith(policy_prefix)
               for uri in targets):
            matching.append(binding)
    require(len(matching) == 1, 'IAM_POLICY_NO_ACREDITA_TODOS_LOS_OBJETOS')

    deny = doc['independent_deny_review']
    require(type(deny) is dict and set(deny) == {
        'source_uri', 'source_generation', 'source_sha256', 'reviewer',
        'principal', 'permission', 'objects_sha256', 'outcome', 'evaluated_at_utc'},
        'IAM_DENY_REVIEW_INCOMPLETA')
    require(type(deny['source_uri']) is str and
            deny['source_uri'] == evidence_prefix+'deny_review.json' and
            type(deny['source_sha256']) is str and HEX.fullmatch(deny['source_sha256']) and
            type(deny['reviewer']) is str and bool(deny['reviewer']) and
            deny['principal'] == expected_member and
            deny['permission'] == 'storage.objects.create' and
            deny['objects_sha256'] == sha256(canonical(targets)).hexdigest() and
            deny['outcome'] == 'NO_APPLICABLE_DENY_VERIFIED' and
            issued <= _utc(deny['evaluated_at_utc']) <= now,
            'IAM_DENY_REVIEW_NO_ACREDITADA')
    _generation(deny['source_generation'])
    return True


def _verified_iam_scope_attestation(client, refs, pub, context, context_sha):
    """Lee los CUATRO documentos de evidencia por generaciones y SHA exactos.

    Firma Quality es requisito, no sustituto de evaluar politica/rol/denegaciones.
    Los tres anexos se fijan por SHA y se vinculan al documento firmado.
    """
    import puerta_silver_operativa_v1_candidata as gate
    require(type(refs) is dict and set(refs) == {
        'uri', 'generation', 'sha256', 'signature_uri',
        'signature_generation', 'signature_sha256'},
        'IAM_FIRMA_REFERENCIAS_INVALIDAS')
    prefix = (f'gs://{CONTROL_BUCKET}/authorizations/silver_documental/'
              f"run_id={context['run_id']}/attempt_id={context['attempt_id']}/")
    require(refs['uri'] == prefix+'iam_scope_attestation.json' and
            refs['signature_uri'] == prefix+'iam_scope_attestation.sig',
            'IAM_FIRMA_RUTA_FUERA_DE_INTENTO')
    raw = _read_pinned(client, refs['uri'], refs['generation'],
                       refs['sha256'], bucket=CONTROL_BUCKET)
    signature = _read_pinned(client, refs['signature_uri'],
                             refs['signature_generation'], refs['signature_sha256'],
                             bucket=CONTROL_BUCKET)
    doc = gate.signed(raw, signature, pub)
    validate_iam_scope_attestation(doc, context, context_sha)
    for field in ('allow_policy', 'role_definition', 'independent_deny_review'):
        evidence = doc[field]
        evidence_raw = _read_pinned(client, evidence['source_uri'],
                                    evidence['source_generation'],
                                    evidence['source_sha256'], bucket=CONTROL_BUCKET)
        parsed = json.loads(evidence_raw)
        require(type(parsed) is dict, 'IAM_EVIDENCIA_NO_JSON:'+field)
        if field == 'allow_policy':
            require(parsed.get('version') == 3 and
                    parsed.get('etag') == evidence['etag'] and
                    parsed.get('bindings') == evidence['bindings'],
                    'IAM_POLICY_SNAPSHOT_DISTINTA')
        elif field == 'role_definition':
            require(parsed.get('name') == evidence['name'] and
                    parsed.get('includedPermissions') == evidence['includedPermissions'],
                    'IAM_ROLE_EVIDENCIA_DISTINTA')
        else:
            require(parsed.get('principal') == evidence['principal'] and
                    parsed.get('permission') == evidence['permission'] and
                    parsed.get('objects_sha256') == evidence['objects_sha256'] and
                    parsed.get('outcome') == evidence['outcome'] and
                    parsed.get('evaluated_at_utc') == evidence['evaluated_at_utc'],
                    'IAM_DENY_REVIEW_EVIDENCIA_DISTINTA')
    return doc, sha256(raw).hexdigest()


def validate_writer_permissions(client, context, entries, launch_refs, *, iam_attestation=None, context_sha=None):
    """Preclaim IAM no destructivo con recursos de OBJETO sujetos a condiciones.

    `Bucket.test_iam_permissions` evalua el bucket, no un objeto con resource.name
    condicionado: no debe usarse para deducir get/create sobre prefijos de objetos.
    No se hacen escrituras, claims ni simulaciones de upload. Toda duda falla cerrado.
    """
    require(type(context) is dict and context.get('product') == PRODUCT and
            context.get('source_dataset') == DATASET and
            context.get('staging_prefix') == expected_staging(
                context.get('run_id'), context.get('attempt_id')),
            'IAM_CONTEXTO_NO_VINCULADO_A_STAGING')
    require(type(launch_refs) is dict and
            all(type(launch_refs.get(k)) is str for k in ('uri','signature_uri')),
            'IAM_REFERENCIAS_CONTEXT_INCOMPLETAS')
    require(type(entries) is list and len(entries) == 1979,
            'IAM_INVENTARIO_SOURCE_INCOMPLETO')

    # Los otros documentos F5 y los artefactos ya se leyeron por generacion+hash
    # antes de esta funcion; comprobamos ademas IAM sobre sus NOMBRES reales.
    readings = [
        (CONTROL_BUCKET, context['bronze_manifest_uri']),
        (CONTROL_BUCKET, launch_refs['uri']),
        (CONTROL_BUCKET, launch_refs['signature_uri']),
        (ARTIFACT_BUCKET, context['entrypoint_uri']),
        (ARTIFACT_BUCKET, context['dependencies_uri']),
    ]
    # Muestras separadas PDF y TXT; la verificacion integral de 1979 bytes
    # por generacion+SHA permanece obligatoria despues del claim autorizado.
    for extension in ('.pdf', '.txt'):
        match = next((e.get('uri') for e in entries if type(e) is dict and
                      type(e.get('uri')) is str and
                      e['uri'].lower().endswith(extension)), None)
        require(match is not None, 'IAM_FUENTE_SIN_'+extension[1:].upper())
        readings.append((BRONZE_BUCKET, match))

    def check_object(bucket_name, uri, perm):
        name = _gcs_uri_parts(uri, bucket_name)
        require(name != '', 'IAM_NOMBRE_OBJETO_VACIO')
        try:
            # GCS Objects.testIamPermissions: usa `resource.name` del objeto,
            # no la comprobacion a nivel del bucket. No crea objetos.
            permissions = client.bucket(bucket_name).blob(name).test_iam_permissions([perm])
        except Exception as exc:
            raise ValueError('IAM_PRECLAIM_NO_VERIFICABLE:'+uri) from exc
        require(type(permissions) in (list,tuple,set) and perm in permissions,
                'IAM_INSUFICIENTE_PRECLAIM:'+uri+':'+perm)

    for bucket_name, uri in readings:
        check_object(bucket_name, uri, 'storage.objects.get')

    # `storage.objects.list` opera a nivel bucket. El listado REAL sobre el
    # prefijo exacto y la comprobacion de staging vacio siguen en execute().
    try:
        allowed = client.bucket(SILVER_BUCKET).test_iam_permissions(
            ['storage.objects.list'])
    except Exception as exc:
        raise ValueError('IAM_PRECLAIM_LIST_NO_VERIFICABLE') from exc
    require(type(allowed) in (list,tuple,set) and
            'storage.objects.list' in allowed,
            'IAM_INSUFICIENTE_PRECLAIM:'+SILVER_BUCKET+':list')

    targets = expected_iam_create_targets(context)
    if iam_attestation is None:
        # Solo compatibilidad de pruebas offline antiguas: si el API HTTP 400
        # para objeto futuro, se RECHAZA, no se deduce un permiso.
        for uri in targets:
            check_object(SILVER_BUCKET, uri, 'storage.objects.create')
    else:
        # En ejecución real la atestación SOLO llega desde la puerta firmada,
        # leída por generación/SHA; se vuelve a evaluar en este preclaim.
        validate_iam_scope_attestation(iam_attestation, context, context_sha)
    return True


def record_failure_candidate(client, prefix, *, claim_generation, attempt_id, exc):
    """Una sola anotacion inmutable best effort; nunca se reintenta el batch."""
    payload={
        'schema_version':'BVL-DOC-STAGING-ERROR-v1',
        'status':'ERROR_TRAS_CLAIM_REQUIERE_CONCILIACION_QUALITY',
        'attempt_id':attempt_id,'claim_generation':str(claim_generation),
        'error_type':type(exc).__name__,
        'gold_consumption_allowed':False,'quality_approved':False,
        'automatic_retry_allowed':False,
    }
    return _create_only(client,prefix+'_quality/failure_candidate.json',
                         canonical(payload),content_type='application/json')


def _verified_context(client, refs, pub, *, attempt):
    """Verifica bytes y firma con la puerta protegida; NUNCA con un booleano local."""
    import puerta_silver_operativa_v1_candidata as gate
    require(type(refs) is dict and set(refs)=={
        'uri','generation','sha256','signature_uri',
        'signature_generation','signature_sha256'},
        'REFERENCIAS_FIRMA_INCOMPLETAS')
    base = f'gs://{CONTROL_BUCKET}/authorizations/silver_documental/'
    require(refs['uri'].startswith(base) and
            refs['uri'].endswith('/launch_context.json') and
            refs['signature_uri'] == refs['uri'][:-len('launch_context.json')] + 'launch_context.sig',
            'RUTA_FIRMA_DOCUMENTAL_INESPERADA')
    raw = _read_pinned(client, refs['uri'],refs['generation'],refs['sha256'],bucket=CONTROL_BUCKET)
    sig = _read_pinned(client,refs['signature_uri'],refs['signature_generation'],
                       refs['signature_sha256'],bucket=CONTROL_BUCKET)
    doc = gate.signed(raw,sig,pub)
    validate_context(doc)
    require(doc['attempt_id'] == attempt, 'FIRMA_DE_OTRO_INTENTO')
    exact=(f'gs://{CONTROL_BUCKET}/authorizations/silver_documental/'
           f"run_id={doc['run_id']}/attempt_id={attempt}/launch_context.json")
    require(refs['uri']==exact,'REFERENCIA_FIRMA_NO_VINCULADA_A_RUN_ATTEMPT')
    return doc,sha256(raw).hexdigest()


def _verify_release(client, context, local_entry, deps):
    entry_raw = Path(local_entry).read_bytes()
    zip_raw = Path(deps).read_bytes()
    require(sha256(entry_raw).hexdigest() == context['entrypoint_sha256'] and
            sha256(zip_raw).hexdigest() == context['dependencies_sha256'],
            'RELEASE_LOCAL_DISTINTO_DEL_FIRMADO')
    for key, raw, kind in (
        ('entrypoint', entry_raw, 'entrypoint'),
        ('dependencies', zip_raw, 'dependencies')):
        signed = _read_pinned(client, context[kind+'_uri'],
                context[kind+'_generation'],context[kind+'_sha256'],
                bucket=ARTIFACT_BUCKET)
        require(signed == raw, 'ARTEFACTO_GCS_NO_COINCIDE_CON_RELEASE')


def _verify_f5_new_product(plan, context):
    """Exige plan REAL de F5 y salida documental explicita nueva.

    Un plan historico que autorice hechos_importancia/eventos DEBE FALLAR.
    Quality debe actualizar su extension contractual antes de ejecutarlo.
    """
    import puerta_silver_operativa_v1_candidata as gate
    require(type(plan) is gate.GCSVerifiedPlan, 'PLAN_F5_NO_VERIFICADO')
    auth = plan.authorization
    require(auth.get('dataset') == DATASET and auth.get('operation') == 'BUILD_SILVER_STAGING' and
            auth.get('mode') == 'SINGLE_SOURCE', 'AUTORIZACION_F5_NO_ES_DOCUMENTAL')
    outputs = auth.get('silver_outputs')
    require(type(outputs) is list and len(outputs)==1 and
            type(outputs[0]) is dict and
            outputs[0].get('name')==PRODUCT and
            outputs[0].get('staging_prefix')==context['staging_prefix'],
            'NO_REUTILIZAR_AUTORIZACION_DE_EVENTOS')
    require(plan.fingerprint == context['authorization_fingerprint'],
            'FINGERPRINT_DISTINTO')
    require(auth.get('attempt_id') == context['attempt_id'] and
            auth.get('run_id') == context['run_id'] and
            auth.get('service_account') == context['service_account'],
            'INTENTO_O_IDENTIDAD_DISTINTOS')
    return True


def execute(args):
    """RUNTIME potencial: SIEMPRE requiere firma Quality + F5 NUEVO alcance.
    Localmente no existe autorizacion documental; no puede consumir claims.
    """
    require(args.ack_supervised, 'SUPERVISION_EXPLICITA_OBLIGATORIA')
    needed = ('contract','quality_public_pem','trusted_refs','launch_refs',
              'iam_attestation_refs','dependencies_zip','attempt_id',
              'processor_commit','batch_id')
    require(all(getattr(args,k,None) for k in needed), 'REFERENCIAS_QUALITY_FALTANTES')
    require(COMMIT.fullmatch(args.processor_commit) is not None,
            'COMMIT_NO_FIJADO')
    from google.cloud import storage
    import puerta_silver_operativa_v1_candidata as gate
    import adaptador_gcs_silver_v1 as gcs
    from fase6_hechos_documental_restringido import classify_official, prepare_document_index
    client = storage.Client(project=PROJECT)
    pub = args.quality_public_pem.read_bytes()
    context, context_sha = _verified_context(client,_read_json(args.launch_refs),pub,
                                               attempt=args.attempt_id)
    require(context['processor_commit']==args.processor_commit and
            context['batch_id']==args.batch_id, 'RELEASE_O_BATCH_DISTINTO')
    _verify_release(client,context,__file__,args.dependencies_zip)
    # F5: CERO bypass. Si el contrato historico devuelve 'eventos', RECHAZAR.
    f5_plan = gate.check_gcs_signed_bundle(client, _read_json(args.trusted_refs),
                                          pub,args.contract.read_bytes())
    _verify_f5_new_product(f5_plan,context)
    original_contract = gate.check_base(args.contract.read_bytes())
    candidates=[x for x in original_contract['datasets'] if x['dataset']==DATASET]
    require(len(candidates)==1,'FUENTE_NO_ESTABLECIDA_EN_F5')
    manifest, entries = gcs.read_official_manifest(client,candidates[0],
                              __import__('puerta_silver_v1_candidata').verify_manifest)
    # Verificar bytes del manifest FIJADOS en la extension documental.
    manifest_raw=_read_pinned(client, context['bronze_manifest_uri'],
             context['bronze_manifest_generation'],context['bronze_manifest_sha256'],
             bucket=CONTROL_BUCKET)
    require(canonical(json.loads(manifest_raw))==canonical(manifest),
            'MANIFEST_F5_Y_EXTENSION_DISTINTOS')
    inv=validate_official_manifest(manifest,entries)
    pdf,txt=classify_official(entries)
    index=prepare_document_index(pdf,txt)
    require(len(index)==990, 'INDEX_INCOMPLETO')
    validate_part_capacity(index)
    # Preclaim: evidencia de IAM firmada Quality, fijada al intento y los
    # diez objetos exactos. El probe historico NO sustituye dicha evidencia.
    iam_doc, iam_sha = _verified_iam_scope_attestation(
        client, _read_json(args.iam_attestation_refs), pub, context, context_sha)
    validate_writer_permissions(client, context, entries,
                                _read_json(args.launch_refs),
                                iam_attestation=iam_doc, context_sha=context_sha)
    print('IAM_ATTESTATION_SHA256:', iam_sha, flush=True)
    object_prefix=_gcs_uri_parts(context['staging_prefix'],SILVER_BUCKET)
    require(not list(client.list_blobs(SILVER_BUCKET,prefix=object_prefix,max_results=1)),
            'STAGING_PREEXISTENTE_REQUIERE_CONCILIACION')
    require(len(entries)==1979 and inv['txt_count']==989,
            'INVENTARIO_INCONSISTENTE_PRECLAIM')
    require((datetime.now(timezone.utc)-f5_plan.checked_at).total_seconds() <= 100,
            'F5_PREFLIGHT_EXPIRADO')
    # Solo aqui se CONSUMIRIA un claim, tras firma, alcance, identidad y checks.
    claim_gen=gate.consume_once(f5_plan, gcs.GCSExclusiveClaimStore(client))
    print('CLAIM_CONSUMIDO_GENERACION:',claim_gen, flush=True)
    try:
        from fase6_hechos_documental_cloud_candidato import exact_reader_from_gcs
        rows, metrics=verify_rows_after_exact_gcs(entries,exact_reader_from_gcs(client))
        receipts=_stage_parquet(client,rows,context['staging_prefix'])
        candidate=candidate_from_verified_rows(context,entries,rows,metrics,receipts,
            claim_generation=claim_gen,context_sha=context_sha,
            manifest_ref={'uri':context['bronze_manifest_uri'],
                          'generation':context['bronze_manifest_generation'],
                          'sha256':context['bronze_manifest_sha256']})
        rec=_create_only(client,
            context['staging_prefix']+'_quality/manifest_candidate_documental.json',
            canonical(candidate),content_type='application/json')
        print(json.dumps({'status':'STAGING_CANDIDATO_PENDIENTE_QUALITY',
                          'candidate_manifest':rec,'claim_generation':str(claim_gen),
                          'quality_approved':False,'gold_consumption_allowed':False},
                          sort_keys=True),flush=True)
        return 0
    except BaseException as exc:
        try:
            rec=record_failure_candidate(client,context['staging_prefix'],
                claim_generation=claim_gen,attempt_id=context['attempt_id'],exc=exc)
            print('RECIBO_FALLO_CANDIDATO:',json.dumps(rec,sort_keys=True),
                  file=sys.stderr,flush=True)
        except BaseException:
            print('NO_SE_PUDO_REGISTRAR_FALLO_EN_STAGING; PRESERVAR_LOGS_CLOUD',
                  file=sys.stderr,flush=True)
        print('ERROR_TRAS_CLAIM: NO_REINTENTAR_AUTOMATICAMENTE; '
              'PRESERVAR_STAGING_Y_RECONCILIAR_CON_QUALITY',file=sys.stderr,flush=True)
        raise


def parser():
    ap=argparse.ArgumentParser(description='BVL Hechos documental launcher supervisado CANDIDATO')
    ap.add_argument('--execute',action='store_true')
    ap.add_argument('--ack-supervised',action='store_true')
    ap.add_argument('--contract',type=Path)
    ap.add_argument('--quality-public-pem',type=Path)
    ap.add_argument('--trusted-refs',type=Path)
    ap.add_argument('--launch-refs',type=Path)
    ap.add_argument('--iam-attestation-refs',type=Path)
    ap.add_argument('--dependencies-zip',type=Path)
    ap.add_argument('--attempt-id')
    ap.add_argument('--processor-commit')
    ap.add_argument('--batch-id')
    return ap


def main(argv=None):
    args=parser().parse_args(argv)
    if not args.execute:
        print('ESTADO: DOCUMENTAL_LAUNCHER_CANDIDATO_SIN_AUTORIZACION')
        print('PRODUCTO:',PRODUCT)
        print('MANIFEST_BRONZE_SHA256:',BRONZE_MANIFEST_SHA)
        print('OBJETOS_FIJADOS: 1979 | PDF: 990 | TXT: 989')
        print('GCS: NO | CLAIMS: NO | DATAPROC: NO | PUBLICACION: NO')
        return 0
    return execute(args)


if __name__ == '__main__':
    raise SystemExit(main())
