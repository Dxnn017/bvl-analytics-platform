#!/usr/bin/env python3
"""F6 Silver restantes: supervised SINGLE_SOURCE candidate, NOT approved yet.

DEFAULT = no execution. This file is a proposal for Quality to ratify with a
new signed REST launch context. No unsigned/legacy authorization can unlock it.
No retries, no overwrite, no delete, no publication of current.json.
NEVER run --execute until Quality signs and authorizes the exact new release.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
for folder in (ROOT/'scripts'/'pipeline', ROOT/'scripts'/'processing'):
    if folder.is_dir():
        sys.path.insert(0, str(folder))

from fase6_restantes_core import (
    SOURCES, FILE_COUNTS, BRONZE_BUCKET, SILVER_BUCKET,
    RUN_REFERENCE, require, validate_source_entries,
)
from fase6_launch_restantes import SUPPORTED

COMMIT_F5 = '41266f0efba4477c30266e19ca3280838ff3d802'

# Map strictly the fields present in the local Spark transform schema.
# Quality must ratify partition choices and output contract before execution.
PARTITIONS = {
    'bcrp/embig': ('anio', 'mes'),
    'smv/cambios_patrimonio': ('ejercicio',),
    'smv/estado_resultados': ('ejercicio',),
    'smv/flujo_efectivo': ('ejercicio',),
    'smv/situacion_financiera': ('ejercicio',),
    'smv/principales_cuentas': ('ejercicio',),
    'smv/cotizaciones': ('anio', 'mes'),
    'smv/montos_sab': ('anio', 'mes'),
    'smv/valores_inscritos': (),
    'smv/hechos_valores_consulta': (),
}


def parser():
    ap = argparse.ArgumentParser(description='Silver restantes candidato supervisado')
    ap.add_argument('--execute', action='store_true', help='EXIGE firma y autorización Quality')
    ap.add_argument('--ack-supervised', action='store_true')
    ap.add_argument('--dataset', choices=sorted(SUPPORTED))
    ap.add_argument('--attempt-id')
    ap.add_argument('--processor-commit')
    ap.add_argument('--batch-id')
    ap.add_argument('--contract', type=Path)
    ap.add_argument('--quality-public-pem', type=Path)
    ap.add_argument('--trusted-refs', type=Path)
    ap.add_argument('--launch-refs', type=Path)
    ap.add_argument('--partial-scope-refs', type=Path, help='REFERENCIAS EXTERNAS firmadas Quality, solo Montos SAB')
    return ap


def validate_cli(args):
    if not args.execute:
        return False
    require(args.ack_supervised is True, 'Sin confirmación de supervisión')
    require(args.dataset in SUPPORTED, 'Hechos/derivado u otro dataset no autorizado')
    require((args.dataset != 'smv/montos_sab' or args.partial_scope_refs is not None) and
            (args.dataset == 'smv/montos_sab' or args.partial_scope_refs is None),
            'Montos SAB requiere scope parcial independiente; no autoriza otros datasets')
    require(all((args.attempt_id, args.processor_commit, args.batch_id,
                 args.contract, args.quality_public_pem, args.trusted_refs,
                 args.launch_refs)), 'Faltan documentos/referencias externas Quality')
    require(type(args.processor_commit) is str and len(args.processor_commit) == 40 and
            all(c in '0123456789abcdef' for c in args.processor_commit) and
            args.processor_commit != COMMIT_F5,
            'Processor commit no vinculado a release F6')
    return True


def check_manifest_capacity(dataset, entries):
    """Preclaim size check; avoid known oversized sources consuming attempts."""
    from fase6_integracion_10_local import MAX_OBJECT_BYTES, MAX_TOTAL_BYTES
    require(dataset in SUPPORTED, 'Dataset bloqueado')
    validate_source_entries(dataset, entries)
    require(len(entries) == FILE_COUNTS[dataset], 'Inventario fuente incompleto')
    require(all(type(f['size_bytes']) is int and
                0 < f['size_bytes'] <= MAX_OBJECT_BYTES for f in entries),
            'Algún objeto excede la capacidad del lector aprobado')
    require(sum(f['size_bytes'] for f in entries) <= MAX_TOTAL_BYTES,
            'Batch excede memoria del lector actual')
    return sum(f['size_bytes'] for f in entries)


def _upload_candidate_manifest(client, prefix, payload, *, filename='manifest_candidate_restantes.json'):
    require(prefix.startswith(f'gs://{SILVER_BUCKET}/staging/') and
            '/run_id=' in prefix and '/attempt_id=' in prefix and prefix.endswith('/'),
            'Prefijo de Quality fuera de staging autorizado')
    require(filename in ('manifest_candidate_restantes.json',
                         'montos_sab_quarantine_audit.json'),
            'Artefacto fuera de contrato')
    uri = prefix + '_quality/' + filename
    key = uri[len(f'gs://{SILVER_BUCKET}/'):]
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                     separators=(',', ':'), allow_nan=False).encode('utf-8')
    blob = client.bucket(SILVER_BUCKET).blob(key)
    blob.upload_from_file(BytesIO(raw), size=len(raw), rewind=True,
                          content_type='application/json', checksum='crc32c',
                          if_generation_match=0, retry=None)
    require(blob.generation is not None, 'GCS no devolvió generación del candidato')
    return {'uri': uri, 'generation': str(blob.generation),
            'sha256': sha256(raw).hexdigest(), 'size_bytes': len(raw)}


def main(argv=None):
    args = parser().parse_args(argv)
    if not validate_cli(args):
        print('ESTADO: BLOQUEADO_POR_DEFECTO')
        print('ALCANCE: 10_PROCESOS_CANDIDATOS_NO_DERIVADO')
        print('FIRMA_CONTEXT_REST: PENDIENTE_RATIFICACION_QUALITY')
        print('GCS: NO | CLAIM: NO | DATAPROC: NO')
        return 0

    # Nothing below is executed without an explicit --execute and signed
    # external references. The F5 verifier controls the effective identity.
    from google.cloud import storage
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyspark.sql import SparkSession
    import puerta_silver_operativa_v1_candidata as gate
    import puerta_silver_v1_candidata as gate_core
    import adaptador_gcs_silver_v1 as gcs
    import fase6_contexto_lanzamiento as old_launch
    import fase6_launch_restantes as launch
    import fase6_restantes_writer as writer
    import fase6_restantes_transform as transform
    import fase6_restantes_preflight as preflight
    import fase6_restantes_core as cat
    import fase6_integracion_10_local as integrator
    import fase6_especializados_valores as valores
    import fase6_especializados_hechos_valores as hechos_valores
    import fase6_especializados_hechos_metadata as hechos_metadata
    import fase6_valores_conciliacion as conciliacion
    import fase6_montos_sab_quarantine as cuarentena
    import fase6_montos_sab_scope_quality as partial
    import fase6_hechos_documental_restringido as hechos_doc
    import fase6_hechos_documental_cloud_candidato as hechos_cloud
    import bronze_to_silver_dataproc_candidato as historical

    client = storage.Client(project='bvl-analytics-2026')
    pub = args.quality_public_pem.read_bytes()
    contract_raw = args.contract.read_bytes()
    trusted_refs = json.loads(args.trusted_refs.read_text(encoding='utf-8'))
    launch_refs = json.loads(args.launch_refs.read_text(encoding='utf-8'))

    # The REST context is a new schema, requiring explicit Quality ratification.
    context, context_sha = launch.verify_signed_launch(
        client, launch_refs, pub, expected_run=RUN_REFERENCE,
        expected_attempt=args.attempt_id)
    runtime = launch.verify_runtime_libs()
    zip_path = launch.verify_runtime_artifacts(context, __file__, (
        gate, gate_core, gcs, old_launch, launch, writer, transform, preflight,
        cat, integrator, valores, hechos_valores, hechos_metadata,
        conciliacion, historical, cuarentena, partial, hechos_doc, hechos_cloud))
    for label in ('entrypoint', 'dependencies'):
        blob_bytes = gcs.read_exact_current_bytes(
            client, context[label+'_uri'], context[label+'_generation'],
            context[label+'_sha256'], allowed_bucket='bvl26-dataproc-staging-7f3a9c')
        local_bytes = Path(__file__).read_bytes() if label == 'entrypoint' else Path(zip_path).read_bytes()
        require(blob_bytes == local_bytes, 'Artefacto runtime distinto a release firmado')

    # Real F5 preflight (verify KMS, official Bronze, claim freshness, identity).
    plan = gate.check_gcs_signed_bundle(client, trusted_refs, pub, contract_raw)
    launch.bind_launch_to_plan(context, plan, dataset=args.dataset,
                               processor_commit=args.processor_commit,
                               batch_id=args.batch_id)

    contract = gate.check_base(contract_raw)
    found = [d for d in contract['datasets'] if d['dataset'] == args.dataset]
    require(len(found) == 1, 'Contrato Bronze no incluye el dataset')
    _, entries = gcs.read_official_manifest(client, found[0], gate_core.verify_manifest)
    total_bytes = check_manifest_capacity(args.dataset, entries)
    partial_scope = None
    if args.dataset == 'smv/montos_sab':
        # Separate Quality KMS signature, exact 14 excluded pins, before claim.
        partial_scope = partial.verify_signed_scope(
            client, json.loads(args.partial_scope_refs.read_text(encoding='utf-8')),
            pub, entries, attempt_id=args.attempt_id,
            fingerprint=plan.fingerprint, launch_sha256=context_sha)
    require((datetime.now(timezone.utc) - plan.checked_at).total_seconds() <= 100,
            'Preflight demasiado antiguo antes de consumir claim')

    # ONE exclusive claim per SIGNED, verifiable attempt. No automatic retry.
    claim_generation = gate.consume_once(plan, gcs.GCSExclusiveClaimStore(client))
    print('CLAIM_CONSUMIDO_GENERACION:', claim_generation, flush=True)

    spark = None
    try:
        spark = (SparkSession.builder.appName('BVL-F6-REST-'+args.dataset.replace('/','-'))
                 .config('spark.sql.ansi.enabled','true')
                 .config('spark.sql.shuffle.partitions','4').getOrCreate())
        objects = []
        for f in entries:
            raw = gcs.read_exact_current_bytes(client, f['uri'], f['generation'], f['sha256'],
                expected_size=f['size_bytes'], allowed_bucket=BRONZE_BUCKET)
            objects.append({**{k:f[k] for k in ('uri','generation','size_bytes','sha256')},
                            'data': raw})
        quarantine_rows = None
        coverage = None
        if args.dataset == 'smv/montos_sab':
            # Full 1221 pinned bytes were verified above by exact GCS read.
            normal, quarantine_rows, metrics = cuarentena.partition_montos_sab(
                [({k:o[k] for k in ('uri','generation','size_bytes','sha256')}, o['data'])
                 for o in objects], expected=cuarentena.OFFICIAL_EXPECTED)
            partial.check_actual_quarantine(partial_scope['excluded'],
                                            quarantine_rows,metrics)
            frame = integrator._records_to_frame(spark, normal)
            output_df, tmetrics = transform.dispatch_transform(args.dataset,frame)
            candidate = integrator.LocalCandidate(
                args.dataset, {SOURCES[args.dataset][0]:output_df},
                {'source': {'dataset':args.dataset,'objects':[
                    {k:o[k] for k in ('uri','generation','size_bytes','sha256')}
                    for o in objects], 'object_count':len(objects),
                    'total_bytes':sum(len(o['data']) for o in objects)},
                 'transformation': {'transform':tmetrics,'coverage':metrics},
                 'products':list(SOURCES[args.dataset]),
                 'publish_allowed':False,'claim_consumed':False})
            # Actual candidate is partial by contract, never mark full.
        else:
            candidate = integrator.local_transform_from_snapshot(spark, args.dataset, objects)
        auth = plan.authorization
        require(set(candidate.products) == set(SOURCES[args.dataset]),
                'Productos incorrectos en batch')
        prefixes = {x['name']:x['staging_prefix'] for x in auth['silver_outputs']}
        receipts = {}
        for product in SOURCES[args.dataset]:
            df = candidate.products[product]
            columns = tuple(PARTITIONS[args.dataset])
            require(all(x in df.columns for x in columns),
                    'Partición aprobada no está presente en DataFrame')
            receipts[product] = writer.stage_product_after_claim(client,
                verified_plan=plan, claim_generation=claim_generation,
                dataset=args.dataset, product=product, df=df,
                partitions=columns, pa=pa, pq=pq)
        quarantine_receipt = None
        if args.dataset == 'smv/montos_sab':
            # Immutable write, no retry, no overwrite, retains all 245 raw records.
            quarantine_receipt = _upload_candidate_manifest(
                client, prefixes[SOURCES[args.dataset][0]],
                {'status':'CUARENTENA_PARCIAL_PENDIENTE_QUALITY',
                 'scope_sha256':partial_scope['scope_sha256'],
                 'metrics':metrics,'files':quarantine_rows,
                 'gold_consumption_allowed':False},
                filename='montos_sab_quarantine_audit.json')
            coverage = partial.coverage_manifest(
                metrics, quality_scope_sha256=partial_scope['scope_sha256'],
                quarantine_receipt=quarantine_receipt)
        # Write only AFTER all product writes complete; for two outputs this is
        # ONE joint report with ALL files. Quality independently verifies it.
        output = {
            'schema_version': 'BVL-SILVER-STAGING-CANDIDATE-REST-v1',
            'status': 'CANDIDATO_PENDIENTE_QUALITY',
            'quality_approved': False, 'official_publication_allowed': False,
            'dataset': args.dataset, 'run_id': auth['run_id'],
            'attempt_id': auth['attempt_id'], 'fingerprint': plan.fingerprint,
            'claim_generation': str(claim_generation),
            'source_object_count': len(entries), 'source_bytes': total_bytes,
            'source': candidate.evidence['source'],
            'quality_metrics': candidate.evidence['transformation'],
            'partial_coverage':coverage,
            'quality_scope_sha256':(partial_scope['scope_sha256'] if partial_scope else None),
            'products': [{'name':p, 'staging_prefix':prefixes[p],
                          'files':receipts[p], 'files_count':len(receipts[p]),
                          'rows':sum(i['rows'] for i in receipts[p])}
                         for p in SOURCES[args.dataset]],
            'processor_commit': args.processor_commit,
            'signed_launch_sha256': context_sha, 'runtime': runtime,
            'batch_id': args.batch_id,
        }
        report = _upload_candidate_manifest(client,
            prefixes[SOURCES[args.dataset][0]], output)
        print(json.dumps({
            'status':'STAGING_CANDIDATO_PENDIENTE_QUALITY',
            'dataset':args.dataset, 'run_id': auth['run_id'],
            'attempt_id':auth['attempt_id'],
            'products': {p:len(receipts[p]) for p in receipts},
            'candidate_manifest':report,
            'published_official':False, 'gold_consumption_allowed':False,
        }, ensure_ascii=False, sort_keys=True), flush=True)
        return 0
    except Exception:
        print('ERROR_TRAS_CLAIM: NO reintentar, preservar staging/claim y conciliar con Quality',
              file=sys.stderr, flush=True)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == '__main__':
    raise SystemExit(main())
