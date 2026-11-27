#!/usr/bin/env python3
"""Quality-signed explicit partial scope for SMV Montos SAB; no cloud I/O by default.

Never authorizes publication or bypasses the Phase 5 gate. The 14 quarantined
pins must match exactly the official Bronze manifest and actual bytes.
"""
from __future__ import annotations
import re
from hashlib import sha256
from fase6_restantes_core import require

SCHEMA = 'BVL-F6-SMV-MONTOS-PARTIAL-SCOPE-CANDIDATE-v1'
STATUS = 'QUALITY_APPROVED_PARTIAL_STAGING_ONLY'
BRONZE_BUCKET = 'bvl26-bronze-7f3a9c'
CONTROL_BUCKET = 'bvl26-control-7f3a9c'
RUN_ID = 'bvl-bronze-inicial-20261125-001'
DATASET = 'smv/montos_sab'
PREFIX = f'gs://{BRONZE_BUCKET}/{DATASET}/run_id={RUN_ID}/'
HEX64 = re.compile(r'[0-9a-f]{64}\Z')
GEN = re.compile(r'[1-9][0-9]*\Z')
FIELDS = frozenset({
    'schema_version','status','quality_approved','dataset','run_id','attempt_id',
    'authorization_fingerprint','signed_launch_sha256','allow_partial_staging',
    'allow_publication','allow_quarantine_as_valid_rows','source_files',
    'normal_files','quarantine_files','source_rows','normal_rows','quarantine_rows',
    'excluded_objects','policy','quality_decision_id'
})
PIN_FIELDS = frozenset({'uri','generation','sha256','size_bytes'})
QUALITY_EXPECTED = {
    'source_files':1221,'normal_files':1207,'quarantine_files':14,
    'source_rows':22156,'normal_rows':21911,'quarantine_rows':245,
}


def _hex(value):
    return type(value) is str and HEX64.fullmatch(value) is not None


def pin_tuple(pin):
    require(type(pin) is dict and set(pin)==PIN_FIELDS, 'Objeto excluido: campos imprevistos')
    require(type(pin['uri']) is str and pin['uri'].startswith(PREFIX) and
            re.search(r'/montos_sab_[0-9]{8}\.json\Z',pin['uri']) is not None and
            '/..' not in pin['uri'] and '//' not in pin['uri'][5:],
            'Objeto fuera de Bronze oficial')
    require(type(pin['generation']) in (str,int) and GEN.fullmatch(str(pin['generation'])),
            'Generacion excluida invalida')
    require(_hex(pin['sha256']), 'SHA256 excluido invalido')
    require(type(pin['size_bytes']) is int and pin['size_bytes']>0,
            'Tamano excluido invalido')
    return (pin['uri'],str(pin['generation']),pin['sha256'],pin['size_bytes'])


def validate_scope(doc, official_entries, *, attempt_id, fingerprint, launch_sha256):
    """No signatures here: caller MUST verify KMS before this check."""
    require(type(doc) is dict and set(doc)==FIELDS, 'Partial scope con campos inesperados')
    expected = {
        'schema_version':SCHEMA,'status':STATUS,'quality_approved':True,
        'dataset':DATASET,'run_id':RUN_ID,'attempt_id':attempt_id,
        'authorization_fingerprint':fingerprint,'signed_launch_sha256':launch_sha256,
        'allow_partial_staging':True,'allow_publication':False,
        'allow_quarantine_as_valid_rows':False,
        'policy':'QUARANTINE_14_ERROR_FILES_NO_SILENT_DROP'
    }
    for key,value in expected.items():
        require(type(doc[key]) is type(value) and doc[key]==value,
                'Campo de alcance parcial no ratificado: '+key)
    require(type(doc['quality_decision_id']) is str and
            re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{7,127}',doc['quality_decision_id']),
            'Falta decision independiente de Quality')
    for key,value in QUALITY_EXPECTED.items():
        require(type(doc[key]) is int and doc[key]==value,
                'Cobertura parcial distinta de Quality: '+key)
    require(type(doc['excluded_objects']) is list and len(doc['excluded_objects'])==14,
            'Se requieren exactamente 14 objetos excluidos')
    excluded=[pin_tuple(x) for x in doc['excluded_objects']]
    require(len({p[0] for p in excluded})==14, 'Exclusion repetida')
    require(type(official_entries) is list and len(official_entries)==1221,
            'Inventario Bronze incompleto')
    official = {}
    for entry in official_entries:
        p=pin_tuple({k:entry[k] for k in PIN_FIELDS})
        require(p[0] not in official, 'Objeto Bronze repetido')
        official[p[0]]=p
    for p in excluded:
        require(official.get(p[0]) == p, 'Objeto excluido distinto de manifest oficial')
    return frozenset(p[0] for p in excluded)


def verify_signed_scope(client, refs, pem, official_entries, *, attempt_id, fingerprint,
                        launch_sha256):
    """External Quality references + exact GCS generations + Phase 5 pinned verifier."""
    import adaptador_gcs_silver_v1 as gcs
    import puerta_silver_operativa_v1_candidata as gate
    require(type(refs) is dict and set(refs)=={
        'uri','generation','sha256','signature_uri','signature_generation','signature_sha256'},
        'Referencias de alcance incompletas')
    base=(f'gs://{CONTROL_BUCKET}/authorizations/silver_partial/{DATASET}/'
          f'run_id={RUN_ID}/attempt_id={attempt_id}/')
    require(refs['uri']==base+'scope.json' and refs['signature_uri']==base+'scope.sig',
            'Referencia independiente fuera de scope')
    for k in ('sha256','signature_sha256'):
        require(_hex(refs[k]),'Hash de referencia invalido')
    for k in ('generation','signature_generation'):
        require(type(refs[k]) in (int,str) and GEN.fullmatch(str(refs[k])),
                'Generacion de referencia invalida')
    raw=gcs.read_exact_current_bytes(client,refs['uri'],refs['generation'],refs['sha256'],
                                      allowed_bucket=CONTROL_BUCKET)
    sig=gcs.read_exact_current_bytes(client,refs['signature_uri'],refs['signature_generation'],
                                     refs['signature_sha256'],allowed_bucket=CONTROL_BUCKET)
    doc=gate.signed(raw,sig,pem)
    excluded=validate_scope(doc,official_entries,attempt_id=attempt_id,
                            fingerprint=fingerprint,launch_sha256=launch_sha256)
    return {'document':doc,'excluded':excluded,'scope_sha256':sha256(raw).hexdigest()}


def check_actual_quarantine(excluded, quarantine, metrics):
    found={x['uri'] for x in quarantine}
    require(found==excluded,'Los 14 objetos clasificados no coinciden con scope firmado')
    for key, value in QUALITY_EXPECTED.items():
        mkey={'source_files':'files_bronze','normal_files':'files_normal',
              'quarantine_files':'files_quarantined','source_rows':'bronze_rows',
              'normal_rows':'rows_normal','quarantine_rows':'rows_quarantined'}[key]
        require(metrics[mkey]==value,'Conteo Bronze diferente del alcance firmado')
    require(metrics['rows_silently_discarded']==0 and not metrics['coverage_full'],
            'Cobertura incompatible con cuarentena')
    return True


def coverage_manifest(metrics, *, quality_scope_sha256, quarantine_receipt):
    require(_hex(quality_scope_sha256),'Scope SHA faltante')
    require(type(quarantine_receipt) is dict and quarantine_receipt.get('sha256'),
            'Falta artefacto auditable de cuarentena')
    return {
        'mode':'PARTIAL_QUARANTINE','status':'CANDIDATO_PARCIAL_PENDIENTE_QUALITY',
        'full_coverage':False,'normal_files':metrics['files_normal'],
        'quarantine_files':metrics['files_quarantined'],
        'normal_rows':metrics['rows_normal'],'quarantine_rows':metrics['rows_quarantined'],
        'bronze_rows':metrics['bronze_rows'],
        'coverage_pct':round(100*metrics['rows_normal']/metrics['bronze_rows'],6),
        'quality_partial_scope_sha256':quality_scope_sha256,
        'quarantine_receipt':quarantine_receipt,
        'allows_official_publication':False,'allows_gold_consumption':False,
    }
