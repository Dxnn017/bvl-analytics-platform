#!/usr/bin/env python3
"""Auditoria OFFLINE, denegacion permanente, de TRANSFER_ALLOWED V2 (borrador)."""
import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

RUN = 'bvl-bronze-inicial-20261125-001'
PROJECT = 'bvl-analytics-2026'
BRONZE = 'gs://bvl26-bronze-7f3a9c/landing/'
CONTROL = f'gs://bvl26-control-7f3a9c/reservations/bronze/{RUN}.json'
PLAN_SHA = '1397f92911b159e8df257fc82a7dd01593fce0d2e898e796a82efc77ae35d90e'
PILOT_SHA = '82f384ab19231d89b4f4487ab7424eb24250943b4bdd40c85e88e8e08b5d16b7'
QUALITY_SHA = '7a8c98a32e141b6e759755f137948bd33ffe192f696d5fc886334bf61aa6df6f'
DRAFT_SHA = 'f3173e22d1e88f5518b5ef476d000a8a2a168df9d1a43d0a63ba285cd925b65e'
RES_SHA = '80cb92939b975659d32514fff3a64a30e02148434222f1391b1afd675913c232'
RES_GEN = '1795640338029794'
DEST_SHA = 'acf864e31fc403faec3bf67ba13555474e4fa185c3475763781ca88fb0b1d923'
QUALITY_SPKI_SHA = '5c70c40088a680cfd7ced1859c4d6b335652d96991cdcae4268819fb4f90f5ce'
TRANSFER_SPKI_SHA = '194eedcdbfc6b09cbf468b434736ef22a14259b83ba5859d5caa8ac6e3ebbb82'
TRANSFER_PEM_SHA = '105c7155465d1deaf35e2386fed26a0b17a1dad996520b011f1be11dfdb7abc2'
KEY_VERSION = ('projects/bvl-analytics-2026/locations/southamerica-west1/'
               'keyRings/bvl-bronze-release-329c/cryptoKeys/transfer-allowed-signer/cryptoKeyVersions/1')

class Rechazo(ValueError): pass

def exigir(test, code):
    if not test: raise Rechazo(code)

def digest(raw): return hashlib.sha256(raw).hexdigest()

def parse_json_strict(raw):
    def no_duplicate(pairs):
        d={}
        for k,v in pairs:
            exigir(k not in d, 'CLAVE_JSON_DUPLICADA')
            d[k]=v
        return d
    return json.loads(raw, object_pairs_hook=no_duplicate)

def verificar_clave(raw, sha_expected):
    key=serialization.load_pem_public_key(raw)
    exigir(isinstance(key,ec.EllipticCurvePublicKey) and isinstance(key.curve,ec.SECP256R1),'CLAVE_NO_ECDSA_P256')
    spki=key.public_bytes(serialization.Encoding.DER,serialization.PublicFormat.SubjectPublicKeyInfo)
    exigir(digest(spki)==sha_expected,'HUELLA_SPKI_DIFERENTE')

def verificar_campos(d, expected, prefix):
    exigir(isinstance(d,dict),'OBJETO_'+prefix)
    for k,v in expected.items():
        exigir(k in d and type(d[k]) is type(v) and d[k]==v,prefix+'_'+k.upper())

def validar_contenido(d, pilot, plan, quality, public_quality, public_transfer):
    """Permite tests negativos por estructura; EN PRODUCCION se exige también validar_raw()."""
    exigir(type(d) is dict,'BORRADOR_RAIZ')
    exigir(set(d)=={'document_type','schema_version','status','project_id','run_id','issuer_candidate',
                   'reservation','quality','scope','policy','signature_proposal','pending','semantic_policy'},'BORRADOR_CLAVES')
    verificar_campos(d,{'document_type':'TRANSFER_ALLOWED_DRAFT_NON_OPERATIVE',
      'schema_version':'329C-TRANSFER-DRAFT-2','status':'BORRADOR_NO_AUTORIZADO_NO_FIRMAR',
      'project_id':PROJECT,'run_id':RUN,
      'issuer_candidate':'sa-bvl-orchestrator@bvl-analytics-2026.iam.gserviceaccount.com'},'BORRADOR')
    verificar_campos(d['reservation'],{'generation':RES_GEN,'sha256':RES_SHA,'uri':CONTROL},'RESERVA')
    verificar_campos(d['quality'],{'candidate_payload_sha256':QUALITY_SHA,
      'official_signed_payload_sha256':None,'public_key_spki_sha256':QUALITY_SPKI_SHA},'QUALITY')
    verificar_campos(d['signature_proposal'],{'algorithm':'EC_SIGN_P256_SHA256','encoding':'ASN.1 DER',
      'detached':True,'kms_key_version':KEY_VERSION,'public_key_pem_sha256':TRANSFER_PEM_SHA,
      'public_key_spki_sha256':TRANSFER_SPKI_SHA},'FIRMA')
    verificar_clave(public_quality,QUALITY_SPKI_SHA)
    verificar_clave(public_transfer,TRANSFER_SPKI_SHA)
    exigir(digest(public_transfer)==TRANSFER_PEM_SHA,'HUELLA_PEM_ORCHESTRATOR')
    pol=d['policy']
    verificar_campos(pol,{'transfer_allowed':False,'full_batch_authorized':False,
      'overwrite_allowed':False,'required_create_precondition':'ifGenerationMatch=0',
      'require_source_hash_and_size_verification':True,
      'require_destination_hash_and_size_verification':True,
      'require_independent_human_approval':True,
      'require_independent_execution_authorization':True,'fail_closed':True},'POLITICA')
    exigir(set(pol)=={'transfer_allowed','full_batch_authorized','overwrite_allowed','required_create_precondition',
                    'require_source_hash_and_size_verification','require_destination_hash_and_size_verification',
                    'require_independent_human_approval','require_independent_execution_authorization','fail_closed'},
           'POLITICA_CLAVES')
    scope=d['scope']
    verificar_campos(scope,{'mode':'PILOTO_12_ARCHIVOS','file_count':12,
      'unique_destination_count':12,'total_bytes':23052390,'pilot_json_sha256':PILOT_SHA,
      'full_plan_sha256':PLAN_SHA,'canonical_destination_list_sha256':DEST_SHA},'ALCANCE')
    exigir(set(scope)=={'mode','file_count','unique_destination_count','total_bytes','pilot_json_sha256',
            'full_plan_sha256','canonical_destination_list_sha256','files'},'ALCANCE_CLAVES')
    chosen=scope['files']; esperar=pilot['archivos_piloto']
    exigir(type(chosen) is list and len(chosen)==12,'ALCANCE_CANTIDAD')
    exigir(len(esperar)==12 and pilot.get('run_id')==RUN,'PILOTO_REFERENCIA')
    exigir(plan.get('run_id')==RUN and len(plan['operaciones'])==3599,'PLAN_REFERENCIA')
    exigir(sum(x['size_bytes'] for x in plan['operaciones'])==393623640,'PLAN_BYTES')
    mapa={}
    for op in plan['operaciones']:
        exigir(op.get('destino') not in mapa,'PLAN_DESTINO_DUPLICADO')
        mapa[op.get('destino')]=op
    destinos=[]; datasets=set(); total=0
    fields={'dataset','origen_hdfs','destino_gcs','bytes_esperados','sha256_esperado'}
    for i,(item,orig) in enumerate(zip(chosen,esperar)):
        exigir(type(item) is dict and set(item)==fields,'ARCHIVO_CAMPOS')
        exigir(type(orig) is dict,'PILOTO_ARCHIVO')
        for campo in fields:
            exigir(type(item[campo]) is type(orig[campo]) and item[campo]==orig[campo],f'ARCHIVO_{i+1}_{campo.upper()}')
        origen=item['origen_hdfs']; dest=item['destino_gcs']; dataset=item['dataset']
        exigir(type(dataset) is str and bool(re.fullmatch('[a-z0-9_]+/[a-z0-9_]+',dataset)),'DATASET_FORMATO')
        origen_pref=f'/datalake/bvl/bronze/{dataset}/'
        destino_pref=f'{BRONZE}{dataset}/run_id={RUN}/'
        exigir(origen.startswith(origen_pref),'ORIGEN_PREFIJO')
        relative=origen[len(origen_pref):]
        exigir(relative and all(x not in ('','.', '..') for x in relative.split('/')) and '\\' not in relative,'RUTA_INSEGURA')
        exigir(dest==destino_pref+relative,'DESTINO_FUERA_CONTRATO')
        exigir(type(item['bytes_esperados']) is int and item['bytes_esperados']>=0,'BYTES_TIPO')
        exigir(type(item['sha256_esperado']) is str and bool(re.fullmatch(r'[0-9a-f]{64}',item['sha256_esperado'])),'SHA_ARCHIVO_FORMATO')
        op=mapa.get(dest)
        exigir(op is not None and (op['dataset'],op['origen'],op['size_bytes'],op['sha256']) ==
               (dataset,origen,item['bytes_esperados'],item['sha256_esperado']),'NO_COINCIDE_PLAN_3599')
        destinos.append(dest);datasets.add(dataset);total+=item['bytes_esperados']
    exigir(len(set(destinos))==12 and len(datasets)==12 and total==23052390,'ALCANCE_12_UNICOS')
    canon=json.dumps(sorted(destinos),ensure_ascii=False,separators=(',',':')).encode('utf-8')
    exigir(digest(canon)==DEST_SHA,'HUELLA_DESTINOS')
    exigir(d.get('semantic_policy')=='Preservar bytes y hashes originales; no declarar excepciones como datos de negocio validos','SEMANTICA')
    exigir(type(d.get('pending')) is list and len(d['pending'])>=1,'PENDIENTES_FALTANTES')
    return {'integridad':'PASS','esquema':'PASS','rutas_y_12_archivos':'PASS',
            'bytes':total,'origenes_HDFS':12,'huellas_kms':'PASS',
            'status':'BORRADOR_NO_OPERATIVO','firma_quality_oficial':False,
            'firma_orchestrator_oficial':False,'autorizacion_humana_operativa':False,
            'autorizacion_ejecucion':False,'transfer_allowed':False,'operaciones_gcs':0}

def validar_raw(draft, pilot, plan, quality, pem_q,pem_t):
    for raw,expected,name in ((draft,DRAFT_SHA,'BORRADOR'),(pilot,PILOT_SHA,'PILOTO'),
                               (plan,PLAN_SHA,'PLAN'),(quality,QUALITY_SHA,'QUALITY')):
        exigir(digest(raw)==expected,'HASH_'+name+'_NO_COINCIDE')
    return validar_contenido(parse_json_strict(draft), parse_json_strict(pilot),parse_json_strict(plan),
                            parse_json_strict(quality),pem_q,pem_t)

def cli():
    ap=argparse.ArgumentParser(description='Auditoría local estricta borrador TRANSFER_ALLOWED V2; jamás habilita GCS')
    ap.add_argument('--draft',default='fixtures/TRANSFER_ALLOWED_V2_BORRADOR_NO_FIRMAR.json')
    ap.add_argument('--pilot',default='fixtures/piloto_12_CANDIDATO_LOCAL.json')
    ap.add_argument('--plan',default='fixtures/plan_3599_CANDIDATO_LOCAL.json')
    ap.add_argument('--quality',default='fixtures/QUALITY_APPROVED_PAYLOAD_PROPUESTO_NO_FIRMAR.json')
    ap.add_argument('--quality-key',default='config/quality329c/keys/bvl_quality_official_v1.pem')
    ap.add_argument('--transfer-key',default='config/quality329c/keys/bvl_transfer_official_v1.pem')
    args=ap.parse_args()
    try:
        out=validar_raw(*(Path(x).read_bytes() for x in (args.draft,args.pilot,args.plan,args.quality,args.quality_key,args.transfer_key)))
        print(json.dumps(out,indent=2,ensure_ascii=False))
        print('RESULTADO: TRANSFER_V2_BORRADOR_LOCAL_COMPATIBLE_SIN_CLOUD')
        return 0
    except Exception as exc:
        print(f'RESULTADO: BORRADOR_V2_RECHAZADO_{type(exc).__name__}: {exc}',file=sys.stderr)
        return 1

if __name__=='__main__':sys.exit(cli())
