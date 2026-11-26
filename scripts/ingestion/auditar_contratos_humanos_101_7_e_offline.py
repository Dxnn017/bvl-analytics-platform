#!/usr/bin/env python3
"""Control OFFLINE, no operativo, de los tres borradores humanos 101.7-E.

Nunca habilita traslados, no lee GCS, no accede a HDFS, no firma con KMS.
Los servicios criptográficos/registro lab son para pruebas con claves de TEST,
no constituyen un camino de activación productiva.
"""
import argparse
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import uuid

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

RUN = 'bvl-bronze-inicial-20261125-001'
PROJECT = 'bvl-analytics-2026'
SCHEMA = 'BVL-HUMAN-AUTHORIZATION-329C-v1-DRAFT'
RESERVATION = {'generation':'1795640338029794',
 'sha256':'80cb92939b975659d32514fff3a64a30e02148434222f1391b1afd675913c232',
 'uri':f'gs://bvl26-control-7f3a9c/reservations/bronze/{RUN}.json'}
QUALITY_PAYLOAD = '7a8c98a32e141b6e759755f137948bd33ffe192f696d5fc886334bf61aa6df6f'
QUALITY_KMS = f'projects/{PROJECT}/locations/southamerica-west1/keyRings/bvl-bronze-release-329c/cryptoKeys/quality-approved-signer/cryptoKeyVersions/1'
TRANSFER_KMS = f'projects/{PROJECT}/locations/southamerica-west1/keyRings/bvl-bronze-release-329c/cryptoKeys/transfer-allowed-signer/cryptoKeyVersions/1'
SCOPE = {'batch_identity_sha256':'e4a3fb69f25ee87a26e71a40506017c199d43cd13735bef3330462c906205a2c',
 'canonical_destination_list_sha256':'acf864e31fc403faec3bf67ba13555474e4fa185c3475763781ca88fb0b1d923',
 'destination_prefix':'gs://bvl26-bronze-7f3a9c/landing/', 'file_count':12,
 'full_plan_sha256':'1397f92911b159e8df257fc82a7dd01593fce0d2e898e796a82efc77ae35d90e',
 'mode':'PILOTO_12_ARCHIVOS',
 'pilot_json_sha256':'82f384ab19231d89b4f4487ab7424eb24250943b4bdd40c85e88e8e08b5d16b7',
 'total_bytes':23052390, 'unique_destination_count':12}
HUMAN = {'identity':'Lizeth - Quality 329C',
 'pem_sha256':'21c2212a5d66f54e76cfd8dd00571dafd49af6984db273f9996473a9487492b6',
 'spki_sha256':'f37b987739c0fc12e4da1d7bb9192c2e4e1f0dad8e65120f1228c27260b4862a',
 'trust_record_candidate_sha256':'482795a1d08a61a2b279d4bffc343af23efe5ffa375d6721e085f797cbaddf61'}
OLD_PEM_SHA = '2c96ef7b07084a88d7e21c0eb01b99c09d2ee74668895ba8ce57083a1bd0c51c'
POLICY = {'max_validity_seconds':86400,'single_use':True,'full_batch_authorized':False,
 'overwrite_allowed':False,'operational_authorization':False}
SIGNATURE = {'algorithm':'ECDSA_P256_SHA256','encoding':'ASN.1 DER','detached':True}
TARGETS = {
 'HUMAN_QUALITY_AUTHORIZATION':{'quality_payload_sha256':QUALITY_PAYLOAD,
                                  'quality_kms_key_version':QUALITY_KMS},
 'HUMAN_TRANSFER_AUTHORIZATION':{'transfer_payload_sha256':None,
    'quality_payload_sha256':QUALITY_PAYLOAD, 'quality_signature_sha256':None,
    'quality_human_authorization_sha256':None, 'quality_human_signature_sha256':None,
    'quality_publication_generation':None,'orchestrator_kms_key_version':TRANSFER_KMS},
 'HUMAN_EXECUTION_AUTHORIZATION':{'quality_payload_sha256':QUALITY_PAYLOAD,
    'quality_signature_sha256':None, 'quality_human_authorization_sha256':None,
    'quality_human_signature_sha256':None,'quality_publication_generation':None,
    'transfer_payload_sha256':None,'transfer_signature_sha256':None,
    'transfer_human_authorization_sha256':None,'transfer_human_signature_sha256':None,
    'transfer_publication_generation':None,'executor_identity':None,'executor_commit':None}
}
ACTIONS = {'HUMAN_QUALITY_AUTHORIZATION':'ISSUE_QUALITY_ONLY',
 'HUMAN_TRANSFER_AUTHORIZATION':'ISSUE_TRANSFER_ONLY',
 'HUMAN_EXECUTION_AUTHORIZATION':'EXECUTE_PILOT_ONLY'}
FILENAMES = {'HUMAN_QUALITY_AUTHORIZATION':'HUMAN_QUALITY_AUTHORIZATION_BORRADOR.json',
 'HUMAN_TRANSFER_AUTHORIZATION':'HUMAN_TRANSFER_AUTHORIZATION_BORRADOR.json',
 'HUMAN_EXECUTION_AUTHORIZATION':'HUMAN_EXECUTION_AUTHORIZATION_BORRADOR.json'}
ZIP_SHA = '78b0544ae1fea9a2d2b38c2206ac6aa65f09c5305894dde542b6a1c55f7c062a'
EXPECTED_FILE_SHA = {
 'HUMAN_QUALITY_AUTHORIZATION_BORRADOR.json':'dc6d11d554b07ae4afe472283bbe34253288dc71a2b32b79ea19b4117b2a6da4',
 'HUMAN_TRANSFER_AUTHORIZATION_BORRADOR.json':'b1712f79cb945a63b95d9337960cf639151bf4e0036e05015e5ba8b387255845',
 'HUMAN_EXECUTION_AUTHORIZATION_BORRADOR.json':'1b6631430492ad1d2b61799109726b00def78282189101be4c552a37464f2b53',
 'LEEME_CONTRATO_PROPUESTO.md':'c52feee21a1c067e3f35df4d0b7c3a0ad3bd83a4e2a700a27489a939e9e7da41'}
LAB_SCHEMA = 'BVL-HUMAN-AUTHORIZATION-329C-LAB-TEST-ONLY'
LAB_STATUS = 'LAB_TEST_ONLY_NOT_VALID_FOR_OPERATIONS'

class Rechazo(ValueError): pass

def exigir(ok, reason):
    if not ok: raise Rechazo(reason)

def sha(data): return hashlib.sha256(data).hexdigest()

def pares_unicos(pairs):
    out={}
    for key,val in pairs:
        exigir(key not in out,'CLAVE_JSON_DUPLICADA')
        out[key]=val
    return out

def parse_strict(raw):
    exigir(type(raw) is bytes,'DOCUMENTO_NO_BYTES')
    return json.loads(raw.decode('utf-8'), object_pairs_hook=pares_unicos,
        parse_constant=lambda x: (_ for _ in ()).throw(Rechazo('CONSTANTE_JSON_INVALIDA')))

def exactamente(obs,expected,code):
    exigir(type(obs) is dict and set(obs)==set(expected),code+'_CAMPOS')
    for key,v in expected.items():
        exigir(type(obs[key]) is type(v) and obs[key]==v,code+'_'+key)

def verificar_pem_humano(raw):
    exigir(sha(raw)!=OLD_PEM_SHA,'CLAVE_ANTERIOR_REVOCADA')
    exigir(sha(raw)==HUMAN['pem_sha256'],'PEM_NO_ANCLADO')
    key=serialization.load_pem_public_key(raw)
    exigir(isinstance(key,ec.EllipticCurvePublicKey) and isinstance(key.curve,ec.SECP256R1), 'CURVA_DISTINTA')
    spki=key.public_bytes(serialization.Encoding.DER,serialization.PublicFormat.SubjectPublicKeyInfo)
    exigir(sha(spki)==HUMAN['spki_sha256'],'SPKI_NO_ANCLADO')
    return key

def validar_borrador(raw, kind):
    exigir(kind in TARGETS,'TIPO_DESCONOCIDO')
    doc=parse_strict(raw)
    exigir(type(doc) is dict and set(doc)=={'schema_version','status','authorization_id','issued_at_utc',
      'expires_at_utc','project_id','run_id','reservation','scope','human_identity','signature',
      'policy','document_type','action','target'},'CAMPOS_PRINCIPALES')
    expected={'schema_version':SCHEMA,'status':'BORRADOR_NO_FIRMAR','authorization_id':None,
     'issued_at_utc':None,'expires_at_utc':None,'project_id':PROJECT,'run_id':RUN,
     'reservation':RESERVATION,'scope':SCOPE,'human_identity':HUMAN,'signature':SIGNATURE,
     'policy':POLICY,'document_type':kind,'action':ACTIONS[kind], 'target':TARGETS[kind]}
    for k,v in expected.items():
        if type(v) is dict: exactamente(doc[k],v,k.upper())
        else: exigir(type(doc[k]) is type(v) and doc[k]==v,'CAMPO_'+k.upper())
    exigir(doc['policy']['operational_authorization'] is False,'AUTORIZACION_NO_PERMITIDA')
    return doc

def auditar_paquete(folder, public_pem, pilot_raw=None, plan_raw=None,
                     quality_raw=None, transfer_raw=None):
    folder=Path(folder)
    sums=(folder/'SHA256SUMS.txt').read_text(encoding='utf-8')
    parsed={}
    for line in sums.splitlines():
        a=line.split()
        exigir(len(a)==2 and a[1] not in parsed,'COMPROBANTE_INVALIDO')
        parsed[a[1]]=a[0]
    exigir(parsed==EXPECTED_FILE_SHA,'LISTA_HASH_DESCONOCIDA')
    for name,hash_expected in EXPECTED_FILE_SHA.items():
        exigir(sha((folder/name).read_bytes())==hash_expected,'HASH_'+name)
    verificar_pem_humano(Path(public_pem).read_bytes())
    docs={k:validar_borrador((folder/f).read_bytes(),k) for k,f in FILENAMES.items()}
    exigir(len({d['action'] for d in docs.values()})==3,'ACCIONES_NO_INDEPENDIENTES')
    # Anclar contra artefactos reales locales, cuando están disponibles.
    for raw,digest,label in ((pilot_raw,SCOPE['pilot_json_sha256'],'PILOTO'),
                              (plan_raw,SCOPE['full_plan_sha256'],'PLAN'),
                              (quality_raw,QUALITY_PAYLOAD,'QUALITY'),
                              (transfer_raw,'f3173e22d1e88f5518b5ef476d000a8a2a168df9d1a43d0a63ba285cd925b65e','TRANSFER')):
        if raw is not None: exigir(sha(raw)==digest,'REFERENCIA_'+label)
    if pilot_raw is not None:
        p=parse_strict(pilot_raw)
        ops=p['archivos_piloto']
        exigir(p['run_id']==RUN and len(ops)==12 and sum(o['bytes_esperados'] for o in ops)==23052390,'PILOTO_ALCANCE')
        destinos=[o['destino_gcs'] for o in ops]
        exigir(len(set(destinos))==12,'DESTINOS_DUPLICADOS')
        canon=json.dumps(sorted(destinos),ensure_ascii=False,separators=(',',':')).encode('utf-8')
        exigir(sha(canon)==SCOPE['canonical_destination_list_sha256'],'HUELLA_DESTINOS')
    return {'estado':'TRES_BORRADORES_COMPATIBLES_NO_OPERATIVOS',
       'contratos':len(docs),'acciones_independientes':3,'archivos_piloto':12,
       'bytes':23052390,'evidencia_firmas_reales':False,
       'autorizacion_humana_efectiva':False,'autorizacion_ejecucion':False,
       'transfer_allowed':False,'operaciones_gcs':0}

def verificar_firma_detached(raw, signature_der, pem, pinned_spki_sha256):
    """Primitiva criptográfica; SOLO certificados/keys de TEST en tests.
    No interpreta ni habilita permisos por sí misma.
    """
    key=serialization.load_pem_public_key(pem)
    exigir(isinstance(key,ec.EllipticCurvePublicKey) and isinstance(key.curve,ec.SECP256R1),'FIRMA_CLAVE_CURVA')
    spki=key.public_bytes(serialization.Encoding.DER,serialization.PublicFormat.SubjectPublicKeyInfo)
    exigir(sha(spki)==pinned_spki_sha256,'FIRMA_CLAVE_NO_CONFIABLE')
    try: key.verify(signature_der,raw,ec.ECDSA(hashes.SHA256()))
    except (InvalidSignature,ValueError) as exc: raise Rechazo('FIRMA_NO_VALIDA') from exc
    return True

def utc_timestamp(v):
    exigir(type(v) is str and re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ',v),'FECHA_FORMATO')
    try: dt=datetime.strptime(v,'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
    except ValueError as ex: raise Rechazo('FECHA_INVALIDA') from ex
    return dt

def validar_lab(raw,kind,now=None):
    """Solo valida prueba sintética; explícitamente NO habilita actuaciones reales."""
    exigir(kind in TARGETS,'TIPO_INVALIDO')
    d=parse_strict(raw)
    exigir(type(d) is dict and set(d)=={'schema_version','status','authorization_id','issued_at_utc',
       'expires_at_utc','project_id','run_id','reservation','scope','human_identity','signature',
       'policy','document_type','action','target'},'CAMPOS_LAB')
    exigir(d['schema_version']==LAB_SCHEMA and d['status']==LAB_STATUS,'DOCUMENTO_NO_ES_LAB')
    exigir(d['document_type']==kind and d['action']==ACTIONS[kind],'ACCION_CRUZADA')
    exigir(d['project_id']==PROJECT and d['run_id']==RUN,'IDENTIDAD_LOTE')
    for f,v in (('reservation',RESERVATION),('scope',SCOPE),('human_identity',HUMAN),('signature',SIGNATURE),('policy',POLICY)):
        exactamente(d[f],v,f.upper())
    exigir(type(d['authorization_id']) is str,'UUID_TIPO')
    try: ident=uuid.UUID(d['authorization_id'])
    except (ValueError,AttributeError) as ex: raise Rechazo('UUID_INVALIDO') from ex
    exigir(str(ident)==d['authorization_id'],'UUID_CANONICO')
    issued=utc_timestamp(d['issued_at_utc']); expires=utc_timestamp(d['expires_at_utc'])
    now=now or datetime.now(timezone.utc)
    exigir(expires>issued and (expires-issued)<=timedelta(hours=24),'VIGENCIA_MAXIMA')
    exigir(issued<=now<expires,'NO_VIGENTE')
    exigir(type(d['target']) is dict and set(d['target'])==set(TARGETS[kind]),'TARGET_CAMPOS')
    exigir(d['policy']['operational_authorization'] is False,'POLITICA_LAB')
    for key,val in TARGETS[kind].items():
        if val is not None:
            exigir(type(d['target'][key]) is type(val) and d['target'][key]==val,'TARGET_'+key)
        else:
            if key in ('executor_identity','executor_commit'):
                exigir(type(d['target'][key]) is str and bool(d['target'][key]),'TARGET_'+key+'_NO_FIJADO')
            elif key.endswith('generation'):
                exigir(type(d['target'][key]) is int and d['target'][key]>0,'TARGET_GENERACION_INVALIDA')
            else:
                exigir(type(d['target'][key]) is str and re.fullmatch('[0-9a-f]{64}',d['target'][key]),'TARGET_SHA_INVALIDO')
    if kind=='HUMAN_EXECUTION_AUTHORIZATION':
        exigir(d['target']['executor_identity']=='sa-bvl-ingestion@bvl-analytics-2026.iam.gserviceaccount.com', 'IDENTIDAD_EJECUTORA')
        exigir(re.fullmatch('[0-9a-f]{40}',d['target']['executor_commit']) is not None,'COMMIT_INCOMPLETO')
    return d

def validar_cadena_lab(q,t,e,qs,ts,es,pem,pinned,now,identity_confirmed=False):
    """Se usa solo con artefactos sintéticos (estado LAB_TEST_ONLY)."""
    exigir(identity_confirmed is True,'IDENTIDAD_NO_CONFIRMADA')
    Q=validar_lab(q,'HUMAN_QUALITY_AUTHORIZATION',now)
    T=validar_lab(t,'HUMAN_TRANSFER_AUTHORIZATION',now)
    E=validar_lab(e,'HUMAN_EXECUTION_AUTHORIZATION',now)
    for raw,sig in ((q,qs),(t,ts),(e,es)):
        verificar_firma_detached(raw,sig,pem,pinned)
    exigir(len({Q['authorization_id'],T['authorization_id'],E['authorization_id']})==3,'AUTORIZACIONES_ID_REPETIDO')
    exigir(utc_timestamp(Q['issued_at_utc'])<=utc_timestamp(T['issued_at_utc'])<=utc_timestamp(E['issued_at_utc']),'ORDEN_CADENA')
    for name,expect in {'quality_human_authorization_sha256':sha(q),'quality_human_signature_sha256':sha(qs)}.items():
        exigir(T['target'][name]==expect,'VINCULO_TRANSFER_'+name)
        exigir(E['target'][name]==expect,'VINCULO_EXEC_'+name)
    for name,expect in {'transfer_human_authorization_sha256':sha(t),'transfer_human_signature_sha256':sha(ts)}.items():
        exigir(E['target'][name]==expect,'VINCULO_EXEC_'+name)
    for f in ('quality_signature_sha256','quality_publication_generation'):
        exigir(T['target'][f]==E['target'][f],'REFERENCIA_Q_NO_COINCIDE')
    exigir(T['target']['transfer_payload_sha256']==E['target']['transfer_payload_sha256'],'REFERENCIA_TRANSFER_NO_COINCIDE')
    # Sin integración con firma KMS/Control ni con aprobación operativa.
    return {'cadena_laboratorio':'VALIDA_SIN_CAPACIDAD_OPERATIVA','transfer_allowed':False,'operaciones_gcs':0}

def verificar_registro_ancorado(registro_raw, sha256_externo_confiable):
    """Comprobación offline. El ancla DEBE venir de un canal independiente,
    no de ningún borrador; por sí sola no autentica al autor.
    """
    exigir(type(sha256_externo_confiable) is str and
           re.fullmatch('[0-9a-f]{64}',sha256_externo_confiable),'ANCLA_EXTERNA_INVALIDA')
    exigir(sha(registro_raw)==sha256_externo_confiable,'REGISTRO_MANIPULADO')
    exigir(type(parse_strict(registro_raw)) is dict,'REGISTRO_JSON_INVALIDO')
    return True

class LibroConsumoLocal:
    """Ejemplo laboratorio SQLite: no sustituye cerrojo distribuido de Control."""
    def __init__(self,db_path): self.db_path=str(db_path)
    def reclamar(self,authorization_id,action,doc_hash):
        exigir(type(authorization_id) is str and uuid.UUID(authorization_id),'UUID')
        exigir(action in ACTIONS.values(),'ACCION')
        exigir(type(doc_hash) is str and re.fullmatch('[0-9a-f]{64}',doc_hash),'HASH')
        with sqlite3.connect(self.db_path,timeout=10, isolation_level='IMMEDIATE') as c:
            c.execute('CREATE TABLE IF NOT EXISTS consumos (authorization_id TEXT PRIMARY KEY, action TEXT NOT NULL, document_hash TEXT NOT NULL, state TEXT NOT NULL, timestamp TEXT NOT NULL)')
            try:
                c.execute('INSERT INTO consumos VALUES (?,?,?,?,?)',
                  (authorization_id,action,doc_hash,'RECLAMADO_LAB',datetime.now(timezone.utc).isoformat()))
            except sqlite3.IntegrityError as e: raise Rechazo('AUTORIZACION_YA_CONSUMIDA') from e
        return True

def main():
    ap=argparse.ArgumentParser(description='Auditar borradores humanos 101.7-E SIN CLOUD; nunca autoriza')
    ap.add_argument('--fixtures',default='fixtures')
    ap.add_argument('--human-key', default='fixtures/CLAVE_HUMANA_REFERENCIA_PUBLICA_LIZETH.pem')
    ap.add_argument('--pilot',default='fixtures/piloto_12_CANDIDATO_LOCAL.json')
    ap.add_argument('--plan',default='fixtures/plan_3599_CANDIDATO_LOCAL.json')
    ap.add_argument('--quality',default='fixtures/QUALITY_APPROVED_PAYLOAD_PROPUESTO_NO_FIRMAR.json')
    ap.add_argument('--transfer',default='fixtures/TRANSFER_ALLOWED_V2_BORRADOR_NO_FIRMAR.json')
    args=ap.parse_args()
    def read_required(p):
        return Path(p).read_bytes()
    try:
        out=auditar_paquete(args.fixtures,args.human_key,read_required(args.pilot),
             read_required(args.plan),read_required(args.quality),read_required(args.transfer))
        print(json.dumps(out,ensure_ascii=False,indent=2))
        print('RESULTADO: CONTRATOS_HUMANOS_101_7_E_BORRADORES_COMPATIBLES_SIN_CLOUD')
        return 0
    except Exception as ex:
        print('RESULTADO: CONTRATOS_HUMANOS_RECHAZADOS:',type(ex).__name__,str(ex),file=sys.stderr)
        return 1
if __name__=='__main__': sys.exit(main())
