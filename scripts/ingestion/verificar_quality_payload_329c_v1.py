#!/usr/bin/env python3
"""Auditoría estrictamente OFFLINE del payload QUALITY_APPROVED 329C v1.

Nunca autoriza transferencias ni usa HDFS/GCS/KMS. La validación criptográfica
solo puede ejecutarse si se entrega una firma DER ya emitida externamente.
La evidencia humana y TRANSFER_ALLOWED carecen todavía de formato ratificado;
por diseño, este módulo siempre devuelve transfer_allowed=False.
"""
import argparse
import hashlib
import hmac
import json
import re
import sys
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

RUN = 'bvl-bronze-inicial-20261125-001'
PROJECT = 'bvl-analytics-2026'
SCHEMA = '329C-PROPUESTA-PAYLOAD-1'
RES_URI = f'gs://bvl26-control-7f3a9c/reservations/bronze/{RUN}.json'
RES_GEN = '1795640338029794'
RES_SHA = '80cb92939b975659d32514fff3a64a30e02148434222f1391b1afd675913c232'
PREFIX = 'gs://bvl26-bronze-7f3a9c/landing/'
PLAN_SHA = '1397f92911b159e8df257fc82a7dd01593fce0d2e898e796a82efc77ae35d90e'
PILOT_SHA = '82f384ab19231d89b4f4487ab7424eb24250943b4bdd40c85e88e8e08b5d16b7'
PAYLOAD_SHA = '7a8c98a32e141b6e759755f137948bd33ffe192f696d5fc886334bf61aa6df6f'
BATCH_SHA = 'e4a3fb69f25ee87a26e71a40506017c199d43cd13735bef3330462c906205a2c'
DEST_SHA = 'acf864e31fc403faec3bf67ba13555474e4fa185c3475763781ca88fb0b1d923'
QUALITY_SPKI = '5c70c40088a680cfd7ced1859c4d6b335652d96991cdcae4268819fb4f90f5ce'
ORCH_SPKI = '194eedcdbfc6b09cbf468b434736ef22a14259b83ba5859d5caa8ac6e3ebbb82'
QUALITY_KEY = f'projects/{PROJECT}/locations/southamerica-west1/keyRings/bvl-bronze-release-329c/cryptoKeys/quality-approved-signer/cryptoKeyVersions/1'
EXPECTED_PREFLIGHT_SHA = '36a74b1b33849668246e266faf48128a55a860021ab6deef30c2114457d76503'

class AuditoriaError(ValueError):
    pass


def exigir(condicion, codigo):
    if not condicion:
        raise AuditoriaError(codigo)


def sha(b):
    return hashlib.sha256(b).hexdigest()


def pares_unicos(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise AuditoriaError('JSON_CLAVE_DUPLICADA:' + key)
        result[key] = value
    return result


def cargar_json_exacto(raw):
    exigir(type(raw) is bytes and bool(raw), 'JSON_BYTES_REQUERIDOS')
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=pares_unicos,
                          parse_constant=lambda x: (_ for _ in ()).throw(AuditoriaError('JSON_CONSTANTE_INVALIDA')))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise AuditoriaError('JSON_INVALIDO') from e


def validar_campos(obj, required, code, *, exact=True):
    exigir(type(obj) is dict, code + '_TIPO')
    if exact:
        exigir(set(obj) == set(required), code + '_CLAVES')
    else:
        exigir(set(required) <= set(obj), code + '_CLAVES')
    for key, expected in required.items():
        observed = obj.get(key)
        exigir(type(observed) is type(expected) and observed == expected,
               code + '_' + key.upper())


def validar_ruta(relative):
    exigir(type(relative) is str and bool(relative), 'RUTA_TIPO')
    exigir('\\' not in relative and '//' not in relative, 'RUTA_BARRAS_INVALIDAS')
    exigir(all(x not in ('', '.', '..') for x in relative.split('/')), 'RUTA_TRAVERSAL')
    exigir(not relative.startswith('/'), 'RUTA_ABSOLUTA')


def auditar_referencias(plan_raw, pilot_raw):
    exigir(sha(plan_raw) == PLAN_SHA, 'PLAN_SHA_NO_COINCIDE')
    exigir(sha(pilot_raw) == PILOT_SHA, 'PILOTO_SHA_NO_COINCIDE')
    plan, pilot = cargar_json_exacto(plan_raw), cargar_json_exacto(pilot_raw)
    exigir(plan.get('run_id') == RUN and pilot.get('run_id') == RUN, 'REFERENCIAS_RUN')
    ops, selected = plan.get('operaciones'), pilot.get('archivos_piloto')
    exigir(type(ops) is list and len(ops) == 3599, 'PLAN_CANTIDAD')
    exigir(type(selected) is list and len(selected) == 12, 'PILOTO_CANTIDAD')
    exigir(sum(op.get('size_bytes', -1) for op in ops) == 393623640, 'PLAN_BYTES')
    mapa = {}
    for op in ops:
        exigir(type(op) is dict, 'OPERACION_TIPO')
        for key in ('origen','destino','dataset','size_bytes','sha256'):
            exigir(key in op, 'OPERACION_CAMPO_' + key)
        dataset = op['dataset']
        exigir(type(dataset) is str and re.fullmatch(r'[a-z0-9_]+/[a-z0-9_]+',dataset) is not None,
               'DATASET_INVALIDO')
        source_prefix = f'/datalake/bvl/bronze/{dataset}/'
        dest_prefix = PREFIX + f'{dataset}/run_id={RUN}/'
        exigir(type(op['origen']) is str and op['origen'].startswith(source_prefix), 'ORIGEN_NO_ADMITIDO')
        exigir(type(op['destino']) is str and op['destino'].startswith(dest_prefix), 'DESTINO_NO_ADMITIDO')
        rel = op['origen'][len(source_prefix):]
        validar_ruta(rel)
        exigir(op['destino'] == dest_prefix + rel, 'ORIGEN_DESTINO_NO_COINCIDEN')
        exigir(type(op['size_bytes']) is int and op['size_bytes'] >= 0, 'TAMANO_TIPO')
        exigir(type(op['sha256']) is str and re.fullmatch(r'[0-9a-f]{64}',op['sha256']), 'HASH_ARCHIVO_INVALIDO')
        exigir(op['destino'] not in mapa, 'DESTINO_DUPLICADO_PLAN')
        mapa[op['destino']] = op
    destinos = []
    datasets = set()
    for item in selected:
        exigir(type(item) is dict and item.get('run_id') == RUN, 'PILOTO_RUN')
        dest = item.get('destino_gcs')
        exigir(type(dest) is str and dest in mapa, 'DESTINO_PILOTO_NO_AUTORIZADO')
        op = mapa[dest]
        exigir(item.get('origen_hdfs') == op['origen'] and item.get('dataset') == op['dataset'],
               'PILOTO_ORIGEN_DATASET')
        exigir(type(item.get('bytes_esperados')) is int and item['bytes_esperados'] == op['size_bytes'],
               'PILOTO_BYTES_ARCHIVO')
        exigir(item.get('sha256_esperado') == op['sha256'], 'PILOTO_SHA_ARCHIVO')
        destinos.append(dest)
        datasets.add(op['dataset'])
    exigir(len(set(destinos)) == 12 and len(datasets) == 12, 'PILOTO_DESTINOS_DATASETS')
    exigir(sum(x['bytes_esperados'] for x in selected) == 23052390, 'PILOTO_BYTES_TOTAL')
    # Lista oficial: JSON compacto, UTF-8, destinos ordenados lexicográficamente.
    canonical = json.dumps(sorted(destinos), ensure_ascii=False,
                           separators=(',',':')).encode('utf-8')
    exigir(sha(canonical) == DEST_SHA, 'HASH_LISTA_DESTINOS_NO_COINCIDE')
    return {'datasets':len(datasets), 'archivos':len(destinos), 'bytes':23052390,
            'canonical_destination_list_sha256': sha(canonical)}


def validar_payload_329c(raw, plan_raw, pilot_raw):
    # El documento propuesto exacto está fijado por SHA; ninguna reserialización.
    exigir(sha(raw) == PAYLOAD_SHA, 'PAYLOAD_SHA_NO_COINCIDE')
    d = cargar_json_exacto(raw)
    exigir(type(d) is dict and set(d) == {
        'authorization_policy','document_type','issuer','preflight_evidence',
        'project_id','reservation','run_id','schema_version','scope',
        'semantic_exceptions','signature','status'
    }, 'PAYLOAD_CLAVES')
    for name,val in {'schema_version':SCHEMA,'document_type':'QUALITY_APPROVED',
                     'issuer':'sa-bvl-quality@bvl-analytics-2026.iam.gserviceaccount.com',
                     'project_id':PROJECT,'run_id':RUN,'status':'QUALITY_APPROVED'}.items():
        exigir(type(d.get(name)) is str and d[name]==val, 'CAMPO_'+name.upper())
    validar_campos(d['authorization_policy'], {
        'execution_authorization':'REQUIRED_INDEPENDENT', 'full_batch_authorized':False,
        'human_approval':'REQUIRED_EXTERNAL_AUTHENTICATED_EXACT_PAYLOAD_SHA256',
        'scope':'QUALITY_ONLY_PILOT_12','transfer_allowed':'REQUIRED_INDEPENDENT'
    },'POLITICA')
    validar_campos(d['reservation'],{
        'business_status':'CANDIDATO','generation':RES_GEN,
        'reservation_status':'RESERVADO','sha256':RES_SHA,'uri':RES_URI
    },'RESERVA')
    validar_campos(d['scope'],{
        'batch_identity_sha256':BATCH_SHA,
        'canonical_destination_list_sha256':DEST_SHA,
        'destination_prefix':PREFIX,'file_count':12,'full_plan_sha256':PLAN_SHA,
        'mode':'PILOTO_12_ARCHIVOS','pilot_json_sha256':PILOT_SHA,
        'total_bytes':23052390,'unique_destination_count':12
    },'ALCANCE')
    validar_campos(d['signature'],{
        'algorithm':'EC_SIGN_P256_SHA256','format':'detached','kms_key_version':QUALITY_KEY
    },'FIRMA_DESCRIPCION')
    validar_campos(d['semantic_exceptions'],{
        'policy':'Conservar bytes y hashes sin reinterpretar como datos de negocio válidos',
        'smv_error_xml_count':2,'txt_0c_count':88
    },'EXCEPCIONES')
    validar_campos(d['preflight_evidence'],{
        'archive_sha256': EXPECTED_PREFLIGHT_SHA,'independent_hdfs_verification':False,
        'issuer_authentication_verified':False,'reported_fail_count':0,
        'reported_gcp_operations':0,'reported_gcs_transfers':0,
        'reported_pass_count':12,'reported_result':'HDFS_PREFLIGHT_PASS',
        'sum_reported_bytes':23052390
    },'PREFLIGHT', exact=False)
    validar_campos(d['preflight_evidence'].get('reports_sha256'),{
        'resumen_preflight.txt':'98492cc5d5de01e561b41216e657db5e98aa9de39efbbdd03d42a87098b9c16d',
        'salida_preflight_hdfs.txt':'3080b8d0cc0924b06762b4dd51e7b020cb44224716329bd233db6a8494896222'
    },'PREFLIGHT_REPORTES')
    exigir(set(d['preflight_evidence']) == {
        'archive_sha256','independent_hdfs_verification','issuer_authentication_verified',
        'reported_fail_count','reported_gcp_operations','reported_gcs_transfers',
        'reported_pass_count','reported_result','reports_sha256','sum_reported_bytes'
    }, 'PREFLIGHT_CAMPOS')
    ref = auditar_referencias(plan_raw, pilot_raw)
    return {'schema_compatible':'PASS','payload_sha256':sha(raw),
            'plan_sha256':sha(plan_raw),'pilot_sha256':sha(pilot_raw),
            'alcance':ref,'firma_quality_autenticada':False,
            'autorizacion_humana_autenticada':False,
            'transfer_allowed_autenticado':False,'autorizacion_ejecucion_autenticada':False,
            'transfer_allowed':False,'operaciones_gcs':0,
            'estado':'PROPUESTA_LOCAL_SIN_FIRMA_NI_LIBERACION'}


def verificar_firma_quality(payload_raw, signature_der, quality_pem):
    exigir(type(signature_der) is bytes and bool(signature_der), 'FIRMA_DER_FALTANTE')
    exigir(type(quality_pem) is bytes and bool(quality_pem), 'PEM_OFICIAL_FALTANTE')
    try:
        key=serialization.load_pem_public_key(quality_pem)
    except (ValueError,TypeError) as e:
        raise AuditoriaError('PEM_INVALIDO') from e
    exigir(isinstance(key,ec.EllipticCurvePublicKey) and isinstance(key.curve,ec.SECP256R1),
           'CURVA_INCORRECTA')
    spki=key.public_bytes(serialization.Encoding.DER,serialization.PublicFormat.SubjectPublicKeyInfo)
    exigir(hmac.compare_digest(sha(spki),QUALITY_SPKI),'QUALITY_CLAVE_NO_OFICIAL')
    try:
        key.verify(signature_der,payload_raw,ec.ECDSA(hashes.SHA256()))
    except (InvalidSignature,ValueError) as e:
        raise AuditoriaError('FIRMA_INVALIDA') from e
    return {'firma_criptografica':'PASS','payload_sha256':sha(payload_raw),
            'transfer_allowed':False}


def verificar_humana_pendiente(template_raw, exact_payload_sha):
    # La plantilla no concede ninguna autoridad y no existe todavía clave de
    # atestación humana aprobada ni registro de revocación/consumo confiable.
    d=cargar_json_exacto(template_raw)
    exigir(type(d) is dict, 'HUMANA_FORMATO')
    exigir(d.get('run_id')==RUN and d.get('payload_sha256')==exact_payload_sha,
           'HUMANA_REFERENCIA')
    raise AuditoriaError('HUMANA_NO_AUTENTICADA_MECANISMO_PENDIENTE')


def puerta_transferencia(*_args,**_kwargs):
    raise PermissionError('BLOQUEO_1017_FIRMA_RELEASE_HUMANA_EJECUCION_NO_AUTORIZADAS')


def main():
    p=argparse.ArgumentParser(description='Compatibilidad local payload 329C; nunca habilita GCS')
    p.add_argument('--payload',required=True,type=Path)
    p.add_argument('--plan',required=True,type=Path)
    p.add_argument('--piloto',required=True,type=Path)
    p.add_argument('--quality-pem',type=Path)
    p.add_argument('--signature-der',type=Path)
    args=p.parse_args()
    if bool(args.quality_pem)!=bool(args.signature_der):
        p.error('Proporcionar ambos --quality-pem y --signature-der o ninguno')
    try:
        result=validar_payload_329c(args.payload.read_bytes(),args.plan.read_bytes(),args.piloto.read_bytes())
        if args.signature_der:
            verificar_firma_quality(args.payload.read_bytes(),args.signature_der.read_bytes(),args.quality_pem.read_bytes())
            result['firma_quality_autenticada']=True  # Solo comprobacion criptográfica local, no publicación GCS
            result['estado']='FIRMA_CRIPTOGRAFICA_PASS_PERO_SIN_RELEASE'
        print(json.dumps(result,ensure_ascii=False,indent=2))
        print('RESULTADO: PAYLOAD_329C_COMPATIBLE_LOCAL_SIN_CLOUD')
        return 0
    except (AuditoriaError,OSError,TypeError,ValueError,KeyError) as e:
        print('RECHAZADO_329C:',type(e).__name__,str(e),file=sys.stderr)
        print('TRANSFER_ALLOWED: false',file=sys.stderr)
        return 1

if __name__=='__main__':
    sys.exit(main())
