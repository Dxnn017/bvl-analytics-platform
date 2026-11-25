#!/usr/bin/env python3
"""Acredita DOS atestaciones Ed25519 independientes y acotadas.

Esta función verifica firmas si se entregan raíces de confianza oficiales
previamente registradas; NO crea claves, NO contacta GCS y NUNCA autoriza
por sí misma la transferencia. Esta implementación es una propuesta local.
"""
import base64
import hashlib
import json
from datetime import datetime, timezone
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.exceptions import InvalidSignature

RUN_ID = "bvl-bronze-inicial-20261125-001"
RESERVA_GENERATION = "1795640338029794"
RESERVA_SHA = "80cb92939b975659d32514fff3a64a30e02148434222f1391b1afd675913c232"
PLAN_SHA = "1397f92911b159e8df257fc82a7dd01593fce0d2e898e796a82efc77ae35d90e"
PILOTO_SHA = "82f384ab19231d89b4f4487ab7424eb24250943b4bdd40c85e88e8e08b5d16b7"
DESTINOS_SHA = "acf864e31fc403faec3bf67ba13555474e4fa185c3475763781ca88fb0b1d923"
BATCH_SHA = "e4a3fb69f25ee87a26e71a40506017c199d43cd13735bef3330462c906205a2c"

class GateError(ValueError): pass

def canonico(data):
    return json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(',',':'), allow_nan=False).encode('utf-8')

def decodificar(data):
    if isinstance(data, bytes): return json.loads(data)
    if isinstance(data, str): return json.loads(data)
    return data

def fecha(value):
    if not isinstance(value,str) or not value.endswith('Z'): raise GateError('FECHA_INVALIDA')
    try: return datetime.fromisoformat(value.replace('Z','+00:00'))
    except ValueError as e: raise GateError('FECHA_INVALIDA') from e

def verificar_atestacion(envelope, *, tipo, expected_key_id, trusted_public_key, ahora):
    envelope=decodificar(envelope)
    if not isinstance(envelope,dict) or set(envelope)!={'payload','key_id','signature_b64','alg'}: raise GateError('SOBRE_INVALIDO')
    if envelope['alg']!='Ed25519' or envelope['key_id']!=expected_key_id: raise GateError('CLAVE_O_ALGORITMO')
    payload=envelope['payload']
    if not isinstance(payload,dict): raise GateError('PAYLOAD_INVALIDO')
    expected={
      'type':tipo,'run_id':RUN_ID,'scope':'PILOT_12','batch_identity_sha256':BATCH_SHA,
      'reservation_generation':RESERVA_GENERATION,'reservation_sha256':RESERVA_SHA,
      'plan_sha256':PLAN_SHA,'pilot_sha256':PILOTO_SHA,
      'selected_destinations_sha256':DESTINOS_SHA,'selected_files':12,'selected_bytes':23052390,
    }
    for k,v in expected.items():
        if payload.get(k)!=v or type(payload.get(k)) is not type(v): raise GateError('REFERENCIA_'+k.upper())
    if set(payload)!=(set(expected)|{'issued_at','expires_at'}): raise GateError('CAMPOS_EXTRA_O_AUSENTES')
    issued,expiry=fecha(payload['issued_at']),fecha(payload['expires_at'])
    if not isinstance(ahora,datetime) or ahora.tzinfo is None: raise GateError('TIEMPO_CONFIABLE_REQUERIDO')
    if not issued<=ahora<expiry or expiry-issued>__import__('datetime').timedelta(hours=24): raise GateError('CADUCADA_O_PREMATURA')
    try:
        firma=base64.b64decode(envelope['signature_b64'],validate=True)
        Ed25519PublicKey.from_public_bytes(trusted_public_key).verify(firma, canonico(payload))
    except (ValueError,InvalidSignature,TypeError) as e: raise GateError('FIRMA_INVALIDA') from e
    return payload

def validar_doble_atestacion(quality, release, *, quality_key_id, quality_public_key,
                             release_key_id, release_public_key, ahora):
    if quality_key_id==release_key_id or quality_public_key==release_public_key:
        raise GateError('FALTA_SEPARACION_DE_FUNCIONES')
    q=verificar_atestacion(quality,tipo='QUALITY_APPROVED',expected_key_id=quality_key_id,
                          trusted_public_key=quality_public_key,ahora=ahora)
    r=verificar_atestacion(release,tipo='TRANSFER_ALLOWED',expected_key_id=release_key_id,
                          trusted_public_key=release_public_key,ahora=ahora)
    return {'atestaciones_criptograficas':'PASS', 'run_id':RUN_ID,
            'aprobacion_quality_firmada':True, 'permiso_piloto_firmado':True,
            'identidad_de_emisores_en_gcp_comprobada':False,
            'anclaje_objetos_gcs_comprobado':False,
            'alcance_cloud_autorizado':False, 'transfer_allowed':False,
            'estado':'PENDIENTE_INTEGRACION_Y_AUTORIZACION_OPERATIVA'}

def puerta_transferencia(*_args,**_kwargs):
    raise PermissionError('DENEGADO: SIN_INTEGRACION_GCS_Y_101_7')
