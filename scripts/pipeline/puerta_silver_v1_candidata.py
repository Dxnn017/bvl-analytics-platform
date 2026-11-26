#!/usr/bin/env python3
"""BVL Fase 5: nucleo de verificacion OFFLINE, no activa Spark ni GCS.

NO es una autorizacion operativa ni realiza llamadas Cloud. Un adaptador GCS
supervisado y el esquema firmado ratificado por Quality aun son necesarios.
"""
import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

# Huella fijada previamente en la configuracion de confianza de Quality 329C.
# Nunca se deriva de la autorizacion o el contrato que se valida.
QUALITY_SPKI_PIN = '5c70c40088a680cfd7ced1859c4d6b335652d96991cdcae4268819fb4f90f5ce'
CONTRACT_SCHEMA = 'BVL-BRONZE-SILVER-CONTRACT-v1'
AUTH_SCHEMA = 'BVL-SILVER-AUTH-v1'
HEX64 = re.compile(r'^[0-9a-f]{64}$')

class GateError(ValueError):
    pass

def require(condition, message):
    if not condition:
        raise GateError(message)

def no_duplicates(pairs):
    out = {}
    for k, v in pairs:
        require(k not in out, f'Clave JSON duplicada: {k}')
        out[k] = v
    return out

def refuse_constant(value):
    raise GateError(f'Constante JSON prohibida: {value}')

def load_strict(raw):
    require(isinstance(raw, bytes), 'Se requieren bytes de archivo')
    require(not raw.startswith(b'\xef\xbb\xbf'), 'BOM prohibido')
    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=no_duplicates, parse_constant=refuse_constant)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise GateError(f'JSON invalido: {exc}') from exc
    require(isinstance(value, dict), 'Objeto JSON requerido')
    return value

def canonical_bytes(obj):
    try:
        return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')
    except (TypeError, ValueError) as exc:
        raise GateError(f'No canonicalizable: {exc}') from exc

def sha256(raw):
    return hashlib.sha256(raw).hexdigest()

def validate_contract(raw, expected_sha256):
    require(HEX64.fullmatch(expected_sha256) is not None, 'SHA-256 externo invalido')
    require(sha256(raw) == expected_sha256, 'SHA-256 contrato no coincide')
    c = load_strict(raw)
    require(c.get('schema_version') == CONTRACT_SCHEMA, 'Esquema contrato distinto')
    require(c.get('phase') == 5 and c.get('phase_5_closed') is False, 'Fase no corresponde')
    require(c.get('bronze', {}).get('status') == 'BRONZE_PUBLICADO', 'Bronze no publicado')
    require(c.get('authorization_protocol', {}).get('schema_version') == AUTH_SCHEMA, 'Protocolo auth distinto')
    require(c.get('authorization_protocol', {}).get('claim_create_if_generation_match') == 0, 'No hay precondicion de creacion exclusiva')
    require(c.get('authorization_protocol', {}).get('single_use_required') is True, 'Consumo unico no requerido')
    require(c.get('authorization_protocol', {}).get('max_validity_seconds') == 14400, 'Vigencia distinta')
    require(c.get('consumer', {}).get('allowed_operation') == 'BUILD_SILVER_STAGING', 'Operacion distinta')
    require(c.get('silver_publication_allowed') is False, 'Contrato debe prohibir publicacion')
    require(c.get('consumer', {}).get('official_publication_allowed') is False, 'Publicacion consumidor permitida')
    require(c.get('cryptographic_protocol', {}).get('public_key_spki_sha256') == QUALITY_SPKI_PIN, 'Huella confianza distinta')
    rows = c.get('datasets')
    require(isinstance(rows, list) and len(rows) == 12, 'Se esperan 12 datasets')
    require(len({x['dataset'] for x in rows}) == 12, 'Dataset repetido')
    require(all(x['run_id'] == c['run_id'] for x in rows), 'Run ID no coincide')
    require(sum(x['files_count'] for x in rows) == 3599, 'Conteo no corresponde')
    require(sum(x['total_bytes'] for x in rows) == 393623640, 'Bytes no corresponden')
    require(all(len(x['bronze_input_uris']) == x['files_count'] for x in rows), 'URIs del contrato incompletas')
    return c

def verify_public_key(public_pem, pin=QUALITY_SPKI_PIN):
    try:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import ec
        key = serialization.load_pem_public_key(public_pem)
        require(isinstance(key, ec.EllipticCurvePublicKey) and isinstance(key.curve, ec.SECP256R1), 'La clave no es ECDSA P-256')
        spki = key.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
        require(sha256(spki) == pin, 'Clave publica no coincide con pin independiente')
        return key
    except (ValueError, TypeError) as exc:
        raise GateError(f'Clave publica invalida: {exc}') from exc

def verify_signed_authorization(raw, der_signature, public_pem, expected_pin=QUALITY_SPKI_PIN):
    """Verificacion criptografica de una propuesta de payload. No concede acceso."""
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    obj = load_strict(raw)
    require(raw == canonical_bytes(obj), 'Autorizacion no es JSON canonico exacto')
    key = verify_public_key(public_pem, expected_pin)
    try:
        key.verify(der_signature, raw, ec.ECDSA(hashes.SHA256()))
    except InvalidSignature as exc:
        raise GateError('Firma de Quality invalida') from exc
    return obj

def utc(value):
    require(isinstance(value, str), 'Timestamp requerido')
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise GateError('Timestamp ISO invalido') from exc
    require(dt.tzinfo is not None and dt.utcoffset().total_seconds() == 0, 'Se requieren timestamps UTC')
    return dt

def validate_simple_scope(auth, contract, now=None):
    """Modelo PROPUESTO de payload simple, pendiente de ratificacion de Quality."""
    now = now or datetime.now(timezone.utc)
    require(auth.get('schema_version') == AUTH_SCHEMA, 'Auth schema distinto')
    require(auth.get('status') == 'SILVER_AUTORIZADO', 'Estado no autorizado')
    require(auth.get('operation') == 'BUILD_SILVER_STAGING', 'Operacion no permitida')
    require(auth.get('scope') == 'DATASET_AND_RUN_ID', 'Alcance multiple NO ratificado')
    require(auth.get('project_id') == contract['project_id'], 'Proyecto distinto')
    require(auth.get('region') == contract['region'], 'Region distinta')
    require(auth.get('run_id') == contract['run_id'], 'Run ID distinto')
    require(auth.get('service_account') == contract['consumer']['service_account'], 'Identidad distinta')
    start, end = utc(auth.get('not_before_utc')), utc(auth.get('expires_at_utc'))
    require(0 < (end-start).total_seconds() <= 14400, 'Vigencia excedida o nula')
    require(start <= now <= end, 'Autorizacion fuera de vigencia')
    ds = next((x for x in contract['datasets'] if x['dataset'] == auth.get('dataset')), None)
    require(ds is not None, 'Dataset fuera del contrato')
    require(ds['bronze_manifest_uri'] == auth.get('manifest_uri'), 'URI manifest distinta')
    require(str(ds['bronze_manifest_generation']) == str(auth.get('manifest_generation')), 'Generacion manifest distinta')
    require(ds['bronze_manifest_sha256'] == auth.get('manifest_sha256'), 'SHA manifest distinto')
    require(auth.get('dependencies', []) == [], 'Dependencias derivadas requieren otro protocolo')
    attempt_id = auth.get('attempt_id')
    require(isinstance(attempt_id, str) and re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}', attempt_id), 'Intento invalido')
    product = ds.get('proposed_silver_product')
    require(isinstance(product, str) and product, 'Mapeo Silver no definido')
    staging = f"gs://{contract['consumer']['silver_bucket']}/staging/{product}/run_id={contract['run_id']}/attempt_id={attempt_id}/"
    require(auth.get('staging_prefix') == staging, 'Staging no coincide o invade otro alcance')
    require(auth.get('official_publication_allowed') is False, 'Publicacion Silver prohibida')
    require(auth.get('overwrite_allowed') is False, 'Overwrite prohibido')
    return ds

def verify_manifest(raw, ds):
    """Verifica un manifest recibido byte-a-byte, sin consultar GCS."""
    require(sha256(raw) == ds['bronze_manifest_sha256'], 'Hash manifest Bronze no coincide')
    m = load_strict(raw)
    require(m.get('dataset') == ds['dataset'] and m.get('run_id') == ds['run_id'], 'Manifest dataset/run distinto')
    require(m.get('status') == 'BRONZE_COMPLETADO' and m.get('quality_approved') is True, 'Manifest Bronze no aprobado')
    require(m.get('silver_consumption_allowed') is False, 'Manifest Bronze alterado')
    files = m.get('files')
    require(isinstance(files, list) and len(files) == ds['files_count'], 'Manifest: cantidad diferente')
    require(m.get('total_bytes') == ds['total_bytes'], 'Manifest: volumen diferente')
    uris = []
    total = 0
    prefix = ds['bronze_manifest_uri'].split('/manifests/')[0].replace('bvl26-control-7f3a9c', 'bvl26-bronze-7f3a9c')
    for f in files:
        require(isinstance(f, dict), 'Entrada manifest invalida')
        uri, gen, size, digest = [f.get(n) for n in ('uri','generation','size_bytes','sha256')]
        require(isinstance(uri,str) and uri.startswith('gs://bvl26-bronze-7f3a9c/') and '/landing/' not in uri, 'Objeto fuera de Bronze oficial')
        require(isinstance(gen, (int,str)) and str(gen).isdigit() and int(gen)>0, 'Generacion de archivo invalida')
        require(isinstance(size,int) and size>=0, 'Tamano invalido')
        require(isinstance(digest,str) and HEX64.fullmatch(digest) is not None, 'SHA objeto invalido')
        total += size
        uris.append(uri)
    require(len(set(uris))==len(uris), 'Objetos duplicados')
    require(set(uris)==set(ds['bronze_input_uris']), 'Lista URIs manifest vs contrato distinta')
    require(total==ds['total_bytes'], 'Suma de tamanos distinta')
    return files

def enforce_contract_execution_block(contract):
    """Bloqueo intencional. Debe fallar con contrato Fase 5 recibido."""
    require(contract.get('consumer',{}).get('spark_execution_allowed') is True, 'Spark bloqueado por contrato Quality')
    require(contract.get('dataproc',{}).get('execution_allowed') is True, 'Dataproc bloqueado por contrato Quality')
    require(contract.get('silver_consumption_allowed') is True, 'Consumo Silver bloqueado por contrato Quality')
    require(contract.get('operational_gate',{}).get('operational_implementation_verified') is True, 'Puerta operativa aun no verificada por Quality')

def claim_once(store, uri, payload):
    """La implementacion del adaptador GCS DEBE usar if_generation_match=0.
    store.create_exclusive(uri, payload) es una interfaz inyectable para pruebas.
    No se llama desde el CLI y no elimina ni reutiliza registros de consumo.
    """
    return store.create_exclusive(uri, payload)

def main():
    ap = argparse.ArgumentParser(description='Fase 5: inspeccion local del contrato Quality. SIN GCS ni Spark.')
    ap.add_argument('--contract', required=True, type=Path)
    ap.add_argument('--sha256', required=True)
    args = ap.parse_args()
    c = validate_contract(args.contract.read_bytes(), args.sha256)
    mapped = [r for r in c['datasets'] if r['proposed_silver_product']]
    print('RESULTADO: CONTRATO_FASE5_VALIDO_SOLO_OFFLINE')
    print('RUN_ID:', c['run_id'])
    print('DATASETS:', len(c['datasets']))
    print('MAPPING_PROPUESTO:',len(mapped),'SIN_MAPEO:',len(c['datasets'])-len(mapped))
    print('FASE5_CERRADA:', c['phase_5_closed'])
    print('AUTORIZACIONES_EMITIDAS:',len(c['authorization_protocol']['issued_authorizations']))
    print('OPERACIONES_GCS: 0')
    print('SPARK_EJECUTADO: NO')
    print('PUERTA_OPERATIVA_COMPLETA: NO')

if __name__=='__main__': main()
