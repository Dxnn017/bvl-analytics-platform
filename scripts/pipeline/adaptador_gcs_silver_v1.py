#!/usr/bin/env python3
"""BVL Fase 5 - Adaptador GCS CANDIDATO (sin CLI / sin ejecucion automatica).

No ejecuta Spark ni publica Silver. Las operaciones I/O solo ocurren si otro
componente invoca funciones de esta biblioteca con un cliente GCS inyectado.
NO invocar create_exclusive antes de autorizacion formal y ratificacion.
"""
import hashlib
import re

BRONZE_BUCKET = 'bvl26-bronze-7f3a9c'
CONTROL_BUCKET = 'bvl26-control-7f3a9c'
CONSUMED_PREFIX = 'authorizations/consumed/silver/run_id='
HEX64 = re.compile(r'^[0-9a-f]{64}$')

class GCSGateError(ValueError):
    pass

def must(ok, reason):
    if not ok:
        raise GCSGateError(reason)

def split_gs_uri(uri):
    must(isinstance(uri, str) and uri.startswith('gs://'), 'URI GCS invalida')
    remain = uri[5:]
    must('/' in remain, 'Falta objeto GCS')
    bucket, key = remain.split('/', 1)
    must(bool(re.fullmatch(r'[a-z0-9][a-z0-9._-]{1,220}', bucket)), 'Bucket invalido')
    must(key and not key.startswith('/') and not key.endswith('/'), 'Ruta objeto invalida')
    must(all(x not in ('.', '..', '') for x in key.split('/')), 'Ruta ambigua')
    must('\\' not in key and '?' not in key and '#' not in key, 'Ruta no canonical')
    return bucket, key

def positive_generation(value):
    must(isinstance(value, (str, int)) and not isinstance(value, bool), 'Generacion invalida')
    value = str(value)
    must(value.isascii() and value.isdecimal() and int(value) > 0, 'Generacion invalida')
    return int(value)

def read_exact_current_bytes(client, uri, generation, expected_sha256, *, expected_size=None, allowed_bucket):
    """Lee la generacion especificada; valida que sea LA ACTUAL, size y SHA-256.

    El cliente debe ser `google.cloud.storage.Client` o doble compatible.
    No lista prefijos, no acepta una ruta `latest` y no modifica GCS.
    """
    bucket_name, name = split_gs_uri(uri)
    must(bucket_name == allowed_bucket, 'Bucket fuera de autorizacion')
    if bucket_name == BRONZE_BUCKET:
        must(not name.startswith('landing/') and '/landing/' not in name, 'Entrada landing prohibida')
    gen = positive_generation(generation)
    must(isinstance(expected_sha256, str) and HEX64.fullmatch(expected_sha256) is not None, 'SHA-256 invalido')
    must(expected_size is None or (isinstance(expected_size, int) and not isinstance(expected_size, bool) and expected_size >= 0), 'Tamano esperado invalido')

    bucket = client.bucket(bucket_name)
    latest = bucket.blob(name)
    latest.reload(retry=None)
    must(positive_generation(latest.generation) == gen, 'Generacion GCS actual distinta')
    if expected_size is not None:
        must(int(latest.size) == expected_size, 'Tamano metadata GCS distinto')

    pinned = bucket.blob(name, generation=gen)
    raw = pinned.download_as_bytes(if_generation_match=gen, retry=None)
    must(isinstance(raw, bytes), 'Descarga GCS no binaria')
    if expected_size is not None:
        must(len(raw) == expected_size, 'Tamano real distinto')
    must(hashlib.sha256(raw).hexdigest() == expected_sha256, 'SHA-256 GCS no coincide')
    # Releer metadatos minimiza la ventana TOCTOU antes del consumo.
    latest.reload(retry=None)
    must(positive_generation(latest.generation) == gen, 'Generacion GCS cambio durante lectura')
    return raw

def read_official_manifest(client, ds, manifest_validator):
    """Solo lee el manifest oficial, verifica generacion+hash y su semantica.

    `manifest_validator` debe ser `verify_manifest` de la puerta offline.
    Los objetos Bronze individuales NO se descargan aqui.
    """
    uri = ds['bronze_manifest_uri']
    _, name = split_gs_uri(uri)
    must('/manifests/bronze/' in '/' + name, 'Ruta de manifest no oficial')
    raw = read_exact_current_bytes(
        client, uri, ds['bronze_manifest_generation'], ds['bronze_manifest_sha256'],
        allowed_bucket=CONTROL_BUCKET)
    rows = manifest_validator(raw, ds)
    return raw, rows

def verify_official_inputs(client, entries):
    """Solo usar DESPUES de verificar autorizacion/scope; lee objetos Bronze.

    La invocacion sobre todos los archivos consume ancho de banda; NO se usa
    para la validacion offline de Fase 5 ni implica permiso para ejecutar Spark.
    """
    seen = set()
    for item in entries:
        uri = item['uri']
        must(uri not in seen, 'URI Bronze duplicada')
        seen.add(uri)
        read_exact_current_bytes(
            client, uri, item['generation'], item['sha256'],
            expected_size=item['size_bytes'], allowed_bucket=BRONZE_BUCKET)
    return len(seen)

def read_signed_authorization_versioned(client, *, auth_uri, auth_generation,
                                       sig_uri, sig_generation,
                                       expected_auth_sha256, expected_sig_sha256,
                                       trusted_public_pem, signature_validator):
    """Verificacion read-only. Las generaciones y hashes DEBEN venir de contexto
    independiente y confiable de Quality, nunca del JSON sin verificar.

    `signature_validator` = verify_signed_authorization de la puerta offline.
    No ejecuta claim, no autoriza Spark y no infiere claves desde payload.
    """
    _, ak = split_gs_uri(auth_uri)
    _, sk = split_gs_uri(sig_uri)
    must(ak.startswith('authorizations/silver/') and ak.endswith('/authorization.json'), 'Ruta auth invalida')
    must(sk == ak[:-len('authorization.json')] + 'authorization.sig', 'Firma fuera de ruta')
    raw = read_exact_current_bytes(client, auth_uri, auth_generation, expected_auth_sha256,
                                   allowed_bucket=CONTROL_BUCKET)
    sig = read_exact_current_bytes(client, sig_uri, sig_generation, expected_sig_sha256,
                                   allowed_bucket=CONTROL_BUCKET)
    return signature_validator(raw, sig, trusted_public_pem)

class GCSExclusiveClaimStore:
    """Adaptador de creacion unica. Sin llamadas hasta invocar create_exclusive.

    Integrar SOLO tras ratificar el formato de fingerprint y recovery, y
    despues de verificaciones de firma, vigencia, alcance y todas las entradas.
    """
    def __init__(self, client):
        self.client = client

    def create_exclusive(self, uri, payload):
        bucket, key = split_gs_uri(uri)
        must(bucket == CONTROL_BUCKET and key.startswith(CONSUMED_PREFIX), 'Destino consumo invalido')
        must(isinstance(payload, bytes) and payload, 'Registro no binario/vacio')
        blob = self.client.bucket(bucket).blob(key)
        # GCS devuelve 412 PreconditionFailed si ya existe una version viva.
        blob.upload_from_string(payload, content_type='application/json',
                                if_generation_match=0, retry=None)
        return positive_generation(blob.generation)
