#!/usr/bin/env python3
"""Simulacion local del CAS GCS del Paso 101.7-E.7. NO ES UN MOTOR GCS.

Solo crea archivos en un directorio local de prueba, nunca importa SDK Cloud.
Un `OK` de este simulador NUNCA concede TRANSFER_ALLOWED.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import uuid

RUN_ID = 'bvl-bronze-inicial-20261125-001'
PROJECT_ID = 'bvl-analytics-2026'
PILOT_SHA256 = '82f384ab19231d89b4f4487ab7424eb24250943b4bdd40c85e88e8e08b5d16b7'
CANONICAL_DEST_SHA256 = 'acf864e31fc403faec3bf67ba13555474e4fa185c3475763781ca88fb0b1d923'
FULL_PLAN_SHA256 = '1397f92911b159e8df257fc82a7dd01593fce0d2e898e796a82efc77ae35d90e'
RESERVATION_SHA256 = '80cb92939b975659d32514fff3a64a30e02148434222f1391b1afd675913c232'
RESERVATION_GENERATION = '1795640338029794'
PEM_SHA256 = '21c2212a5d66f54e76cfd8dd00571dafd49af6984db273f9996473a9487492b6'
SPKI_SHA256 = 'f37b987739c0fc12e4da1d7bb9192c2e4e1f0dad8e65120f1228c27260b4862a'
REVOKED_PEM_SHA256 = '2c96ef7b07084a88d7e21c0eb01b99c09d2ee74668895ba8ce57083a1bd0c51c'
CONSUMER_PROPOSED = 'sa-bvl-auth-consumer@bvl-analytics-2026.iam.gserviceaccount.com'
INGESTOR_EXISTING = 'sa-bvl-ingestion@bvl-analytics-2026.iam.gserviceaccount.com'
KIND = ('HUMAN_QUALITY_AUTHORIZATION','HUMAN_TRANSFER_AUTHORIZATION','HUMAN_EXECUTION_AUTHORIZATION')
HEX64 = re.compile(r'[a-f0-9]{64}\Z')

class Rechazo(ValueError):
    pass


def requerir(expr, motivo):
    if not expr:
        raise Rechazo(motivo)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def verificar_vinculos(referencias, blobs, *, firmas, anclajes_confiables, anclajes_firmas):
    """Corrobora hashes de bytes y cruces; NO autentica su emisor.

    anclajes_confiables debe provenir de canal independiente, no del JSON recibido.
    """
    requerir(all(type(x) is dict for x in (referencias,blobs,firmas,anclajes_confiables,anclajes_firmas)),
             'ESTRUCTURA_REFERENCIAS_INVALIDA')
    requerir(referencias.get('run_id') == RUN_ID and referencias.get('project_id') == PROJECT_ID,
             'OTRO_LOTE')
    requerir(referencias.get('reservation') == {
        'generation': RESERVATION_GENERATION, 'sha256': RESERVATION_SHA256},
        'RESERVA_ALTERADA')
    requerir(referencias.get('scope') == {
        'mode': 'PILOTO_12_ARCHIVOS', 'file_count': 12, 'total_bytes': 23052390,
        'pilot_sha256': PILOT_SHA256, 'destination_sha256': CANONICAL_DEST_SHA256,
        'full_plan_sha256': FULL_PLAN_SHA256, 'full_batch_authorized': False,
        'overwrite_allowed': False}, 'ALCANCE_INVALIDO')
    requerir(set(blobs) == set(KIND), 'EVIDENCIAS_FALTANTES_O_ADICIONALES')
    requerir(set(referencias.get('documents', {})) == set(KIND), 'REFERENCIAS_INCOMPLETAS')
    requerir(set(anclajes_confiables) == set(KIND), 'ANCLAJES_INCOMPLETOS')
    requerir(set(firmas)==set(KIND) and set(anclajes_firmas)==set(KIND) and
             set(referencias.get('signatures',{}))==set(KIND), 'FIRMAS_FALTANTES')
    observados = {}
    for k in KIND:
        raw = blobs[k]
        requerir(type(raw) is bytes, 'NO_BYTES_'+k)
        try:
            obj = json.loads(raw.decode('utf-8'))
        except Exception as exc:
            raise Rechazo('JSON_INVALIDO_'+k) from exc
        requerir(type(obj) is dict and obj.get('document_type') == k,
                 'TIPO_CRUZADO_'+k)
        digest = sha(raw)
        requerir(HEX64.fullmatch(referencias['documents'][k]) is not None,
                 'REFERENCIA_HASH_INVALIDA')
        requerir(digest == referencias['documents'][k], 'REFERENCIA_MANIPULADA_'+k)
        requerir(digest == anclajes_confiables[k], 'ANCLAJE_NO_CONFIABLE_'+k)
        firma_raw=firmas[k]
        requerir(type(firma_raw) is bytes and bool(firma_raw), 'FIRMA_NO_BYTES_'+k)
        firma_hash=sha(firma_raw)
        requerir(firma_hash == referencias['signatures'][k] and firma_hash==anclajes_firmas[k],
                 'FIRMA_MODIFICADA_O_NO_ANCLADA_'+k)
        # La comprobacion ECDSA P-256 de la firma se realiza en el validador E previo.
        observados[k] = digest
    requerir(len(set(observados.values())) == len(KIND), 'DOCUMENTOS_DUPLICADOS')
    # Los documentos definitivos deben enlazar las autorizaciones previas,
    # pero los borradores aportados tienen null. No inferir los valores oficiales.
    for k in ('HUMAN_TRANSFER_AUTHORIZATION','HUMAN_EXECUTION_AUTHORIZATION'):
        obj=json.loads(blobs[k])
        target=obj.get('target', {})
        requerir(target.get('quality_human_authorization_sha256') == observados[KIND[0]],
                 'CRUCE_QUALITY_INVALIDO_'+k)
    execution = json.loads(blobs[KIND[2]])
    requerir(execution.get('target',{}).get('transfer_human_authorization_sha256') == observados[KIND[1]],
             'CRUCE_TRANSFER_INVALIDO')
    return {'vinculos_locales':'PASS', 'operaciones_gcs':0, 'transfer_allowed':False}


def verificar_confianza(registro_raw, *, pin_registro_independiente, pem_raw, lista_revocacion):
    """Verifica integridad referencial. Pin/CRL requieren procedencia externa autenticada."""
    requerir(type(registro_raw) is bytes and type(pem_raw) is bytes, 'TIPO_DE_REGISTRO')
    requerir(type(pin_registro_independiente) is str and HEX64.fullmatch(pin_registro_independiente),
             'PIN_REGISTRO_NO_FIJADO')
    requerir(sha(registro_raw) == pin_registro_independiente, 'REGISTRO_ALTERADO')
    requerir(sha(pem_raw) == PEM_SHA256, 'PEM_REEMPLAZADO')
    requerir(isinstance(lista_revocacion, (set, frozenset)) and REVOKED_PEM_SHA256 in lista_revocacion,
             'REVOCACION_NO_ACREDITADA')
    requerir(PEM_SHA256 not in lista_revocacion, 'CLAVE_ACTUAL_REVOCADA')
    try:
        r=json.loads(registro_raw)
    except Exception as e:
        raise Rechazo('REGISTRO_JSON_INVALIDO') from e
    requerir(r.get('schema') == 'BVL-HUMAN-TRUST-RECORD-329C-v1' and
             r.get('public_key_file_sha256') == PEM_SHA256 and
             r.get('public_key_spki_sha256') == SPKI_SHA256 and
             r.get('revoked_previous_fingerprint') == REVOKED_PEM_SHA256 and
             r.get('trust_for') == 'HUMAN_IDENTITY_SIGNATURE_CHECKS_ONLY' and
             r.get('cloud_transfer_allowed') is False and
             r.get('operational_authorization') is False,
             'REGISTRO_FUERA_DE_POLITICA')
    return {'confianza_local':'PIN_VALIDO', 'identidad_operativa':False}


def ruta_simulada(raiz, authorization_id):
    try:
        u=uuid.UUID(authorization_id)
    except (TypeError, ValueError, AttributeError) as exc:
        raise Rechazo('AUTHORIZATION_UUID_INVALIDO') from exc
    requerir(str(u) == authorization_id, 'UUID_NO_CANONICO')
    raiz = Path(raiz)
    return raiz / 'authorizations' / 'consumed' / 'bronze' / f'run_id={RUN_ID}' / (authorization_id + '.json')


def consumir_simulado(raiz, authorization_id, hashes_vinculados, *, actor=CONSUMER_PROPOSED):
    """Emula `upload(...,if_generation_match=0)` mediante O_EXCL en filesystem.

    Esta funcion NO tiene opcion de conectarse a Google Cloud y NO autoriza migrar.
    """
    requerir(actor == CONSUMER_PROPOSED, 'IDENTIDAD_CONSUMIDORA_NO_ESPERADA')
    requerir(type(hashes_vinculados) is dict and set(hashes_vinculados) == set(KIND),
             'TRIPLE_NO_VINCULADA')
    for k in KIND:
        requerir(isinstance(hashes_vinculados[k], str) and HEX64.fullmatch(hashes_vinculados[k]),
                 'HUELLA_AUSENTE_'+k)
    p=ruta_simulada(raiz, authorization_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload={'schema':'BVL-E7-SIMULACION-NO-OPERATIVA', 'run_id':RUN_ID,
             'authorization_id':authorization_id, 'actor_propuesto':actor,
             'document_hashes':hashes_vinculados, 'scope':'PILOTO_12_ARCHIVOS',
             'gcs_operations':0, 'transfer_allowed':False,
             'storage_precondition_simulated':'ifGenerationMatch=0'}
    b=(json.dumps(payload,sort_keys=True,separators=(',',':'))+'\n').encode()
    try:
        fd=os.open(str(p), os.O_WRONLY|os.O_CREAT|os.O_EXCL, 0o600)
    except FileExistsError:
        return 'CONFLICTO_412_REUTILIZACION_BLOQUEADA'
    except OSError:
        return 'ERROR_REGISTRO_FAIL_CLOSED'
    try:
        with os.fdopen(fd,'wb') as file:
            file.write(b); file.flush(); os.fsync(file.fileno())
    except OSError:
        return 'ERROR_ESCRITURA_FAIL_CLOSED'
    return 'RECLAMO_SIMULADO_201_NO_HABILITA_MIGRACION'


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--demo-offline',action='store_true',required=True)
    args=ap.parse_args()
    import concurrent.futures
    import tempfile
    with tempfile.TemporaryDirectory(prefix='bvl_101_7_e7_') as tmp:
        a=str(uuid.uuid4())
        h={k:sha(k.encode()) for k in KIND} # hashes SINTETICOS de DEMOSTRACION
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            outcomes=list(pool.map(lambda _: consumir_simulado(tmp,a,h), range(2)))
        assert sorted(outcomes)==sorted(['RECLAMO_SIMULADO_201_NO_HABILITA_MIGRACION',
                                         'CONFLICTO_412_REUTILIZACION_BLOQUEADA'])
        print('DOS_EJECUTORES:', ', '.join(outcomes))
        print('OPERACIONES_GCS: 0')
        print('TRANSFER_ALLOWED: false')
        print('RESULTADO: E7_CONSUMO_ATOMICO_SIMULADO_OK_NO_CLOUD')

if __name__=='__main__':
    sys.exit(main())
