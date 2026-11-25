"""Validador de contenido GCS con lector inyectado; JAMAS habilita Cloud.

Las referencias oficiales deben venir de un origen confiable independiente.
Una referencia, hash o campo 'approved_by' no acredita quien publico el objeto.
"""
import hashlib
import hmac
import json
from verificador_release_329c_offline import (
    RUN_ID, RESERVA_URI, RESERVA_GENERATION, RESERVA_SHA,
    PLAN_SHA, PILOTO_SHA, DESTINOS_SHA, LOTE_SHA)

APPROVAL_URI = 'gs://bvl26-control-7f3a9c/approvals/bronze/' + RUN_ID + '.json'

class ControlError(ValueError):
    pass

def require(ok, code):
    if not ok:
        raise ControlError(code)

def verificar_ref(ref, uri, fixed_generation=None, fixed_sha=None):
    require(type(ref) is dict and set(ref) == {'uri','generation','sha256'}, 'REF_FORMATO')
    require(ref['uri'] == uri, 'REF_RUTA')
    require(type(ref['generation']) is int and ref['generation'] > 0, 'REF_GENERACION')
    sha = ref['sha256']
    require(type(sha) is str and len(sha)==64 and all(c in '0123456789abcdef' for c in sha), 'REF_SHA')
    if fixed_generation is not None:
        require(str(ref['generation']) == str(fixed_generation), 'GEN_RESERVA')
    if fixed_sha is not None:
        require(hmac.compare_digest(sha,fixed_sha), 'SHA_RESERVA')

def validar_control_inyectado(lector, referencias, plan_bytes, piloto_bytes):
    """Lector opcional solo para pruebas: lector(uri,generation)->bytes.

    En producción este lector debe usar sa-bvl-ingestion, descarga por
    generación exacta y sin reintentos automáticos. Esta funcion no lo invoca
    si falla cualquier verificación local de referencias y plan.
    """
    require(callable(lector), 'LECTOR_AUSENTE')
    require(type(referencias) is dict and set(referencias)=={'reservation','approval'}, 'REFERENCIAS')
    r, q = referencias['reservation'], referencias['approval']
    verificar_ref(r,RESERVA_URI,int(RESERVA_GENERATION),RESERVA_SHA)
    verificar_ref(q,APPROVAL_URI)
    require(type(plan_bytes) is bytes and hashlib.sha256(plan_bytes).hexdigest()==PLAN_SHA,'PLAN_HASH')
    require(type(piloto_bytes) is bytes and hashlib.sha256(piloto_bytes).hexdigest()==PILOTO_SHA,'PILOTO_HASH')
    result=[]
    for ref in (r,q):
        contenido=lector(ref['uri'],ref['generation'])
        require(type(contenido) is bytes, 'LECTURA_FORMATO')
        require(hmac.compare_digest(hashlib.sha256(contenido).hexdigest(),ref['sha256']), 'CONTENIDO_HASH')
        result.append(json.loads(contenido))
    reserva, approval=result
    require(type(reserva) is dict and reserva.get('run_id')==RUN_ID, 'RESERVA_RUN')
    require(reserva.get('reservation_status')=='RESERVADO' and reserva.get('status')=='CANDIDATO', 'RESERVA_ESTADO')
    require(type(approval) is dict and approval.get('run_id')==RUN_ID, 'APPROVAL_RUN')
    require(approval.get('scope')=='PILOT_12' and approval.get('approval_state')=='QUALITY_APPROVED', 'APPROVAL_ESTADO')
    require(approval.get('transfer_allowed') is False and approval.get('draft_only') is False, 'APPROVAL_PREMATURA')
    require(approval.get('reservation_ref',{}).get('uri')==RESERVA_URI and
            str(approval['reservation_ref'].get('generation'))==RESERVA_GENERATION and
            approval['reservation_ref'].get('sha256')==RESERVA_SHA, 'APPROVAL_RESERVA')
    for campo, expected in {
        'batch_identity_sha256':LOTE_SHA,'plan_sha256':PLAN_SHA,'pilot_sha256':PILOTO_SHA,
        'selected_destinations_sha256':DESTINOS_SHA,'selected_files':12,'selected_bytes':23052390
    }.items():
        require(approval.get(campo)==expected, 'APPROVAL_'+campo)
    return {'lecturas_gcs_solicitadas':2,'integridad':'PASS',
            'procedencia_quality_autenticada':False,'transfer_allowed':False,
            'estado':'BLOQUEADO_HASTA_ATTESTACION_Y_101_7'}

def despacho(*args,**kwargs):
    raise PermissionError('BLOQUEADO_QUALITY_Y_TRANSFER_ALLOWED_NO_EMITIDOS')
