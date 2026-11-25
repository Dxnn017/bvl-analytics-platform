#!/usr/bin/env python3
"""Verificador *estructural*, sin red. NO autentica escritores ni habilita Cloud.

La comprobacion de procedencia debe implementarse aparte con mecanismo de
confianza comprobable (identidad/escritura IAM + auditoria o firma autenticada)
y una atestacion independiente de autorizacion 101.7. Este modulo NO concede permiso.
"""
from __future__ import annotations
import hashlib
import json
from dataclasses import dataclass

RUN_ID = "bvl-bronze-inicial-20261125-001"
RESERVA_URI = "gs://bvl26-control-7f3a9c/reservations/bronze/bvl-bronze-inicial-20261125-001.json"
RESERVA_GENERATION = "1795640338029794"
RESERVA_SHA = "80cb92939b975659d32514fff3a64a30e02148434222f1391b1afd675913c232"
PLAN_SHA = "1397f92911b159e8df257fc82a7dd01593fce0d2e898e796a82efc77ae35d90e"
PILOTO_SHA = "82f384ab19231d89b4f4487ab7424eb24250943b4bdd40c85e88e8e08b5d16b7"
DESTINOS_SHA = "acf864e31fc403faec3bf67ba13555474e4fa185c3475763781ca88fb0b1d923"
LOTE_SHA = "e4a3fb69f25ee87a26e71a40506017c199d43cd13735bef3330462c906205a2c"

class ReleaseError(ValueError):
    pass

def exigir(condicion, motivo):
    if not condicion: raise ReleaseError(motivo)

def validar_preaprobacion_local(aprobacion_bytes, plan_bytes, piloto_bytes):
    """Comprueba consistencia documental, nunca emite permiso."""
    a = json.loads(aprobacion_bytes)
    exigir(isinstance(a, dict), "APROBACION_TIPO")
    ref = a.get("reservation_ref")
    exigir(isinstance(ref, dict), "RESERVA_REF_AUSENTE")
    for k,v in {"uri":RESERVA_URI,"generation":RESERVA_GENERATION,"sha256":RESERVA_SHA}.items():
        exigir(str(ref.get(k)) == v, "RESERVA_REF_"+k.upper())
    for k,v in {"run_id":RUN_ID,"scope":"PILOT_12", "batch_identity_sha256":LOTE_SHA,
                "plan_sha256":PLAN_SHA,"pilot_sha256":PILOTO_SHA,
                "selected_destinations_sha256":DESTINOS_SHA,"selected_files":12,
                "selected_bytes":23052390}.items():
        exigir(a.get(k)==v,"CAMPO_"+k.upper())
    exigir(hashlib.sha256(plan_bytes).hexdigest()==PLAN_SHA,"PLAN_BYTES_NO_COINCIDEN")
    exigir(hashlib.sha256(piloto_bytes).hexdigest()==PILOTO_SHA,"PILOTO_BYTES_NO_COINCIDEN")
    plan=json.loads(plan_bytes); piloto=json.loads(piloto_bytes)
    exigir(plan.get('run_id')==RUN_ID and piloto.get('run_id')==RUN_ID,"RUN_ID_EN_PLAN_PILOTO")
    seleccion=piloto.get('archivos_piloto')
    exigir(isinstance(seleccion,list) and len(seleccion)==12,"PILOTO_CANTIDAD")
    exigir(len({x.get('destino_gcs') for x in seleccion})==12,"DESTINOS_NO_UNICOS")
    exigir(sum(x.get('bytes_esperados',0) for x in seleccion)==23052390,"BYTES_PILOTO")
    exigir(a.get('transfer_allowed') is False,"APROBACION_NO_DEBE_AUTORIZAR_TRANSFERENCIA")
    exigir(a.get('draft_only') is True,"SOLO_ADMITE_BORRADOR_LOCAL")
    exigir(a.get('verified_writer_identity') is False,"IDENTIDAD_NO_SE_AUTENTICA_AQUI")
    return {'documental': 'PASS', 'run_id':RUN_ID, 'archivos':12,
            'emisor_autenticado':False, 'release_101_7_autenticado':False,
            'transfer_allowed':False, 'operaciones_gcp':0}

def puerta_transferencia(*args, **kwargs):
    """Bloqueo incondicional del adaptador offline."""
    raise PermissionError('BLOQUEADO_RELEASE_AUTENTICADO_Y_101_7_PENDIENTES')
