#!/usr/bin/env python3
"""Pruebas de diseño 101.7-E.7: SIN Cloud, sin firmas oficiales y sin habilitación operativa.

No se conecta al orquestador productivo. No debe utilizarse como autorización.
"""
from __future__ import annotations
import hashlib
import json
import re
import threading
from dataclasses import dataclass, field

RUN_ID = 'bvl-bronze-inicial-20261125-001'
PROJECT = 'bvl-analytics-2026'
RESERVATION_URI = f'gs://bvl26-control-7f3a9c/reservations/bronze/{RUN_ID}.json'
RESERVATION_GEN = '1795640338029794'
RESERVATION_SHA = '80cb92939b975659d32514fff3a64a30e02148434222f1391b1afd675913c232'
PILOT_SHA = '82f384ab19231d89b4f4487ab7424eb24250943b4bdd40c85e88e8e08b5d16b7'
SCHEMA = 'BVL-OPERATION-FINGERPRINT-329C-v1'
OPERATION_TYPE = 'BRONZE_PILOT_TRANSFER'
DEST_LIST_SHA = 'acf864e31fc403faec3bf67ba13555474e4fa185c3475763781ca88fb0b1d923'

class Blocked(Exception):
    pass


def _valid_sha(v):
    return isinstance(v, str) and re.fullmatch(r'[0-9a-f]{64}', v) is not None


def pilot_intent(*, run_id=RUN_ID, project_id=PROJECT, reservation_uri=RESERVATION_URI,
                 layer='bronze', reservation_generation=RESERVATION_GEN,
                 reservation_sha256=RESERVATION_SHA, pilot_sha256=PILOT_SHA,
                 destinations_sha256=DEST_LIST_SHA):
    """Ocho campos oficiales. URI y layer son controles de alcance externos al hash.

    No se incluye UUID, instante, firma o ID de intento. Las referencias
    se comparan con los identificadores locales del piloto ya congelado;
    esto NO comprueba la existencia o integridad actual de objetos Cloud.
    """
    if (run_id != RUN_ID or project_id != PROJECT or layer != 'bronze'
            or reservation_uri != RESERVATION_URI
            or type(reservation_generation) is not str
            or reservation_generation != RESERVATION_GEN
            or reservation_sha256 != RESERVATION_SHA
            or pilot_sha256 != PILOT_SHA
            or destinations_sha256 != DEST_LIST_SHA):
        raise Blocked('INTENCION_PILOTO_NO_COINCIDE_CON_REFERENCIAS_APROBADAS')
    if not all(_valid_sha(x) for x in (reservation_sha256, pilot_sha256, destinations_sha256)):
        raise Blocked('HUELLA_INVALIDA')
    return {
        'schema_version': SCHEMA,
        'project_id': project_id,
        'run_id': run_id,
        'operation_type': OPERATION_TYPE,
        'reservation_generation': reservation_generation,
        'reservation_sha256': reservation_sha256,
        'pilot_json_sha256': pilot_sha256,
        'canonical_destination_list_sha256': destinations_sha256,
    }


def semantic_canonical_bytes(intent):
    """Solo serializa el contrato oficial validado; no acepta campos extra."""
    if not isinstance(intent, dict) or len(intent) != 8:
        raise Blocked('INTENCION_INVALIDA')
    if not all(type(value) is str for value in intent.values()):
        raise Blocked('TIPO_CANONICO_DEBE_SER_STRING')
    if set(intent) != set(pilot_intent()) or intent != pilot_intent():
        raise Blocked('INTENCION_NO_APROBADA')
    return json.dumps(intent, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False).encode('utf-8')


def semantic_sha256(intent):
    return hashlib.sha256(semantic_canonical_bytes(intent)).hexdigest()


def claim_name(fingerprint, run_id=RUN_ID):
    """Ruta EXACTA ratificada por Quality, sin prefijo sem- ni carpeta adicional."""
    if (not _valid_sha(fingerprint) or run_id != RUN_ID
            or fingerprint != semantic_sha256(pilot_intent())):
        raise Blocked('CLAIM_INVALIDO')
    return f'authorizations/consumed/bronze/run_id={run_id}/{fingerprint}.json'

@dataclass
class OfflineClaims:
    claimed: set = field(default_factory=set)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def try_claim(self, name):
        if not isinstance(name, str) or not re.fullmatch(
                rf'authorizations/consumed/bronze/run_id={RUN_ID}/[0-9a-f]{{64}}\.json', name):
            raise Blocked('PREFIJO_INVALIDO')
        # Los claims de recuperacion de laboratorio siguen siendo NO operativos.
        # Su autenticacion/autorizacion real no se implementa en este modulo.
        with self.lock:
            if name in self.claimed:
                raise Blocked('CONFLICTO_412_SIMULADO')
            self.claimed.add(name)
        return 'RECLAMO_201_SIMULADO_NO_AUTORIZA_TRANSFERENCIA'


def pending_after_failure(expected, transferred, *, prior_claim_consumed, old_executor_stopped,
                          reconciled, recovery_human_authorized):
    """Simulation only. Booleans are *not* sufficient evidence in production."""
    if not prior_claim_consumed:
        raise Blocked('RECLAMO_ORIGINAL_NO_CONSUMIDO')
    if not old_executor_stopped:
        raise Blocked('EJECUTOR_ANTERIOR_NO_DETENIDO')
    if not reconciled:
        raise Blocked('RECONCILIACION_PENDIENTE')
    if not recovery_human_authorized:
        raise Blocked('NUEVA_AUTORIZACION_HUMANA_AUSENTE')
    if len(expected) != 12 or len(set(expected)) != 12:
        raise Blocked('INVENTARIO_PILOTO_INVALIDO')
    if len(set(transferred)) != len(transferred) or not set(transferred).issubset(set(expected)):
        raise Blocked('TRANSFERIDOS_INVALIDOS')
    remaining = sorted(set(expected) - set(transferred))
    if not remaining:
        raise Blocked('SIN_PENDIENTES_NO_HAY_RECUPERACION')
    return remaining


def recovery_claim_name(original_semantic_sha256, pending_names):
    if not _valid_sha(original_semantic_sha256):
        raise Blocked('RECLAMO_ORIGINAL_INVALIDO')
    if not pending_names or len(pending_names) != len(set(pending_names)):
        raise Blocked('PENDIENTES_INVALIDOS')
    # Excludes UUID, timestamps, signature; recovery uses separate restricted scope.
    obj = {'schema':'BVL-E7-RECOVERY-v1','original_claim':original_semantic_sha256,
           'run_id':RUN_ID,'pending':sorted(pending_names)}
    digest = hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return f'authorizations/consumed/bronze/run_id={RUN_ID}/{digest}.json'


def main():
    f = semantic_sha256(pilot_intent())
    ledger = OfflineClaims()
    marker = claim_name(f)
    assert '201_SIMULADO' in ledger.try_claim(marker)
    try:
        ledger.try_claim(marker)
    except Blocked as e:
        assert str(e) == 'CONFLICTO_412_SIMULADO'
    else:
        raise AssertionError('Un segundo reclamo no fue bloqueado')
    print('ESQUEMA:', SCHEMA)
    print('CAMPOS:', len(pilot_intent()))
    print('BYTES_CANONICOS:', len(semantic_canonical_bytes(pilot_intent())))
    print('HUELLA_SEMANTICA:', f)
    print('MARCADOR_SIMULADO:', marker)
    print('DOS_RECLAMOS:', '1_ACEPTADO_1_BLOQUEADO')
    print('NO_HABILITA_CLOUD: true')
    print('FENCING_REAL_INGESTION: PENDIENTE_DE_DISENO_Y_PRUEBA_CLOUD_AUTORIZADA')
    print('RESULTADO: E7_SEMANTICA_RECUPERACION_LOCAL_OK')

if __name__ == '__main__':
    main()
