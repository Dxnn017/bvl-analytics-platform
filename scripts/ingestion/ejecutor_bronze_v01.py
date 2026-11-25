#!/usr/bin/env python3
"""Integracion Bronze: preflight local + lectura Control por generacion/SHA.

Estado de seguridad (2026-11-25): PASO 101.7 VIGENTE.
No habilita escrituras ni acepta flags de liberacion por CLI.
El mecanismo oficial de autenticacion de la orden de liberacion aun requiere
validacion y autorizacion de Quality. En consecuencia no existe modo --execute.
"""
import argparse
import hashlib
import hmac
import json
import re
from pathlib import Path

from orquestar_piloto_bronze_local import integrar
from transferir_hdfs_gcs import TRANSFERENCIAS_HABILITADAS
from validar_control_bronze_local import validar_local

CONTROL_BUCKET = 'bvl26-control-7f3a9c'
RUN_RE = re.compile(r'bvl-bronze-inicial-\d{8}-\d{3}\Z')
HEX_RE = re.compile(r'[a-f0-9]{64}\Z')


class IntegracionError(ValueError):
    pass


def exigir(condition, error):
    if not condition:
        raise IntegracionError(error)


def nombre_control(run_id, tipo):
    exigir(isinstance(run_id, str) and RUN_RE.fullmatch(run_id), 'RUN_ID_INVALIDO')
    exigir(tipo in ('reservations', 'approvals'), 'TIPO_CONTROL_INVALIDO')
    return f'{tipo}/bronze/{run_id}.json'


def validar_referencia(referencia, run_id, tipo):
    """Referencia ESPECIFICADA por un canal confiable, nunca por el objeto mismo.

    Esta funcion valida datos, NO autentica la procedencia del canal.
    Solo el verificador oficial de release futuro podra suministrarlos en produccion.
    """
    exigir(isinstance(referencia, dict), 'REFERENCIA_TIPO')
    esperado = f'gs://{CONTROL_BUCKET}/{nombre_control(run_id, tipo)}'
    exigir(referencia.get('uri') == esperado, 'CONTROL_URI_NO_CANONICA')
    gen = referencia.get('generation')
    sha = referencia.get('sha256')
    exigir(type(gen) is int and gen > 0, 'GENERACION_REFERENCIA_INVALIDA')
    exigir(isinstance(sha, str) and HEX_RE.fullmatch(sha), 'SHA_REFERENCIA_INVALIDO')
    return gen, sha


def leer_control_lectura_unica(cliente_gcs, run_id, tipo, referencia):
    """Solo GET; generation y sha de entrada deben venir de fuente confiable.

    Esta funcion tampoco prueba quien creo el objeto. Calidad IAM/auditoria se
    valida EXTERNAMENTE: metadata.emitter_sa NO constituye prueba de identidad.
    """
    gen, sha = validar_referencia(referencia, run_id, tipo)
    ruta = nombre_control(run_id, tipo)
    objeto = cliente_gcs.bucket(CONTROL_BUCKET).blob(ruta)
    objeto.reload(if_generation_match=gen)
    exigir(type(objeto.generation) is int and objeto.generation == gen,
           'CONTROL_GENERACION_NO_COINCIDE')
    contenido = objeto.download_as_bytes(if_generation_match=gen)
    exigir(isinstance(contenido, bytes), 'CONTROL_PAYLOAD_NO_BYTES')
    exigir(hmac.compare_digest(hashlib.sha256(contenido).hexdigest(), sha),
           'CONTROL_SHA_NO_COINCIDE')
    try:
        data = json.loads(contenido)
    except (ValueError, UnicodeDecodeError) as exc:
        raise IntegracionError('CONTROL_JSON_INVALIDO') from exc
    exigir(isinstance(data, dict), 'CONTROL_JSON_NO_OBJETO')
    exigir(data.get('run_id') == run_id, 'CONTROL_RUN_NO_COINCIDE')
    return contenido, data


def validar_cadena_local(plan, control_candidato, reserva_bytes, aprobacion_bytes,
                         reserva_gen, aprobacion_gen, aprobacion_ref):
    """Validacion de laboratorio, SIN autenticar origen ni levantar Paso 101.7.

    Los 12 manifests en el control candidato aun deben cotejarse contra el
    despliegue oficial inmutable y los logs IAM por Quality.
    """
    exigir(isinstance(aprobacion_ref, dict), 'APROBACION_REFERENCIA_AUSENTE')
    gen, esperado = validar_referencia(aprobacion_ref, plan.get('run_id'), 'approvals')
    exigir(gen == aprobacion_gen, 'APROBACION_GENERACION_NO_COINCIDE')
    exigir(hmac.compare_digest(hashlib.sha256(aprobacion_bytes).hexdigest(), esperado),
           'APROBACION_SHA_NO_COINCIDE')
    r = validar_local(plan, control_candidato, reserva_bytes, aprobacion_bytes, reserva_gen)
    exigir(r['transfer_allowed'] is False, 'VALIDADOR_LOCAL_HABILITO_TRANSFERENCIA')
    return {
        'run_id': plan['run_id'], 'contrato_structural': 'OK',
        'reserva_generation': reserva_gen, 'aprobacion_generation': aprobacion_gen,
        'datos_manifests': 12, 'archivos_plan': r['archivos'],
        'emisor_autenticado': False, 'release_autenticado': False,
        'transfer_allowed': False, 'transferencias_gcs': 0,
        'estado': 'VALIDACION_LOCAL_NO_OPERATIVA',
    }



def verificar_cadena_control_solo_lectura(cliente_gcs, plan, control_candidato, referencias):
    """Consulta ambas rutas exactas y contrasta el contrato COMPLETO local.

    ``referencias`` solo es apto para ensayos si no existe un canal oficial de
    release autenticado. Por diseno esta funcion NUNCA habilita transferencias.
    """
    exigir(isinstance(plan, dict) and isinstance(referencias, dict), 'ENTRADAS_TIPO')
    run = plan.get('run_id')
    exigir(isinstance(run, str) and RUN_RE.fullmatch(run), 'RUN_ID_INVALIDO')
    r_ref = referencias.get('reservation')
    a_ref = referencias.get('approval')
    # Validamos ambas referencias ANTES de cualquier lectura remota.
    r_gen, _ = validar_referencia(r_ref, run, 'reservations')
    a_gen, _ = validar_referencia(a_ref, run, 'approvals')
    reserva_bytes, reserva = leer_control_lectura_unica(
        cliente_gcs, run, 'reservations', r_ref)
    aprobacion_bytes, aprobacion = leer_control_lectura_unica(
        cliente_gcs, run, 'approvals', a_ref)
    exigir(reserva.get('status') == 'RESERVED', 'RESERVA_NO_RESERVED')
    exigir(aprobacion.get('status') == 'QUALITY_APPROVED',
           'APROBACION_NO_QUALITY_APPROVED')
    return validar_cadena_local(plan, control_candidato, reserva_bytes,
                               aprobacion_bytes, r_gen, a_gen, a_ref)

def ejecutar_piloto(*_args, **_kwargs):
    """Barrera de ejecucion. No hay via local para habilitarla.

    El codigo futuro debe integrar la atestacion de Quality y verificaciones
    IAM reales ANTES de llamar a transferir_archivo. No basta un JSON o flag.
    """
    raise PermissionError('BLOQUEADO|PASO_101_7|RELEASE_OFICIAL_NO_VERIFICABLE')


def main():
    parser = argparse.ArgumentParser(
        description='Preflight integrado piloto BVL (sin Cloud, sin transferencias)')
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--piloto', type=Path, required=True)
    parser.add_argument('--hdfs-evidencia', type=Path, required=True)
    args = parser.parse_args()
    try:
        exigir(TRANSFERENCIAS_HABILITADAS is False, 'NUCLEO_CLOUD_NO_BLOQUEADO')
        reporte = integrar(
            args.plan.read_bytes(),
            json.loads(args.piloto.read_bytes()),
            json.loads(args.hdfs_evidencia.read_bytes()),
        )
    except (IntegracionError, ValueError, OSError, TypeError, KeyError) as exc:
        parser.exit(1, f'RECHAZADO|{exc}\n')
    print('===== PREFLIGHT EJECUTOR BRONZE v0.1 (SOLO LOCAL) =====')
    print('RUN_ID:', reporte['run_id'])
    print('DATASETS:', reporte['datasets'])
    print('ARCHIVOS:', reporte['archivos'])
    print('BYTES:', reporte['bytes_totales'])
    print('HDFS_VERIFICADOS:', reporte['hdfs_verificados'])
    print('TRANSFER_ALLOWED: False')
    print('OPERACIONES_GCS: 0')
    print('PENDIENTE: autenticidad IAM, release Quality y permisos autorizados')
    print('RESULTADO: EJECUTOR_PREFLIGHT_LOCAL_OK')


if __name__ == '__main__':
    main()
