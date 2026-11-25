#!/usr/bin/env python3
"""Puerta de integración Bronze: enlaza coordinador y lectura de Quality.

FASE LOCAL 2026-11-25: sólo preflight offline por CLI. La comprobación GCS
es una función inyectable que se ensaya con dobles, no se expone por CLI.
NO autentica el canal de liberación del Paso 101.7 ni habilita escrituras.
"""
import argparse
import hashlib
import json
from pathlib import Path

import transferir_hdfs_gcs as nucleo
from ejecutor_bronze_v01 import verificar_cadena_control_solo_lectura
from orquestar_transferencias_bronze_v01 import seleccionar_lote
from adaptador_329a_reserva_local import validar as validar_contrato_329a


class PuertaError(ValueError):
    pass


def exigir(condicion, codigo):
    if not condicion:
        raise PuertaError(codigo)


def preflight_unificado(plan_bytes, alcance, piloto=None, evidencia_hdfs=None):
    """Selecciona 12/3599 con los validadores existentes; no accede a red."""
    exigir(nucleo.TRANSFERENCIAS_HABILITADAS is False,
           'ALTO_NUCLEO_CLOUD_HABILITADO')
    lote = seleccionar_lote(plan_bytes, alcance, piloto, evidencia_hdfs)
    exigir(lote['transfer_allowed'] is False and
           lote['publicacion'] == 'NO_PUBLICADO', 'LOTE_DEBE_ESTAR_BLOQUEADO')
    return lote


def verificar_reserva_y_aprobacion_solo_lectura(lote, plan, control, referencias, cliente):
    """Combina selección de lote con verificación GET de Control.

    Sólo callable de prueba/integración autorizada. Las referencias pueden
    probar integridad, NO su procedencia. Por eso nunca devuelve permiso.
    """
    exigir(isinstance(lote, dict) and isinstance(plan, dict), 'DOCUMENTOS_INVALIDOS')
    exigir(lote.get('transfer_allowed') is False, 'LOTE_NO_BLOQUEADO')
    exigir(lote.get('run_id') == plan.get('run_id'), 'LOTE_RUN_NO_COINCIDE')
    exigir(lote.get('plan_sha256_local'), 'PLAN_SHA_FALTANTE')
    exigir(isinstance(referencias, dict) and
           set(referencias) == {'reservation', 'approval'},
           'REFERENCIAS_CONTROL_REQUERIDAS')
    exigir(cliente is not None, 'CLIENTE_LECTURA_REQUERIDO')
    resultado = verificar_cadena_control_solo_lectura(
        cliente, plan, control, referencias)
    exigir(resultado['archivos_plan'] == 3599 and
           resultado['datos_manifests'] == 12 and
           resultado['run_id'] == lote['run_id'], 'CONTRATO_NO_COINCIDE')
    exigir(resultado['transfer_allowed'] is False,
           'VALIDADOR_CONTROL_INTENTO_HABILITAR_TRANSFERENCIA')
    # Los manifests del control candidato NO son una raíz de confianza final.
    # La procedencia IAM/auditoría y release oficial deben verificarse fuera.
    return {
        'run_id': lote['run_id'],
        'alcance': lote['alcance'],
        'plan_sha256_local': lote['plan_sha256_local'],
        'archivos_seleccionados': lote['archivos'],
        'bytes_seleccionados': lote['bytes'],
        'archivos_plan': resultado['archivos_plan'],
        'manifests_contrastados_estructuralmente': 12,
        'reserva_generation': resultado['reserva_generation'],
        'aprobacion_generation': resultado['aprobacion_generation'],
        'control_get_estructural': 'OK',
        'emisor_quality_autenticado': False,
        'catalogo_oficial_anclado': False,
        'release_101_7_autenticado': False,
        'transfer_allowed': False,
        'operaciones_gcs_escritura': 0,
        'publicacion': 'NO_PUBLICADO',
        'estado': 'PENDIENTE_RELEASE_OFICIAL_QUALITY',
    }


def auditar_contrato_329a_preflight_local(reserva_local, plan_bytes, piloto_local):
    """Compatibilidad real 329A integrada a puerta 329C. SOLO OFFLINE.

    No acepta referencias como autenticacion. No puede habilitar Cloud.
    """
    exigir(nucleo.TRANSFERENCIAS_HABILITADAS is False,
           'ALTO_NUCLEO_CLOUD_HABILITADO')
    exigir(isinstance(reserva_local, dict) and isinstance(plan_bytes, bytes) and
           isinstance(piloto_local, dict), 'CONTRATO_TIPOS_INVALIDOS')
    plan = json.loads(plan_bytes)
    comprobacion = validar_contrato_329a(reserva_local, plan, plan_bytes, piloto_local)
    exigir(comprobacion['quality_approved'] is False and
           comprobacion['transfer_allowed'] is False and
           comprobacion['operaciones_gcp'] == 0,
           'CONTRATO_NO_BLOQUEADO')
    return dict(comprobacion,
                integracion_motor='PUERTA_329C_CONTRATO_329A_OFFLINE_PASS',
                estado_operativo='BLOQUEADO_SIN_RELEASE_AUTENTICADO',
                cloud_habilitado=False)


def verificar_contenido_quality_con_lector_inyectado(lector, refs, plan_bytes, piloto_bytes):
    """Cotejo de integridad de lectura en Control, SIN liberar el motor."""
    from puerta_control_gcs_inyectable_329c import validar_control_inyectado
    return validar_control_inyectado(lector,refs,plan_bytes,piloto_bytes)



def auditar_integracion_quality_329c_local(plan_bytes, piloto_bytes, aprobacion_borrador_bytes):
    """Integra plan 3599 + piloto 12 + draft Quality al coordinador, OFFLINE.

    No usa GCS/HDFS, no autentica emisores y JAMAS concede TRANSFER_ALLOWED.
    Los SHA anclados provienen del paquete Quality entregado para integracion
    documental, no de un permiso operativo de transferencia.
    """
    from verificador_release_329c_offline import (
        PLAN_SHA, PILOTO_SHA, RUN_ID, validar_preaprobacion_local,
    )

    exigir(nucleo.TRANSFERENCIAS_HABILITADAS is False, 'NUCLEO_NO_BLOQUEADO')
    exigir(type(plan_bytes) is bytes and type(piloto_bytes) is bytes and
           type(aprobacion_borrador_bytes) is bytes, 'INSUMOS_BYTES')
    exigir(hashlib.sha256(plan_bytes).hexdigest() == PLAN_SHA, 'PLAN_SHA_329C')
    exigir(hashlib.sha256(piloto_bytes).hexdigest() == PILOTO_SHA, 'PILOTO_SHA_329C')
    plan = json.loads(plan_bytes)
    piloto = json.loads(piloto_bytes)
    borrador = json.loads(aprobacion_borrador_bytes)
    exigir(type(plan) is dict and type(piloto) is dict and type(borrador) is dict,
           'JSON_OBJETO')
    # No tolerar marcadores de habilitacion inyectados en un borrador local.
    exigir(borrador.get('draft_only') is True and
           borrador.get('verified_writer_identity') is False and
           borrador.get('transfer_allowed') is False and
           borrador.get('step_101_7_authorized') is not True and
           borrador.get('operational_gate_authorized') is not True and
           borrador.get('is_operational') is not True,
           'APROBACION_LOCAL_DEBE_PERMANECER_BORRADOR')
    validacion_quality = validar_preaprobacion_local(
        aprobacion_borrador_bytes, plan_bytes, piloto_bytes)
    lote = seleccionar_lote(plan_bytes, 'completo')
    exigir(lote['run_id'] == RUN_ID and lote['archivos'] == 3599 and
           lote['bytes'] == 393623640, 'LOTE_329C_INCONSISTENTE')
    exigir(piloto.get('run_id') == RUN_ID and piloto.get('archivos') == 12 and
           piloto.get('datasets') == 12 and piloto.get('bytes_esperados') == 23052390,
           'PILOTO_329C_INCONSISTENTE')
    exigir(piloto.get('quality_approved') is False and
           piloto.get('transfer_allowed') is False and
           piloto.get('publicacion') == 'NO_PUBLICADO', 'PILOTO_DEBE_ESTAR_BLOQUEADO')
    inventario = {o['destino']: o for o in lote['operaciones']}
    vistos = set()
    ds = set()
    total = 0
    for item in piloto['archivos_piloto']:
        exigir(type(item) is dict, 'PILOTO_ITEM_TIPO')
        destino = item.get('destino_gcs')
        op = inventario.get(destino)
        exigir(op is not None and destino not in vistos, 'PILOTO_DESTINO_NO_APROBADO')
        exigir(item.get('run_id') == RUN_ID and item.get('dataset') == op['dataset'] and
               item.get('origen_hdfs') == op['origen'] and
               item.get('bytes_esperados') == op['size_bytes'] and
               item.get('sha256_esperado') == op['sha256'],
               'PILOTO_ITEM_NO_COINCIDE_PLAN')
        vistos.add(destino)
        ds.add(op['dataset'])
        total += op['size_bytes']
    exigir(len(vistos) == len(ds) == 12 and total == 23052390,
           'PILOTO_329C_CANTIDAD_BYTES')
    exigir(validacion_quality['transfer_allowed'] is False and
           lote['transfer_allowed'] is False, 'INTENTO_HABILITACION')
    return {
        'run_id': RUN_ID, 'plan_sha256': PLAN_SHA, 'piloto_sha256': PILOTO_SHA,
        'datasets_lote': lote['datasets'], 'archivos_lote': 3599,
        'archivos_piloto': 12, 'bytes_piloto': total,
        'integracion_local': 'PASS',
        'quality_approved_publicado_en_gcs': False,
        'autor_quality_autenticado': False,
        'transfer_allowed_emitido': False,
        'release_101_7_autenticado': False,
        'transfer_allowed': False, 'operaciones_gcs': 0,
        'estado': 'BLOQUEADO_HASTA_PROCEDENCIA_Y_RELEASE_AUTENTICADOS',
    }

def transferir_operativamente(*_args, **_kwargs):
    """Punto único de denegación en esta versión: sin efecto secundario."""
    raise PermissionError('BLOQUEADO|PASO_101_7|RELEASE_OFICIAL_NO_INTEGRADO')


def main():
    cli = argparse.ArgumentParser(
        description='Puerta Bronze v0.1, unifica preflight; SIN operaciones Cloud')
    cli.add_argument('--preflight-only', action='store_true', required=True)
    cli.add_argument('--alcance', choices=('piloto', 'completo'), required=True)
    cli.add_argument('--plan', type=Path, required=True)
    cli.add_argument('--piloto', type=Path)
    cli.add_argument('--hdfs-evidencia', type=Path)
    args = cli.parse_args()
    try:
        p = json.loads(args.piloto.read_bytes()) if args.piloto else None
        h = json.loads(args.hdfs_evidencia.read_bytes()) if args.hdfs_evidencia else None
        lote = preflight_unificado(args.plan.read_bytes(), args.alcance, p, h)
    except (ValueError, OSError, TypeError, KeyError, PermissionError) as exc:
        cli.exit(1, f'RECHAZADO|{exc}\n')
    print('===== PUERTA QUALITY BRONZE v0.1, SOLO LOCAL =====')
    print('RUN_ID:', lote['run_id'])
    print('ALCANCE:', lote['alcance'])
    print('ARCHIVOS:', lote['archivos'])
    print('BYTES:', lote['bytes'])
    print('PLAN_SHA256:', lote['plan_sha256_local'])
    print('TRANSFER_ALLOWED: False')
    print('OPERACIONES_GCS: 0')
    print('PENDIENTE: aprobación IAM, release Quality autenticado y 101.7')
    print('RESULTADO: PUERTA_BRONZE_PREPARADA_SIN_CLOUD')


if __name__ == '__main__':
    main()
