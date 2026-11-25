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
