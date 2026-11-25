#!/usr/bin/env python3
"""Coordinador Bronze: seleccion de piloto/lote, despacho y recibos por archivo.

Paso 101.7 VIGENTE. El CLI solo tiene --preflight-only (cero Cloud).
El trabajador interno permite ensayos con un transferidor falso; NO autentica
Quality ni constituye un punto de entrada operativo.
"""

import argparse
import fcntl
import hashlib
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from preparar_piloto_bronze_local import validar_plan
from orquestar_piloto_bronze_local import integrar
import transferir_hdfs_gcs as nucleo


IDENTIDAD = 'sa-bvl-ingestion@bvl-analytics-2026.iam.gserviceaccount.com'


class OrquestadorError(ValueError):
    pass


def exigir(condicion, codigo):
    if not condicion:
        raise OrquestadorError(codigo)


def seleccionar_lote(plan_bytes, alcance, piloto=None, evidencia_hdfs=None):
    """Seleccion validada 1:1 contra el plan original de 3599 destinos."""
    plan = json.loads(plan_bytes)
    run, _, conteos = validar_plan(plan)
    ops = plan['operaciones']
    exigir(alcance in ('piloto', 'completo'), 'ALCANCE_INVALIDO')
    exigir(len(ops) == 3599 and len(conteos) == 12, 'PLAN_INCOMPLETO')
    por_destino = {o['destino']: o for o in ops}
    exigir(len(por_destino) == 3599, 'DESTINOS_DUPLICADOS')
    if alcance == 'piloto':
        exigir(isinstance(piloto, dict) and isinstance(evidencia_hdfs, dict),
               'PILOTO_EVIDENCIA_REQUERIDA')
        previo = integrar(plan_bytes, piloto, evidencia_hdfs)
        exigir(previo['hdfs_verificados'] == 12 and previo['transfer_allowed'] is False,
               'PILOTO_NO_VERIFICADO')
        seleccion = []
        for item in piloto['archivos_piloto']:
            op = por_destino.get(item['destino_gcs'])
            exigir(op is not None and op['origen'] == item['origen_hdfs'],
                   'PILOTO_NO_COINCIDE_PLAN')
            seleccion.append(op)
        exigir(len(seleccion) == 12, 'PILOTO_CANTIDAD')
    else:
        exigir(piloto is None and evidencia_hdfs is None,
               'COMPLETO_NO_ACEPTA_PILOTO')
        seleccion = ops
    for op in seleccion:
        nucleo.validar_operacion(op)
    destinos = [op['destino'] for op in seleccion]
    exigir(len(destinos) == len(set(destinos)), 'LOTE_DESTINOS_DUPLICADOS')
    return {
        'run_id': run,
        'alcance': alcance,
        'plan_sha256_local': hashlib.sha256(plan_bytes).hexdigest(),
        'datasets': len({op['dataset'] for op in seleccion}),
        'archivos': len(seleccion),
        'bytes': sum(op['size_bytes'] for op in seleccion),
        'operaciones': seleccion,
        'autenticidad_quality_verificada': False,
        'release_101_7_verificado': False,
        'transfer_allowed': False,
        'publicacion': 'NO_PUBLICADO',
    }


def validar_recibo(op, recibo, contexto=None):
    """Valida recibos del nucleo, no solo su estado nominal."""
    exigir(isinstance(recibo, dict), 'RECIBO_TIPO')
    campos = {
        'run_id': op['run_id'], 'dataset': op['dataset'],
        'origen_hdfs': op['origen'], 'destino_gcs': op['destino'],
        'bytes_esperados': op['size_bytes'],
        'bytes_observados': op['size_bytes'],
        'sha256_esperado': op['sha256'], 'sha256_gcs': op['sha256'],
        'validacion': 'INTEGRIDAD_VERIFICADA',
    }
    for campo, valor in campos.items():
        exigir(recibo.get(campo) == valor, 'RECIBO_INCONSISTENTE:' + campo)
    exigir(recibo.get('identidad_esperada') == IDENTIDAD,
           'RECIBO_IDENTIDAD_ESPERADA')
    exigir(recibo.get('estado') in ('TRANSFERIDO', 'REINTENTO_EXISTENTE'),
           'RECIBO_ESTADO')
    exigir(type(recibo.get('generacion_gcs')) is int and
           recibo['generacion_gcs'] > 0, 'RECIBO_GENERACION')
    if contexto is not None:
        # Solo cotejo de trazabilidad: NO autentica a Quality ni el release.
        _, referencias = nucleo.validar_trazabilidad(op, contexto)
        adicionales = {
            'plan_sha256': contexto['plan_sha256'],
            'quality_release_id': contexto['release_id'],
            'alcance': contexto['alcance'],
            'reserva': referencias['reservation'],
            'aprobacion': referencias['approval'],
        }
        for campo, esperado in adicionales.items():
            exigir(recibo.get(campo) == esperado,
                   'RECIBO_TRAZABILIDAD:' + campo)
    return dict(recibo)


def registrar_evento(archivo_abierto, evento):
    data = (json.dumps(evento, ensure_ascii=False, sort_keys=True) + '\n').encode()
    archivo_abierto.write(data)
    archivo_abierto.flush()
    os.fsync(archivo_abierto.fileno())


def transferir_mediante_nucleo(op, cliente_gcs, contexto=None):
    """Adaptador al nucleo real, tambien bloqueado por Paso 101.7.

    NUNCA invocado por el CLI ni por el ensayo local. Solo podra utilizarse
    desde un futuro gateway de Quality debidamente autenticado.
    """
    return nucleo.transferir_archivo(op, cliente_gcs, contexto=contexto)


def es_error_transitorio(exc):
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return True
    codigo = getattr(exc, 'code', None)
    if callable(codigo):
        codigo = codigo()
    return codigo in (429, 500, 502, 503, 504)


def _procesar_ensayo_con_inyeccion(lote, transferidor_falso, journal, reintentos=2):
    """Solo tests: trabaja con una funcion falsa; nunca autentica ni llama GCS.

    La version Cloud solo podra reutilizar este patron despues de que Quality
    defina el adaptador verificador de release. No hay ruta CLI para invocarlo.
    """
    exigir(isinstance(lote, dict) and lote.get('transfer_allowed') is False,
           'LOTE_NO_LOCAL')
    exigir(callable(transferidor_falso), 'TRANSFERIDOR_FALSO_REQUERIDO')
    exigir(type(reintentos) is int and 0 <= reintentos <= 3, 'REINTENTOS_INVALIDOS')
    journal = Path(journal)
    exigir(journal.parent.is_dir(), 'JOURNAL_DIRECTORIO')
    resultados = []
    # Registro local append-only; jamas se usa para omitir la verificacion GCS.
    with journal.open('ab', buffering=0) as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        for op in lote['operaciones']:
            resultado = None
            for intento in range(1, reintentos + 2):
                evento = {
                    'tipo': 'ENSAYO_LOCAL_SIN_CLOUD',
                    'timestamp_utc': datetime.now(timezone.utc).isoformat(),
                    'plan_sha256_local': lote['plan_sha256_local'],
                    'alcance': lote['alcance'],
                    'intento': intento,
                    'run_id': op['run_id'], 'dataset': op['dataset'],
                    'origen_hdfs': op['origen'], 'destino_gcs': op['destino'],
                    'bytes_esperados': op['size_bytes'], 'sha256_esperado': op['sha256'],
                }
                try:
                    recibo = validar_recibo(op, transferidor_falso(op))
                    evento.update(recibo)
                    evento['tipo'] = 'ENSAYO_LOCAL_SIN_CLOUD'
                    registrar_evento(fh, evento)
                    resultado = evento
                    break
                except Exception as exc:
                    if es_error_transitorio(exc):
                        evento.update(estado='ERROR_TRANSITORIO', error=type(exc).__name__)
                        registrar_evento(fh, evento)
                        if intento > reintentos:
                            raise OrquestadorError('ERROR_TRANSITORIO_AGOTADO') from exc
                    else:
                        evento.update(estado='RECHAZADO', error=str(exc)[:240])
                        registrar_evento(fh, evento)
                        raise
            exigir(resultado is not None, 'TRANSFERENCIA_SIN_RECIBO')
            resultados.append(resultado)
    return {
        'tipo': 'ENSAYO_LOCAL_SIN_CLOUD',
        'run_id': lote['run_id'], 'alcance': lote['alcance'],
        'archivos_verificados_simulados': len(resultados),
        'bytes_verificados_simulados': sum(r['bytes_observados'] for r in resultados),
        'estados': dict(Counter(r['estado'] for r in resultados)),
        'operaciones_gcs': 0,
        'transfer_allowed': False,
        'publicacion': 'NO_PUBLICADO',
    }


def ejecutar_operativo(*_args, **_kwargs):
    """Barreras definitivas: no se acepta habilitacion por argumento ni JSON.

    Requiere posteriormente adaptador de release autenticado de Quality,
    permiso IAM y liberacion independiente, todos NO disponibles al dia de hoy.
    """
    if not nucleo.TRANSFERENCIAS_HABILITADAS:
        raise PermissionError('BLOQUEADO|PASO_101_7|QUALITY_NO_AUTORIZO')
    raise PermissionError('BLOQUEADO|RELEASE_QUALITY_AUTENTICADO_NO_IMPLEMENTADO')


def main():
    cli = argparse.ArgumentParser(description='Coordinador Bronze 12 o 3599, SOLO PREFLIGHT LOCAL')
    cli.add_argument('--plan', required=True, type=Path)
    cli.add_argument('--alcance', required=True, choices=('piloto', 'completo'))
    cli.add_argument('--piloto', type=Path)
    cli.add_argument('--hdfs-evidencia', type=Path)
    cli.add_argument('--preflight-only', action='store_true', required=True)
    args = cli.parse_args()
    try:
        exigir(nucleo.TRANSFERENCIAS_HABILITADAS is False, 'NUCLEO_TRANSFERENCIA_HABILITADO')
        p = json.loads(args.piloto.read_bytes()) if args.piloto else None
        h = json.loads(args.hdfs_evidencia.read_bytes()) if args.hdfs_evidencia else None
        lote = seleccionar_lote(args.plan.read_bytes(), args.alcance, p, h)
    except (OSError, ValueError, KeyError, TypeError, PermissionError) as exc:
        cli.exit(1, f'RECHAZADO|{exc}\n')
    print('===== ORQUESTADOR TRANSFERENCIAS v0.1 - SOLO LOCAL =====')
    print('RUN_ID:', lote['run_id'])
    print('ALCANCE:', lote['alcance'])
    print('DATASETS:', lote['datasets'])
    print('ARCHIVOS:', lote['archivos'])
    print('BYTES:', lote['bytes'])
    print('OPERACIONES_GCS: 0')
    print('TRANSFER_ALLOWED: False')
    print('PENDIENTE: release autenticado Quality, IAM y Paso 101.7')
    print('RESULTADO: ORQUESTADOR_PREPARADO_SIN_CLOUD')


if __name__ == '__main__':
    main()
