#!/usr/bin/env python3
"""INC-F6-SMV-003, particion estricta de Montos SAB sin publicar Silver.

Requiere bytes completos y metadatos FIJADOS por Quality. Solo las respuestas
ResponseCode=0/Consulta generada son validas para transformar. Los 14 archivos
con error -2147024864 se conservan en cuarentena con sus 245 registros. Esta
funcion NO sirve de autorizacion de lote parcial.
"""
from __future__ import annotations
import hashlib
import json
import re
from collections import Counter
from pathlib import PurePosixPath

from fase6_restantes_core import ScopeError, require

PREFIX = ('gs://bvl26-bronze-7f3a9c/smv/montos_sab/'
          'run_id=bvl-bronze-inicial-20261125-001/')
ERR_LOG = ("The process cannot access the file "
           "'\\\\alpha\\areas\\oti\\Log_Aplicaciones\\OpenData\\Info\\LogInfo_20260928.txt' "
           "because it is being used by another process.")
MAX_ITEM_BYTES = 32 * 1024 * 1024
MAX_RECORDS = 200000


def _strict_load(raw):
    require(type(raw) is bytes and 0 < len(raw) <= MAX_ITEM_BYTES,
            'Montos SAB: bytes no validos')
    def uniq(pairs):
        x={}
        for key,value in pairs:
            require(key not in x, 'Montos SAB: clave JSON repetida')
            x[key]=value
        return x
    try:
        x=json.loads(raw.decode('utf-8-sig'), object_pairs_hook=uniq,
                     parse_constant=lambda v: (_ for _ in ()).throw(ValueError(v)))
    except (ValueError,UnicodeError) as ex:
        raise ScopeError('Montos SAB: JSON no valido') from ex
    require(type(x) is dict and set(x) == {'ResponseCode','Message','Resultado'},
            'Montos SAB: wrapper/estructura invalida')
    require(type(x['ResponseCode']) is str and type(x['Message']) is str,
            'Montos SAB: estado no valido')
    require(type(x['Resultado']) is list and len(x['Resultado'])>0 and
            all(type(v) is dict for v in x['Resultado']),
            'Montos SAB: Resultado no es lista de registros')
    return x


def _verify_pin(pin, raw):
    require(type(pin) is dict, 'Montos SAB: falta objeto fijado')
    uri=pin.get('uri')
    require(isinstance(uri,str) and uri.startswith(PREFIX) and
            re.fullmatch(r'montos_sab_[0-9]{8}\.json',PurePosixPath(uri).name),
            'Montos SAB: URI no coincide con alcance oficial')
    require(type(pin.get('size_bytes')) is int and pin['size_bytes']==len(raw),
            'Montos SAB: tamano no coincide')
    digest=hashlib.sha256(raw).hexdigest()
    require(type(pin.get('sha256')) is str and pin['sha256']==digest,
            'Montos SAB: hash no coincide')
    require(re.fullmatch(r'[0-9]+',str(pin.get('generation',''))) is not None,
            'Montos SAB: generacion no fijada')
    return uri,digest


def partition_montos_sab(items, *, expected=None):
    """items: [(pin, raw_bytes), ...], sin leer GCS ni escribir Silver.

    Devuelve (registros_normales, archivos_cuarentena, metricas). Las 245
    filas de error permanecen dentro de cada elemento de cuarentena, ademas
    de estar conservadas intactas en Bronze. No transforma esos registros.
    """
    require(type(items) in (list,tuple) and len(items)>0,
            'Montos SAB: no hay objetos')
    seen=set(); normal=[]; quarantined=[]; n_files=0; n_rows=0
    for item in items:
        require(type(item) is tuple and len(item)==2,
                'Montos SAB: se requiere pareja (pin, bytes)')
        pin,raw=item
        uri,digest=_verify_pin(pin,raw)
        require(uri not in seen,'Montos SAB: objeto repetido')
        seen.add(uri)
        obj=_strict_load(raw)
        rows=obj['Resultado']
        n_rows+=len(rows)
        require(n_rows<=MAX_RECORDS, 'Montos SAB: maximo de registros')
        code,msg=obj['ResponseCode'],obj['Message']
        if code=='0' and msg=='Consulta generada':
            normal.extend(rows)
            n_files+=1
        elif code=='-2147024864' and msg==ERR_LOG:
            quarantined.append({
                'uri':uri, 'generation':str(pin['generation']),
                'sha256':digest,'size_bytes':len(raw),
                'fecha_nominal_archivo':PurePosixPath(uri).name[len('montos_sab_'):len('montos_sab_')+8],
                'codigo_error_smv':code,'mensaje_error_smv':msg,
                'registro_cuarentena':rows,
                'registros_cuarentena':len(rows),
                'estado':'CUARENTENA_ERROR_SERVICIO_NO_ACREDITADO',
            })
        else:
            raise ScopeError('Montos SAB: respuesta distinta del contrato acreditado')
    metricas={
        'dataset':'smv/montos_sab', 'files_bronze':len(items),
        'files_normal':n_files,'files_quarantined':len(quarantined),
        'bronze_rows':n_rows,'rows_normal':len(normal),
        'rows_quarantined':sum(x['registros_cuarentena'] for x in quarantined),
        'rows_silently_discarded':0,
        'status':'CANDIDATO_PARCIAL_NO_AUTORIZADO',
        'coverage_full':len(quarantined)==0,
        'quality_scope_ratification_required':bool(quarantined),
    }
    require(metricas['rows_normal'] + metricas['rows_quarantined']==n_rows,
            'Montos SAB: conciliacion no coincide')
    if expected is not None:
        require(type(expected) is dict and all(metricas.get(k)==v for k,v in expected.items()),
                'Montos SAB: conteos diferentes de evidencia Quality')
    return normal,quarantined,metricas


OFFICIAL_EXPECTED = {
    'files_bronze':1221,'files_normal':1207,'files_quarantined':14,
    'bronze_rows':22156,'rows_normal':21911,'rows_quarantined':245,
}
