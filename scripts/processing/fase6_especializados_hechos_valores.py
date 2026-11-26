#!/usr/bin/env python3
"""BVL F6: conciliacion pura de 22 XML originales + 1 JSON oficial.

No usa requests/SOAP, no lee GCS, no crea Spark ni escribe Silver.
Solo aceptar bytes previamente verificados por generacion/SHA contra manifests.
Error codes SMV 106/107 se registran como incidencias, nunca como registros.
"""
from __future__ import annotations
from collections import Counter, defaultdict
from hashlib import sha256
import json
import re
import xml.etree.ElementTree as ET

class HechosValoresQualityError(ValueError):
    pass


def ensure(ok, msg):
    if not ok:
        raise HechosValoresQualityError(msg)


def normalized(value):
    return str(value or '').strip()


def pair(record):
    return (normalized(record.get('NemonicoValor')).upper(),
            normalized(record.get('RazonSocial')))


def parse_json(raw):
    ensure(type(raw) is bytes and 0 < len(raw) <= 32*1024*1024, 'JSON fuera de rango')
    def strict_pairs(pairs):
        obj = {}
        for k, v in pairs:
            ensure(k not in obj, f'Clave JSON repetida: {k}')
            obj[k] = v
        return obj
    try:
        records = json.loads(raw.decode('utf-8-sig'), object_pairs_hook=strict_pairs,
            parse_constant=lambda v: (_ for _ in ()).throw(ValueError(v)))
    except (UnicodeDecodeError, ValueError) as ex:
        raise HechosValoresQualityError('JSON invalido') from ex
    ensure(isinstance(records, list), 'JSON esperado como array historico')
    ensure(all(isinstance(x, dict) for x in records), 'Filas JSON no son objetos')
    return records


def parse_xml(raw):
    ensure(type(raw) is bytes and 0 < len(raw) <= 8*1024*1024, 'XML fuera de rango')
    # ElementTree no necesita DTD: bloquear cualquier declaracion con entidad externa.
    ensure(re.search(br'<!\s*(?:DOCTYPE|ENTITY)\b', raw, flags=re.I) is None,
           'XML DTD/ENTITY prohibido')
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as ex:
        raise HechosValoresQualityError('XML invalido') from ex
    def loc(tag):
        return str(tag).split('}')[-1]
    rows = []
    errors = []
    for elem in root.iter():
        name = loc(elem.tag)
        if name == 'ValorObtenidoBE':
            row = {}
            for child in elem:
                key = loc(child.tag)
                ensure(key not in row, f'XML campo duplicado: {key}')
                row[key] = normalized(child.text)
            rows.append(row)
        elif name == 'CodigoError' and normalized(elem.text) not in ('', '0'):
            errors.append(normalized(elem.text))
    ensure(not errors or (set(errors).issubset({'106', '107'}) and not rows),
           'Error SMV desconocido o mezcla error/registros')
    return rows, errors


def reconcile(xml_items, json_bytes):
    """xml_items = iterable[(uri, bytes)] from exactly 22 pinned official inputs.

    Se compara multiset (nemonico, razon social) para conservar duplicados.
    Si los datos reales contradicen esta clave, Quality debe ratificar esquema;
    no modificarla silenciosamente para forzar igualdad.
    """
    ensure(isinstance(xml_items, (list, tuple)) and len(xml_items) == 22,
           'Se esperan 22 XML oficiales')
    ensure(all(isinstance(v, tuple) and len(v) == 2 and isinstance(v[0], str)
               and v[0].endswith('.xml') and type(v[1]) is bytes for v in xml_items),
           'XML con referencia de objeto invalida')
    uris = [u for u, _ in xml_items]
    ensure(len(set(uris)) == 22, 'XML oficial repetido')
    js = parse_json(json_bytes)
    xml_pairs = Counter()
    lineage = defaultdict(set)
    errors = []
    valid_xml = 0
    for uri, raw in xml_items:
        rows, codes = parse_xml(raw)
        for code in codes:
            errors.append({'uri':uri, 'codigo_smv':code})
        if rows: valid_xml += 1
        for row in rows:
            key = pair(row)
            ensure(bool(key[0]), f'XML nemonico vacio {uri}')
            xml_pairs[key] += 1
            lineage[key].add(uri)
    js_pairs = Counter()
    for row in js:
        key = pair(row)
        ensure(key[0] and normalized(row.get('empresa_consultada')),
               'JSON sin nemonico o empresa consultada')
        js_pairs[key] += 1
    ensure(js_pairs == xml_pairs, 'Inconsistencia XML/JSON: filas, duplicados o identidad')
    normalized_rows = []
    for row in js:
        key = pair(row)
        out = dict(row)
        out['nemonico_smv'] = key[0]
        out['razon_social_smv'] = key[1]
        out['empresa_consultada'] = normalized(row['empresa_consultada'])
        out['_xml_sources'] = sorted(lineage[key])
        normalized_rows.append(out)
    # Stable sorting; retain duplicate records (do not silently deduplicate).
    normalized_rows.sort(key=lambda r: (r['nemonico_smv'],r['razon_social_smv'],r['empresa_consultada']))
    return normalized_rows, {
        'dataset':'smv/hechos_valores_consulta',
        'product':'referencias_smv/hechos_valores_consulta',
        'xml_count':22, 'xml_nonempty_count':valid_xml,
        'xml_error_106_107':errors,
        'json_row_count':len(js), 'xml_row_count':sum(xml_pairs.values()),
        'unique_nemonico_razon':len(xml_pairs),
        'json_sha256':sha256(json_bytes).hexdigest(),
        'xml_json_multiset_equal':True,
        'requires_quality_review_of_errors':bool(errors),
    }
