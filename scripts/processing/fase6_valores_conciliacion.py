#!/usr/bin/env python3
"""Conciliacion LOCAL (sin GCS) de 289 XML + 1 JSON de Valores SMV.

No autoriza transformar ni publicar. Los bytes requieren preflight de Quality
separado y los resultados deben cotejarse con evidencia Bronze oficial.
"""
from collections import Counter
import hashlib
import re
from pathlib import PurePosixPath
from fase6_restantes_core import require
from fase6_restantes_transform import load_json_records
from fase6_especializados_hechos_valores import parse_xml


def normalize(value):
    return str(value or '').strip()


def ruc_from_uri(uri):
    name = PurePosixPath(uri).name
    require(re.fullmatch(r'[0-9]{11}\.xml', name) is not None,
            'Valores: nombre XML no permite fijar RUC; revisar contrato')
    return name[:-4]


def reconcile_valores(xml_items, json_raw, *, expected_xml=289):
    """Retorna registros JSON auditados; compara multiset del contenido XML.

    Las columnas de auditoria añadidas al JSON historico no estan en el XML,
    por lo que se comparan todos los atributos XML presentes, mas el RUC.
    No se eliminan duplicados ni se consulta SOAP.
    """
    require(isinstance(xml_items, (list, tuple)) and len(xml_items) == expected_xml,
            'Valores: conteo de XML distinto al inventario autorizado')
    require(len({uri for uri, _ in xml_items}) == len(xml_items),
            'Valores: XML repetido')
    json_rows = load_json_records([json_raw], required_wrappers=('Resultado',))
    require(all(isinstance(r, dict) and normalize(r.get('RUC')) and
                normalize(r.get('NemonicoValor')) for r in json_rows),
            'Valores: clave RUC/nemonico vacia en JSON')

    xml_records = []
    error_codes = []
    xml_with_records = 0
    fields = set()
    for uri, raw in xml_items:
        require(type(raw) is bytes, 'Valores: XML no es bytes')
        ruc = ruc_from_uri(uri)
        rows, errors = parse_xml(raw)
        if errors:
            error_codes.extend({'uri': uri, 'code': c} for c in errors)
        if rows:
            xml_with_records += 1
        for row in rows:
            require('RUC' not in row or normalize(row['RUC']) == ruc,
                    'Valores: RUC contenido difiere del nombre XML')
            normalized = {'RUC': ruc}
            for k, v in row.items():
                normalized[k] = normalize(v)
                fields.add(k)
            xml_records.append(normalized)
    require(len(xml_records) == len(json_rows),
            'Valores: numero de registros XML y JSON no coincide')
    fields.add('RUC')
    # En cada clave se comparan todos los atributos extraidos de XML. Esto
    # evita una conciliacion engañosa basada solo en el ticker.
    ordered_fields = sorted(fields)
    def signature(r):
        return tuple(normalize(r.get(k)) for k in ordered_fields)
    require(Counter(map(signature, xml_records)) == Counter(map(signature, json_rows)),
            'Valores: datos, multiplicidades o atributos XML/JSON no coinciden')
    return json_rows, {
        'dataset': 'smv/valores_inscritos',
        'xml_count': len(xml_items),
        'json_files': 1,
        'xml_with_records': xml_with_records,
        'xml_records': len(xml_records),
        'json_records': len(json_rows),
        'xml_errors_106_107': error_codes,
        'reconciled_fields': ordered_fields,
        'xml_json_exact_multiset': True,
        'quality_status': 'CANDIDATO_PENDIENTE_QUALITY',
    }
