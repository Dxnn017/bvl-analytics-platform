#!/usr/bin/env python3
"""Validador PURE de alcance Hechos documental. NO es un launcher autorizado.

No GCS, KMS, Spark, HDFS, claims, escritura ni publicación. Solo propuesta
para que Quality ratifique un nuevo alcance firmado antes de habilitar ejecución.
"""
from __future__ import annotations
from collections import Counter
from datetime import datetime
from hashlib import sha256
from pathlib import PurePosixPath
import re

DATASET = 'smv/hechos_importancia'
RUN = 'bvl-bronze-inicial-20261125-001'
BUCKET = 'bvl26-bronze-7f3a9c'
PRODUCT_PROPOSED = 'hechos_importancia/documental_restringido'
OLD_PRODUCT_BLOCKED = 'hechos_importancia/eventos'
MANIFEST_GENERATION = '1795680153321464'
MANIFEST_SHA256 = 'd2c5f5d5a363e271a770497b4af6feedde454f999bc2fe7696a5c0ab0784eab0'
PREFIX = f'gs://{BUCKET}/{DATASET}/run_id={RUN}/'
NAME = re.compile(r'(\d{8})_([A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12})\.(pdf|txt)\Z')
HEX = re.compile(r'[a-f0-9]{64}\Z')
MAX_BYTES = 64 * 1024 * 1024


def require(yes: bool, reason: str) -> None:
    if not yes:
        raise ValueError(reason)


def validate_inventory(files: object) -> dict:
    """Prechequeo ESTRUCTURAL, no reemplaza firma ni lectura de bytes GCS."""
    require(type(files) is list and len(files) == 1979, 'INVENTARIO_1979_OBLIGATORIO')
    seen_uris, seen_sources, pdf, txt = set(), set(), {}, {}
    for item in files:
        require(type(item) is dict, 'ENTRADA_INVALIDA')
        uri, source = item.get('uri'), item.get('source_hdfs')
        require(type(uri) is str and uri.startswith(PREFIX), 'BRONZE_FUERA_DE_SCOPE')
        require(type(source) is str and source.startswith('/datalake/bvl/bronze/smv/hechos_importancia/'), 'HDFS_FUERA_DE_SCOPE')
        require(uri not in seen_uris and source not in seen_sources, 'OBJETO_REPETIDO')
        seen_uris.add(uri); seen_sources.add(source)
        require('..' not in PurePosixPath(uri).parts and '..' not in PurePosixPath(source).parts, 'TRAVERSAL')
        filename = PurePosixPath(source).name
        match = NAME.fullmatch(filename)
        require(match is not None, 'NOMBRE_INVALIDO')
        try:
            datetime.strptime(match.group(1), '%Y%m%d')
        except ValueError as exc:
            raise ValueError('FECHA_NOMINAL_INVALIDA') from exc
        ext = match.group(3)
        subdir = 'pdf' if ext == 'pdf' else 'texto_extraido'
        segment = f'/{subdir}/ingestion_date=2026-10-04/{filename}'
        require(uri.endswith(segment) and source.endswith(segment), 'RUTA_NO_COINCIDE')
        generation, size, digest = item.get('generation'), item.get('size_bytes'), item.get('sha256')
        require(type(generation) in (str, int) and str(generation).isascii() and str(generation).isdecimal() and int(generation) > 0, 'GENERACION_INVALIDA')
        require(type(size) is int and 0 < size <= MAX_BYTES, 'TAMANO_INVALIDO')
        require(type(digest) is str and HEX.fullmatch(digest) is not None, 'SHA_INVALIDO')
        target = pdf if ext == 'pdf' else txt
        require(filename not in target, 'NOMBRE_REPETIDO')
        target[filename] = digest
    require(len(pdf) == 990 and len(txt) == 989, 'CONTEO_PDF_TXT_INVALIDO')
    require(len(set(pdf.values())) == 989 and len(pdf) - len(set(pdf.values())) == 1, 'DUPLICADO_SHA_INESPERADO')
    sha_names = {}
    for name, h in pdf.items():
        sha_names.setdefault(h, []).append(name)
    require(sorted(len(v) for v in sha_names.values())[-1] == 2, 'MAS_DE_DOS_PDF_IDENTICOS')
    missing = []
    canonical_reused = 0
    for name, h in pdf.items():
        partner = name[:-4] + '.txt'
        if partner in txt:
            continue
        candidates = [n[:-4] + '.txt' for n in sha_names[h] if (n[:-4] + '.txt') in txt]
        if len(candidates) == 1:
            canonical_reused += 1
        else:
            missing.append(name)
    require(not missing and canonical_reused == 1, 'TEXTOS_NO_CONCILIADOS')
    return {
        'source_dataset': DATASET,
        'source_object_count': len(files),
        'pdf_count': len(pdf),
        'txt_count': len(txt),
        'unique_pdf_sha256': len(sha_names),
        'duplicate_pdf_by_sha256': 1,
        'canonical_txt_reuse': 1,
        'quarantined_historical_events': 27,
        'proposed_product': PRODUCT_PROPOSED,
        'historic_event_product_allowed': False,
        'manifest_generation_expected': MANIFEST_GENERATION,
        'manifest_sha256_expected': MANIFEST_SHA256,
        'bytes_read_from_gcs': False,
        'quality_approved': False,
        'claim_allowed': False,
        'publish_allowed': False,
        'status': 'SOLO_PRECHEQUEO_ESTRUCTURAL_PENDIENTE_QUALITY',
    }


def require_quality_signed_contract(_anything=None) -> None:
    """Fail closed deliberado: reemplazar SOLO con contrato y launcher ratificados."""
    raise PermissionError('BLOQUEADO: FALTA NUEVO ALCANCE Y CONTEXTO FIRMADO QUALITY')


if __name__ == '__main__':
    print('HECHOS_LAUNCH_SCOPE: CANDIDATO_SIN_EJECUCION')
    print('STATUS: BLOQUEADO_POR_DEFECTO; NO GCS, NO CLAIM, NO PUBLICACION')
