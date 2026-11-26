#!/usr/bin/env python3
"""Serializador Silver candidato multi-producto a Parquet/Snappy create-only.

NO verifica firmas ni consume claims. Usar exclusivamente DESPUES de la puerta
operativa F5, contexto firmado F6 y permiso de Quality para cada intento.
NO publica versiones oficiales ni actualiza current.json.
"""
from __future__ import annotations
from collections import defaultdict
from datetime import date, datetime
from hashlib import sha256
from io import BytesIO
import math

from fase6_restantes_core import require, expected_prefix, SOURCES, SILVER_BUCKET

MAX_OUTPUT_ROWS = 250_000
MAX_PARTITIONS = 800
MAX_ROWS_PER_OBJECT = 3000
MAX_BYTES_PER_OBJECT = 16 * 1024 * 1024


def _arrow_type(data_type, pa):
    from pyspark.sql.types import (StringType, IntegerType, LongType, ShortType,
                                   FloatType, DoubleType, BooleanType,
                                   DateType, TimestampType, DecimalType)
    if isinstance(data_type, StringType): return pa.string()
    if isinstance(data_type, IntegerType): return pa.int32()
    if isinstance(data_type, (LongType, ShortType)): return pa.int64()
    if isinstance(data_type, FloatType): return pa.float32()
    if isinstance(data_type, DoubleType): return pa.float64()
    if isinstance(data_type, BooleanType): return pa.bool_()
    if isinstance(data_type, DateType): return pa.date32()
    if isinstance(data_type, TimestampType): return pa.timestamp('us')
    if isinstance(data_type, DecimalType): return pa.decimal128(data_type.precision, data_type.scale)
    raise ValueError('Tipo Spark anidado/no soportado: ' + str(data_type))


def schema_for_arrow(spark_schema, partition_fields, pa):
    require(isinstance(partition_fields, (tuple, list)) and
            len(set(partition_fields)) == len(partition_fields), 'Particiones invalidas')
    names = [f.name for f in spark_schema.fields]
    require(all(name in names for name in partition_fields), 'Particion sin columna')
    require(len(names) == len(set(names)), 'Columnas duplicadas')
    non_part = [f for f in spark_schema.fields if f.name not in partition_fields]
    require(non_part, 'Esquema no contiene datos')
    return pa.schema([pa.field(f.name, _arrow_type(f.dataType, pa), nullable=f.nullable)
                      for f in non_part])


def collect_groups(df, partition_fields, max_rows=MAX_OUTPUT_ROWS):
    """Acotado a datasets pequenos/medianos; no recomendado para PDFs ni XML.

    Rechaza NaN/Inf y particiones NULL; no compacta filas ni hace casts.
    """
    fields = [f.name for f in df.schema.fields]
    require(all(name in fields for name in partition_fields), 'Particion no presente')
    groups = defaultdict(list)
    seen = 0
    for row in df.toLocalIterator():
        r = row.asDict(recursive=True)
        require(set(r) == set(fields), 'Columnas en fila no coinciden')
        partition = tuple(r.get(f) for f in partition_fields)
        require(all(type(v) is int and 0 <= v <= 2200 for v in partition),
                'Particion nula/no entera/fuera de rango')
        # No convertir datos silenciosamente a NULL por valores no finitos.
        require(not any(isinstance(v, float) and not math.isfinite(v) for v in r.values()),
                'Valor numerico no finito')
        groups[partition].append({k: v for k, v in r.items() if k not in partition_fields})
        seen += 1
        require(seen <= max_rows, 'Limite memoria/filas excedido: aplicar estrategia por lotes')
        require(len(groups) <= MAX_PARTITIONS, 'Exceso de particiones')
    require(seen > 0, 'Silver candidato sin registros')
    return dict(sorted(groups.items())), seen


def iter_parquet_parts(df, partition_fields, pa, pq):
    """Genera bytes Parquet localmente, sin Cloud, rename ni overwrite."""
    schema = schema_for_arrow(df.schema, partition_fields, pa)
    groups, count = collect_groups(df, partition_fields)
    for partition, records in groups.items():
        for chunk_no in range(0, len(records), MAX_ROWS_PER_OBJECT):
            rows = records[chunk_no:chunk_no+MAX_ROWS_PER_OBJECT]
            table = pa.Table.from_pylist(rows, schema=schema)
            out = BytesIO()
            pq.write_table(table, out, compression='snappy')
            raw = out.getvalue()
            require(0 < len(raw) <= MAX_BYTES_PER_OBJECT, 'Parquet excede limite create-only')
            yield partition, chunk_no // MAX_ROWS_PER_OBJECT, raw, len(rows)


def create_only(client, uri, data):
    """I/O explícito, SOLO invocable tras autorización y claim real previos."""
    require(isinstance(uri, str) and uri.startswith(f'gs://{SILVER_BUCKET}/staging/')
            and uri.endswith('.snappy.parquet') and '..' not in uri and '//' not in uri[5:],
            'Destino create-only inválido')
    require(isinstance(data, bytes) and len(data) > 0, 'Parquet vacio')
    key = uri[len(f'gs://{SILVER_BUCKET}/'):]
    blob = client.bucket(SILVER_BUCKET).blob(key)
    blob.upload_from_file(BytesIO(data), rewind=True, size=len(data),
                          if_generation_match=0, retry=None,
                          checksum='crc32c', content_type='application/octet-stream')
    require(blob.generation is not None, 'GCS no devolvio generation')
    return {'uri': uri, 'generation': str(blob.generation),
            'sha256': sha256(data).hexdigest(), 'size_bytes': len(data)}


def stage_product_after_claim(client, *, verified_plan, claim_generation, dataset, product, df, partitions, pa, pq):
    """Primitiva para un lanzador firmado futuro; NO es un entrypoint CLI.

    No llamar sin `check_gcs_signed_bundle` + `consume_once` validados. Los
    parámetros `auth` por sí solos no demuestran firma; Quality debe revisar
    el runner antes de habilitar cualquier ejecución GCS.
    """
    import puerta_silver_operativa_v1_candidata as gate
    from fase6_restantes_core import validate_authorized_scope
    require(isinstance(verified_plan, gate.GCSVerifiedPlan),
            'Prohibido staging sin plan preflight real de GCS')
    require(type(claim_generation) is int and claim_generation > 0,
            'No se recibio generacion del claim exclusivo')
    auth = verified_plan.authorization
    expected = validate_authorized_scope(auth, dataset)
    require(product in SOURCES[dataset], 'Producto fuera del dataset')
    prefix = expected[product]
    expected_assertion = expected_prefix(product, auth['run_id'], auth['attempt_id'])
    require(prefix == expected_assertion, 'Destino distinto al firmado')
    files = []
    for values, index, raw, nrows in iter_parquet_parts(df, partitions, pa, pq):
        suffix = ''.join(f'{name}={value}/' for name, value in zip(partitions, values))
        uri = f'{prefix}{suffix}part-{index:05d}-{sha256(raw).hexdigest()[:16]}.snappy.parquet'
        receipt = create_only(client, uri, raw)
        receipt['rows'] = nrows
        files.append(receipt)
    require(bool(files), 'No se genero Parquet')
    return files
