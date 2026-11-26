#!/usr/bin/env python3
"""Preflight de lectura exclusivamente de Bronze oficial para Fase6 restante.

Biblioteca, SIN CLI de ejecucion. Quien llame debe obtener plan NUEVO mediante
`gate.check_gcs_signed_bundle(...)`, verificar el contexto Quality firmado del
codigo restante y consumir claim solo al lanzar trabajo real autorizado.
"""
from __future__ import annotations
from datetime import datetime, timezone
from hashlib import sha256

from fase6_restantes_core import (
    SOURCES, CANDIDATE_TRANSFORMS, BRONZE_BUCKET,
    ScopeError, validate_source_entries, validate_authorized_scope, require
)
from fase6_restantes_transform import load_bcrp_csv, load_json_records, spark_data_frame

MAX_TOTAL_BYTES = 256 * 1024 * 1024
MAX_OBJECT_BYTES = 32 * 1024 * 1024


def read_source_snapshot_after_signed_preflight(client, verified_plan, contract_bytes, dataset):
    """Lectura de generaciones exactas DESPUES de preflight firmado, sin claim.

    NO autoriza ni consume. Los resultados deben procesarse inmediatamente.
    """
    import puerta_silver_operativa_v1_candidata as gate
    import puerta_silver_v1_candidata as core
    import adaptador_gcs_silver_v1 as gcs
    require(isinstance(verified_plan, gate.GCSVerifiedPlan),
            'Plan offline o no emitido por puerta GCS')
    require(dataset in CANDIDATE_TRANSFORMS, 'Transformacion todavia no certificada')
    require((datetime.now(timezone.utc) - verified_plan.checked_at).total_seconds() <= 90,
            'Preflight GCS demasiado antiguo: nuevo intento supervisado')
    auth = verified_plan.authorization
    validate_authorized_scope(auth, dataset)
    contract = gate.check_base(contract_bytes)
    matches = [d for d in contract['datasets'] if d['dataset'] == dataset]
    require(len(matches) == 1, 'Dataset sin contrato Bronze oficial')
    raw_manifest, entries = gcs.read_official_manifest(client, matches[0], core.verify_manifest)
    validate_source_entries(dataset, entries)
    result = []
    total_bytes = 0
    for item in entries:
        size = item['size_bytes']
        require(size <= MAX_OBJECT_BYTES and total_bytes + size <= MAX_TOTAL_BYTES,
                'Volumen supera lectura controlada; se requiere otro lector aprobado')
        payload = gcs.read_exact_current_bytes(
            client, item['uri'], item['generation'], item['sha256'],
            expected_size=size, allowed_bucket=BRONZE_BUCKET)
        total_bytes += len(payload)
        result.append(payload)
    return result, {
        'dataset': dataset,
        'object_count': len(entries),
        'bytes_verified': total_bytes,
        'manifest_sha256': sha256(raw_manifest).hexdigest(),
        'objects': [{'uri': i['uri'], 'generation': str(i['generation']),
                     'sha256': i['sha256'], 'size_bytes': i['size_bytes']}
                    for i in entries]
    }


def build_source_dataframe(spark, dataset, pinned_raw):
    """Convierte bytes YA validados a DataFrame, sin rutas HDFS ni GCS."""
    require(dataset in CANDIDATE_TRANSFORMS, 'Dataset sin transformador candidato')
    if dataset == 'bcrp/embig':
        require(len(pinned_raw) == 1, 'EMBIG requiere un CSV oficial')
        rows = load_bcrp_csv(pinned_raw[0], 'PD04709XD')
    else:
        rows = load_json_records(pinned_raw,
                                 required_wrappers=('Resultado', 'data', 'resultado', 'Data'))
    # Datos JSON pueden combinar numeros/strings: los casts controlados los
    # ejecuta Spark. Se rechazan tipos anidados no modelados por el contrato.
    normalized = []
    for rec in rows:
        clean = {}
        for key, val in rec.items():
            require(isinstance(key, str) and key and isinstance(val, (str, int, float, bool, type(None))),
                    f'Campo JSON no escalar / clave invalida: {key!r}')
            clean[key] = None if val is None else str(val)
        normalized.append(clean)
    from pyspark.sql.types import StructType, StructField, StringType
    columns = sorted({k for row in normalized for k in row})
    require(0 < len(columns) <= 300, 'Cantidad de columnas fuera de limite')
    # Esquema explicito StringType permite nulos, evita inferencias ambiguas.
    schema = StructType([StructField(name, StringType(), True) for name in columns])
    return spark.createDataFrame([tuple(r.get(k) for k in columns) for r in normalized], schema)
