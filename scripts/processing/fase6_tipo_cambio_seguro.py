#!/usr/bin/env python3
"""Utilidades puras y escritor GCS create-only para piloto BVL Fase 6.

NO valida autorizaciones por si mismo. Solo debe invocarse desde el batch
supervisado DESPUES de check_gcs_signed_bundle + consume_once de Quality.
"""
from __future__ import annotations

import csv
from datetime import date
from hashlib import sha256
from io import BytesIO, StringIO
import json
import re

BRONZE_DATASET = "bcrp/tipo_cambio"
SILVER_PRODUCT = "macroeconomia/tipo_cambio"
SILVER_BUCKET = "bvl26-silver-7f3a9c"
MAX_CSV_BYTES = 8 * 1024 * 1024
MAX_CSV_ROWS = 30000
MAX_OUTPUT_ROWS = 30000
MAX_PARTITIONS = 480

class PilotoError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise PilotoError(message)


def parse_bcrp_csv_pinned(raw: bytes):
    """Devuelve encabezados y filas fieles al CSV fijado, limitado por tamaño.

    Usa csv.reader (permite comillas y saltos de linea embebidos) y no lista
    prefijos de GCS. Las entradas llegan desde una lectura por generacion/hash.
    """
    require(isinstance(raw, bytes) and 0 < len(raw) <= MAX_CSV_BYTES,
            "CSV BCRP vacio o supera limite seguro del piloto")
    try:
        decoded = raw.decode("utf-8-sig", errors="strict")
        reader = csv.reader(StringIO(decoded, newline=""), strict=True)
        header = next(reader)
        # Spark CSV asigna _c0 al primer encabezado sin nombre. El lector
        # fijo conserva esa semantica para no rechazar el CSV BCRP original.
        if header and header[0].strip() == "":
            header[0] = "_c0"
        require(header and len(set(header)) == len(header),
                "Cabecera vacia o columnas duplicadas")
        require({"_c0", "PD04638PD"}.issubset(set(header)),
                "El CSV no contiene las columnas BCRP ratificadas")
        rows = []
        for row in reader:
            require(len(row) == len(header), "Fila CSV BCRP con numero de campos distinto")
            rows.append(row)
            require(len(rows) <= MAX_CSV_ROWS,
                    "CSV BCRP excede limite de filas del piloto")
        require(bool(rows), "CSV BCRP sin filas de datos")
        return header, rows
    except (UnicodeError, csv.Error, StopIteration) as exc:
        raise PilotoError(f"No se pudo interpretar CSV BCRP: {exc}") from exc


def stage_prefix_from_authorization(auth: dict):
    require(isinstance(auth, dict), "Autorizacion no es objeto")
    require(auth.get("mode") == "SINGLE_SOURCE" and
            auth.get("dataset") == BRONZE_DATASET and
            auth.get("operation") == "BUILD_SILVER_STAGING",
            "Autorizacion no corresponde a tipo de cambio")
    outs = auth.get("silver_outputs")
    require(isinstance(outs, list) and len(outs) == 1 and
            outs[0].get("name") == SILVER_PRODUCT,
            "Salida Silver fuera de alcance")
    expected = (f"gs://{SILVER_BUCKET}/staging/{SILVER_PRODUCT}/"
                f"run_id={auth['run_id']}/attempt_id={auth['attempt_id']}/")
    require(outs[0].get("staging_prefix") == expected,
            "Staging fuera de ruta firmada")
    return expected


def partition_records(rows):
    """Agrupa SOLO las columnas de datos; anio/mes van en la ruta Hive."""
    groups = {}
    require(len(rows) <= MAX_OUTPUT_ROWS, "Silver excede limite del piloto")
    for rec in rows:
        fecha = rec["fecha"]
        anio, mes = rec["anio"], rec["mes"]
        require(isinstance(fecha, date) and type(anio) is int and
                type(mes) is int and fecha.year == anio and fecha.month == mes,
                "Particion incompatible con fecha")
        val = rec["tipo_cambio_venta"]
        require(val is None or (isinstance(val, (int, float)) and
                not isinstance(val, bool) and float("-inf") < float(val) < float("inf")),
                "Valor numerico no finito")
        groups.setdefault((anio, mes), []).append({
            "fecha": fecha,
            "tipo_cambio_venta": None if val is None else float(val),
        })
    require(0 < len(groups) <= MAX_PARTITIONS, "Numero de particiones fuera de rango")
    for group in groups.values():
        group.sort(key=lambda item: item["fecha"])
    return dict(sorted(groups.items()))


def parquet_partition_bytes(records, pa, pq):
    """Parquet local en memoria; sin committer Hadoop, rename ni delete GCS."""
    schema = pa.schema([("fecha", pa.date32()),
                        ("tipo_cambio_venta", pa.float64())])
    table = pa.Table.from_pylist(records, schema=schema)
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()


def upload_create_only(client, uri: str, content: bytes, mime: str):
    require(isinstance(uri, str) and uri.startswith(f"gs://{SILVER_BUCKET}/staging/"),
            "Publicacion fuera de staging Silver")
    require(isinstance(content, bytes) and content, "Contenido vacio")
    key = uri[len(f"gs://{SILVER_BUCKET}/"):]
    require("//" not in key and ".." not in key and not key.endswith("/"),
            "Ruta GCS no canonical")
    blob = client.bucket(SILVER_BUCKET).blob(key)
    # Exclusividad de objeto; retry desactivado expresamente.
    blob.upload_from_file(BytesIO(content), size=len(content), content_type=mime,
                          rewind=True, if_generation_match=0, retry=None,
                          checksum="crc32c")
    require(blob.generation is not None, "No se recibio generacion GCS")
    return {
        "uri": uri,
        "generation": str(blob.generation),
        "sha256": sha256(content).hexdigest(),
        "size_bytes": len(content),
    }


def upload_partitioned_candidate(client, prefix: str, groups: dict, pa, pq):
    require(isinstance(prefix, str) and
            prefix.startswith(f"gs://{SILVER_BUCKET}/staging/{SILVER_PRODUCT}/") and
            prefix.endswith("/"), "Prefijo Silver no reconocido")
    output = []
    for (year, month), records in groups.items():
        require(1990 <= year <= 2100 and 1 <= month <= 12,
                "Particion fuera del intervalo BCRP")
        raw = parquet_partition_bytes(records, pa, pq)
        uri = (f"{prefix}anio={year}/mes={month}/"
               f"part-00000-{sha256(raw).hexdigest()[:16]}.snappy.parquet")
        part = upload_create_only(client, uri, raw, "application/octet-stream")
        part["rows"] = len(records)
        output.append(part)
    return output


def encode_candidate_manifest(run_id, attempt_id, fingerprint, source, output, metrics, commit):
    require(isinstance(output, list) and output,
            "No hay archivos Silver staging")
    doc = {
        "schema_version": "BVL-SILVER-STAGING-CANDIDATE-v1",
        "status": "CANDIDATO_PENDIENTE_QUALITY",
        "quality_approved": False,
        "official_publication_allowed": False,
        "run_id": run_id,
        "attempt_id": attempt_id,
        "fingerprint": fingerprint,
        "dataset": BRONZE_DATASET,
        "product": SILVER_PRODUCT,
        "processor_commit": commit,
        "source_bronze": source,
        "records": metrics,
        "files_count": len(output),
        "files": output,
        "bytes_total": sum(f["size_bytes"] for f in output),
    }
    return json.dumps(doc, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")
