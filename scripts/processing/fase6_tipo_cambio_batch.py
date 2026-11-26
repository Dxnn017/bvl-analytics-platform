#!/usr/bin/env python3
"""BVL Fase 6. Primer batch supervisado, sin ejecucion por defecto.

NO ejecutar mientras Quality no haya ratificado este escritor y lanzamiento.
La autorizacion es externa, firmada y temporal. Nunca genera ni renueva auth.
El modo --execute crea claim y staging; no dispone de retry, update ni delete.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import sys
import time

# Debian: scripts/pipeline y processing en el arbol del repo. En Dataproc:
# proporcionar las dependencias ya aplanadas mediante --py-files (.zip).
root = Path(__file__).resolve().parents[2]
for directory in (root / "scripts" / "pipeline", root / "scripts" / "processing"):
    if directory.is_dir():
        sys.path.insert(0, str(directory))

import fase6_tipo_cambio_seguro as piloto

GATE_COMMIT_APROBADO = "41266f0efba4477c30266e19ca3280838ff3d802"


def parser():
    ap = argparse.ArgumentParser(description="BVL Fase 6: batch supervised Tipo Cambio")
    ap.add_argument("--execute", action="store_true", help="Requiere autorizacion vigente y Quality")
    ap.add_argument("--ack-supervised", action="store_true", help="Confirmacion operativa; NO reemplaza firma")
    ap.add_argument("--contract", type=Path)
    ap.add_argument("--quality-public-pem", type=Path)
    ap.add_argument("--trusted-refs", type=Path, help="Referencias independientes entregadas por Quality")
    ap.add_argument("--processor-commit", help="Nuevo commit del procesador F6 ratificado por Quality")
    return ap


def main():
    args = parser().parse_args()
    if not args.execute:
        print("ESTADO: BLOQUEADO_POR_DEFECTO")
        print("MODO: INSPECCION_LOCAL_SIN_GCS_SIN_SPARK")
        print("BRONZE:", piloto.BRONZE_DATASET)
        print("SILVER:", piloto.SILVER_PRODUCT)
        print("NO SE CONSUMIO CLAIM NI SE ESCRIBIO GCS")
        return
    if not args.ack_supervised:
        raise SystemExit("BLOQUEADO: falta confirmacion de lanzamiento supervisado")
    if not all((args.contract, args.quality_public_pem, args.trusted_refs)):
        raise SystemExit("BLOQUEADO: faltan referencias y contrato Quality")
    if (not isinstance(args.processor_commit, str) or len(args.processor_commit) != 40 or
            any(c not in "0123456789abcdef" for c in args.processor_commit) or
            args.processor_commit == GATE_COMMIT_APROBADO):
        raise SystemExit("BLOQUEADO: requiere NUEVO commit Fase 6 revisado por Quality; 41266f0 solo cubre puerta")

    # No importar librerias GCS / Spark hasta entrar en ejecucion explicita.
    from google.cloud import storage
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType
    import adaptador_gcs_silver_v1 as gcs
    import puerta_silver_operativa_v1_candidata as gate
    import puerta_silver_v1_candidata as core
    from bronze_to_silver_dataproc_candidato import transform_tipo_cambio_bcrp

    contract_raw = args.contract.read_bytes()
    pem = args.quality_public_pem.read_bytes()
    refs = json.loads(args.trusted_refs.read_text(encoding="utf-8"))
    client = storage.Client(project="bvl-analytics-2026")

    # Comprobacion de firmas, generaciones, hashes, identidad efectiva y
    # archivos Bronze oficiales. Solo lectura y SIN reclamo.
    plan = gate.check_gcs_signed_bundle(client, refs, pem, contract_raw)
    auth = plan.authorization
    prefix = piloto.stage_prefix_from_authorization(auth)
    contract = gate.check_base(contract_raw)
    ds = next(d for d in contract["datasets"] if d["dataset"] == piloto.BRONZE_DATASET)
    _, manifest_files = gcs.read_official_manifest(client, ds, core.verify_manifest)
    piloto.require(len(manifest_files) == 1, "Piloto BCRP requiere un archivo Bronze oficial")
    source = manifest_files[0]
    piloto.require(source["size_bytes"] <= piloto.MAX_CSV_BYTES,
                   "Entrada supera limite de memoria del piloto")
    raw = gcs.read_exact_current_bytes(
        client, source["uri"], source["generation"], source["sha256"],
        expected_size=source["size_bytes"], allowed_bucket=gcs.BRONZE_BUCKET,
    )
    header, rows = piloto.parse_bcrp_csv_pinned(raw)
    piloto.require((datetime.now(timezone.utc) - plan.checked_at).total_seconds() <= 100,
                   "Preflight demasiado antiguo antes del claim; NO se consume")

    # Exito significa que fue posible crear en Control el claim unico.
    # Desde aqui: NUNCA reintentar automaticamente si falla Spark/GCS.
    store = gcs.GCSExclusiveClaimStore(client)
    claim_generation = gate.consume_once(plan, store)
    print("CLAIM_CONSUMIDO_GENERACION:", claim_generation, flush=True)
    start = time.monotonic()
    spark = None
    try:
        spark = SparkSession.builder.appName("BVL-F6-TipoCambio-Autorizado").getOrCreate()
        schema = StructType([StructField(c, StringType(), True) for c in header])
        data = spark.sparkContext.parallelize(rows, min(2, max(1, len(rows))))
        df = spark.createDataFrame(data, schema=schema)
        final, metrics = transform_tipo_cambio_bcrp(df)
        collected = []
        # Limite explicito: SOLO para el primer archivo BCRP (~miles de filas).
        for record in final.select("fecha", "tipo_cambio_venta", "anio", "mes").toLocalIterator():
            collected.append(record.asDict())
            piloto.require(len(collected) <= piloto.MAX_OUTPUT_ROWS,
                           "Datos Silver exceden limite seguro de memoria")
        piloto.require(len(collected) == metrics["silver_rows"],
                       "Conteo recolectado distinto de conteo de calidad")
        groups = piloto.partition_records(collected)
        files = piloto.upload_partitioned_candidate(client, prefix, groups, pa, pq)
        metrics["processing_seconds"] = round(time.monotonic() - start, 3)
        candidate = piloto.encode_candidate_manifest(
            auth["run_id"], auth["attempt_id"], plan.fingerprint,
            {"uri": source["uri"], "generation": str(source["generation"]),
             "sha256": source["sha256"], "size_bytes": source["size_bytes"],
             "manifest_uri": ds["bronze_manifest_uri"],
             "manifest_generation": str(ds["bronze_manifest_generation"])},
            files, metrics, args.processor_commit,
        )
        report = piloto.upload_create_only(
            client, prefix + "_quality/manifest_candidate.json", candidate,
            "application/json")
        print(json.dumps({
            "status": "STAGING_CANDIDATO_PENDIENTE_QUALITY",
            "run_id": auth["run_id"],
            "attempt_id": auth["attempt_id"],
            "claim_generation": str(claim_generation),
            "rows": metrics["silver_rows"],
            "files_count": len(files),
            "candidate_manifest_uri": report["uri"],
            "candidate_manifest_generation": report["generation"],
            "quality_approved": False,
            "official_silver_published": False,
        }, sort_keys=True), flush=True)
    except Exception:
        print("ERROR_TRAS_CLAIM: preservar staging, conciliar con Quality; NO reintentar", flush=True)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
