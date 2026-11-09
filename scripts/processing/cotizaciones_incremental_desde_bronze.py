#!/usr/bin/env python3
"""Prepara SOLO particiones Silver afectadas por cotizaciones nuevas o corregidas.

No modifica Silver vigente. Las entradas pueden ser JSON Bronze oficiales o Parquet Silver
(normalizado) para reproducir pruebas historicas con datos reales. Genera staging, auditoria
y manifest para publicacion manual controlada.
"""
import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

KEYS = ["fecha_cotizacion", "valor"]
PARTS = ["anio", "mes"]
DEFAULT_CURRENT = "/datalake/bvl/silver/mercado_diario/cotizaciones"
STAGE_ROOT = "/datalake/bvl/staging/cotizaciones_incremental"


def hdfs(*args, check=True):
    p = subprocess.run(["hdfs", "dfs", *map(str, args)], text=True, capture_output=True)
    if check and p.returncode != 0:
        raise RuntimeError(f"hdfs dfs {' '.join(map(str, args))}: {p.stderr.strip()}")
    return p


def fingerprint(path):
    """Detecta cambios entre preparacion y publicacion por listado de archivos/bytes/mtime."""
    if hdfs("-test", "-e", path, check=False).returncode != 0:
        return None
    raw = hdfs("-ls", "-R", path).stdout
    lines = []
    for line in raw.splitlines():
        fields = line.split()
        if len(fields) >= 8 and fields[0].startswith("-"):
            lines.append("|".join([fields[4], fields[5], fields[6], fields[-1]]))
    return hashlib.sha256("\n".join(sorted(lines)).encode("utf-8")).hexdigest()


def load_bronze(spark, paths, when):
    wrappers = spark.read.option("multiLine", "true").json(paths)
    if "Resultado" not in wrappers.columns:
        raise RuntimeError("Los JSON nuevos deben contener el array 'Resultado'")
    raw = wrappers.select(F.explode(F.col("Resultado")).alias("row")).select("row.*")
    expected = ["FechaCotizacion", "Valor", "Descripcion", "FechaAnterior",
                "Cierre_Anterior", "Cierre_Actual", "Apertura_Actual", "Maxima_Actual",
                "Minima_Actual", "Promedio_Actual", "Monto_Negociado", "FechaRegistro"]
    missing = [c for c in expected if c not in raw.columns]
    if missing:
        raise RuntimeError(f"Bronze: faltan columnas {missing}")
    df = (raw.select(
        F.col("FechaCotizacion").alias("fecha_cotizacion_raw"),
        F.col("Valor").alias("valor"),
        F.col("Descripcion").alias("descripcion"),
        F.col("FechaAnterior").alias("fecha_anterior_raw"),
        F.col("Cierre_Anterior").cast("double").alias("cierre_anterior"),
        F.col("Cierre_Actual").cast("double").alias("cierre_actual"),
        F.col("Apertura_Actual").cast("double").alias("apertura_actual"),
        F.col("Maxima_Actual").cast("double").alias("maxima_actual"),
        F.col("Minima_Actual").cast("double").alias("minima_actual"),
        F.col("Promedio_Actual").cast("double").alias("promedio_actual"),
        F.col("Monto_Negociado").cast("double").alias("monto_negociado"),
        F.col("FechaRegistro").alias("fecha_registro_raw")
    ).withColumn("fecha_cotizacion", F.to_date(F.substring("fecha_cotizacion_raw", 1, 10), "dd/MM/yyyy"))
     .withColumn("fecha_anterior", F.to_date(F.substring("fecha_anterior_raw", 1, 10), "dd/MM/yyyy"))
     .withColumn("valor", F.trim("valor"))
     .withColumn("descripcion", F.trim("descripcion"))
     .withColumn("anio", F.year("fecha_cotizacion"))
     .withColumn("mes", F.month("fecha_cotizacion"))
     .withColumn("fuente", F.lit("SMV"))
     .withColumn("fecha_ingesta", F.to_date(F.lit(when))))
    return df


def count_duplicate_keys(df):
    return df.groupBy(*KEYS).count().filter(F.col("count") > 1).limit(1).count()


def validate_keys(df, label):
    invalid = df.filter(F.col(KEYS[0]).isNull() | F.col(KEYS[1]).isNull() |
                        (F.trim(F.col(KEYS[1])) == "")).limit(1).count()
    if invalid:
        raise RuntimeError(f"{label}: clave nula/vacia. Revisar quarantine antes de seguir")


def payload_cols(df):
    # fecha_ingesta es solo linaje: no representa un cambio bursatil.
    return [c for c in df.columns if c not in KEYS + ["fecha_ingesta"]]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    source = ap.add_mutually_exclusive_group(required=True)
    source.add_argument("--bronze", nargs="+", help="Archivo(s), directorio(s) o glob(s) JSON de Bronze HDFS")
    source.add_argument("--incoming-parquet", help="Lote ya normalizado, para pruebas y reutilizacion")
    ap.add_argument("--ingestion-date", help="Fecha YYYY-MM-DD requerida con --bronze")
    ap.add_argument("--current", default=DEFAULT_CURRENT)
    ap.add_argument("--stage-root", default=STAGE_ROOT)
    ap.add_argument("--allow-corrections", action="store_true",
                    help="Autorizar reemplazo de registros con igual fecha+valor, con auditoria")
    ap.add_argument("--audit-dir", default="outputs/quality/incremental")
    args = ap.parse_args()
    if args.bronze and not args.ingestion_date:
        ap.error("Con --bronze debes indicar --ingestion-date YYYY-MM-DD")
    if args.ingestion_date:
        datetime.strptime(args.ingestion_date, "%Y-%m-%d")
    current = args.current.rstrip("/")
    stage_root = args.stage_root.rstrip("/")
    if current == stage_root or current.startswith(stage_root + "/") or stage_root.startswith(current + "/"):
        raise SystemExit("ERROR: la ruta staging no puede solaparse con Silver vigente")
    if not current.startswith("/datalake/bvl/") or not stage_root.startswith("/datalake/bvl/staging/"):
        raise SystemExit("ERROR: las rutas deben pertenecer a /datalake/bvl/ y staging controlado")
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex[:8]
    spark = (SparkSession.builder.appName("BVL-Cotizaciones-Stage-Meses-Afectados")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.parquet.compression.codec", "snappy")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        old = spark.read.parquet(current)
        if args.bronze:
            new = load_bronze(spark, args.bronze, args.ingestion_date)
        else:
            new = spark.read.parquet(args.incoming_parquet)
        old_schema = {f.name: f.dataType.simpleString() for f in old.schema.fields}
        new_schema = {f.name: f.dataType.simpleString() for f in new.schema.fields}
        if old_schema != new_schema:
            raise RuntimeError(f"Los esquemas no coinciden. OLD={old_schema} NEW={new_schema}")
        for name, df in [("Silver actual", old), ("Nuevo lote", new)]:
            validate_keys(df, name)
        if count_duplicate_keys(old):
            raise RuntimeError("Silver actual ya contiene claves duplicadas")
        raw_new_count = new.count()
        if raw_new_count == 0:
            raise RuntimeError("Lote vacio: no se realizo ninguna modificacion")
        # Conflicto en un mismo lote: si dos filas de la misma fecha/valor discrepan,
        # no elegir arbitrariamente una version.
        payload = payload_cols(new)
        hashed = new.withColumn("_payload", F.sha2(F.to_json(F.struct(*[F.col(c) for c in payload]),
                              {"ignoreNullFields": "false"}), 256))
        conflicts = (hashed.groupBy(*KEYS).agg(F.countDistinct("_payload").alias("n"))
                     .filter("n > 1").limit(1).count())
        if conflicts:
            raise RuntimeError("Lote con versiones conflictivas por fecha+valor; se requiere conciliacion")
        clean = new.dropDuplicates(KEYS)
        deduped = clean.count()
        existing_keys = old.select(*KEYS)
        added = clean.join(existing_keys, KEYS, "left_anti")
        new_count = added.count()
        common = clean.alias("n").join(old.alias("o"), KEYS, "inner")
        diff = reduce(lambda x, y: x | y,
                      [~F.col(f"n.`{c}`").eqNullSafe(F.col(f"o.`{c}`")) for c in payload], F.lit(False))
        modified_keys = common.filter(diff).select(*KEYS).distinct()
        modified_count = modified_keys.count()
        same_count = deduped - new_count - modified_count
        report = {"run": run, "fecha_utc": datetime.now(timezone.utc).isoformat(),
                  "current": current, "fuente": args.bronze or args.incoming_parquet,
                  "filas_lote": raw_new_count, "filas_lote_unicas": deduped,
                  "repetidas_lote": raw_new_count - deduped,
                  "nuevas": new_count, "sin_cambio": same_count,
                  "corregidas": modified_count, "silver_antes": old.count(),
                  "politica_correcciones": "ACEPTADAS" if args.allow_corrections else "BLOQUEADAS"}
        output_dir = Path(args.audit_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        audit_path = output_dir / f"cotizaciones_{run}.json"
        def save_audit():
            audit_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print("\n========== AUDITORIA INCREMENTAL COTIZACIONES ==========")
        for k in ("silver_antes", "filas_lote", "repetidas_lote", "nuevas", "sin_cambio", "corregidas"):
            print(f"{k}: {report[k]}")
        if modified_count and not args.allow_corrections:
            report["estado"] = "BLOQUEADO_CORRECCIONES"
            save_audit()
            print(f"AUDITORIA|{audit_path}")
            raise RuntimeError("Se encontraron correcciones: auditar y repetir con --allow-corrections si procede")
        if new_count + modified_count == 0:
            report["estado"] = "SIN_CAMBIOS"
            report["silver_despues_esperado"] = report["silver_antes"]
            save_audit()
            print("SIN_CAMBIOS|0 particiones modificadas; Silver vigente intacto")
            print(f"AUDITORIA|{audit_path}")
            return
        changed_keys = added.select(*KEYS).unionByName(modified_keys).distinct()
        updates = clean.join(changed_keys, KEYS, "inner")
        partitions = sorted([(int(r["anio"]), int(r["mes"])) for r in
                             updates.select(*PARTS).distinct().collect()])
        if not partitions or any(y < 1900 or m < 1 or m > 12 for y, m in partitions):
            raise RuntimeError("Fechas invalidas: no es posible determinar particiones")
        stage = f"{stage_root}/run_{run}"
        if hdfs("-test", "-e", stage, check=False).returncode == 0:
            raise RuntimeError("La ruta staging ya existe")
        report.update(stage=stage, estado="PREPARANDO", particiones=[])
        for y, m in partitions:
            active_part = f"{current}/anio={y}/mes={m}"
            stage_part = f"{stage}/anio={y}/mes={m}"
            before_fingerprint = fingerprint(active_part)
            historical = old.filter((F.col("anio") == y) & (F.col("mes") == m))
            incoming_part = updates.filter((F.col("anio") == y) & (F.col("mes") == m))
            historical_untouched = historical.join(changed_keys, KEYS, "left_anti")
            final_part = historical_untouched.unionByName(incoming_part)
            expected_count = final_part.count()
            expected_keys = final_part.select(*KEYS).distinct().count()
            if expected_count != expected_keys:
                raise RuntimeError(f"Particion {y}/{m} contiene claves duplicadas")
            # Leer de Silver actual, escribir en otra ruta independiente: no hay overwrite sobre input.
            (final_part.drop(*PARTS).write.mode("errorifexists")
             .option("compression", "snappy").parquet(stage_part))
            persisted = spark.read.parquet(stage_part)
            got = persisted.count()
            got_keys = persisted.select(*KEYS).distinct().count()
            if got != expected_count or got_keys != expected_count:
                raise RuntimeError(f"Staging invalido {y}/{m}: {got}/{got_keys}, esperado {expected_count}")
            if fingerprint(active_part) != before_fingerprint:
                raise RuntimeError(f"Particion Silver {y}/{m} cambio durante el stage; abortar y reconstruir")
            report["particiones"].append({"anio": y, "mes": m,
                "destino": active_part, "staging": stage_part,
                "fingerprint_original": before_fingerprint,
                "fingerprint_staging": fingerprint(stage_part), "filas_staging": got})
            print(f"PARTICION_STAGE|{y}|{m}|{got}|{stage_part}")
        report["silver_despues_esperado"] = report["silver_antes"] + new_count
        report["estado"] = "STAGING_VALIDADO_NO_PUBLICADO"
        save_audit()
        print(f"STAGING_VALIDADO|{stage}")
        print(f"MANIFEST|{audit_path}")
        print(f"PARTICIONES_AFECTADAS|{len(partitions)}")
        print("SILVER_OFICIAL|SIN_CAMBIOS")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
