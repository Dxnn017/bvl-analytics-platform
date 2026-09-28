#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path
import json
import time


# ============================================================
# CONFIGURACIÓN
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "quality"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATASETS = [
    {
        "name": "cotizaciones",
        "source": "SMV",
        "format": "json",
        "record_field": "Resultado",
        "path": "/datalake/bvl/bronze/smv/cotizaciones",
    },
    {
        "name": "indices",
        "source": "SMV",
        "format": "json",
        "record_field": "Resultado",
        "path": "/datalake/bvl/bronze/smv/indices",
    },
    {
        "name": "estado_resultados",
        "source": "SMV",
        "format": "json",
        "record_field": "data",
        "path": "/datalake/bvl/bronze/smv/estado_resultados",
    },
    {
        "name": "cambios_patrimonio",
        "source": "SMV",
        "format": "json",
        "record_field": "data",
        "path": "/datalake/bvl/bronze/smv/cambios_patrimonio",
    },
    {
        "name": "situacion_financiera",
        "source": "SMV",
        "format": "json",
        "record_field": "data",
        "path": "/datalake/bvl/bronze/smv/situacion_financiera",
    },
    {
        "name": "flujo_efectivo",
        "source": "SMV",
        "format": "json",
        "record_field": "data",
        "path": "/datalake/bvl/bronze/smv/flujo_efectivo",
    },
    {
        "name": "tipo_cambio",
        "source": "BCRP",
        "format": "csv",
        "path": "/datalake/bvl/bronze/bcrp/tipo_cambio",
    },
    {
        "name": "embig",
        "source": "BCRP",
        "format": "csv",
        "path": "/datalake/bvl/bronze/bcrp/embig",
    },
]

# ============================================================
# LECTURA
# ============================================================

def read_dataset(spark, config):

    if config["format"] == "json":

        raw = (
            spark.read
            .option("multiLine", "true")
            .option("recursiveFileLookup", "true")
            .json(config["path"])
        )

        record_field = config["record_field"]

        return (
            raw
            .select(
                F.explode(
                    F.col(record_field)
                ).alias("record")
            )
            .select("record.*")
        )

    if config["format"] == "csv":

        return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("recursiveFileLookup", "true")
            .csv(config["path"])
        )

    raise ValueError(
        f"Formato no soportado: {config['format']}"
    )

# ============================================================
# PERFILADO
# ============================================================

def profile_dataset(spark, config):

    print()
    print("=" * 75)
    print(f"DATASET : {config['name']}")
    print(f"FUENTE  : {config['source']}")
    print(f"FORMATO : {config['format'].upper()}")
    print("=" * 75)

    start = time.time()

    df = read_dataset(spark, config)

    total_rows = df.count()
    total_columns = len(df.columns)

    duplicate_rows = None

    print(f"Registros   : {total_rows:,}")
    print(f"Columnas    : {total_columns}")
    print(f"Duplicados  : se evaluaran en Silver")

    print()
    print("SCHEMA:")
    df.printSchema()

    nulls = {}

    if df.columns:

        expressions = []

        for column in df.columns:

            expressions.append(
                F.sum(
                    F.when(
                        F.col(column).isNull(),
                        1
                    ).otherwise(0)
                ).alias(column)
            )

        null_row = df.select(expressions).first()

        if null_row:
            nulls = null_row.asDict()

    print()
    print("NULOS:")

    for column, value in nulls.items():
        print(f"  {column:<30} {value}")

    print()
    print("MUESTRA:")

    df.show(
        3,
        truncate=False,
        vertical=False
    )

    elapsed = round(time.time() - start, 3)

    print(f"Tiempo perfilado: {elapsed} s")

    profile = {
        "dataset": config["name"],
        "source": config["source"],
        "format": config["format"],
        "path": config["path"],
        "records": total_rows,
        "columns_count": total_columns,
        "columns": df.columns,
        "schema": {
            field.name: field.dataType.simpleString()
            for field in df.schema.fields
        },
        "nulls": nulls,
        "duplicate_rows": duplicate_rows,
        "profiling_seconds": elapsed,
    }

    return profile


# ============================================================
# MAIN
# ============================================================

def main():

    spark = (
        SparkSession.builder
        .appName("BVL-Bronze-Profiling")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 75)
    print(" BVL ANALYTICS - PERFILADO DE ZONA BRONZE")
    print("=" * 75)

    profiles = []

    for config in DATASETS:

        try:
            profile = profile_dataset(
                spark,
                config
            )

            profiles.append(profile)

        except Exception as exc:

            print()
            print(
                f"[ERROR] {config['name']}: {exc}"
            )

            profiles.append({
                "dataset": config["name"],
                "source": config["source"],
                "status": "ERROR",
                "error": str(exc),
            })

    output_file = (
        OUTPUT_DIR /
        "bronze_profile_summary.json"
    )

    with open(
        output_file,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            profiles,
            file,
            ensure_ascii=False,
            indent=2
        )

    print()
    print("=" * 75)
    print(" PERFILADO TERMINADO")
    print("=" * 75)
    print(f"Reporte: {output_file}")

    spark.stop()


if __name__ == "__main__":
    main()


