from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = (
    "/datalake/bvl/silver/"
    "finanzas_empresariales/principales_cuentas"
)

OUT = Path(
    "/media/sf_bvl_shared/"
    "puentes_smv/rucs_principales_cuentas.csv"
)

spark = (
    SparkSession.builder
    .appName("BVL-Universo-RUC-SMV")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(HDFS)

rucs = (
    df
    .filter(F.col("ruc_valido") == True)
    .select(
        "ruc",
        "nombre_empresa",
        "rpj",
        "tipo_empresa",
        "tipo_sector"
    )
    .groupBy("ruc")
    .agg(
        F.first("nombre_empresa", ignorenulls=True)
            .alias("nombre_empresa"),
        F.first("rpj", ignorenulls=True)
            .alias("rpj"),
        F.first("tipo_empresa", ignorenulls=True)
            .alias("tipo_empresa"),
        F.first("tipo_sector", ignorenulls=True)
            .alias("tipo_sector")
    )
    .orderBy("ruc")
)

total = rucs.count()

print("=" * 70)
print("UNIVERSO DE RUC - PRINCIPALES CUENTAS SMV")
print("=" * 70)
print("RUC distintos:", total)

if total != 289:
    spark.stop()
    raise RuntimeError(
        f"Se esperaban 289 RUC y se obtuvieron {total}"
    )

OUT.parent.mkdir(
    parents=True,
    exist_ok=True
)

rows = rucs.collect()

with open(
    OUT,
    "w",
    encoding="utf-8-sig"
) as f:

    f.write(
        "ruc,nombre_empresa,rpj,tipo_empresa,tipo_sector\n"
    )

    import csv
    writer = csv.writer(f)

    for r in rows:
        writer.writerow([
            r["ruc"],
            r["nombre_empresa"],
            r["rpj"],
            r["tipo_empresa"],
            r["tipo_sector"]
        ])

print("Archivo generado:")
print(OUT)

print("\nPrimeros registros:")
rucs.show(10, truncate=False)

spark.stop()
