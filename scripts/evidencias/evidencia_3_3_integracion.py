from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

spark = (
    SparkSession.builder
    .appName("Evidencia_3_3_Integracion")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

ruta = "/datalake/bvl/silver/referencias_smv/puente_ruc_nemonico"

df = spark.read.parquet(ruta)

print()
print("=" * 90)
print("3.3 - VALIDACION DEL PUENTE EMPRESARIAL RUC - NEMONICO")
print("=" * 90)

total = df.count()

ruc_distintos = df.select("ruc").distinct().count()

nemonicos = df.select("nemonico_valor").distinct().count()

pares = (
    df.select(
        "ruc",
        "nemonico_valor"
    )
    .distinct()
    .count()
)

duplicados = (
    df.groupBy(
        "ruc",
        "nemonico_valor"
    )
    .count()
    .filter(col("count") > 1)
    .count()
)

nemonicos_multi_ruc = (
    df.groupBy("nemonico_valor")
    .agg(
        countDistinct("ruc").alias("cantidad_ruc")
    )
    .filter(col("cantidad_ruc") > 1)
    .count()
)

ruc_nulos = df.filter(
    col("ruc").isNull()
    | (col("ruc") == "")
).count()

nem_nulos = df.filter(
    col("nemonico_valor").isNull()
    | (col("nemonico_valor") == "")
).count()

print()
print("REGISTROS")
print("-" * 60)
print(f"Registros del puente             : {total:,}")
print(f"RUC distintos                    : {ruc_distintos:,}")
print(f"Nemonicos distintos              : {nemonicos:,}")
print(f"Pares RUC + nemonico distintos   : {pares:,}")

print()
print("VALIDACION DE UNICIDAD")
print("-" * 60)
print(f"Pares duplicados                  : {duplicados:,}")
print(f"Nemonicos asociados a >1 RUC     : {nemonicos_multi_ruc:,}")

print()
print("VALIDACION DE COMPLETITUD")
print("-" * 60)
print(f"RUC nulos/vacios                  : {ruc_nulos:,}")
print(f"Nemonicos nulos/vacios            : {nem_nulos:,}")

print()
print("MUESTRA DEL PUENTE")
print("-" * 60)

columnas_muestra = [
    c for c in [
        "ruc",
        "razon_social",
        "nemonico_valor",
        "codigo_isin",
        "tipo_valor"
    ]
    if c in df.columns
]

df.select(*columnas_muestra).show(10, False)

print()
print("=" * 90)

if (
    duplicados == 0
    and nemonicos_multi_ruc == 0
    and ruc_nulos == 0
    and nem_nulos == 0
):
    print("RESULTADO: PUENTE RUC-NEMONICO VALIDADO CORRECTAMENTE")
else:
    print("RESULTADO: REVISAR VALIDACIONES DEL PUENTE")

print("=" * 90)

spark.stop()
