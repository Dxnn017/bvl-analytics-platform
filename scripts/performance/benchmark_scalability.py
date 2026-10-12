import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

if len(sys.argv) != 2:
    print("Uso: benchmark_scalability.py FACTOR")
    sys.exit(1)

FACTOR = int(sys.argv[1])

if FACTOR not in (1, 10, 100):
    raise ValueError("El factor debe ser 1, 10 o 100")

COT = "/datalake/bvl/silver/mercado_diario/cotizaciones"
TC = "/datalake/bvl/silver/macroeconomia/tipo_cambio"
EMBIG = "/datalake/bvl/silver/macroeconomia/embig"

OUT = f"/tmp/bvl_scalability/factor_{FACTOR}"

spark = (
    SparkSession.builder
    .appName(f"Escalabilidad_BVL_{FACTOR}x")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print()
print("=" * 90)
print(f"3.7 - PRUEBA DE ESCALABILIDAD {FACTOR}x")
print("=" * 90)
print(f"APPLICATION_ID|{spark.sparkContext.applicationId}")

# ----------------------------------------------------------
# Lectura de fuentes reales
# ----------------------------------------------------------

cot = spark.read.parquet(COT)
tc = spark.read.parquet(TC)
embig = spark.read.parquet(EMBIG)

requeridas = [
    "fecha_cotizacion",
    "valor",
    "cierre_actual",
    "cierre_anterior",
    "maxima_actual",
    "minima_actual",
    "promedio_actual",
    "monto_negociado",
    "anio",
    "mes"
]

faltantes = [c for c in requeridas if c not in cot.columns]

if faltantes:
    raise RuntimeError(
        "Faltan columnas en Cotizaciones: " + ", ".join(faltantes)
    )

base_rows = cot.count()
filas_esperadas = base_rows * FACTOR

print()
print("CONFIGURACION")
print("-" * 90)
print(f"Factor de replicacion           : {FACTOR}x")
print(f"Registros reales               : {base_rows:,}")
print(f"Registros esperados            : {filas_esperadas:,}")
print("Recursos Spark                 : 1 executor / 1 core / 512 MB")
print(f"Salida temporal                : {OUT}")

# ----------------------------------------------------------
# Cronometrar pipeline
# ----------------------------------------------------------

inicio = time.perf_counter()

replicas = (
    spark.range(FACTOR)
    .select(col("id").alias("replica_id"))
)

datos = cot.crossJoin(replicas)

datos = (
    datos
    .withColumn(
        "variacion_precio",
        col("cierre_actual") - col("cierre_anterior")
    )
    .withColumn(
        "retorno_diario_pct",
        when(
            col("cierre_anterior").isNotNull()
            & (col("cierre_anterior") != 0),
            (
                (col("cierre_actual") - col("cierre_anterior"))
                / col("cierre_anterior")
            ) * 100.0
        )
    )
    .withColumn(
        "rango_intradia",
        col("maxima_actual") - col("minima_actual")
    )
    .withColumn(
        "rango_intradia_pct",
        when(
            col("promedio_actual").isNotNull()
            & (col("promedio_actual") != 0),
            (
                (col("maxima_actual") - col("minima_actual"))
                / col("promedio_actual")
            ) * 100.0
        )
    )
)

tc_join = (
    tc.select(
        col("fecha").alias("fecha_tc"),
        "tipo_cambio_venta"
    )
)

embig_join = (
    embig.select(
        col("fecha").alias("fecha_embig"),
        "embig_peru"
    )
)

integrado = (
    datos
    .join(
        tc_join,
        datos["fecha_cotizacion"] == tc_join["fecha_tc"],
        "left"
    )
    .drop("fecha_tc")
    .join(
        embig_join,
        datos["fecha_cotizacion"] == embig_join["fecha_embig"],
        "left"
    )
    .drop("fecha_embig")
)

# Escritura temporal para materializar TODO el pipeline.
(
    integrado
    .write
    .mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("anio", "mes")
    .parquet(OUT)
)

fin = time.perf_counter()
tiempo = fin - inicio

print()
print("RESULTADO")
print("-" * 90)
print(f"Registros procesados esperados : {filas_esperadas:,}")
print(f"Tiempo pipeline                : {tiempo:.2f} s")
print(f"Salida                         : {OUT}")

print()
print(
    f"SCALABILITY_RESULT|{FACTOR}|"
    f"{filas_esperadas}|{tiempo:.4f}|{OUT}"
)

print("=" * 90)

spark.stop()
