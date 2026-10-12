import time
import statistics

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg

spark = (
    SparkSession.builder
    .appName("Benchmark_3_6_Particionado")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

ORIGEN = "/datalake/bvl/silver/mercado_diario/cotizaciones"
SIN_PART = "/tmp/bvl_benchmark/cotizaciones_sin_particionar"

ANIO = 2025
MES = 3
REPETICIONES = 7

print()
print("=" * 88)
print("3.6 - OPTIMIZACION DEL RENDIMIENTO")
print("COMPARACION: PARQUET SIN PARTICIONAR VS PARTICIONADO")
print("=" * 88)

# ============================================================
# 1. LEER DATASET REAL
# ============================================================

df = spark.read.parquet(ORIGEN)

total = df.count()

print()
print("DATASET REAL")
print("-" * 88)
print(f"Registros totales               : {total:,}")
print(f"Periodo evaluado                : {ANIO}-{MES:02d}")
print(f"Repeticiones por escenario      : {REPETICIONES}")

# ============================================================
# 2. GENERAR COPIA SIN PARTICIONAR
# ============================================================

print()
print("Preparando copia temporal sin particionar...")

(
    df
    .repartition(4)
    .write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(SIN_PART)
)

sin_part = spark.read.parquet(SIN_PART)

# ============================================================
# 3. FUNCION DE CONSULTA
# ============================================================

def ejecutar_consulta(dataframe):

    inicio = time.perf_counter()

    resultado = (
        dataframe
        .filter(
            (col("anio") == ANIO)
            & (col("mes") == MES)
        )
        .agg(
            count("*").alias("registros"),
            spark_sum("monto_negociado").alias("monto_total"),
            avg("cierre_actual").alias("cierre_promedio")
        )
        .collect()[0]
    )

    fin = time.perf_counter()

    return fin - inicio, resultado


# ============================================================
# 4. VALIDAR MISMO RESULTADO
# ============================================================

t1, resultado_sin = ejecutar_consulta(sin_part)
t2, resultado_part = ejecutar_consulta(df)

mismo_resultado = (
    resultado_sin["registros"] == resultado_part["registros"]
)

print()
print("VALIDACION FUNCIONAL")
print("-" * 88)
print(f"Filas obtenidas sin particionar : {resultado_sin['registros']:,}")
print(f"Filas obtenidas particionadas   : {resultado_part['registros']:,}")
print(f"Mismo resultado                 : {'SI' if mismo_resultado else 'NO'}")

if not mismo_resultado:
    raise RuntimeError("Los escenarios no producen el mismo resultado.")

# Limpiar cache de Spark antes del benchmark.
spark.catalog.clearCache()

# ============================================================
# 5. BENCHMARK
# ============================================================

tiempos_sin = []
tiempos_part = []

print()
print("EJECUCIONES MEDIDAS")
print("-" * 88)

for i in range(REPETICIONES):

    # Alternamos el orden para reducir sesgo de cache de sistema.
    if i % 2 == 0:

        ts, _ = ejecutar_consulta(sin_part)
        tp, _ = ejecutar_consulta(df)

    else:

        tp, _ = ejecutar_consulta(df)
        ts, _ = ejecutar_consulta(sin_part)

    tiempos_sin.append(ts)
    tiempos_part.append(tp)

    print(
        f"Prueba {i+1}: "
        f"sin particionar={ts:.4f}s | "
        f"particionado={tp:.4f}s"
    )

# ============================================================
# 6. RESULTADOS
# ============================================================

mediana_sin = statistics.median(tiempos_sin)
mediana_part = statistics.median(tiempos_part)

promedio_sin = statistics.mean(tiempos_sin)
promedio_part = statistics.mean(tiempos_part)

mejora = (
    ((mediana_sin - mediana_part) / mediana_sin) * 100
    if mediana_sin > 0
    else 0
)

factor = (
    mediana_sin / mediana_part
    if mediana_part > 0
    else 0
)

print()
print("=" * 88)
print("RESULTADO DEL BENCHMARK")
print("=" * 88)

print(f"Mediana SIN particionar         : {mediana_sin:.4f} s")
print(f"Mediana PARTICIONADO            : {mediana_part:.4f} s")

print(f"Promedio SIN particionar        : {promedio_sin:.4f} s")
print(f"Promedio PARTICIONADO           : {promedio_part:.4f} s")

print(f"Mejora sobre la mediana         : {mejora:.2f}%")
print(f"Factor de aceleracion           : {factor:.2f}x")

print()
print("OPTIMIZACION EVALUADA")
print("-" * 88)
print("Antes   : Parquet sin particionado.")
print("Despues : Parquet particionado por anio y mes.")
print(f"Filtro   : anio={ANIO}, mes={MES}")

print()
print("=" * 88)

if mediana_part < mediana_sin:
    print("RESULTADO: EL PARTICIONAMIENTO REDUJO EL TIEMPO DE CONSULTA")
else:
    print("RESULTADO: NO SE OBSERVO MEJORA TEMPORAL EN ESTE VOLUMEN")

print("=" * 88)

spark.stop()
