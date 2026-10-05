from pyspark.sql import SparkSession
from pyspark.sql import functions as F

VALORES = (
    "/datalake/bvl/silver/"
    "referencias_smv/valores_inscritos"
)

PUENTE = (
    "/datalake/bvl/silver/"
    "referencias_smv/puente_ruc_nemonico"
)

spark = (
    SparkSession.builder
    .appName("Validacion-Valores-SMV-Silver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("VALIDACION SILVER - VALORES INSCRITOS SMV")
print("=" * 80)

# ============================================================
# VALORES INSCRITOS
# ============================================================

valores = spark.read.parquet(VALORES)

print()
print("=" * 80)
print("VALORES INSCRITOS")
print("=" * 80)

print("Registros:", f"{valores.count():,}")

print(
    "RUC distintos:",
    f"{valores.select('ruc').distinct().count():,}"
)

print(
    "Nemonicos distintos:",
    f"{valores.select('nemonico_valor').distinct().count():,}"
)

print(
    "Pares RUC+nemonico:",
    f"{valores.select('ruc', 'nemonico_valor').distinct().count():,}"
)

print(
    "RUC nulos/vacios:",
    valores.filter(
        F.col("ruc").isNull()
        | (F.trim("ruc") == "")
    ).count()
)

print(
    "Nemonicos nulos/vacios:",
    valores.filter(
        F.col("nemonico_valor").isNull()
        | (F.trim("nemonico_valor") == "")
    ).count()
)

print()
print("TIPOS DE VALOR")

(
    valores
    .groupBy("tipo_valor")
    .count()
    .orderBy(F.desc("count"))
    .show(50, truncate=False)
)

print()
print("SCHEMA VALORES")
valores.printSchema()

# ============================================================
# PUENTE
# ============================================================

puente = spark.read.parquet(PUENTE)

print()
print("=" * 80)
print("PUENTE RUC <-> NEMONICO")
print("=" * 80)

print(
    "Registros:",
    f"{puente.count():,}"
)

print(
    "RUC distintos:",
    f"{puente.select('ruc').distinct().count():,}"
)

print(
    "Nemonicos distintos:",
    f"{puente.select('nemonico_valor').distinct().count():,}"
)

duplicados = (
    puente
    .groupBy(
        "ruc",
        "nemonico_valor"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

ambiguos = (
    puente
    .groupBy(
        "nemonico_valor"
    )
    .agg(
        F.countDistinct("ruc").alias("rucs")
    )
    .filter(
        F.col("rucs") > 1
    )
    .count()
)

print(
    "Duplicados RUC+nemonico:",
    duplicados
)

print(
    "Nemonicos asociados >1 RUC:",
    ambiguos
)

print()
print("SCHEMA PUENTE")
puente.printSchema()

# ============================================================
# VALIDACION FINAL
# ============================================================

assert valores.count() == 1008
assert puente.count() == 831
assert duplicados == 0
assert ambiguos == 0

print()
print("=" * 80)
print("RESULTADO: SILVER VALORES SMV VALIDADO CORRECTAMENTE")
print("=" * 80)

spark.stop()
