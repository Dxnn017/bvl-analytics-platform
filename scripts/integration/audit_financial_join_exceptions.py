from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FIN = (
    "/datalake/bvl/silver/"
    "finanzas_empresariales/principales_cuentas"
)

PUENTE = (
    "/datalake/bvl/silver/"
    "referencias_smv/puente_ruc_nemonico"
)

COT = (
    "/datalake/bvl/silver/"
    "mercado_diario/cotizaciones"
)

spark = (
    SparkSession.builder
    .appName("BVL-Financial-Join-Exceptions")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

fin = spark.read.parquet(FIN)
puente = spark.read.parquet(PUENTE)
cot = spark.read.parquet(COT)

# ============================================================
# NORMALIZACION
# ============================================================

fin = (
    fin
    .withColumn(
        "ruc_norm",
        F.trim(F.col("ruc").cast("string"))
    )
    .withColumn(
        "ejercicio_join",
        F.col("ejercicio").cast("int")
    )
)

if "trimestre_num" in fin.columns:
    fin = fin.withColumn(
        "trimestre_join",
        F.col("trimestre_num").cast("int")
    )
else:
    fin = fin.withColumn(
        "trimestre_join",
        F.when(
            F.lower("trimestre").contains("1er"),
            1
        )
        .when(
            F.lower("trimestre").contains("2do"),
            2
        )
        .when(
            F.lower("trimestre").contains("3er"),
            3
        )
        .when(
            F.lower("trimestre").contains("4to"),
            4
        )
    )

puente = (
    puente
    .withColumn(
        "ruc_norm",
        F.trim(F.col("ruc").cast("string"))
    )
    .withColumn(
        "nemonico_norm",
        F.upper(F.trim("nemonico_valor"))
    )
)

cot = cot.withColumn(
    "valor_norm",
    F.upper(F.trim("valor"))
)

if "fecha_cotizacion" in cot.columns:
    cot = cot.withColumn(
        "fecha_join",
        F.col("fecha_cotizacion").cast("date")
    )
else:
    cot = cot.withColumn(
        "fecha_join",
        F.col("fecha").cast("date")
    )

cot = (
    cot
    .withColumn(
        "ejercicio_join",
        F.year("fecha_join")
    )
    .withColumn(
        "trimestre_join",
        F.quarter("fecha_join")
    )
)

# ============================================================
# COTIZACIONES CON RUC
# ============================================================

cot_ruc = (
    cot.alias("c")
    .join(
        puente.alias("p"),
        F.col("c.valor_norm")
        == F.col("p.nemonico_norm"),
        "inner"
    )
)

# ============================================================
# 1. VERIFICAR BNB VALORES
# ============================================================

RUC_BNB = "20513808519"

bnb_cot = (
    cot_ruc
    .filter(
        F.col("p.ruc_norm") == RUC_BNB
    )
)

print("=" * 85)
print("1. IMPACTO DE LAS COLISIONES BNB VALORES")
print("=" * 85)

print(
    "Cotizaciones asociadas al RUC 20513808519:",
    bnb_cot.count()
)

print(
    "Nemonicos asociados:",
    bnb_cot
    .select("p.nemonico_norm")
    .distinct()
    .count()
)

if bnb_cot.count() > 0:
    (
        bnb_cot
        .select(
            "p.ruc_norm",
            "p.razon_social",
            "p.nemonico_norm"
        )
        .distinct()
        .show(50, truncate=False)
    )

# ============================================================
# 2. PERIODOS COTIZADOS
# ============================================================

periodos_cot = (
    cot_ruc
    .select(
        F.col("p.ruc_norm").alias("ruc_norm"),
        F.col("p.razon_social").alias("razon_social"),
        "ejercicio_join",
        "trimestre_join"
    )
    .distinct()
)

periodos_fin = (
    fin
    .filter(
        F.col("ruc_norm").rlike(r"^\d{11}$")
    )
    .select(
        "ruc_norm",
        "ejercicio_join",
        "trimestre_join"
    )
    .distinct()
)

faltantes = (
    periodos_cot
    .join(
        periodos_fin,
        [
            "ruc_norm",
            "ejercicio_join",
            "trimestre_join"
        ],
        "left_anti"
    )
    .orderBy(
        "ruc_norm",
        "ejercicio_join",
        "trimestre_join"
    )
)

print()
print("=" * 85)
print("2. PERIODOS COTIZADOS SIN PRINCIPALES CUENTAS")
print("=" * 85)

print(
    "Cantidad:",
    faltantes.count()
)

faltantes.show(
    50,
    truncate=False
)

# ============================================================
# 3. COLISIONES QUE REALMENTE INTERSECTAN COTIZACIONES
# ============================================================

grain_fin = (
    fin
    .filter(
        F.col("ruc_norm").rlike(r"^\d{11}$")
    )
    .groupBy(
        "ruc_norm",
        "ejercicio_join",
        "trimestre_join"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
)

periodos_cot_simple = (
    periodos_cot
    .select(
        "ruc_norm",
        "ejercicio_join",
        "trimestre_join"
    )
)

colisiones_relevantes = (
    grain_fin
    .join(
        periodos_cot_simple,
        [
            "ruc_norm",
            "ejercicio_join",
            "trimestre_join"
        ],
        "inner"
    )
)

print()
print("=" * 85)
print("3. COLISIONES FINANCIERAS QUE AFECTAN COTIZACIONES")
print("=" * 85)

print(
    "Grupos:",
    colisiones_relevantes.count()
)

colisiones_relevantes.show(
    20,
    truncate=False
)

print()
print("=" * 85)
print("FIN AUDITORIA")
print("=" * 85)

spark.stop()
