from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path
import json

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

OUT = Path(
    "outputs/quality/"
    "financial_join_grain_audit.json"
)

spark = (
    SparkSession.builder
    .appName("BVL-Financial-Join-Grain-Audit")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 85)
print("AUDITORIA DE GRANULARIDAD - COTIZACIONES + FINANZAS")
print("=" * 85)

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

cot = (
    cot
    .withColumn(
        "valor_norm",
        F.upper(F.trim("valor"))
    )
)

if "fecha_cotizacion" in cot.columns:
    cot = cot.withColumn(
        "fecha_join",
        F.col("fecha_cotizacion").cast("date")
    )
elif "fecha" in cot.columns:
    cot = cot.withColumn(
        "fecha_join",
        F.col("fecha").cast("date")
    )
else:
    raise RuntimeError(
        f"No se encontro fecha en Cotizaciones: {cot.columns}"
    )

cot = (
    cot
    .withColumn(
        "ejercicio_join",
        F.year("fecha_join")
    )
    .withColumn(
        "trimestre_num",
        F.quarter("fecha_join")
    )
)

# ============================================================
# COTIZACIONES CON RUC
# ============================================================

cot_ruc = (
    cot.alias("c")
    .join(
        puente.select(
            "ruc_norm",
            "nemonico_norm",
            "razon_social"
        ).alias("p"),
        F.col("c.valor_norm")
        == F.col("p.nemonico_norm"),
        "inner"
    )
)

print()
print("=" * 85)
print("1. UNIVERSO COTIZACIONES CON IDENTIDAD EMPRESARIAL")
print("=" * 85)

print(
    "Filas Cotizaciones con RUC:",
    f"{cot_ruc.count():,}"
)

print(
    "RUC distintos:",
    cot_ruc
    .select("p.ruc_norm")
    .distinct()
    .count()
)

# ============================================================
# DISTRIBUCION FINANCIERA
# ============================================================

print()
print("=" * 85)
print("2. VARIANTES DE INFORMACION FINANCIERA")
print("=" * 85)

for campo in [
    "tipo_informacion",
    "metodo_flujo_efectivo",
    "moneda"
]:
    if campo in fin.columns:
        print()
        print("CAMPO:", campo)

        (
            fin
            .groupBy(campo)
            .count()
            .orderBy(F.desc("count"))
            .show(50, truncate=False)
        )

# ============================================================
# MAPEAR TRIMESTRE
# ============================================================

if "trimestre_num" not in fin.columns:

    if "trimestre" not in fin.columns:
        raise RuntimeError(
            "Principales Cuentas no posee trimestre ni trimestre_num."
        )

    fin = (
        fin
        .withColumn(
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
    )

else:
    fin = fin.withColumn(
        "trimestre_join",
        F.col("trimestre_num")
    )

fin = fin.withColumn(
    "ejercicio_join",
    F.col("ejercicio").cast("int")
)

# ============================================================
# GRANULARIDAD RUC + AÑO + TRIMESTRE
# ============================================================

print()
print("=" * 85)
print("3. GRANULARIDAD FINANCIERA BASE")
print("=" * 85)

grain = (
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
)

print(
    "Grupos RUC+año+trimestre:",
    f"{grain.count():,}"
)

print(
    "Grupos con >1 fila:",
    f"{grain.filter(F.col('count') > 1).count():,}"
)

print()

(
    grain
    .groupBy("count")
    .count()
    .orderBy("count")
    .show(30, truncate=False)
)

# ============================================================
# GRANULARIDAD COMPLETA
# ============================================================

dimensiones = [
    "ruc_norm",
    "ejercicio_join",
    "trimestre_join"
]

for c in [
    "tipo_informacion",
    "rpj",
    "metodo_flujo_efectivo",
    "moneda"
]:
    if c in fin.columns:
        dimensiones.append(c)

grain_full = (
    fin
    .filter(
        F.col("ruc_norm").rlike(r"^\d{11}$")
    )
    .groupBy(*dimensiones)
    .count()
)

grupos_full_repetidos = (
    grain_full
    .filter(F.col("count") > 1)
    .count()
)

print()
print("=" * 85)
print("4. GRANULARIDAD CON DIMENSIONES FINANCIERAS")
print("=" * 85)

print(
    "Dimensiones:",
    " + ".join(dimensiones)
)

print(
    "Grupos:",
    f"{grain_full.count():,}"
)

print(
    "Grupos repetidos:",
    f"{grupos_full_repetidos:,}"
)

# ============================================================
# COBERTURA PARA LOS RUC QUE COTIZAN
# ============================================================

ruc_cot = (
    cot_ruc
    .select(
        F.col("p.ruc_norm").alias("ruc_norm")
    )
    .distinct()
)

ruc_fin = (
    fin
    .select("ruc_norm")
    .filter(
        F.col("ruc_norm").rlike(r"^\d{11}$")
    )
    .distinct()
)

ruc_cot_count = ruc_cot.count()

ruc_cot_fin = (
    ruc_cot
    .join(
        ruc_fin,
        "ruc_norm",
        "inner"
    )
    .count()
)

print()
print("=" * 85)
print("5. COBERTURA FINANCIERA DE EMPRESAS COTIZADAS")
print("=" * 85)

print(
    "RUC vinculados a Cotizaciones:",
    ruc_cot_count
)

print(
    "RUC con Principales Cuentas:",
    ruc_cot_fin
)

print(
    "Cobertura:",
    (
        f"{100*ruc_cot_fin/ruc_cot_count:.2f}%"
        if ruc_cot_count
        else "0.00%"
    )
)

# ============================================================
# COBERTURA POR RUC + AÑO + TRIMESTRE
# ============================================================

periodos_cot = (
    cot_ruc
    .select(
        F.col("p.ruc_norm").alias("ruc_norm"),
        "ejercicio_join",
        F.col("trimestre_num").alias("trimestre_join")
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

periodos_total = periodos_cot.count()

periodos_match = (
    periodos_cot
    .join(
        periodos_fin,
        [
            "ruc_norm",
            "ejercicio_join",
            "trimestre_join"
        ],
        "inner"
    )
    .count()
)

print()
print("=" * 85)
print("6. COBERTURA RUC + AÑO + TRIMESTRE")
print("=" * 85)

print(
    "Periodos empresariales cotizados:",
    periodos_total
)

print(
    "Periodos con datos financieros:",
    periodos_match
)

print(
    "Cobertura:",
    (
        f"{100*periodos_match/periodos_total:.2f}%"
        if periodos_total
        else "0.00%"
    )
)

# ============================================================
# DETALLE DE LAS COLISIONES YA CONOCIDAS
# ============================================================

print()
print("=" * 85)
print("7. PRIMEROS GRUPOS FINANCIEROS REPETIDOS")
print("=" * 85)

(
    grain_full
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
    .show(20, truncate=False)
)

# ============================================================
# JSON
# ============================================================

resultado = {
    "cotizaciones_con_ruc": cot_ruc.count(),

    "ruc_cotizaciones": ruc_cot_count,

    "ruc_cotizaciones_con_finanzas": ruc_cot_fin,

    "cobertura_ruc_finanzas_pct": (
        round(
            100 * ruc_cot_fin / ruc_cot_count,
            4
        )
        if ruc_cot_count
        else 0
    ),

    "periodos_ruc_trimestre_cotizaciones": (
        periodos_total
    ),

    "periodos_con_finanzas": (
        periodos_match
    ),

    "cobertura_periodos_pct": (
        round(
            100 * periodos_match / periodos_total,
            4
        )
        if periodos_total
        else 0
    ),

    "grupos_granularidad_completa_repetidos": (
        grupos_full_repetidos
    ),

    "dimensiones_granularidad": dimensiones
}

OUT.parent.mkdir(
    parents=True,
    exist_ok=True
)

OUT.write_text(
    json.dumps(
        resultado,
        ensure_ascii=False,
        indent=2
    ),
    encoding="utf-8"
)

print()
print("=" * 85)
print("ARCHIVO GENERADO")
print("=" * 85)

print(OUT)

print()
print("=" * 85)
print("FIN AUDITORIA")
print("=" * 85)

spark.stop()
