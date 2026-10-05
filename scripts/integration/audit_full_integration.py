from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
from pathlib import Path

# ============================================================
# CONFIGURACION
# ============================================================

PRINCIPALES = (
    "/datalake/bvl/silver/"
    "finanzas_empresariales/principales_cuentas"
)

PUENTE = (
    "/datalake/bvl/silver/"
    "referencias_smv/puente_ruc_nemonico"
)

COTIZACIONES = (
    "/datalake/bvl/silver/"
    "mercado_diario/cotizaciones"
)

MONTOS_SAB = (
    "/datalake/bvl/silver/"
    "mercado_diario/montos_sab"
)

TIPO_CAMBIO = (
    "/datalake/bvl/silver/"
    "macroeconomia/tipo_cambio"
)

EMBIG = (
    "/datalake/bvl/silver/"
    "macroeconomia/embig"
)

OUT = Path(
    "outputs/quality/"
    "full_integration_audit.json"
)

# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("BVL-Full-Integration-Audit")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 85)
print("AUDITORIA COMPLETA DE INTEGRACION")
print("=" * 85)

# ============================================================
# CARGA
# ============================================================

fin = spark.read.parquet(PRINCIPALES)
puente = spark.read.parquet(PUENTE)
cot = spark.read.parquet(COTIZACIONES)
sab = spark.read.parquet(MONTOS_SAB)
tc = spark.read.parquet(TIPO_CAMBIO)
embig = spark.read.parquet(EMBIG)

# ============================================================
# NORMALIZACION MINIMA
# ============================================================

fin = fin.withColumn(
    "ruc_norm",
    F.trim(F.col("ruc").cast("string"))
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

# detectar nombre de fecha Cotizaciones
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
        f"No se encontró columna de fecha en Cotizaciones: {cot.columns}"
    )

# detectar fecha de Montos SAB
if "fecha_negociacion" in sab.columns:
    sab = sab.withColumn(
        "fecha_join",
        F.col("fecha_negociacion").cast("date")
    )
elif "fecha" in sab.columns:
    sab = sab.withColumn(
        "fecha_join",
        F.col("fecha").cast("date")
    )
else:
    raise RuntimeError(
        f"No se encontró columna de fecha en Montos SAB: {sab.columns}"
    )

# ============================================================
# 1. PRINCIPALES CUENTAS -> PUENTE
# ============================================================

print()
print("=" * 85)
print("1. PRINCIPALES CUENTAS -> PUENTE RUC-NEMONICO")
print("=" * 85)

ruc_fin = (
    fin
    .filter(
        F.col("ruc_norm").rlike(r"^\d{11}$")
    )
    .select("ruc_norm")
    .distinct()
)

ruc_puente = (
    puente
    .select("ruc_norm")
    .distinct()
)

total_ruc_fin = ruc_fin.count()
total_ruc_puente = ruc_puente.count()

ruc_match = (
    ruc_fin
    .join(
        ruc_puente,
        "ruc_norm",
        "inner"
    )
    .count()
)

ruc_sin_valores = (
    ruc_fin
    .join(
        ruc_puente,
        "ruc_norm",
        "left_anti"
    )
    .count()
)

cobertura_ruc = (
    100.0 * ruc_match / total_ruc_fin
    if total_ruc_fin
    else 0
)

print("RUC financieros válidos :", total_ruc_fin)
print("RUC en puente            :", total_ruc_puente)
print("RUC con valores inscritos:", ruc_match)
print("RUC financieros sin valor:", ruc_sin_valores)
print("Cobertura RUC            :", f"{cobertura_ruc:.2f}%")

# ============================================================
# 2. PUENTE -> COTIZACIONES
# ============================================================

print()
print("=" * 85)
print("2. PUENTE RUC-NEMONICO -> COTIZACIONES")
print("=" * 85)

nem_puente = (
    puente
    .select("nemonico_norm")
    .distinct()
)

valores_cot = (
    cot
    .select("valor_norm")
    .distinct()
)

total_nem_puente = nem_puente.count()
total_valores_cot = valores_cot.count()

nem_match = (
    nem_puente
    .join(
        valores_cot,
        nem_puente.nemonico_norm == valores_cot.valor_norm,
        "inner"
    )
    .count()
)

nem_sin_match = total_nem_puente - nem_match

print("Nemonicos puente      :", total_nem_puente)
print("Valores Cotizaciones :", total_valores_cot)
print("Nemonicos con match  :", nem_match)
print("Nemonicos sin match  :", nem_sin_match)

# ============================================================
# 3. FILAS DE COTIZACIONES CON RUC
# ============================================================

cot_empresa = (
    cot.alias("c")
    .join(
        puente.alias("p"),
        F.col("c.valor_norm") == F.col("p.nemonico_norm"),
        "left"
    )
)

total_cot = cot.count()

cot_con_ruc = (
    cot_empresa
    .filter(
        F.col("p.ruc_norm").isNotNull()
    )
    .count()
)

cot_sin_ruc = total_cot - cot_con_ruc

pct_cot_ruc = (
    100.0 * cot_con_ruc / total_cot
    if total_cot
    else 0
)

print()
print("=" * 85)
print("3. COBERTURA EMPRESARIAL DE COTIZACIONES")
print("=" * 85)

print("Cotizaciones totales :", f"{total_cot:,}")
print("Cotizaciones con RUC :", f"{cot_con_ruc:,}")
print("Cotizaciones sin RUC :", f"{cot_sin_ruc:,}")
print("Cobertura            :", f"{pct_cot_ruc:.2f}%")

# ============================================================
# 4. COBERTURA POR AÑO
# ============================================================

print()
print("=" * 85)
print("4. COBERTURA RUC POR AÑO")
print("=" * 85)

anio_expr = F.year("fecha_join")

base_anual = (
    cot
    .withColumn("anio_join", anio_expr)
    .groupBy("anio_join")
    .count()
    .withColumnRenamed("count", "total")
)

match_anual = (
    cot_empresa
    .filter(F.col("p.ruc_norm").isNotNull())
    .withColumn(
        "anio_join",
        F.year(F.col("c.fecha_join"))
    )
    .groupBy("anio_join")
    .count()
    .withColumnRenamed("count", "con_ruc")
)

cobertura_anual_df = (
    base_anual
    .join(
        match_anual,
        "anio_join",
        "left"
    )
    .fillna(0, ["con_ruc"])
    .withColumn(
        "pct",
        F.round(
            F.col("con_ruc") * 100.0 / F.col("total"),
            2
        )
    )
    .orderBy("anio_join")
)

cobertura_anual_df.show(
    20,
    truncate=False
)

# ============================================================
# 5. COTIZACIONES -> BCRP
# ============================================================

print()
print("=" * 85)
print("5. COBERTURA TEMPORAL CON BCRP")
print("=" * 85)

fechas_cot = (
    cot
    .select("fecha_join")
    .where(F.col("fecha_join").isNotNull())
    .distinct()
)

fechas_tc = (
    tc
    .select(
        F.col("fecha").alias("fecha_join")
    )
    .distinct()
)

fechas_embig = (
    embig
    .select(
        F.col("fecha").alias("fecha_join")
    )
    .distinct()
)

total_fechas_cot = fechas_cot.count()

fechas_tc_match = (
    fechas_cot
    .join(
        fechas_tc,
        "fecha_join",
        "inner"
    )
    .count()
)

fechas_embig_match = (
    fechas_cot
    .join(
        fechas_embig,
        "fecha_join",
        "inner"
    )
    .count()
)

pct_tc = (
    100.0 * fechas_tc_match / total_fechas_cot
    if total_fechas_cot
    else 0
)

pct_embig = (
    100.0 * fechas_embig_match / total_fechas_cot
    if total_fechas_cot
    else 0
)

print("Fechas Cotizaciones :", total_fechas_cot)
print(
    "Con Tipo Cambio    :",
    fechas_tc_match,
    f"({pct_tc:.2f}%)"
)
print(
    "Con EMBIG          :",
    fechas_embig_match,
    f"({pct_embig:.2f}%)"
)

# ============================================================
# 6. MONTOS SAB
# ============================================================

print()
print("=" * 85)
print("6. GRANULARIDAD MONTOS SAB")
print("=" * 85)

print("Columnas Montos SAB:", sab.columns)

total_sab = sab.count()

fechas_sab = (
    sab
    .select("fecha_join")
    .where(F.col("fecha_join").isNotNull())
    .distinct()
    .count()
)

print("Registros SAB       :", f"{total_sab:,}")
print("Fechas SAB distintas:", f"{fechas_sab:,}")

if "codigo_sab" in sab.columns:
    total_sabs = (
        sab
        .select("codigo_sab")
        .distinct()
        .count()
    )

    clave_sab = (
        sab
        .select(
            "fecha_join",
            "codigo_sab"
        )
        .distinct()
        .count()
    )

    print("SAB distintas       :", total_sabs)
    print(
        "Claves fecha+SAB   :",
        f"{clave_sab:,}"
    )

# cobertura temporal SAB vs Cotizaciones
fechas_sab_df = (
    sab
    .select("fecha_join")
    .where(F.col("fecha_join").isNotNull())
    .distinct()
)

fechas_sab_match = (
    fechas_cot
    .join(
        fechas_sab_df,
        "fecha_join",
        "inner"
    )
    .count()
)

pct_sab_fecha = (
    100.0 * fechas_sab_match / total_fechas_cot
    if total_fechas_cot
    else 0
)

print(
    "Fechas Cotizaciones con SAB:",
    fechas_sab_match,
    f"({pct_sab_fecha:.2f}%)"
)

# ============================================================
# 7. CARDINALIDADES
# ============================================================

print()
print("=" * 85)
print("7. CARDINALIDADES PROPUESTAS")
print("=" * 85)

print(
    "Principales -> Puente : 1 RUC puede tener N nemonicos"
)

print(
    "Puente -> Cotizaciones: 1 nemonico puede tener N fechas"
)

print(
    "Cotizaciones -> BCRP  : N cotizaciones de una fecha -> 1 indicador diario"
)

print(
    "Montos SAB            : fact independiente por fecha + SAB"
)

# ============================================================
# RESUMEN JSON
# ============================================================

cobertura_anual = {
    str(r["anio_join"]): {
        "total": int(r["total"]),
        "con_ruc": int(r["con_ruc"]),
        "pct": float(r["pct"])
    }
    for r in cobertura_anual_df.collect()
}

resultado = {
    "principales_a_puente": {
        "ruc_financieros": total_ruc_fin,
        "ruc_puente": total_ruc_puente,
        "ruc_match": ruc_match,
        "ruc_sin_valores": ruc_sin_valores,
        "cobertura_pct": round(cobertura_ruc, 4)
    },

    "puente_a_cotizaciones": {
        "nemonicos_puente": total_nem_puente,
        "valores_cotizaciones": total_valores_cot,
        "nemonicos_match": nem_match,
        "nemonicos_sin_match": nem_sin_match
    },

    "cotizaciones_con_ruc": {
        "total": total_cot,
        "con_ruc": cot_con_ruc,
        "sin_ruc": cot_sin_ruc,
        "cobertura_pct": round(pct_cot_ruc, 4),
        "por_anio": cobertura_anual
    },

    "cobertura_bcrp": {
        "fechas_cotizaciones": total_fechas_cot,
        "tipo_cambio_match": fechas_tc_match,
        "tipo_cambio_pct": round(pct_tc, 4),
        "embig_match": fechas_embig_match,
        "embig_pct": round(pct_embig, 4)
    },

    "montos_sab": {
        "registros": total_sab,
        "fechas_distintas": fechas_sab,
        "fechas_match_cotizaciones": fechas_sab_match,
        "cobertura_fecha_pct": round(pct_sab_fecha, 4)
    }
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
