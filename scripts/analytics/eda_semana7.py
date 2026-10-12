from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, sum as spark_sum,
    avg, count, lag
)
from pyspark.sql.window import Window

import os
import pandas as pd
import matplotlib.pyplot as plt


spark = (
    SparkSession.builder
    .appName("EDA_BVL_Semana7")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

COT = "/datalake/bvl/silver/mercado_diario/cotizaciones"
EMBIG = "/datalake/bvl/silver/macroeconomia/embig"

OUT_FIG = "outputs/figures"
OUT_ANALYTICS = "outputs/analytics"

os.makedirs(OUT_FIG, exist_ok=True)
os.makedirs(OUT_ANALYTICS, exist_ok=True)


# ============================================================
# CARGA
# ============================================================

cot = spark.read.parquet(COT)
embig = spark.read.parquet(EMBIG)

print()
print("=" * 90)
print("4.1 - ANALISIS EXPLORATORIO DE DATOS")
print("=" * 90)

print(f"Cotizaciones cargadas : {cot.count():,}")
print(f"EMBIG cargado         : {embig.count():,}")


# ============================================================
# PREPARACION DEL RETORNO
# ============================================================

w = (
    Window
    .partitionBy("valor")
    .orderBy("fecha_cotizacion")
)

cot_ret = (
    cot
    .withColumn(
        "cierre_previo",
        lag("cierre_actual").over(w)
    )
    .withColumn(
        "retorno_pct",
        (
            (col("cierre_actual") - col("cierre_previo"))
            / col("cierre_previo")
        ) * 100.0
    )
)


# ============================================================
# GRAFICO 1
# Evolucion mensual del monto negociado
# ============================================================

mensual = (
    cot
    .filter(col("monto_negociado").isNotNull())
    .groupBy("anio", "mes")
    .agg(
        spark_sum("monto_negociado").alias("monto_negociado")
    )
    .orderBy("anio", "mes")
)

pdf_mensual = mensual.toPandas()

pdf_mensual["periodo"] = pd.to_datetime(
    dict(
        year=pdf_mensual["anio"],
        month=pdf_mensual["mes"],
        day=1
    )
)

plt.figure(figsize=(10, 5))
plt.plot(
    pdf_mensual["periodo"],
    pdf_mensual["monto_negociado"]
)
plt.title("Evolución mensual del monto negociado")
plt.xlabel("Periodo")
plt.ylabel("Monto negociado")
plt.xticks(rotation=45)
plt.tight_layout()

ruta1 = f"{OUT_FIG}/eda_01_monto_mensual.png"
plt.savefig(ruta1, dpi=180)
plt.close()


# ============================================================
# GRAFICO 2
# Retorno promedio diario vs EMBIG
# ============================================================

retorno_diario = (
    cot_ret
    .filter(
        col("retorno_pct").isNotNull()
        & col("fecha_cotizacion").isNotNull()
    )
    .groupBy("fecha_cotizacion")
    .agg(
        avg("retorno_pct").alias("retorno_promedio_pct")
    )
)

embig_valido = (
    embig
    .filter(
        col("fecha").isNotNull()
        & col("embig_peru").isNotNull()
    )
    .select(
        col("fecha").alias("fecha_join"),
        "embig_peru"
    )
)

retorno_embig = (
    retorno_diario
    .join(
        embig_valido,
        retorno_diario["fecha_cotizacion"]
        == embig_valido["fecha_join"],
        "inner"
    )
    .select(
        "fecha_cotizacion",
        "retorno_promedio_pct",
        "embig_peru"
    )
)

pdf_embig = retorno_embig.toPandas()

plt.figure(figsize=(8, 5))
plt.scatter(
    pdf_embig["embig_peru"],
    pdf_embig["retorno_promedio_pct"],
    alpha=0.6
)
plt.title("Retorno promedio diario frente al EMBIG Perú")
plt.xlabel("EMBIG Perú (puntos básicos)")
plt.ylabel("Retorno promedio diario (%)")
plt.tight_layout()

ruta2 = f"{OUT_FIG}/eda_02_retorno_embig.png"
plt.savefig(ruta2, dpi=180)
plt.close()

correlacion = (
    pdf_embig["retorno_promedio_pct"]
    .corr(pdf_embig["embig_peru"])
)


# ============================================================
# GRAFICO 3
# Top 10 valores por monto negociado
# ============================================================

top_valores = (
    cot
    .filter(
        col("valor").isNotNull()
        & col("monto_negociado").isNotNull()
    )
    .groupBy("valor")
    .agg(
        spark_sum("monto_negociado").alias("monto_total"),
        count("*").alias("cotizaciones")
    )
    .orderBy(col("monto_total").desc())
    .limit(10)
)

pdf_top = top_valores.toPandas()

plt.figure(figsize=(9, 5))
plt.bar(
    pdf_top["valor"],
    pdf_top["monto_total"]
)
plt.title("Top 10 valores por monto negociado")
plt.xlabel("Valor")
plt.ylabel("Monto negociado acumulado")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()

ruta3 = f"{OUT_FIG}/eda_03_top_valores.png"
plt.savefig(ruta3, dpi=180)
plt.close()


# ============================================================
# RESUMEN PARA EL INFORME
# ============================================================

print()
print("1. EVOLUCION MENSUAL")
print("-" * 90)

max_mes = pdf_mensual.loc[
    pdf_mensual["monto_negociado"].idxmax()
]

min_mes = pdf_mensual.loc[
    pdf_mensual["monto_negociado"].idxmin()
]

print(
    f"Mayor monto mensual : "
    f"{int(max_mes['anio'])}-{int(max_mes['mes']):02d} "
    f"-> {max_mes['monto_negociado']:.2f}"
)

print(
    f"Menor monto mensual : "
    f"{int(min_mes['anio'])}-{int(min_mes['mes']):02d} "
    f"-> {min_mes['monto_negociado']:.2f}"
)


print()
print("2. RETORNO VS EMBIG")
print("-" * 90)

print(f"Observaciones emparejadas : {len(pdf_embig):,}")
print(f"Correlacion Pearson       : {correlacion:.4f}")


print()
print("3. TOP 10 VALORES POR MONTO NEGOCIADO")
print("-" * 90)

for i, row in pdf_top.iterrows():
    print(
        f"{i+1:02d}. "
        f"{row['valor']:<15} "
        f"Monto={row['monto_total']:.2f} | "
        f"Cotizaciones={int(row['cotizaciones']):,}"
    )


print()
print("=" * 90)
print("FIGURAS GENERADAS")
print("=" * 90)

print(ruta1)
print(ruta2)
print(ruta3)

print("=" * 90)

spark.stop()
