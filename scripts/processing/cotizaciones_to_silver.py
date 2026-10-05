import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURACIÓN
# ============================================================

FECHA_INGESTA = "2026-10-04"

BRONZE = (
    "/datalake/bvl/bronze/smv/cotizaciones/"
    f"ingestion_date={FECHA_INGESTA}/cotizaciones_*.json"
)

SILVER = (
    "/datalake/bvl/silver/mercado_diario/cotizaciones"
)

SUMMARY_LOCAL = Path(
    "outputs/quality/cotizaciones_historicas_silver_summary.json"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("BVL-Cotizaciones-Historicas-Silver")
    .config("spark.sql.shuffle.partitions", "4")
    .config(
        "spark.sql.parquet.compression.codec",
        "snappy"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 1. LEER LOS 60 JSON WRAPPER
# ============================================================

print("=" * 78)
print("COTIZACIONES HISTORICAS SMV - BRONZE -> SILVER")
print("=" * 78)

wrappers = (
    spark.read
    .option("multiLine", "true")
    .json(BRONZE)
)

archivos_wrapper = wrappers.count()

print(
    "Archivos/wrappers leídos :",
    archivos_wrapper
)

if archivos_wrapper != 60:
    spark.stop()
    raise RuntimeError(
        f"Se esperaban 60 archivos y se obtuvieron {archivos_wrapper}"
    )


# ============================================================
# 2. EXTRAER ARRAY RESULTADO
# ============================================================

if "Resultado" not in wrappers.columns:
    spark.stop()
    raise RuntimeError(
        "No existe el campo Resultado en los JSON de Cotizaciones."
    )

cot = (
    wrappers
    .select(
        F.explode(
            F.col("Resultado")
        ).alias("registro")
    )
    .select("registro.*")
)


raw_count = cot.count()

print(
    "Registros Bronze         :",
    f"{raw_count:,}"
)

if raw_count != 83130:
    spark.stop()
    raise RuntimeError(
        f"Se esperaban 83,130 registros y se obtuvieron {raw_count:,}"
    )


# ============================================================
# 3. NORMALIZACIÓN
# ============================================================

silver = (
    cot

    .select(
        F.col("FechaCotizacion")
            .alias("fecha_cotizacion_raw"),

        F.col("Valor")
            .alias("valor"),

        F.col("Descripcion")
            .alias("descripcion"),

        F.col("FechaAnterior")
            .alias("fecha_anterior_raw"),

        F.col("Cierre_Anterior")
            .cast("double")
            .alias("cierre_anterior"),

        F.col("Cierre_Actual")
            .cast("double")
            .alias("cierre_actual"),

        F.col("Apertura_Actual")
            .cast("double")
            .alias("apertura_actual"),

        F.col("Maxima_Actual")
            .cast("double")
            .alias("maxima_actual"),

        F.col("Minima_Actual")
            .cast("double")
            .alias("minima_actual"),

        F.col("Promedio_Actual")
            .cast("double")
            .alias("promedio_actual"),

        F.col("Monto_Negociado")
            .cast("double")
            .alias("monto_negociado"),

        F.col("FechaRegistro")
            .alias("fecha_registro_raw")
    )

    .withColumn(
        "fecha_cotizacion",
        F.to_date(
            F.substring(
                F.col("fecha_cotizacion_raw"),
                1,
                10
            ),
            "dd/MM/yyyy"
        )
    )

    .withColumn(
        "fecha_anterior",
        F.to_date(
            F.substring(
                F.col("fecha_anterior_raw"),
                1,
                10
            ),
            "dd/MM/yyyy"
        )
    )

    .withColumn(
        "valor",
        F.trim(
            F.col("valor")
        )
    )

    .withColumn(
        "descripcion",
        F.trim(
            F.col("descripcion")
        )
    )

    .withColumn(
        "anio",
        F.year(
            F.col("fecha_cotizacion")
        )
    )

    .withColumn(
        "mes",
        F.month(
            F.col("fecha_cotizacion")
        )
    )

    .withColumn(
        "fuente",
        F.lit("SMV")
    )

    .withColumn(
        "fecha_ingesta",
        F.to_date(
            F.lit(FECHA_INGESTA)
        )
    )
)


# ============================================================
# 4. DATA QUALITY
# ============================================================

print("\n" + "=" * 78)
print("VALIDACION PREVIA")
print("=" * 78)

total = silver.count()

fechas_nulas = (
    silver
    .filter(
        F.col("fecha_cotizacion").isNull()
    )
    .count()
)

valor_nulo = (
    silver
    .filter(
        F.col("valor").isNull()
        |
        (F.col("valor") == "")
    )
    .count()
)

claves_unicas = (
    silver
    .select(
        "fecha_cotizacion",
        "valor"
    )
    .distinct()
    .count()
)

duplicados_extra = (
    total - claves_unicas
)

fecha_min = (
    silver
    .agg(
        F.min("fecha_cotizacion")
    )
    .first()[0]
)

fecha_max = (
    silver
    .agg(
        F.max("fecha_cotizacion")
    )
    .first()[0]
)


print(
    "Registros totales       :",
    f"{total:,}"
)

print(
    "FechaCotizacion nula    :",
    f"{fechas_nulas:,}"
)

print(
    "Valor vacío/nulo        :",
    f"{valor_nulo:,}"
)

print(
    "Claves únicas           :",
    f"{claves_unicas:,}"
)

print(
    "Duplicados adicionales  :",
    f"{duplicados_extra:,}"
)

print(
    "Fecha mínima            :",
    fecha_min
)

print(
    "Fecha máxima            :",
    fecha_max
)


# ============================================================
# 5. DISTRIBUCIÓN POR AÑO
# ============================================================

print("\nREGISTROS POR AÑO")

por_anio = (
    silver
    .groupBy("anio")
    .count()
    .orderBy("anio")
)

por_anio.show(
    20,
    truncate=False
)

conteos_anio = {
    int(r["anio"]): int(r["count"])
    for r in por_anio.collect()
}


esperado = {
    2021: 15878,
    2022: 13303,
    2023: 13012,
    2024: 17016,
    2025: 23921
}


# ============================================================
# 6. VALIDACIONES CRÍTICAS
# ============================================================

errores = []

if total != 83130:
    errores.append(
        f"Total incorrecto: {total:,}"
    )

if fechas_nulas != 0:
    errores.append(
        f"Fechas nulas: {fechas_nulas:,}"
    )

if valor_nulo != 0:
    errores.append(
        f"Valores vacíos/nulos: {valor_nulo:,}"
    )

if claves_unicas != 83130:
    errores.append(
        f"Clave FechaCotizacion+Valor no es única: "
        f"{claves_unicas:,} claves"
    )

if duplicados_extra != 0:
    errores.append(
        f"Duplicados adicionales: {duplicados_extra:,}"
    )

for anio, cantidad in esperado.items():

    obtenido = conteos_anio.get(
        anio,
        0
    )

    if obtenido != cantidad:

        errores.append(
            f"{anio}: esperado {cantidad:,}, "
            f"obtenido {obtenido:,}"
        )


if errores:

    print("\n" + "!" * 78)
    print("VALIDACION FALLIDA")
    print("NO SE MODIFICARA SILVER")
    print("!" * 78)

    for error in errores:
        print(" -", error)

    spark.stop()
    raise SystemExit(1)


print()
print("VALIDACIONES PREVIAS: OK")


# ============================================================
# 7. ESCRIBIR SILVER
# ============================================================

print("\n" + "=" * 78)
print("REEMPLAZANDO SILVER COTIZACIONES")
print("=" * 78)

(
    silver
    .write
    .mode("overwrite")
    .partitionBy(
        "anio",
        "mes"
    )
    .parquet(
        SILVER
    )
)


# ============================================================
# 8. VALIDACIÓN POSTERIOR
# ============================================================

silver_final = (
    spark.read
    .parquet(SILVER)
)

final_total = silver_final.count()

final_claves = (
    silver_final
    .select(
        "fecha_cotizacion",
        "valor"
    )
    .distinct()
    .count()
)

final_anios = (
    silver_final
    .select("anio")
    .distinct()
    .orderBy("anio")
    .collect()
)

final_anios = [
    int(r["anio"])
    for r in final_anios
]


print(
    "Registros Silver        :",
    f"{final_total:,}"
)

print(
    "Claves únicas Silver    :",
    f"{final_claves:,}"
)

print(
    "Años Silver             :",
    final_anios
)


if (
    final_total != 83130
    or
    final_claves != 83130
    or
    final_anios != [2021, 2022, 2023, 2024, 2025]
):

    spark.stop()

    raise RuntimeError(
        "Falló la validación posterior de Silver."
    )


# ============================================================
# 9. RESUMEN PARA GITHUB
# ============================================================

SUMMARY_LOCAL.parent.mkdir(
    parents=True,
    exist_ok=True
)

summary = {
    "fuente": "SMV",
    "dataset": "Cotizaciones históricas",
    "fecha_ingesta": FECHA_INGESTA,

    "bronze": {
        "archivos_mensuales": 60,
        "registros": raw_count
    },

    "silver": {
        "registros": final_total,
        "claves_unicas_fecha_valor": final_claves,
        "duplicados_adicionales": duplicados_extra,
        "fecha_minima": str(fecha_min),
        "fecha_maxima": str(fecha_max),
        "anios": final_anios,
        "registros_por_anio": conteos_anio
    },

    "clave_negocio": [
        "fecha_cotizacion",
        "valor"
    ],

    "formato": "Parquet",
    "compresion": "Snappy",

    "hdfs": SILVER
}


with open(
    SUMMARY_LOCAL,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        summary,
        f,
        ensure_ascii=False,
        indent=2
    )


print("\n" + "=" * 78)
print("COTIZACIONES SILVER ACTUALIZADO CORRECTAMENTE")
print("=" * 78)

print(
    "Ruta HDFS:",
    SILVER
)

print(
    "Resumen:",
    SUMMARY_LOCAL
)

print("=" * 78)


spark.stop()
