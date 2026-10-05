import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURACION
# ============================================================

FECHA_INGESTA = "2026-10-04"

BRONZE = (
    "/datalake/bvl/bronze/smv/montos_sab/"
    f"ingestion_date={FECHA_INGESTA}/montos_sab_*.json"
)

SILVER = (
    "/datalake/bvl/silver/mercado_diario/montos_sab"
)

SUMMARY_LOCAL = Path(
    "outputs/quality/montos_sab_silver_summary.json"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("BVL-Montos-SAB-Silver")
    .config("spark.sql.shuffle.partitions", "4")
    .config(
        "spark.sql.parquet.compression.codec",
        "snappy"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 1. LEER LOS JSON DIARIOS
# ============================================================

print("=" * 78)
print("MONTOS INTERMEDIADOS SAB - BRONZE -> SILVER")
print("=" * 78)

wrappers = (
    spark.read
    .option("multiLine", "true")
    .json(BRONZE)
)

archivos_wrapper = wrappers.count()

print(
    "Archivos/wrappers leidos :",
    f"{archivos_wrapper:,}"
)

print(
    "Columnas raiz            :",
    wrappers.columns
)

if archivos_wrapper != 1221:
    spark.stop()
    raise RuntimeError(
        f"Se esperaban 1,221 archivos y se obtuvieron "
        f"{archivos_wrapper:,}"
    )


# ============================================================
# 2. IDENTIFICAR EL ARRAY DE REGISTROS
# ============================================================

candidatos = [
    "Resultado",
    "resultado",
    "data",
    "Data",
    "result",
    "value"
]

campo_array = None

for candidato in candidatos:
    if candidato in wrappers.columns:
        campo_array = candidato
        break

if campo_array is None:
    spark.stop()
    raise RuntimeError(
        "No se encontro un array de registros conocido "
        f"en las columnas raiz: {wrappers.columns}"
    )

print(
    "Array de registros       :",
    campo_array
)

sab = (
    wrappers
    .select(
        F.explode(
            F.col(campo_array)
        ).alias("registro")
    )
    .select("registro.*")
)


raw_count = sab.count()

print(
    "Registros Bronze         :",
    f"{raw_count:,}"
)

print("\nCOLUMNAS FUENTE")

for c in sab.columns:
    print(" -", c)


# ============================================================
# 3. VALIDAR COLUMNAS DE NEGOCIO
# ============================================================

requeridas = [
    "_FechaNegociacionConsulta",
    "CodigoSAB"
]

faltantes = [
    c
    for c in requeridas
    if c not in sab.columns
]

if faltantes:
    spark.stop()
    raise RuntimeError(
        "Faltan columnas requeridas: "
        + ", ".join(faltantes)
    )


# ============================================================
# 4. NORMALIZACION
# ============================================================

silver = (
    sab

    .withColumn(
        "fecha_negociacion",
        F.coalesce(
            F.to_date(
                F.substring(
                    F.col("_FechaNegociacionConsulta"),
                    1,
                    10
                ),
                "dd/MM/yyyy"
            ),
            F.to_date(
                F.substring(
                    F.col("_FechaNegociacionConsulta"),
                    1,
                    10
                ),
                "yyyy-MM-dd"
            )
        )
    )

    .withColumn(
        "codigo_sab",
        F.trim(
            F.col("CodigoSAB").cast("string")
        )
    )

    .withColumn(
        "anio",
        F.year(
            F.col("fecha_negociacion")
        )
    )

    .withColumn(
        "mes",
        F.month(
            F.col("fecha_negociacion")
        )
    )

    .withColumn(
        "fuente_datos",
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
# 5. DATA QUALITY
# ============================================================

print("\n" + "=" * 78)
print("VALIDACION PREVIA")
print("=" * 78)

total = silver.count()

fecha_nula = (
    silver
    .filter(
        F.col("fecha_negociacion").isNull()
    )
    .count()
)

codigo_nulo = (
    silver
    .filter(
        F.col("codigo_sab").isNull()
        |
        (F.col("codigo_sab") == "")
    )
    .count()
)

sab_distintas = (
    silver
    .select("codigo_sab")
    .distinct()
    .count()
)

claves_unicas = (
    silver
    .select(
        "fecha_negociacion",
        "codigo_sab"
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
        F.min("fecha_negociacion")
    )
    .first()[0]
)

fecha_max = (
    silver
    .agg(
        F.max("fecha_negociacion")
    )
    .first()[0]
)


print(
    "Registros totales        :",
    f"{total:,}"
)

print(
    "Fecha consulta nula      :",
    f"{fecha_nula:,}"
)

print(
    "CodigoSAB vacio/nulo     :",
    f"{codigo_nulo:,}"
)

print(
    "SAB distintas            :",
    f"{sab_distintas:,}"
)

print(
    "Claves unicas            :",
    f"{claves_unicas:,}"
)

print(
    "Duplicados adicionales   :",
    f"{duplicados_extra:,}"
)

print(
    "Fecha minima             :",
    fecha_min
)

print(
    "Fecha maxima             :",
    fecha_max
)


# ============================================================
# 6. DISTRIBUCION POR AÑO
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
    2021: 4887,
    2022: 4454,
    2023: 4359,
    2024: 4149,
    2025: 4307
}


# ============================================================
# 7. VALIDACIONES CRITICAS
# ============================================================

errores = []

if total != 22156:
    errores.append(
        f"Total esperado 22,156; obtenido {total:,}"
    )

if fecha_nula != 0:
    errores.append(
        f"Fecha consulta nula: {fecha_nula:,}"
    )

if codigo_nulo != 0:
    errores.append(
        f"CodigoSAB vacio/nulo: {codigo_nulo:,}"
    )

if sab_distintas != 22:
    errores.append(
        f"SAB distintas esperadas 22; "
        f"obtenidas {sab_distintas:,}"
    )

if claves_unicas != 22156:
    errores.append(
        "fecha_negociacion + CodigoSAB "
        f"no es unica: {claves_unicas:,}"
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
            f"{anio}: esperado {cantidad:,}; "
            f"obtenido {obtenido:,}"
        )


if errores:

    print("\n" + "!" * 78)
    print("VALIDACION FALLIDA")
    print("SILVER NO SERA ESCRITO")
    print("!" * 78)

    for error in errores:
        print(" -", error)

    spark.stop()
    raise SystemExit(1)


print()
print("VALIDACIONES PREVIAS: OK")


# ============================================================
# 8. ESCRIBIR PARQUET SILVER
# ============================================================

print("\n" + "=" * 78)
print("ESCRIBIENDO SILVER MONTOS SAB")
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
# 9. VALIDACION POSTERIOR
# ============================================================

silver_final = (
    spark.read
    .parquet(SILVER)
)

final_total = silver_final.count()

final_claves = (
    silver_final
    .select(
        "fecha_negociacion",
        "codigo_sab"
    )
    .distinct()
    .count()
)

final_anios = [
    int(r["anio"])
    for r in (
        silver_final
        .select("anio")
        .distinct()
        .orderBy("anio")
        .collect()
    )
]


print(
    "Registros Silver         :",
    f"{final_total:,}"
)

print(
    "Claves unicas Silver     :",
    f"{final_claves:,}"
)

print(
    "Años Silver              :",
    final_anios
)


if (
    final_total != 22156
    or
    final_claves != 22156
    or
    final_anios != [2021, 2022, 2023, 2024, 2025]
):

    spark.stop()

    raise RuntimeError(
        "Fallo la validacion posterior de Silver."
    )


# ============================================================
# 10. RESUMEN PARA REPOSITORIO
# ============================================================

SUMMARY_LOCAL.parent.mkdir(
    parents=True,
    exist_ok=True
)

summary = {
    "fuente": "SMV",
    "dataset": "Montos Intermediados SAB",
    "fecha_ingesta": FECHA_INGESTA,

    "bronze": {
        "archivos_diarios": archivos_wrapper,
        "registros": raw_count
    },

    "silver": {
        "registros": final_total,
        "sab_distintas": sab_distintas,
        "claves_unicas_fecha_codigo_sab": final_claves,
        "duplicados_adicionales": duplicados_extra,
        "fecha_minima": str(fecha_min),
        "fecha_maxima": str(fecha_max),
        "anios": final_anios,
        "registros_por_anio": conteos_anio
    },

    "clave_negocio": [
        "fecha_negociacion",
        "codigo_sab"
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
print("MONTOS SAB SILVER COMPLETADO CORRECTAMENTE")
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
