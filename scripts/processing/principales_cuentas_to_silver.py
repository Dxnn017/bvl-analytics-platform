import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURACION
# ============================================================

FECHA_INGESTA = "2026-10-04"

BRONZE = (
    "/datalake/bvl/bronze/smv/principales_cuentas/"
    f"ingestion_date={FECHA_INGESTA}/principales_*.json"
)

SILVER = (
    "/datalake/bvl/silver/finanzas_empresariales/"
    "principales_cuentas"
)

SUMMARY_LOCAL = Path(
    "outputs/quality/principales_cuentas_silver_summary.json"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("BVL-Principales-Cuentas-Silver")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 1. LEER BRONZE
# ============================================================

print("=" * 78)
print("PRINCIPALES CUENTAS FINANCIERAS SMV - BRONZE -> SILVER")
print("=" * 78)

entrada = (
    spark.read
    .option("multiLine", "true")
    .json(BRONZE)
)

# Cantidad real de archivos físicos leídos por Spark
archivos = len(entrada.inputFiles())

print(f"Archivos JSON leidos      : {archivos:,}")
print(f"Columnas detectadas       : {entrada.columns}")

if archivos != 20:
    spark.stop()
    raise RuntimeError(
        f"Se esperaban 20 archivos y se obtuvieron {archivos:,}"
    )


# ============================================================
# 2. IDENTIFICAR ESTRUCTURA DE LOS JSON
# ============================================================

# Esta fuente histórica puede venir:
# A) directamente como lista de registros, o
# B) envuelta en Resultado/data/etc.
#
# En nuestros 20 archivos históricos Spark ya entrega
# directamente las filas financieras.

columnas_registro = {
    "RUC",
    "RPJ",
    "Ejercicio",
    "Trimestre",
    "ActivoTotal"
}

if columnas_registro.issubset(set(entrada.columns)):

    print("Estructura JSON           : registros directos")
    raw = entrada

else:

    candidatos = [
        "Resultado",
        "resultado",
        "data",
        "Data",
        "result",
        "value"
    ]

    campo_array = next(
        (
            c
            for c in candidatos
            if c in entrada.columns
        ),
        None
    )

    if campo_array is None:
        spark.stop()
        raise RuntimeError(
            "No se reconoce la estructura JSON. "
            f"Columnas: {entrada.columns}"
        )

    print(
        "Estructura JSON           : wrapper ->",
        campo_array
    )

    raw = (
        entrada
        .select(
            F.explode(
                F.col(campo_array)
            ).alias("registro")
        )
        .select("registro.*")
    )


raw_count = raw.count()

print(f"Registros Bronze          : {raw_count:,}")

if raw_count != 5249:
    spark.stop()
    raise RuntimeError(
        f"Se esperaban 5,249 registros y se obtuvieron "
        f"{raw_count:,}"
    )


# ============================================================
# 3. VALIDAR COLUMNAS
# ============================================================

COLUMNAS_ORIGINALES = [
    "RPJ",
    "RUC",
    "NombreEmpresa",
    "TipoEmpresa",
    "TipoSector",
    "CIIU",
    "Ejercicio",
    "Trimestre",
    "TipoInformacion",
    "MetodoFlujoEfectivo",
    "Moneda",
    "ActivoTotal",
    "PatrimonioTotal",
    "TotalIngreso",
    "UtilidadNeta",
    "PasivoTotal"
]

faltantes = [
    c for c in COLUMNAS_ORIGINALES
    if c not in raw.columns
]

if faltantes:
    spark.stop()
    raise RuntimeError(
        "Faltan columnas: " + ", ".join(faltantes)
    )


# ============================================================
# 4. NORMALIZACION
# ============================================================

silver = (
    raw
    .select(
        F.trim(F.col("RPJ").cast("string")).alias("rpj"),
        F.trim(F.col("RUC").cast("string")).alias("ruc"),
        F.trim(F.col("NombreEmpresa").cast("string"))
            .alias("nombre_empresa"),
        F.trim(F.col("TipoEmpresa").cast("string"))
            .alias("tipo_empresa"),
        F.trim(F.col("TipoSector").cast("string"))
            .alias("tipo_sector"),
        F.trim(F.col("CIIU").cast("string")).alias("ciiu"),

        F.col("Ejercicio").cast("int").alias("ejercicio"),

        F.trim(F.col("Trimestre").cast("string"))
            .alias("trimestre"),

        F.trim(F.col("TipoInformacion").cast("string"))
            .alias("tipo_informacion"),

        F.trim(F.col("MetodoFlujoEfectivo").cast("string"))
            .alias("metodo_flujo_efectivo"),

        F.trim(F.col("Moneda").cast("string")).alias("moneda"),

        F.col("ActivoTotal").cast("double")
            .alias("activo_total"),

        F.col("PatrimonioTotal").cast("double")
            .alias("patrimonio_total"),

        F.col("TotalIngreso").cast("double")
            .alias("total_ingreso"),

        F.col("UtilidadNeta").cast("double")
            .alias("utilidad_neta"),

        F.col("PasivoTotal").cast("double")
            .alias("pasivo_total")
    )

    .withColumn(
        "trimestre_num",
        F.regexp_extract(
            F.col("trimestre"),
            r"^([1-4])",
            1
        ).cast("int")
    )

    .withColumn(
        "ruc_valido",
        (
            F.col("ruc").rlike(r"^[0-9]{11}$")
            &
            (F.col("ruc") != "00000000000")
            &
            (F.col("ruc") != "0")
        )
    )

    .withColumn(
        "fuente_datos",
        F.lit("SMV")
    )

    .withColumn(
        "fecha_ingesta",
        F.to_date(F.lit(FECHA_INGESTA))
    )
)


# ============================================================
# 5. CLAVE DE NEGOCIO
# ============================================================

CAMPOS_CLAVE = [
    "ruc",
    "ejercicio",
    "trimestre",
    "tipo_informacion",
    "rpj",
    "metodo_flujo_efectivo",
    "moneda"
]

silver = silver.withColumn(
    "clave_negocio",
    F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(
                    F.col(c).cast("string"),
                    F.lit("<NULL>")
                )
                for c in CAMPOS_CLAVE
            ]
        ),
        256
    )
)


# ============================================================
# 6. ID TECNICO DE FILA COMPLETA
# ============================================================

CAMPOS_FILA = [
    "rpj",
    "ruc",
    "nombre_empresa",
    "tipo_empresa",
    "tipo_sector",
    "ciiu",
    "ejercicio",
    "trimestre",
    "tipo_informacion",
    "metodo_flujo_efectivo",
    "moneda",
    "activo_total",
    "patrimonio_total",
    "total_ingreso",
    "utilidad_neta",
    "pasivo_total"
]

silver = silver.withColumn(
    "id_registro",
    F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(
                    F.col(c).cast("string"),
                    F.lit("<NULL>")
                )
                for c in CAMPOS_FILA
            ]
        ),
        256
    )
)


# ============================================================
# 7. DETECTAR COLISIONES
# ============================================================

conteo_claves = (
    silver
    .groupBy("clave_negocio")
    .count()
    .withColumnRenamed(
        "count",
        "cantidad_clave_negocio"
    )
)

silver = (
    silver
    .join(
        conteo_claves,
        on="clave_negocio",
        how="left"
    )
    .withColumn(
        "colision_clave_negocio",
        F.col("cantidad_clave_negocio") > 1
    )
)


# ============================================================
# 8. DATA QUALITY
# ============================================================

print("\n" + "=" * 78)
print("VALIDACION PREVIA")
print("=" * 78)

total = silver.count()

claves_negocio = (
    silver
    .select("clave_negocio")
    .distinct()
    .count()
)

grupos_colision = (
    conteo_claves
    .filter(
        F.col("cantidad_clave_negocio") > 1
    )
    .count()
)

registros_extra = total - claves_negocio

ids_tecnicos = (
    silver
    .select("id_registro")
    .distinct()
    .count()
)

duplicados_exactos = total - ids_tecnicos

ruc_invalidos = (
    silver
    .filter(~F.col("ruc_valido"))
    .count()
)

ruc_validos_distintos = (
    silver
    .filter(F.col("ruc_valido"))
    .select("ruc")
    .distinct()
    .count()
)

empresas_distintas = (
    silver
    .select("nombre_empresa")
    .distinct()
    .count()
)


print(f"Registros totales           : {total:,}")
print(f"Claves negocio distintas    : {claves_negocio:,}")
print(f"Grupos con colision         : {grupos_colision:,}")
print(f"Registros extra por colision: {registros_extra:,}")
print(f"IDs tecnicos distintos      : {ids_tecnicos:,}")
print(f"Duplicados exactos          : {duplicados_exactos:,}")
print(f"RUC invalidos               : {ruc_invalidos:,}")
print(f"RUC validos distintos       : {ruc_validos_distintos:,}")
print(f"Empresas distintas          : {empresas_distintas:,}")


# ============================================================
# 9. DISTRIBUCIONES
# ============================================================

print("\nREGISTROS POR EJERCICIO")

por_anio = (
    silver
    .groupBy("ejercicio")
    .count()
    .orderBy("ejercicio")
)

por_anio.show(20, truncate=False)

print("\nREGISTROS POR TRIMESTRE")

por_trimestre = (
    silver
    .groupBy("trimestre_num")
    .count()
    .orderBy("trimestre_num")
)

por_trimestre.show(10, truncate=False)


conteos_anio = {
    int(r["ejercicio"]): int(r["count"])
    for r in por_anio.collect()
}

conteos_tri = {
    int(r["trimestre_num"]): int(r["count"])
    for r in por_trimestre.collect()
}


esperado_anio = {
    2021: 1080,
    2022: 1065,
    2023: 1049,
    2024: 1029,
    2025: 1026
}

esperado_tri = {
    1: 1318,
    2: 1312,
    3: 1310,
    4: 1309
}


# ============================================================
# 10. VALIDACIONES CRITICAS
# ============================================================

errores = []

if total != 5249:
    errores.append(
        f"Total esperado 5,249; obtenido {total:,}"
    )

if claves_negocio != 5246:
    errores.append(
        f"Claves esperadas 5,246; obtenidas {claves_negocio:,}"
    )

if grupos_colision != 3:
    errores.append(
        f"Grupos con colision esperados 3; "
        f"obtenidos {grupos_colision:,}"
    )

if registros_extra != 3:
    errores.append(
        f"Registros extra esperados 3; "
        f"obtenidos {registros_extra:,}"
    )

if ids_tecnicos != 5249:
    errores.append(
        f"IDs tecnicos unicos esperados 5,249; "
        f"obtenidos {ids_tecnicos:,}"
    )

if duplicados_exactos != 0:
    errores.append(
        f"Duplicados exactos encontrados: {duplicados_exactos:,}"
    )

if ruc_invalidos != 85:
    errores.append(
        f"RUC invalidos esperados 85; obtenidos {ruc_invalidos:,}"
    )

if ruc_validos_distintos != 289:
    errores.append(
        f"RUC validos distintos esperados 289; "
        f"obtenidos {ruc_validos_distintos:,}"
    )

for anio, esperado in esperado_anio.items():
    obtenido = conteos_anio.get(anio, 0)

    if obtenido != esperado:
        errores.append(
            f"{anio}: esperado {esperado:,}; "
            f"obtenido {obtenido:,}"
        )

for tri, esperado in esperado_tri.items():
    obtenido = conteos_tri.get(tri, 0)

    if obtenido != esperado:
        errores.append(
            f"Trimestre {tri}: esperado {esperado:,}; "
            f"obtenido {obtenido:,}"
        )


if errores:

    print("\n" + "!" * 78)
    print("VALIDACION FALLIDA")
    print("SILVER NO SERA ESCRITO")
    print("!" * 78)

    for e in errores:
        print(" -", e)

    spark.stop()
    raise SystemExit(1)


print("\nVALIDACIONES PREVIAS: OK")


# ============================================================
# 11. ESCRITURA SILVER
# ============================================================

print("\n" + "=" * 78)
print("ESCRIBIENDO SILVER PRINCIPALES CUENTAS")
print("=" * 78)

(
    silver
    .write
    .mode("overwrite")
    .partitionBy(
        "ejercicio",
        "trimestre_num"
    )
    .parquet(SILVER)
)


# ============================================================
# 12. VALIDACION POSTERIOR
# ============================================================

final = spark.read.parquet(SILVER)

final_total = final.count()

final_ids = (
    final
    .select("id_registro")
    .distinct()
    .count()
)

final_claves = (
    final
    .select("clave_negocio")
    .distinct()
    .count()
)

print(f"Registros Silver          : {final_total:,}")
print(f"IDs tecnicos Silver       : {final_ids:,}")
print(f"Claves negocio Silver     : {final_claves:,}")


if (
    final_total != 5249
    or final_ids != 5249
    or final_claves != 5246
):
    spark.stop()
    raise RuntimeError(
        "Fallo la validacion posterior de Silver."
    )


# ============================================================
# 13. RESUMEN
# ============================================================

SUMMARY_LOCAL.parent.mkdir(
    parents=True,
    exist_ok=True
)

summary = {
    "fuente": "SMV",
    "dataset": "Principales Cuentas Financieras",
    "fecha_ingesta": FECHA_INGESTA,

    "bronze": {
        "archivos": archivos,
        "registros": raw_count
    },

    "silver": {
        "registros": final_total,
        "ids_tecnicos_unicos": final_ids,
        "claves_negocio_distintas": final_claves,
        "grupos_colision": grupos_colision,
        "registros_extra_por_colision": registros_extra,
        "duplicados_exactos": duplicados_exactos,
        "ruc_invalidos": ruc_invalidos,
        "ruc_validos_distintos": ruc_validos_distintos,
        "empresas_distintas": empresas_distintas,
        "registros_por_anio": conteos_anio,
        "registros_por_trimestre": conteos_tri
    },

    "clave_negocio_base": CAMPOS_CLAVE,

    "nota_clave": (
        "La clave de negocio presenta 3 colisiones reales "
        "de la fuente. No se eliminan registros."
    ),

    "identificador_tecnico": "id_registro",

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
print("PRINCIPALES CUENTAS SILVER COMPLETADO CORRECTAMENTE")
print("=" * 78)

print("Ruta HDFS:", SILVER)
print("Resumen:", SUMMARY_LOCAL)

print("=" * 78)

spark.stop()
