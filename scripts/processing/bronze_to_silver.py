#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path
import json
import time


# ============================================================
# CONFIGURACIÓN
# ============================================================

BRONZE = "/datalake/bvl/bronze"
SILVER = "/datalake/bvl/silver"

PROJECT_ROOT = Path(__file__).resolve().parents[2]

QUALITY_OUTPUT = PROJECT_ROOT / "outputs" / "quality"
QUALITY_OUTPUT.mkdir(parents=True, exist_ok=True)


# ============================================================
# FUNCIONES GENERALES
# ============================================================

def read_smv_json(spark, path, record_field):
    """
    Los JSON de SMV tienen un objeto envolvente:
    ResponseCode / Message / Resultado o data.
    Se extrae el arreglo real de registros.
    """

    raw = (
        spark.read
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(path)
    )

    return (
        raw
        .select(
            F.explode(
                F.col(record_field)
            ).alias("record")
        )
        .select("record.*")
    )


def read_bcrp_csv(spark, path):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("recursiveFileLookup", "true")
        .csv(path)
    )


def normalize_timestamp(column):
    """
    Convierte fechas SMV con formato:
    28/03/2025 12:00:00 a. m.
    27/03/2025 05:57:33 p. m.
    """

    cleaned = (
        F.regexp_replace(column, r" a\. m\.", " AM")
    )

    cleaned = (
        F.regexp_replace(cleaned, r" p\. m\.", " PM")
    )

    return F.to_timestamp(
        cleaned,
        "dd/MM/yyyy hh:mm:ss a"
    )


def normalize_date(column):
    """Fecha simple dd/MM/yyyy."""

    return F.to_date(
        column,
        "dd/MM/yyyy"
    )

def normalize_smv_date(column):
    """
    Convierte fechas SMV como:
    28/03/2025 12:00:00 a. m.
    27/03/2025 05:57:33 p. m.
    a DateType: yyyy-MM-dd.
    """

    return F.to_date(
        F.substring(
            F.trim(column),
            1,
            10
        ),
        "dd/MM/yyyy"
    )

def normalize_bcrp_date(column):
    """
    Normaliza fechas BCRP como:
    02Ene97 -> 1997-01-02
    01Ene98 -> 1998-01-01
    27Set26 -> 2026-09-27

    Se traduce el mes español y se determina
    explícitamente el siglo del año.
    """

    c = F.trim(column)

    months = {
        "Ene": "01",
        "Feb": "02",
        "Mar": "03",
        "Abr": "04",
        "May": "05",
        "Jun": "06",
        "Jul": "07",
        "Ago": "08",
        "Set": "09",
        "Sep": "09",
        "Oct": "10",
        "Nov": "11",
        "Dic": "12",
    }

    day = F.substring(c, 1, 2)
    month_text = F.substring(c, 3, 3)
    year_2 = F.substring(c, 6, 2).cast("int")

    month_number = None

    for esp, number in months.items():
        condition = month_text == esp

        if month_number is None:
            month_number = F.when(condition, F.lit(number))
        else:
            month_number = month_number.when(
                condition,
                F.lit(number)
            )

    month_number = month_number.otherwise(F.lit(None))

    # Datos BCRP: 97-99 corresponden a 1997-1999;
    # 00-26 corresponden a 2000-2026.
    full_year = (
        F.when(
            year_2 >= 90,
            year_2 + 1900
        )
        .otherwise(year_2 + 2000)
    )

    normalized = F.concat(
        full_year.cast("string"),
        F.lit("-"),
        month_number,
        F.lit("-"),
        day
    )

    return F.to_date(normalized, "yyyy-MM-dd")

def write_silver(df, path, partitions=None):
    """
    Escritura Silver en Parquet + Snappy.
    """

    writer = (
        df.write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
    )

    if partitions:
        writer = writer.partitionBy(*partitions)

    writer.save(path)


def quality_record(
    dataset,
    input_rows,
    output_rows,
    duplicates_removed,
    invalid_rows,
    elapsed
):
    return {
        "dataset": dataset,
        "bronze_rows": input_rows,
        "silver_rows": output_rows,
        "duplicates_removed": duplicates_removed,
        "invalid_or_filtered_rows": invalid_rows,
        "processing_seconds": round(elapsed, 3),
    }


# ============================================================
# MERCADO DIARIO
# ============================================================

def process_cotizaciones(spark):

    start = time.time()

    path = f"{BRONZE}/smv/cotizaciones"

    df = read_smv_json(
        spark,
        path,
        "Resultado"
    )

    before = df.count()

    df = (
        df
        .withColumnRenamed("Valor", "valor")
        .withColumnRenamed("Descripcion", "descripcion")
        .withColumnRenamed("FechaCotizacion", "fecha_cotizacion")
        .withColumnRenamed("FechaAnterior", "fecha_anterior")
        .withColumnRenamed("FechaRegistro", "fecha_registro")
        .withColumnRenamed("Apertura_Actual", "apertura_actual")
        .withColumnRenamed("Cierre_Actual", "cierre_actual")
        .withColumnRenamed("Cierre_Anterior", "cierre_anterior")
        .withColumnRenamed("Maxima_Actual", "maxima_actual")
        .withColumnRenamed("Minima_Actual", "minima_actual")
        .withColumnRenamed("Monto_Negociado", "monto_negociado")
        .withColumnRenamed("Promedio_Actual", "promedio_actual")
    )

    df = (
        df
        .withColumn(
       	     "fecha_cotizacion",
             normalize_smv_date(F.col("fecha_cotizacion"))
         )
        .withColumn(
            "fecha_anterior",
            normalize_smv_date(F.col("fecha_anterior"))
         )
        .withColumn(
            "fecha_registro",
            normalize_smv_date(F.col("fecha_registro"))
         )
    )

 
    dedup = df.dropDuplicates()

    after_dedup = dedup.count()

    duplicates = before - after_dedup

    dedup = (
        dedup
        .withColumn(
            "anio",
            F.year("fecha_cotizacion")
        )
        .withColumn(
            "mes",
            F.month("fecha_cotizacion")
        )
    )

    write_silver(
        dedup,
        f"{SILVER}/mercado_diario/cotizaciones",
        ["anio", "mes"]
    )

    elapsed = time.time() - start

    return quality_record(
        "cotizaciones",
        before,
        after_dedup,
        duplicates,
        0,
        elapsed
    )


def process_indices(spark):

    start = time.time()

    df = read_smv_json(
        spark,
        f"{BRONZE}/smv/indices",
        "Resultado"
    )

    before = df.count()

    df = (
        df
        .withColumnRenamed("Fecha", "fecha")
        .withColumnRenamed("Clase", "clase")
        .withColumnRenamed("Tipo", "tipo")
        .withColumnRenamed("Indice", "indice")
        .withColumnRenamed("ClaseIndice", "clase_indice")
        .withColumnRenamed("anterior", "anterior")
        .withColumnRenamed("apertura", "apertura")
        .withColumnRenamed("maxima", "maxima")
        .withColumnRenamed("minima", "minima")
        .withColumnRenamed("ultimo", "ultimo")
        .withColumnRenamed("variacion", "variacion")
    )

    df = (
        df
        .withColumn(
            "fecha",
            normalize_smv_date(F.col("fecha"))
        )
    )

    dedup = df.dropDuplicates()

    after_dedup = dedup.count()

    duplicates = before - after_dedup

    dedup = (
        dedup
        .withColumn("anio", F.year("fecha"))
        .withColumn("mes", F.month("fecha"))
    )

    write_silver(
        dedup,
        f"{SILVER}/mercado_diario/indices",
        ["anio", "mes"]
    )

    return quality_record(
        "indices",
        before,
        after_dedup,
        duplicates,
        0,
        time.time() - start
    )


# ============================================================
# MACROECONOMÍA BCRP
# ============================================================

def process_tipo_cambio(spark):

    start = time.time()

    df = read_bcrp_csv(
        spark,
        f"{BRONZE}/bcrp/tipo_cambio"
    )

    before = df.count()

    # La fila descriptiva del BCRP tiene _c0 = NULL.
    df = (
        df
        .filter(F.col("_c0").isNotNull())
        .select(
            normalize_bcrp_date(
                F.col("_c0")
            ).alias("fecha"),

            F.when(
                F.trim(F.col("PD04638PD")).isin(
                    "n.d.",
                    "N.D.",
                    ""
                ),
                None
            )
            .otherwise(
                F.regexp_replace(
                    F.col("PD04638PD"),
                    ",",
                    "."
                )
            )
            .cast("double")
            .alias("tipo_cambio_venta")
        )
    )

    # Fecha inválida = registro inválido.
    df = df.filter(
        F.col("fecha").isNotNull()
    )

    dedup = df.dropDuplicates(["fecha"])

    after = dedup.count()

    removed = before - after

    dedup = (
        dedup
        .withColumn("anio", F.year("fecha"))
        .withColumn("mes", F.month("fecha"))
    )

    write_silver(
        dedup,
        f"{SILVER}/macroeconomia/tipo_cambio",
        ["anio", "mes"]
    )

    return quality_record(
        "tipo_cambio",
        before,
        after,
        0,
        removed,
        time.time() - start
    )


def process_embig(spark):

    start = time.time()

    df = read_bcrp_csv(
        spark,
        f"{BRONZE}/bcrp/embig"
    )

    before = df.count()

    df = (
        df
        .filter(F.col("_c0").isNotNull())
        .select(
            normalize_bcrp_date(
                F.col("_c0")
            ).alias("fecha"),

            F.regexp_replace(
                F.col("PD04709XD"),
                ",",
                "."
            )
            .cast("double")
            .alias("embig_peru")
        )
    )

    df = df.filter(
        F.col("fecha").isNotNull()
    )

    dedup = df.dropDuplicates(["fecha"])

    after = dedup.count()

    removed = before - after

    dedup = (
        dedup
        .withColumn("anio", F.year("fecha"))
        .withColumn("mes", F.month("fecha"))
    )

    write_silver(
        dedup,
        f"{SILVER}/macroeconomia/embig",
        ["anio", "mes"]
    )

    return quality_record(
        "embig",
        before,
        after,
        0,
        removed,
        time.time() - start
    )


# ============================================================
# FINANZAS EMPRESARIALES
# ============================================================

def clean_financial_common(df):

    rename_map = {
        "CIIU": "ciiu",
        "Cuenta": "cuenta",
        "DescripcionCuenta": "descripcion_cuenta",
        "Ejercicio": "ejercicio",
        "FechaRegistro": "fecha_registro",
        "MetodoFlujoEfectivo": "metodo_flujo_efectivo",
        "Moneda": "moneda",
        "NombreEmpresa": "nombre_empresa",
        "RPJ": "rpj",
        "RUC": "ruc",
        "TipoEmpresa": "tipo_empresa",
        "TipoInformacion": "tipo_informacion",
        "TipoSector": "tipo_sector",
        "Trimestre": "trimestre",
        "Monto1": "monto1",
        "Monto2": "monto2",
        "Monto3": "monto3",
        "Monto4": "monto4",
    }

    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    if "fecha_registro" in df.columns:
        df = df.withColumn(
            "fecha_registro",
            normalize_date(
                F.col("fecha_registro")
            )
        )

    if "ejercicio" in df.columns:
        df = df.withColumn(
            "ejercicio",
            F.col("ejercicio").cast("int")
        )

    if "ruc" in df.columns:
        df = df.withColumn(
            "ruc",
            F.trim(F.col("ruc"))
        )

    if "rpj" in df.columns:
        df = df.withColumn(
            "rpj",
            F.trim(F.col("rpj"))
        )

    return df


def process_financial(
    spark,
    dataset,
    record_field,
    extra_renames=None
):

    start = time.time()

    path = f"{BRONZE}/smv/{dataset}"

    df = read_smv_json(
        spark,
        path,
        record_field
    )

    before = df.count()

    df = clean_financial_common(df)

    if extra_renames:

        for old, new in extra_renames.items():

            if old in df.columns:

                df = df.withColumnRenamed(
                    old,
                    new
                )

    dedup = df.dropDuplicates()

    after = dedup.count()

    duplicates = before - after

    write_silver(
        dedup,
        f"{SILVER}/finanzas_empresariales/{dataset}",
        ["ejercicio"]
        if "ejercicio" in dedup.columns
        else None
    )

    return quality_record(
        dataset,
        before,
        after,
        duplicates,
        0,
        time.time() - start
    )


# ============================================================
# MAIN
# ============================================================

def main():

    spark = (
        SparkSession.builder
        .appName("BVL-Bronze-To-Silver")
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 75)
    print(" BVL ANALYTICS - BRONZE TO SILVER")
    print("=" * 75)

    results = []

    processors = [
        ("cotizaciones", lambda: process_cotizaciones(spark)),
        ("indices", lambda: process_indices(spark)),
        ("tipo_cambio", lambda: process_tipo_cambio(spark)),
        ("embig", lambda: process_embig(spark)),

        (
            "estado_resultados",
            lambda: process_financial(
                spark,
                "estado_resultados",
                "data"
            )
        ),

        (
            "situacion_financiera",
            lambda: process_financial(
                spark,
                "situacion_financiera",
                "data"
            )
        ),

        (
            "flujo_efectivo",
            lambda: process_financial(
                spark,
                "flujo_efectivo",
                "data"
            )
        ),

        (
            "cambios_patrimonio",
            lambda: process_financial(
                spark,
                "cambios_patrimonio",
                "data",
                {
                    "DescripcionColumna":
                        "descripcion_columna",

                    "OrdenColumna":
                        "orden_columna",
                }
            )
        ),
    ]

    for name, processor in processors:

        print()
        print("=" * 75)
        print(f"PROCESANDO: {name}")
        print("=" * 75)

        try:

            result = processor()

            results.append(result)

            print(
                f"[SUCCESS] "
                f"{result['bronze_rows']:,} → "
                f"{result['silver_rows']:,}"
            )

        except Exception as exc:

            print(f"[ERROR] {name}: {exc}")

            results.append({
                "dataset": name,
                "status": "ERROR",
                "error": str(exc),
            })

    # --------------------------------------------------------
    # REPORTE
    # --------------------------------------------------------

    report = (
        QUALITY_OUTPUT /
        "silver_processing_summary.json"
    )

    with open(
        report,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            results,
            file,
            ensure_ascii=False,
            indent=2
        )

    print()
    print("=" * 75)
    print(" RESUMEN SILVER")
    print("=" * 75)

    for result in results:

        if result.get("status") == "ERROR":

            print(
                f"{result['dataset']:<25} ERROR"
            )

        else:

            print(
                f"{result['dataset']:<25} "
                f"{result['bronze_rows']:>8,} → "
                f"{result['silver_rows']:>8,}"
            )

    print()
    print(f"Reporte: {report}")
    print("=" * 75)

    spark.stop()


if __name__ == "__main__":
    main()

