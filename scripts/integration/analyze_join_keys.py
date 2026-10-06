#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path
import json


# ============================================================
# CONFIGURACIÓN
# ============================================================

SILVER = "/datalake/bvl/silver"

PROJECT_ROOT = Path(__file__).resolve().parents[2]

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "quality"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def normalize_company_name(column):
    """
    Normalización EXPLORATORIA de nombres empresariales.

    No constituye todavía la clave definitiva.
    Solo sirve para medir qué tan viable sería relacionar
    Descripcion (cotizaciones) con NombreEmpresa (SMV financiero).
    """

    c = F.upper(F.trim(column))

    # Eliminar signos y espacios repetidos.
    c = F.regexp_replace(c, r"[^A-Z0-9 ]", " ")
    c = F.regexp_replace(c, r"\s+", " ")

    return F.trim(c)


def date_stats(df, column):
    row = (
        df
        .agg(
            F.min(column).alias("min"),
            F.max(column).alias("max")
        )
        .first()
    )

    return {
        "min": str(row["min"]) if row["min"] else None,
        "max": str(row["max"]) if row["max"] else None,
    }


def print_title(text):
    print()
    print("=" * 80)
    print(text)
    print("=" * 80)


# ============================================================
# MAIN
# ============================================================

def main():

    spark = (
        SparkSession.builder
        .appName("BVL-Integration-Analysis")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # --------------------------------------------------------
    # LECTURA SILVER
    # --------------------------------------------------------

    cot = spark.read.parquet(
        f"{SILVER}/mercado_diario/cotizaciones"
    )

    indices = spark.read.parquet(
        f"{SILVER}/mercado_diario/indices"
    )

    tc = spark.read.parquet(
        f"{SILVER}/macroeconomia/tipo_cambio"
    )

    embig = spark.read.parquet(
        f"{SILVER}/macroeconomia/embig"
    )

    er = spark.read.parquet(
        f"{SILVER}/finanzas_empresariales/estado_resultados"
    )

    sf = spark.read.parquet(
        f"{SILVER}/finanzas_empresariales/situacion_financiera"
    )

    fe = spark.read.parquet(
        f"{SILVER}/finanzas_empresariales/flujo_efectivo"
    )

    cp = spark.read.parquet(
        f"{SILVER}/finanzas_empresariales/cambios_patrimonio"
    )

    report = {}

    # ========================================================
    # 1. COBERTURA TEMPORAL
    # ========================================================

    print_title("1. COBERTURA TEMPORAL")

    temporal = {
        "cotizaciones":
            date_stats(cot, "fecha_cotizacion"),

        "indices":
            date_stats(indices, "fecha"),

        "tipo_cambio":
            date_stats(tc, "fecha"),

        "embig":
            date_stats(embig, "fecha"),
    }

    for dataset, values in temporal.items():
        print(
            f"{dataset:<20} "
            f"{values['min']} -> {values['max']}"
        )

    report["temporal"] = temporal

    # ========================================================
    # 2. FECHAS DE MERCADO
    # ========================================================

    print_title("2. FECHAS ÚNICAS DEL MERCADO")

    cot_dates = (
        cot
        .select(
            F.to_date("fecha_cotizacion").alias("fecha")
        )
        .distinct()
    )

    ind_dates = (
        indices
        .select("fecha")
        .distinct()
    )

    tc_dates = (
        tc
        .select("fecha")
        .distinct()
    )

    embig_dates = (
        embig
        .select("fecha")
        .distinct()
    )

    cot_date_count = cot_dates.count()

    print(
        "Fechas distintas Cotizaciones:",
        cot_date_count
    )

    # ========================================================
    # 3. COBERTURA DE JOIN DIARIO
    # ========================================================

    print_title("3. COBERTURA DE JOIN POR FECHA")

    def coverage(target, target_name):

        matched = (
            cot_dates
            .join(
                target,
                ["fecha"],
                "inner"
            )
            .count()
        )

        percentage = (
            matched / cot_date_count * 100
            if cot_date_count
            else 0
        )

        print(
            f"{target_name:<20} "
            f"{matched:>5}/{cot_date_count:<5} "
            f"({percentage:.2f}%)"
        )

        return {
            "matched_dates": matched,
            "cotizacion_dates": cot_date_count,
            "coverage_percent":
                round(percentage, 2)
        }

    coverage_results = {
        "indices":
            coverage(ind_dates, "Índices"),

        "tipo_cambio":
            coverage(tc_dates, "Tipo Cambio"),

        "embig":
            coverage(embig_dates, "EMBIG"),
    }

    report["daily_join_coverage"] = coverage_results

    # ========================================================
    # 4. COTIZACIONES: VALORES / DESCRIPCIONES
    # ========================================================

    print_title("4. VALORES COTIZADOS")

    distinct_values = (
        cot
        .select(
            "valor",
            "descripcion"
        )
        .distinct()
    )

    value_count = distinct_values.count()

    company_desc_count = (
        cot
        .select("descripcion")
        .distinct()
        .count()
    )

    print("Combinaciones Valor-Descripción:", value_count)
    print("Descripciones distintas:", company_desc_count)

    distinct_values.orderBy(
        "descripcion",
        "valor"
    ).show(
        50,
        truncate=False
    )

    report["cotizaciones"] = {
        "distinct_value_description":
            value_count,

        "distinct_descriptions":
            company_desc_count,
    }

    # ========================================================
    # 5. FINANZAS: AÑOS Y EMPRESAS
    # ========================================================

    print_title("5. PERIODOS FINANCIEROS")

    financials = {
        "estado_resultados": er,
        "situacion_financiera": sf,
        "flujo_efectivo": fe,
        "cambios_patrimonio": cp,
    }

    financial_report = {}

    for name, df in financials.items():

        years = [
            row["ejercicio"]
            for row in (
                df
                .select("ejercicio")
                .distinct()
                .orderBy("ejercicio")
                .collect()
            )
        ]

        rucs = (
            df.select("ruc")
            .where(F.col("ruc").isNotNull())
            .distinct()
            .count()
            if "ruc" in df.columns
            else 0
        )

        rpjs = (
            df.select("rpj")
            .where(F.col("rpj").isNotNull())
            .distinct()
            .count()
            if "rpj" in df.columns
            else 0
        )

        companies = (
            df.select("nombre_empresa")
            .where(
                F.col("nombre_empresa").isNotNull()
            )
            .distinct()
            .count()
            if "nombre_empresa" in df.columns
            else 0
        )

        print()
        print(name)
        print("  Ejercicios:", years)
        print("  Empresas:", companies)
        print("  RUC:", rucs)
        print("  RPJ:", rpjs)

        financial_report[name] = {
            "years": years,
            "companies": companies,
            "distinct_ruc": rucs,
            "distinct_rpj": rpjs,
        }

    report["financial_periods"] = financial_report

    # ========================================================
    # 6. CANDIDATO DE CLAVE FINANCIERA
    # ========================================================

    print_title(
        "6. VALIDACIÓN DE CLAVE COMPUESTA FINANCIERA"
    )

    key_results = {}

    for name, df in financials.items():

        candidate = [
            col
            for col in [
                "ruc",
                "ejercicio",
                "trimestre",
                "cuenta",
                "tipo_informacion"
            ]
            if col in df.columns
        ]

        total = df.count()

        unique_keys = (
            df
            .select(*candidate)
            .dropDuplicates()
            .count()
        )

        duplicate_keys = total - unique_keys

        print(
            f"{name:<25} "
            f"filas={total:<8} "
            f"claves_unicas={unique_keys:<8} "
            f"repeticiones={duplicate_keys}"
        )

        key_results[name] = {
            "candidate_key": candidate,
            "rows": total,
            "unique_keys": unique_keys,
            "duplicate_key_rows":
                duplicate_keys,
        }

    report["financial_candidate_keys"] = key_results

    # ========================================================
    # 7. EMPRESAS VS DESCRIPCIÓN DE COTIZACIÓN
    # ========================================================

    print_title(
        "7. COBERTURA EXPLORATORIA VALOR ↔ EMPRESA"
    )

    cot_companies = (
        cot
        .select(
            "valor",
            "descripcion"
        )
        .distinct()
        .withColumn(
            "empresa_normalizada",
            normalize_company_name(
                F.col("descripcion")
            )
        )
    )

    financial_companies = (
        sf
        .select(
            "ruc",
            "rpj",
            "nombre_empresa"
        )
        .distinct()
        .withColumn(
            "empresa_normalizada",
            normalize_company_name(
                F.col("nombre_empresa")
            )
        )
    )

    possible_bridge = (
        cot_companies
        .join(
            financial_companies,
            ["empresa_normalizada"],
            "left"
        )
    )

    total_values = (
        cot_companies
        .select("valor")
        .distinct()
        .count()
    )

    matched_values = (
        possible_bridge
        .where(
            F.col("ruc").isNotNull()
            | F.col("rpj").isNotNull()
        )
        .select("valor")
        .distinct()
        .count()
    )

    bridge_percentage = (
        matched_values / total_values * 100
        if total_values
        else 0
    )

    print("Valores distintos:", total_values)
    print(
        "Valores con coincidencia empresarial:",
        matched_values
    )
    print(
        f"Cobertura exploratoria: "
        f"{bridge_percentage:.2f}%"
    )

    print()
    print("VALORES SIN COINCIDENCIA:")

    (
        possible_bridge
        .where(
            F.col("ruc").isNull()
            & F.col("rpj").isNull()
        )
        .select(
            "valor",
            "descripcion"
        )
        .distinct()
        .orderBy("descripcion")
        .show(
            100,
            truncate=False
        )
    )

    report["bridge_exploratory"] = {
        "distinct_values": total_values,
        "matched_values": matched_values,
        "coverage_percent":
            round(bridge_percentage, 2),
        "warning":
            (
                "Coincidencia por nombre normalizado "
                "solo exploratoria; no usar como "
                "clave definitiva sin validar "
                "fuente institucional."
            ),
    }

    # ========================================================
    # GUARDAR REPORTE
    # ========================================================

    output = (
        OUTPUT_DIR /
        "integration_analysis.json"
    )

    with open(
        output,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            report,
            f,
            ensure_ascii=False,
            indent=2
        )

    print_title("ANÁLISIS TERMINADO")

    print("Reporte:", output)

    spark.stop()


if __name__ == "__main__":
    main()



