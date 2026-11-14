
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


FUENTES = {
    "mercado_diario/cotizaciones": "fecha_cotizacion",
    "mercado_diario/montos_sab": "fecha_negociacion",
    "mercado_diario/indices": "fecha",
    "macroeconomia/tipo_cambio": "fecha",
    "macroeconomia/embig": "fecha",
    "hechos_importancia/eventos": "fecha_presentacion_date",
    "hechos_importancia/identidad_empresarial":
        "fecha_presentacion_date"
}


def main():
    parser = argparse.ArgumentParser(
        description="Auditoria de cobertura temporal Silver"
    )

    parser.add_argument("--silver-root", required=True)

    args = parser.parse_args()
    root = args.silver_root.rstrip("/")

    spark = (
        SparkSession.builder
        .appName("BVL_Cobertura_Temporal")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n" + "=" * 80)
        print("AUDITORIA DE COBERTURA TEMPORAL")
        print("=" * 80)

        print("\n1. COBERTURA POR DATASET")

        for dataset, columna_fecha in FUENTES.items():

            df = spark.read.parquet(
                f"{root}/{dataset}"
            )

            print(f"\nDATASET: {dataset}")

            df.agg(
                F.count("*").alias("registros"),
                F.min(columna_fecha).alias("fecha_min"),
                F.max(columna_fecha).alias("fecha_max"),
                F.countDistinct(columna_fecha)
                .alias("fechas_distintas"),
                F.count(
                    F.when(
                        F.col(columna_fecha).isNull(),
                        1
                    )
                ).alias("fechas_nulas")
            ).show(truncate=False)

            (
                df.groupBy(
                    F.year(columna_fecha).alias("anio")
                )
                .count()
                .orderBy("anio")
                .show(50, truncate=False)
            )

        print("\n2. COBERTURA FINANCIERA")

        finanzas = spark.read.parquet(
            f"{root}/finanzas_empresariales/principales_cuentas"
        )

        (
            finanzas.groupBy(
                "ejercicio",
                "trimestre_num"
            )
            .agg(
                F.count("*").alias("registros"),
                F.countDistinct("rpj").alias("empresas")
            )
            .orderBy("ejercicio", "trimestre_num")
            .show(50, truncate=False)
        )

        print("\n3. COBERTURA MACRO SOBRE COTIZACIONES")

        cotizaciones = spark.read.parquet(
            f"{root}/mercado_diario/cotizaciones"
        )

        tipo_cambio = spark.read.parquet(
            f"{root}/macroeconomia/tipo_cambio"
        )

        embig = spark.read.parquet(
            f"{root}/macroeconomia/embig"
        )

        fechas_cotizaciones = (
            cotizaciones.select(
                F.col("fecha_cotizacion").alias("fecha")
            )
            .distinct()
        )

        fechas_tc = (
            tipo_cambio.select("fecha")
            .distinct()
            .withColumn("existe_tc", F.lit(1))
        )

        fechas_embig = (
            embig.select("fecha")
            .distinct()
            .withColumn("existe_embig", F.lit(1))
        )

        cobertura = (
            fechas_cotizaciones
            .join(fechas_tc, "fecha", "left")
            .join(fechas_embig, "fecha", "left")
        )

        cobertura.agg(
            F.count("*").alias("fechas_cotizacion"),
            F.count("existe_tc").alias("fechas_con_tc"),
            F.count("existe_embig").alias("fechas_con_embig"),
            F.count(
                F.when(
                    F.col("existe_tc").isNotNull()
                    & F.col("existe_embig").isNotNull(),
                    1
                )
            ).alias("fechas_con_ambos")
        ).show(truncate=False)

        print("\n4. FECHAS SIN COBERTURA MACROECONOMICA")

        (
            cobertura.filter(
                F.col("existe_tc").isNull()
                | F.col("existe_embig").isNull()
            )
            .orderBy("fecha")
            .show(30, truncate=False)
        )

        print("\n5. COHERENCIA TEMPORAL DE EVENTOS")

        eventos = spark.read.parquet(
            f"{root}/hechos_importancia/eventos"
        )

        (
            eventos.groupBy(
                "coherencia_temporal",
                "usar_silver_principal"
            )
            .agg(
                F.count("*").alias("registros"),
                F.min("fecha_presentacion_date")
                .alias("fecha_min"),
                F.max("fecha_presentacion_date")
                .alias("fecha_max")
            )
            .show(50, truncate=False)
        )

        print("\nAUDITORIA FINALIZADA")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
