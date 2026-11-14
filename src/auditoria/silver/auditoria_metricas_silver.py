
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser(
        description="Auditoria de metricas Silver"
    )

    parser.add_argument("--silver-root", required=True)

    args = parser.parse_args()
    root = args.silver_root.rstrip("/")

    spark = (
        SparkSession.builder
        .appName("BVL_Auditoria_Metricas")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    def leer(dataset):
        return spark.read.parquet(f"{root}/{dataset}")

    def perfilar(df, columnas):

        for nombre in columnas:

            c = F.col(nombre)

            print(f"\nCOLUMNA: {nombre}")

            df.agg(
                F.count("*").alias("total"),
                F.count(
                    F.when(c.isNull(), 1)
                ).alias("nulos"),
                F.count(
                    F.when(F.isnan(c), 1)
                ).alias("nan"),
                F.count(
                    F.when(
                        (c == float("inf"))
                        | (c == float("-inf")),
                        1
                    )
                ).alias("infinitos"),
                F.count(
                    F.when(c == 0, 1)
                ).alias("ceros"),
                F.count(
                    F.when(c < 0, 1)
                ).alias("negativos"),
                F.min(c).alias("minimo"),
                F.max(c).alias("maximo")
            ).show(truncate=False)

    try:
        cotizaciones = leer(
            "mercado_diario/cotizaciones"
        )

        sab = leer(
            "mercado_diario/montos_sab"
        )

        finanzas = leer(
            "finanzas_empresariales/principales_cuentas"
        )

        tc = leer(
            "macroeconomia/tipo_cambio"
        )

        print("\n" + "=" * 80)
        print("1. CALIDAD DE PRECIOS BURSATILES")
        print("=" * 80)

        perfilar(
            cotizaciones,
            [
                "cierre_anterior",
                "cierre_actual",
                "apertura_actual",
                "maxima_actual",
                "minima_actual",
                "monto_negociado"
            ]
        )

        print("\nMAXIMO MENOR QUE MINIMO")

        print(
            cotizaciones.filter(
                F.col("maxima_actual")
                < F.col("minima_actual")
            ).count()
        )

        print("\n" + "=" * 80)
        print("2. CALIDAD DE MONTOS SAB")
        print("=" * 80)

        perfilar(
            sab,
            [
                "MontoCompras",
                "MontoVentas",
                "MontoTotal",
                "PorcentajeCompras",
                "PorcentajeVentas",
                "PorcentajeTotal"
            ]
        )

        print("\nDIFERENCIAS ENTRE TOTAL Y COMPRAS + VENTAS")

        sab.withColumn(
            "diferencia",
            F.abs(
                F.col("MontoTotal")
                - F.col("MontoCompras")
                - F.col("MontoVentas")
            )
        ).agg(
            F.max("diferencia").alias("diferencia_maxima"),
            F.count(
                F.when(
                    F.col("diferencia") > 0.01,
                    1
                )
            ).alias("registros_con_diferencia")
        ).show(truncate=False)

        print("\nSUMA DIARIA DE PORCENTAJES SAB")

        sab.groupBy("fecha_negociacion").agg(
            F.sum("PorcentajeTotal").alias("suma_porcentaje")
        ).agg(
            F.min("suma_porcentaje").alias("minimo"),
            F.max("suma_porcentaje").alias("maximo"),
            F.avg("suma_porcentaje").alias("promedio")
        ).show(truncate=False)

        print("\n" + "=" * 80)
        print("3. CALIDAD DE METRICAS FINANCIERAS")
        print("=" * 80)

        perfilar(
            finanzas,
            [
                "activo_total",
                "pasivo_total",
                "patrimonio_total",
                "total_ingreso",
                "utilidad_neta"
            ]
        )

        print("\nIDENTIDAD CONTABLE")

        finanzas.withColumn(
            "diferencia_contable",
            F.abs(
                F.col("activo_total")
                - F.col("pasivo_total")
                - F.col("patrimonio_total")
            )
        ).agg(
            F.max("diferencia_contable").alias("diferencia_maxima"),
            F.count(
                F.when(
                    F.col("diferencia_contable") > 1,
                    1
                )
            ).alias("diferencias_mayores_a_uno")
        ).show(truncate=False)

        print("\n" + "=" * 80)
        print("4. FECHAS CON TIPO DE CAMBIO NULO")
        print("=" * 80)

        (
            cotizaciones
            .withColumnRenamed("fecha_cotizacion", "fecha")
            .join(
                tc.select("fecha", "tipo_cambio_venta"),
                "fecha",
                "left"
            )
            .filter(F.col("tipo_cambio_venta").isNull())
            .groupBy("fecha")
            .count()
            .orderBy("fecha")
            .show(100, truncate=False)
        )

        print("\nAUDITORIA FINALIZADA")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
