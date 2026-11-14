
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def numero_valido(columna):
    c = F.col(columna)

    return (
        c.isNotNull()
        & ~F.isnan(c)
        & (F.abs(c) != float("inf"))
    )


def validar_clave(df, claves, nombre):

    condicion = F.lit(False)

    for clave in claves:
        condicion = condicion | (
            F.col(clave).isNull()
            | (F.trim(F.col(clave).cast("string")) == "")
        )

    invalidos = df.filter(condicion).count()

    duplicados = (
        df.groupBy(*claves)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"\nValidacion de clave: {nombre}")
    print(f"Claves invalidas: {invalidos}")
    print(f"Grupos duplicados: {duplicados}")

    if invalidos > 0 or duplicados > 0:
        raise ValueError(
            f"Clave invalida o duplicada en {nombre}"
        )


def construir_gold(cotizaciones, tc, embig, puente):

    # 1. Seleccion y estandarizacion de cotizaciones

    mercado = cotizaciones.select(
        F.col("fecha_cotizacion").alias("fecha"),
        F.col("valor").alias("nemonico"),
        "descripcion",
        "fecha_anterior",
        F.col("cierre_anterior").alias("precio_anterior"),
        F.col("cierre_actual").alias("precio_cierre"),
        F.col("apertura_actual").alias("precio_apertura"),
        F.col("maxima_actual").alias("precio_maximo"),
        F.col("minima_actual").alias("precio_minimo"),
        F.col("promedio_actual").alias("precio_promedio"),
        "monto_negociado",
        "fuente",
        "fecha_ingesta"
    )

    # 2. Integracion macroeconomica

    mercado = (
        mercado.join(
            tc.select(
                "fecha",
                "tipo_cambio_venta"
            ),
            "fecha",
            "left"
        )
        .join(
            embig.select(
                "fecha",
                "embig_peru"
            ),
            "fecha",
            "left"
        )
    )

    # 3. Integracion empresarial

    referencia = puente.select(
        F.col("nemonico_valor").alias("nemonico"),
        "ruc",
        "razon_social",
        "codigo_isin",
        "tipo_valor",
        F.col("moneda").alias("moneda_referencia"),
        "fecha_inscripcion"
    )

    mercado = mercado.join(
        referencia,
        "nemonico",
        "left"
    )

    # 4. Indicadores de calidad

    mercado = (
        mercado.withColumn(
            "precio_anterior_valido",
            numero_valido("precio_anterior")
            & (F.col("precio_anterior") > 0)
        )
        .withColumn(
            "precio_cierre_valido",
            numero_valido("precio_cierre")
            & (F.col("precio_cierre") > 0)
        )
        .withColumn(
            "rango_intradia_valido",
            numero_valido("precio_maximo")
            & numero_valido("precio_minimo")
            & (F.col("precio_minimo") > 0)
            & (
                F.col("precio_maximo")
                >= F.col("precio_minimo")
            )
        )
        .withColumn(
            "tipo_cambio_disponible",
            numero_valido("tipo_cambio_venta")
            & (F.col("tipo_cambio_venta") > 0)
        )
        .withColumn(
            "embig_disponible",
            numero_valido("embig_peru")
            & (F.col("embig_peru") >= 0)
        )
        .withColumn(
            "empresa_vinculada",
            F.col("ruc").isNotNull()
            & (F.trim("ruc") != "")
        )
        .withColumn(
            "inscripcion_compatible",
            F.when(
                F.col("empresa_vinculada")
                & F.col("fecha_inscripcion").isNotNull(),
                F.col("fecha")
                >= F.col("fecha_inscripcion")
            ).otherwise(F.lit(None).cast("boolean"))
        )
        .withColumn(
            "monto_negociado_valido",
            numero_valido("monto_negociado")
            & (F.col("monto_negociado") >= 0)
        )
        .withColumn(
            "monto_negociado_positivo",
            F.col("monto_negociado_valido")
            & (F.col("monto_negociado") > 0)
        )
    )

    # 5. Calculo de indicadores bursatiles

    mercado = (
        mercado.withColumn(
            "retorno_calculable",
            F.col("precio_anterior_valido")
            & F.col("precio_cierre_valido")
        )
        .withColumn(
            "variacion_precio",
            F.when(
                F.col("retorno_calculable"),
                F.col("precio_cierre")
                - F.col("precio_anterior")
            )
        )
        .withColumn(
            "retorno_diario_pct",
            F.when(
                F.col("retorno_calculable"),
                (
                    F.col("precio_cierre")
                    / F.col("precio_anterior") - 1
                ) * 100
            )
        )
        .withColumn(
            "rango_intradia",
            F.when(
                F.col("rango_intradia_valido"),
                F.col("precio_maximo")
                - F.col("precio_minimo")
            )
        )
        .withColumn(
            "rango_intradia_pct",
            F.when(
                F.col("rango_intradia_valido")
                & F.col("precio_anterior_valido"),
                (
                    F.col("precio_maximo")
                    - F.col("precio_minimo")
                ) / F.col("precio_anterior") * 100
            )
        )
    )

    # 6. Indicadores temporales y de trazabilidad

    mercado = (
        mercado.withColumn(
            "anio",
            F.year("fecha")
        )
        .withColumn(
            "mes",
            F.month("fecha")
        )
        .withColumn(
            "trimestre",
            F.quarter("fecha")
        )
        .withColumn(
            "dias_desde_cierre_anterior",
            F.datediff(
                "fecha",
                "fecha_anterior"
            )
        )
        .withColumn(
            "macro_disponible",
            F.col("tipo_cambio_disponible")
            & F.col("embig_disponible")
        )
    )

    return mercado


def main():

    parser = argparse.ArgumentParser(
        description="Construccion Gold mercado_valor_diario"
    )

    parser.add_argument(
        "--silver-root",
        required=True
    )

    parser.add_argument(
        "--gold-root",
        required=True
    )

    args = parser.parse_args()

    silver_root = args.silver_root.rstrip("/")
    gold_root = args.gold_root.rstrip("/")

    ruta_salida = (
        f"{gold_root}/mercado/mercado_valor_diario"
    )

    spark = (
        SparkSession.builder
        .appName("BVL_Gold_Mercado_Valor_Diario")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    gold = None

    try:

        print("\n" + "=" * 80)
        print("BVL ANALYTICS - GOLD MERCADO VALOR DIARIO")
        print("=" * 80)

        # Lectura de fuentes

        cotizaciones = spark.read.parquet(
            f"{silver_root}/mercado_diario/cotizaciones"
        )

        tc = spark.read.parquet(
            f"{silver_root}/macroeconomia/tipo_cambio"
        )

        embig = spark.read.parquet(
            f"{silver_root}/macroeconomia/embig"
        )

        puente = spark.read.parquet(
            f"{silver_root}/referencias_smv/puente_ruc_nemonico"
        )

        total_silver = cotizaciones.count()

        print(f"\nCotizaciones Silver: {total_silver}")

        # Validacion de claves de entrada

        validar_clave(
            cotizaciones,
            ["valor", "fecha_cotizacion"],
            "cotizaciones"
        )

        validar_clave(
            tc,
            ["fecha"],
            "tipo_cambio"
        )

        validar_clave(
            embig,
            ["fecha"],
            "embig"
        )

        validar_clave(
            puente,
            ["nemonico_valor"],
            "puente_ruc_nemonico"
        )

        # Construccion Gold

        gold = construir_gold(
            cotizaciones,
            tc,
            embig,
            puente
        ).cache()

        total_gold = gold.count()

        # Validacion de claves Gold

        validar_clave(
            gold,
            ["nemonico", "fecha"],
            "mercado_valor_diario"
        )

        # Validacion de conservacion de registros

        if total_silver != total_gold:
            raise ValueError(
                "La integracion modifico la cantidad "
                "de cotizaciones"
            )

        print("\n" + "=" * 80)
        print("METRICAS DE CALIDAD GOLD")
        print("=" * 80)

        gold.agg(
            F.count("*").alias("registros"),
            F.countDistinct("nemonico")
            .alias("nemonicos"),
            F.countDistinct("fecha")
            .alias("fechas"),
            F.count(
                F.when(
                    F.col("retorno_diario_pct").isNull(),
                    1
                )
            ).alias("retornos_nulos"),
            F.count(
                F.when(
                    F.col("tipo_cambio_venta").isNull(),
                    1
                )
            ).alias("tc_nulos"),
            F.count(
                F.when(
                    F.col("embig_peru").isNull(),
                    1
                )
            ).alias("embig_nulos"),
            F.count(
                F.when(
                    ~F.col("empresa_vinculada"),
                    1
                )
            ).alias("sin_empresa")
        ).show(truncate=False)

        print("\nCOBERTURA TEMPORAL")

        gold.agg(
            F.min("fecha").alias("fecha_min"),
            F.max("fecha").alias("fecha_max")
        ).show(truncate=False)

        print("\nREGISTROS POR ANIO")

        gold.groupBy("anio").count().orderBy(
            "anio"
        ).show(truncate=False)

        print("\nESQUEMA GOLD")

        gold.printSchema()

        print("\nMUESTRA GOLD")

        gold.select(
            "fecha",
            "nemonico",
            "precio_cierre",
            "retorno_diario_pct",
            "monto_negociado",
            "tipo_cambio_venta",
            "embig_peru",
            "empresa_vinculada"
        ).orderBy(
            "fecha",
            "nemonico"
        ).show(10, truncate=False)

        # Escritura segura en ubicacion de pruebas

        print(f"\nRuta de salida: {ruta_salida}")

        (
            gold.write
            .mode("errorifexists")
            .option("compression", "snappy")
            .partitionBy("anio", "mes")
            .parquet(ruta_salida)
        )

        print("\n" + "=" * 80)
        print("CONSTRUCCION GOLD FINALIZADA")
        print("=" * 80)

        print(f"Registros Silver: {total_silver}")
        print(f"Registros Gold: {total_gold}")
        print(f"Ruta Gold: {ruta_salida}")

    finally:

        if gold is not None:
            gold.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
