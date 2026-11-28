
import argparse
import json
from cobertura_sab import propagar_cobertura

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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


def construir_gold(sab, cobertura=None, version_silver=None):

    # 1. Seleccion y estandarizacion

    gold = sab.select(
        F.col("fecha_negociacion").alias("fecha"),
        F.col("codigo_sab"),
        F.col("SocAgenteBolsa").alias(
            "sociedad_agente_bolsa"
        ),
        F.col("MontoCompras").cast(
            "decimal(24,2)"
        ).alias("monto_compras"),
        F.col("MontoVentas").cast(
            "decimal(24,2)"
        ).alias("monto_ventas"),
        F.col("MontoTotal").cast(
            "decimal(24,2)"
        ).alias("monto_total"),
        F.col("PorcentajeCompras").alias(
            "porcentaje_compras_original"
        ),
        F.col("PorcentajeVentas").alias(
            "porcentaje_ventas_original"
        ),
        F.col("PorcentajeTotal").alias(
            "porcentaje_total_original"
        ),
        "fuente_datos",
        "fecha_ingesta"
    )

    # 2. Indicadores de calidad monetaria

    gold = (
        gold.withColumn(
            "montos_validos",
            F.coalesce(
                F.col("monto_compras").isNotNull()
                & F.col("monto_ventas").isNotNull()
                & F.col("monto_total").isNotNull()
                & (F.col("monto_compras") >= 0)
                & (F.col("monto_ventas") >= 0)
                & (F.col("monto_total") >= 0),
                F.lit(False)
            )
        )
        .withColumn(
            "diferencia_monetaria",
            F.col("monto_total")
            - F.col("monto_compras")
            - F.col("monto_ventas")
        )
        .withColumn(
            "total_consistente",
            F.coalesce(
                F.col("montos_validos")
                & (
                    F.abs("diferencia_monetaria")
                    <= F.lit("0.01").cast("decimal(24,2)")
                ),
                F.lit(False)
            )
        )
        .withColumn(
            "saldo_neto",
            F.when(
                F.col("montos_validos"),
                F.col("monto_compras")
                - F.col("monto_ventas")
            )
        )
    )

    # 3. Ventanas de agregacion diaria

    ventana_diaria = Window.partitionBy("fecha")

    gold = (
        gold.withColumn(
            "total_compras_diario",
            F.sum("monto_compras").over(
                ventana_diaria
            )
        )
        .withColumn(
            "total_ventas_diario",
            F.sum("monto_ventas").over(
                ventana_diaria
            )
        )
        .withColumn(
            "total_actividad_diaria",
            F.sum("monto_total").over(
                ventana_diaria
            )
        )
        .withColumn(
            "cantidad_sab_diaria",
            F.count("*").over(
                ventana_diaria
            )
        )
        .withColumn(
            "registros_invalidos_diarios",
            F.sum(
                F.when(
                    ~F.col("montos_validos")
                    | ~F.col("total_consistente"),
                    1
                ).otherwise(0)
            ).over(ventana_diaria)
        )
        .withColumn(
            "actividad_diaria_valida",
            F.col("registros_invalidos_diarios") == 0
        )
    )

    # 4. Participaciones porcentuales

    gold = (
        gold.withColumn(
            "participacion_compras_pct",
            F.when(
                F.col("actividad_diaria_valida")
                & (F.col("total_compras_diario") > 0),
                (
                    F.col("monto_compras").cast("double")
                    / F.col("total_compras_diario").cast(
                        "double"
                    )
                ) * 100
            )
        )
        .withColumn(
            "participacion_ventas_pct",
            F.when(
                F.col("actividad_diaria_valida")
                & (F.col("total_ventas_diario") > 0),
                (
                    F.col("monto_ventas").cast("double")
                    / F.col("total_ventas_diario").cast(
                        "double"
                    )
                ) * 100
            )
        )
        .withColumn(
            "participacion_total_pct",
            F.when(
                F.col("actividad_diaria_valida")
                & (F.col("total_actividad_diaria") > 0),
                (
                    F.col("monto_total").cast("double")
                    / F.col("total_actividad_diaria").cast(
                        "double"
                    )
                ) * 100
            )
        )
    )

    # 5. Comparacion con porcentajes Silver

    gold = (
        gold.withColumn(
            "diferencia_participacion_compras",
            F.col("participacion_compras_pct")
            - F.col("porcentaje_compras_original")
        )
        .withColumn(
            "diferencia_participacion_ventas",
            F.col("participacion_ventas_pct")
            - F.col("porcentaje_ventas_original")
        )
        .withColumn(
            "diferencia_participacion_total",
            F.col("participacion_total_pct")
            - F.col("porcentaje_total_original")
        )
    )

    # 6. Ranking diario

    ventana_ranking = Window.partitionBy(
        "fecha"
    ).orderBy(
        F.col("monto_total").desc_nulls_last(),
        F.col("codigo_sab").asc()
    )

    gold = gold.withColumn(
        "ranking_diario",
        F.when(
            F.col("actividad_diaria_valida")
            & (F.col("total_actividad_diaria") > 0),
            F.row_number().over(ventana_ranking)
        )
    )

    # 7. Concentracion de actividad: HHI

    gold = (
        gold.withColumn(
            "aporte_hhi",
            F.pow(
                F.col("participacion_total_pct"),
                2
            )
        )
        .withColumn(
            "hhi_diario",
            F.when(
                F.col("actividad_diaria_valida")
                & (F.col("total_actividad_diaria") > 0),
                F.sum("aporte_hhi").over(
                    ventana_diaria
                )
            )
        )
    )

    # 8. Indicadores temporales

    gold = (
        gold.withColumn(
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
    )

    return propagar_cobertura(gold, cobertura, version_silver)


def main():

    parser = argparse.ArgumentParser(
        description="Construccion Gold actividad_sab_diaria"
    )

    parser.add_argument(
        "--silver-root",
        required=True
    )

    parser.add_argument(
        "--gold-root",
        required=True
    )

    parser.add_argument("--cobertura-json")
    parser.add_argument("--version-silver")
    args = parser.parse_args()

    cobertura = None
    if args.cobertura_json:
        with open(args.cobertura_json, encoding="utf-8") as archivo:
            cobertura = json.load(archivo)["registros"]

    silver_root = args.silver_root.rstrip("/")
    gold_root = args.gold_root.rstrip("/")

    ruta_salida = (
        f"{gold_root}/mercado/actividad_sab_diaria"
    )

    spark = (
        SparkSession.builder
        .appName("BVL_Gold_Actividad_SAB_Diaria")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    gold = None

    try:

        print("\n" + "=" * 80)
        print("BVL ANALYTICS - GOLD ACTIVIDAD SAB DIARIA")
        print("=" * 80)

        # Lectura Silver

        sab = spark.read.parquet(
            f"{silver_root}/mercado_diario/montos_sab"
        )

        total_silver = sab.count()

        print(f"\nRegistros Silver: {total_silver}")

        # Validacion de entrada

        validar_clave(
            sab,
            ["codigo_sab", "fecha_negociacion"],
            "montos_sab"
        )

        # Construccion Gold

        gold = construir_gold(sab, cobertura, args.version_silver).cache()

        total_gold = gold.count()

        # Validacion de salida

        validar_clave(
            gold,
            ["codigo_sab", "fecha"],
            "actividad_sab_diaria"
        )

        if total_silver != total_gold:
            raise ValueError(
                "La transformacion modifico "
                "la cantidad de registros"
            )

        # Metricas de calidad

        print("\n" + "=" * 80)
        print("METRICAS DE CALIDAD GOLD")
        print("=" * 80)

        gold.agg(
            F.count("*").alias("registros"),
            F.countDistinct("codigo_sab").alias("sab"),
            F.countDistinct("fecha").alias("fechas"),
            F.count(
                F.when(
                    ~F.col("montos_validos"),
                    1
                )
            ).alias("montos_invalidos"),
            F.count(
                F.when(
                    ~F.col("total_consistente"),
                    1
                )
            ).alias("totales_inconsistentes"),
            F.count(
                F.when(
                    F.col("participacion_total_pct").isNull(),
                    1
                )
            ).alias("participaciones_nulas"),
            F.count(
                F.when(
                    F.col("ranking_diario").isNull(),
                    1
                )
            ).alias("rankings_nulos")
        ).show(truncate=False)

        # Verificacion de participaciones diarias

        print("\nVALIDACION DE PARTICIPACIONES DIARIAS")

        participaciones = gold.groupBy("fecha").agg(
            F.sum(
                "participacion_total_pct"
            ).alias("suma_participacion"),
            F.count("*").alias("registros"),
            F.count(
                "participacion_total_pct"
            ).alias("participaciones_validas")
        )

        participaciones.agg(
            F.min("suma_participacion").alias("minimo"),
            F.max("suma_participacion").alias("maximo"),
            F.avg("suma_participacion").alias("promedio")
        ).show(truncate=False)

        fechas_inconsistentes = participaciones.filter(
            F.col("suma_participacion").isNull()
            | (
                F.col("registros")
                != F.col("participaciones_validas")
            )
            | (
                F.abs(
                    F.col("suma_participacion") - 100
                ) > 0.000001
            )
        ).count()

        print(
            "Fechas con participacion inconsistente: "
            f"{fechas_inconsistentes}"
        )

        if fechas_inconsistentes > 0:
            raise ValueError(
                "Participaciones diarias inconsistentes"
            )

        # Comparacion porcentajes originales

        print("\nDIFERENCIAS CON PORCENTAJES SILVER")

        gold.agg(
            F.max(
                F.abs("diferencia_participacion_compras")
            ).alias("max_diferencia_compras"),
            F.max(
                F.abs("diferencia_participacion_ventas")
            ).alias("max_diferencia_ventas"),
            F.max(
                F.abs("diferencia_participacion_total")
            ).alias("max_diferencia_total")
        ).show(truncate=False)

        # Verificacion HHI

        print("\nCONCENTRACION DIARIA HHI")

        hhi = gold.select(
            "fecha",
            "hhi_diario"
        ).distinct()

        hhi.agg(
            F.min("hhi_diario").alias("hhi_minimo"),
            F.max("hhi_diario").alias("hhi_maximo"),
            F.avg("hhi_diario").alias("hhi_promedio")
        ).show(truncate=False)

        hhi_invalidos = hhi.filter(
            F.col("hhi_diario").isNull()
            | F.isnan("hhi_diario")
            | (F.col("hhi_diario") <= 0)
            | (F.col("hhi_diario") > 10000.000001)
        ).count()

        print(f"HHI invalidos: {hhi_invalidos}")

        if hhi_invalidos > 0:
            raise ValueError("Valores HHI invalidos")

        # Cobertura temporal

        print("\nCOBERTURA TEMPORAL")

        gold.agg(
            F.min("fecha").alias("fecha_min"),
            F.max("fecha").alias("fecha_max")
        ).show(truncate=False)

        print("\nREGISTROS POR ANIO")

        gold.groupBy(
            "anio"
        ).count().orderBy(
            "anio"
        ).show(truncate=False)

        # Esquema

        print("\nESQUEMA GOLD")

        gold.printSchema()

        # Muestra

        print("\nMUESTRA GOLD")

        gold.select(
            "fecha",
            "codigo_sab",
            "sociedad_agente_bolsa",
            "monto_total",
            "saldo_neto",
            "participacion_total_pct",
            "ranking_diario",
            "hhi_diario"
        ).orderBy(
            "fecha",
            "ranking_diario"
        ).show(15, truncate=False)

        # Escritura segura

        print(f"\nRuta de salida: {ruta_salida}")

        (
            gold.repartition("anio", "mes")
            .write
            .mode("errorifexists")
            .option("compression", "snappy")
            .partitionBy("anio", "mes")
            .parquet(ruta_salida)
        )
        print("\n" + "=" * 80)
        print("CONSTRUCCION GOLD SAB FINALIZADA")
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
