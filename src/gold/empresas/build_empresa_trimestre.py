
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def numero_valido(columna):

    c = F.col(columna)

    return F.coalesce(
        c.isNotNull()
        & ~F.isnan(c.cast("double"))
        & (F.abs(c.cast("double")) != float("inf")),
        F.lit(False)
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


def construir_gold(finanzas):

    # 1. Seleccion de campos originales

    gold = finanzas.select(
        "id_registro",
        "clave_negocio",
        "rpj",
        "ruc",
        "ruc_valido",
        "nombre_empresa",
        "tipo_empresa",
        "tipo_sector",
        "ciiu",
        "ejercicio",
        "trimestre_num",
        F.col("trimestre").alias("trimestre_original"),
        "tipo_informacion",
        "metodo_flujo_efectivo",
        F.col("moneda").alias("moneda_original"),
        "activo_total",
        "pasivo_total",
        "patrimonio_total",
        "total_ingreso",
        "utilidad_neta",
        "cantidad_clave_negocio",
        "colision_clave_negocio",
        "fuente_datos",
        "fecha_ingesta"
    )

    # 2. Normalizacion monetaria

    moneda_normalizada = F.upper(
        F.trim(F.col("moneda_original"))
    )

    gold = gold.withColumn(
        "moneda",
        F.when(
            moneda_normalizada.isin(
                "SOLES",
                "SOL",
                "PEN"
            ),
            F.lit("PEN")
        ).when(
            moneda_normalizada.isin(
                "DOLARES",
                "DÓLARES",
                "D LARES",
                "USD"
            ),
            F.lit("USD")
        ).otherwise(
            F.lit(None).cast("string")
        )
    )

    # 3. Indicadores temporales

    gold = (
        gold.withColumn(
            "anio",
            F.col("ejercicio").cast("int")
        )
        .withColumn(
            "trimestre",
            F.col("trimestre_num").cast("int")
        )
        .withColumn(
            "periodo_valido",
            F.coalesce(
                F.col("anio").between(1900, 9999)
                & F.col("trimestre").between(1, 4),
                F.lit(False)
            )
        )
        .withColumn(
            "periodo",
            F.when(
                F.col("periodo_valido"),
                F.concat(
                    F.col("anio").cast("string"),
                    F.lit("-T"),
                    F.col("trimestre").cast("string")
                )
            )
        )
        .withColumn(
            "fecha_fin_trimestre",
            F.when(
                F.col("periodo_valido"),
                F.last_day(
                    F.make_date(
                        F.col("anio"),
                        F.col("trimestre") * 3,
                        F.lit(1)
                    )
                )
            )
        )
    )

    # 4. Calidad de los campos financieros

    gold = (
        gold.withColumn(
            "activo_valido",
            numero_valido("activo_total")
            & (F.col("activo_total") > 0)
        )
        .withColumn(
            "pasivo_valido",
            numero_valido("pasivo_total")
            & (F.col("pasivo_total") >= 0)
        )
        .withColumn(
            "patrimonio_valido",
            numero_valido("patrimonio_total")
        )
        .withColumn(
            "patrimonio_positivo",
            F.col("patrimonio_valido")
            & (F.col("patrimonio_total") > 0)
        )
        .withColumn(
            "ingreso_valido",
            numero_valido("total_ingreso")
        )
        .withColumn(
            "ingreso_positivo",
            F.col("ingreso_valido")
            & (F.col("total_ingreso") > 0)
        )
        .withColumn(
            "utilidad_valida",
            numero_valido("utilidad_neta")
        )
        .withColumn(
            "moneda_valida",
            F.col("moneda").isNotNull()
        )
    )

    # 5. Validacion de identidad contable

    gold = (
        gold.withColumn(
            "identidad_contable_evaluable",
            numero_valido("activo_total")
            & numero_valido("pasivo_total")
            & numero_valido("patrimonio_total")
        )
        .withColumn(
            "diferencia_contable",
            F.when(
                F.col("identidad_contable_evaluable"),
                F.col("activo_total")
                - F.col("pasivo_total")
                - F.col("patrimonio_total")
            )
        )
        .withColumn(
            "identidad_contable_valida",
            F.coalesce(
                F.col("identidad_contable_evaluable")
                & (
                    F.abs("diferencia_contable")
                    <= 1
                ),
                F.lit(False)
            )
        )
    )

    # 6. Validacion de colisiones

    # Recalcular cantidad de registros por clave
    # empresarial, sin eliminar las filas conflictivas.

    claves = [
        "rpj",
        "ejercicio",
        "trimestre_num"
    ]

    cantidades = gold.groupBy(*claves).agg(
        F.count("*").alias(
            "cantidad_registros_empresa_periodo"
        )
    )

    gold = gold.join(
        cantidades,
        claves,
        "left"
    )

    gold = gold.withColumn(
        "colision_financiera",
        F.coalesce(
            F.col("colision_clave_negocio"),
            F.lit(True)
        )
        | F.coalesce(
            F.col("cantidad_registros_empresa_periodo") > 1,
            F.lit(True)
        )
    )

    # 7. Elegibilidad general para KPIs

    gold = (
        gold.withColumn(
            "sin_incidencias_estructurales",
            ~F.col("colision_financiera")
            & F.col("identidad_contable_valida")
        )
        .withColumn(
            "apto_kpi_general",
            F.col("sin_incidencias_estructurales")
            & F.col("periodo_valido")
            & F.col("moneda_valida")
        )
    )

    # 8. Elegibilidad por indicador

    gold = (
        gold.withColumn(
            "roa_calculable",
            F.col("apto_kpi_general")
            & F.col("activo_valido")
            & F.col("utilidad_valida")
        )
        .withColumn(
            "roe_calculable",
            F.col("apto_kpi_general")
            & F.col("patrimonio_positivo")
            & F.col("utilidad_valida")
        )
        .withColumn(
            "endeudamiento_calculable",
            F.col("apto_kpi_general")
            & F.col("activo_valido")
            & F.col("pasivo_valido")
        )
        .withColumn(
            "margen_neto_calculable",
            F.col("apto_kpi_general")
            & F.col("ingreso_positivo")
            & F.col("utilidad_valida")
        )
        .withColumn(
            "autonomia_calculable",
            F.col("apto_kpi_general")
            & F.col("activo_valido")
            & F.col("patrimonio_valido")
        )
    )

    # 9. Calculo de KPIs financieros

    gold = (
        gold.withColumn(
            "roa_aproximado_pct",
            F.when(
                F.col("roa_calculable"),
                F.col("utilidad_neta")
                / F.col("activo_total") * 100
            )
        )
        .withColumn(
            "roe_aproximado_pct",
            F.when(
                F.col("roe_calculable"),
                F.col("utilidad_neta")
                / F.col("patrimonio_total") * 100
            )
        )
        .withColumn(
            "endeudamiento_pct",
            F.when(
                F.col("endeudamiento_calculable"),
                F.col("pasivo_total")
                / F.col("activo_total") * 100
            )
        )
        .withColumn(
            "margen_neto_pct",
            F.when(
                F.col("margen_neto_calculable"),
                F.col("utilidad_neta")
                / F.col("total_ingreso") * 100
            )
        )
        .withColumn(
            "autonomia_financiera_pct",
            F.when(
                F.col("autonomia_calculable"),
                F.col("patrimonio_total")
                / F.col("activo_total") * 100
            )
        )
    )

    # 10. Clasificacion del resultado financiero

    gold = (
        gold.withColumn(
            "resultado_financiero",
            F.when(
                ~F.col("apto_kpi_general")
                | ~F.col("utilidad_valida"),
                F.lit("NO_EVALUABLE")
            )
            .when(
                F.col("utilidad_neta") > 0,
                F.lit("GANANCIA")
            )
            .when(
                F.col("utilidad_neta") < 0,
                F.lit("PERDIDA")
            )
            .otherwise(
                F.lit("EQUILIBRIO")
            )
        )
        .withColumn(
            "patrimonio_negativo",
            F.coalesce(
                F.col("patrimonio_valido")
                & (F.col("patrimonio_total") < 0),
                F.lit(False)
            )
        )
    )

    # 11. Motivos de calidad

    gold = gold.withColumn(
        "motivos_calidad",
        F.concat_ws(
            ";",
            F.when(
                F.col("colision_financiera"),
                F.lit("COLISION_FINANCIERA")
            ),
            F.when(
                ~F.col("identidad_contable_valida"),
                F.lit("IDENTIDAD_CONTABLE_INVALIDA")
            ),
            F.when(
                ~F.col("activo_valido"),
                F.lit("ACTIVO_NO_POSITIVO_O_INVALIDO")
            ),
            F.when(
                ~F.col("patrimonio_positivo"),
                F.lit("PATRIMONIO_NO_POSITIVO_O_INVALIDO")
            ),
            F.when(
                ~F.col("ingreso_positivo"),
                F.lit("INGRESO_NO_POSITIVO_O_INVALIDO")
            ),
            F.when(
                ~F.col("utilidad_valida"),
                F.lit("UTILIDAD_INVALIDA")
            ),
            F.when(
                ~F.col("pasivo_valido"),
                F.lit("PASIVO_INVALIDO")
            ),
            F.when(
                ~F.col("moneda_valida"),
                F.lit("MONEDA_NO_RECONOCIDA")
            ),
            F.when(
                ~F.col("periodo_valido"),
                F.lit("PERIODO_INVALIDO")
            ),
            F.when(
                ~F.coalesce(
                    F.col("ruc_valido"),
                    F.lit(False)
                ),
                F.lit("RUC_NO_VALIDO")
            )
        )
    )

    return gold


def main():

    parser = argparse.ArgumentParser(
        description="Construccion Gold empresa_trimestre"
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
        f"{gold_root}/empresas/empresa_trimestre"
    )

    spark = (
        SparkSession.builder
        .appName("BVL_Gold_Empresa_Trimestre")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    gold = None

    try:

        print("\n" + "=" * 80)
        print("BVL ANALYTICS - GOLD EMPRESA TRIMESTRE")
        print("=" * 80)

        # Lectura Silver

        finanzas = spark.read.parquet(
            f"{silver_root}/finanzas_empresariales/"
            "principales_cuentas"
        )

        total_silver = finanzas.count()

        print(f"\nRegistros Silver: {total_silver}")

        # Validacion del identificador tecnico

        validar_clave(
            finanzas,
            ["id_registro"],
            "principales_cuentas"
        )

        # Construccion Gold

        gold = construir_gold(finanzas).cache()

        total_gold = gold.count()

        validar_clave(
            gold,
            ["id_registro"],
            "empresa_trimestre"
        )

        if total_silver != total_gold:
            raise ValueError(
                "La transformacion modifico la cantidad "
                "de registros financieros"
            )

        # 1. Metricas de calidad

        print("\n" + "=" * 80)
        print("METRICAS DE CALIDAD FINANCIERA")
        print("=" * 80)

        gold.agg(
            F.count("*").alias("registros"),
            F.countDistinct("rpj").alias("empresas"),
            F.count(
                F.when(
                    F.col("colision_financiera"),
                    1
                )
            ).alias("registros_colision"),
            F.count(
                F.when(
                    ~F.col("identidad_contable_valida"),
                    1
                )
            ).alias("identidad_invalida"),
            F.count(
                F.when(
                    F.col("sin_incidencias_estructurales"),
                    1
                )
            ).alias("sin_incidencias"),
            F.count(
                F.when(
                    F.col("apto_kpi_general"),
                    1
                )
            ).alias("aptos_kpi_general")
        ).show(truncate=False)

        # 2. Disponibilidad de KPIs

        print("\nDISPONIBILIDAD DE KPIs")

        gold.agg(
            F.count("roa_aproximado_pct").alias(
                "roa_disponible"
            ),
            F.count("roe_aproximado_pct").alias(
                "roe_disponible"
            ),
            F.count("endeudamiento_pct").alias(
                "endeudamiento_disponible"
            ),
            F.count("margen_neto_pct").alias(
                "margen_disponible"
            ),
            F.count("autonomia_financiera_pct").alias(
                "autonomia_disponible"
            )
        ).show(truncate=False)

        # 3. Validacion de registros excluidos

        print("\nCONTROL DE REGISTROS NO APTOS")

        kpis = [
            "roa_aproximado_pct",
            "roe_aproximado_pct",
            "endeudamiento_pct",
            "margen_neto_pct",
            "autonomia_financiera_pct"
        ]

        tiene_kpi = F.lit(False)

        for kpi in kpis:
            tiene_kpi = tiene_kpi | F.col(kpi).isNotNull()

        errores_exclusion = gold.filter(
            ~F.col("apto_kpi_general")
            & tiene_kpi
        ).count()

        print(
            "Registros no aptos con KPIs calculados: "
            f"{errores_exclusion}"
        )

        if errores_exclusion > 0:
            raise ValueError(
                "Existen KPIs en registros no aptos"
            )

        # 4. Validacion de valores no finitos

        print("\nCONTROL DE KPIs NO FINITOS")

        for kpi in kpis:

            invalidos = gold.filter(
                F.col(kpi).isNotNull()
                & ~numero_valido(kpi)
            ).count()

            print(f"{kpi}: {invalidos}")

            if invalidos > 0:
                raise ValueError(
                    f"Indicador no finito: {kpi}"
                )

        # 5. Distribucion monetaria

        print("\nDISTRIBUCION POR MONEDA")

        gold.groupBy(
            "moneda_original",
            "moneda"
        ).count().orderBy(
            "moneda"
        ).show(truncate=False)

        # 6. Resultados financieros

        print("\nCLASIFICACION FINANCIERA")

        gold.groupBy(
            "resultado_financiero"
        ).count().orderBy(
            "resultado_financiero"
        ).show(truncate=False)

        # 7. Cobertura temporal

        print("\nCOBERTURA TEMPORAL")

        gold.agg(
            F.min("fecha_fin_trimestre").alias(
                "fecha_min"
            ),
            F.max("fecha_fin_trimestre").alias(
                "fecha_max"
            )
        ).show(truncate=False)

        print("\nREGISTROS POR ANIO Y TRIMESTRE")

        gold.groupBy(
            "anio",
            "trimestre"
        ).count().orderBy(
            "anio",
            "trimestre"
        ).show(30, truncate=False)

        # 8. Registros con incidencias estructurales

        print("\nREGISTROS CON INCIDENCIAS ESTRUCTURALES")

        gold.filter(
            ~F.col("sin_incidencias_estructurales")
        ).select(
            "rpj",
            "nombre_empresa",
            "periodo",
            "colision_financiera",
            "diferencia_contable",
            "apto_kpi_general",
            "motivos_calidad"
        ).orderBy(
            "rpj",
            "periodo"
        ).show(50, truncate=False)

        # 9. Esquema

        print("\nESQUEMA GOLD")

        gold.printSchema()

        # 10. Muestra de indicadores

        print("\nMUESTRA GOLD FINANCIERA")

        gold.filter(
            F.col("apto_kpi_general")
        ).select(
            "periodo",
            "rpj",
            "nombre_empresa",
            "moneda",
            "activo_total",
            "utilidad_neta",
            "roa_aproximado_pct",
            "roe_aproximado_pct",
            "endeudamiento_pct",
            "margen_neto_pct",
            "resultado_financiero"
        ).orderBy(
            "periodo",
            "rpj"
        ).show(15, truncate=False)

        # 11. Escritura segura

        print(f"\nRuta de salida: {ruta_salida}")

        (
            gold.repartition(
                "anio",
                "trimestre"
            )
            .write
            .mode("errorifexists")
            .option("compression", "snappy")
            .partitionBy("anio", "trimestre")
            .parquet(ruta_salida)
        )

        print("\n" + "=" * 80)
        print("CONSTRUCCION GOLD FINANCIERA FINALIZADA")
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
