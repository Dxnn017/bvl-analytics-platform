
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def numero_valido(columna):

    c = F.col(columna).cast("double")

    return F.coalesce(
        c.isNotNull()
        & ~F.isnan(c)
        & (F.abs(c) != float("inf")),
        F.lit(False)
    )


def main():

    parser = argparse.ArgumentParser(
        description="Validacion Gold empresa_trimestre"
    )

    parser.add_argument("--silver-root", required=True)
    parser.add_argument("--gold-root", required=True)

    args = parser.parse_args()

    silver_root = args.silver_root.rstrip("/")
    gold_root = args.gold_root.rstrip("/")

    spark = (
        SparkSession.builder
        .appName("BVL_Validacion_Gold_Finanzas")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

        silver = spark.read.parquet(
            f"{silver_root}/finanzas_empresariales/"
            "principales_cuentas"
        )

        gold = spark.read.parquet(
            f"{gold_root}/empresas/empresa_trimestre"
        )

        archivos_parquet = len(gold.inputFiles())

        gold = gold.cache()

        controles = []

        def validar(nombre, condicion):

            estado = "OK" if condicion else "ERROR"

            controles.append((nombre, condicion))

            print(f"{nombre}: {estado}")

        print("\n" + "=" * 80)
        print("VALIDACION GOLD - EMPRESA TRIMESTRE")
        print("=" * 80)

        # 1. Conservacion de registros

        print("\n1. CONSERVACION DE REGISTROS")

        total_silver = silver.count()
        total_gold = gold.count()

        print(f"Registros Silver: {total_silver}")
        print(f"Registros Gold: {total_gold}")
        print(f"Columnas Gold: {len(gold.columns)}")

        validar(
            "Igualdad de cantidades",
            total_silver == total_gold
        )

        # 2. Integridad de identificadores

        print("\n2. INTEGRIDAD DE IDENTIFICADORES")

        claves_silver = silver.select("id_registro")
        claves_gold = gold.select("id_registro")

        invalidos = gold.filter(
            F.col("id_registro").isNull()
            | (F.trim("id_registro") == "")
        ).count()

        duplicados = (
            claves_gold.groupBy("id_registro")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        faltantes = claves_silver.join(
            claves_gold,
            "id_registro",
            "left_anti"
        ).count()

        adicionales = claves_gold.join(
            claves_silver,
            "id_registro",
            "left_anti"
        ).count()

        print(f"Identificadores invalidos: {invalidos}")
        print(f"Identificadores duplicados: {duplicados}")
        print(f"Registros faltantes: {faltantes}")
        print(f"Registros adicionales: {adicionales}")

        validar("Identificadores completos", invalidos == 0)
        validar("Identificadores unicos", duplicados == 0)
        validar("Sin registros perdidos", faltantes == 0)
        validar("Sin registros adicionales", adicionales == 0)

        # 3. Conservacion de valores originales

        print("\n3. CONSERVACION DE VALORES ORIGINALES")

        campos = [
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
            "tipo_informacion",
            "metodo_flujo_efectivo",
            "activo_total",
            "pasivo_total",
            "patrimonio_total",
            "total_ingreso",
            "utilidad_neta",
            "cantidad_clave_negocio",
            "colision_clave_negocio",
            "fuente_datos",
            "fecha_ingesta"
        ]

        originales = silver.select(
            "id_registro",
            *[
                F.col(campo).alias(f"{campo}_silver")
                for campo in campos
            ],
            F.col("moneda").alias("moneda_silver"),
            F.col("trimestre").alias("trimestre_silver")
        )

        revision = gold.join(
            originales,
            "id_registro",
            "inner"
        ).cache()

        diferencia_original = F.lit(False)

        for campo in campos:

            diferencia_original = (
                diferencia_original
                | ~F.col(campo).eqNullSafe(
                    F.col(f"{campo}_silver")
                )
            )

        diferencia_original = (
            diferencia_original
            | ~F.col("moneda_original").eqNullSafe(
                F.col("moneda_silver")
            )
            | ~F.col("trimestre_original").eqNullSafe(
                F.col("trimestre_silver")
            )
        )

        modificados = revision.filter(
            diferencia_original
        ).count()

        print(f"Registros originales modificados: {modificados}")

        validar(
            "Conservacion de valores originales",
            modificados == 0
        )

        # 4. Validacion de colisiones financieras

        print("\n4. COLISIONES FINANCIERAS")

        agrupacion = [
            "rpj",
            "ejercicio",
            "trimestre_num"
        ]

        cantidades = silver.groupBy(*agrupacion).agg(
            F.count("*").alias("cantidad_esperada")
        )

        revision = revision.join(
            cantidades,
            agrupacion,
            "left"
        )

        revision = revision.withColumn(
            "colision_esperada",
            F.coalesce(
                F.col("colision_clave_negocio_silver"),
                F.lit(True)
            )
            | F.coalesce(
                F.col("cantidad_esperada") > 1,
                F.lit(True)
            )
        )

        errores_colision = revision.filter(
            ~F.col("colision_financiera").eqNullSafe(
                F.col("colision_esperada")
            )
            | ~F.col(
                "cantidad_registros_empresa_periodo"
            ).eqNullSafe(
                F.col("cantidad_esperada")
            )
        ).count()

        print(f"Colisiones mal identificadas: {errores_colision}")

        validar(
            "Identificacion correcta de colisiones",
            errores_colision == 0
        )

        # 5. Validacion de identidad contable

        print("\n5. IDENTIDAD CONTABLE")

        evaluable = (
            numero_valido("activo_total_silver")
            & numero_valido("pasivo_total_silver")
            & numero_valido("patrimonio_total_silver")
        )

        revision = (
            revision.withColumn(
                "identidad_evaluable_esperada",
                evaluable
            )
            .withColumn(
                "diferencia_esperada",
                F.when(
                    F.col("identidad_evaluable_esperada"),
                    F.col("activo_total_silver")
                    - F.col("pasivo_total_silver")
                    - F.col("patrimonio_total_silver")
                )
            )
            .withColumn(
                "identidad_esperada",
                F.coalesce(
                    F.col("identidad_evaluable_esperada")
                    & (F.abs("diferencia_esperada") <= 1),
                    F.lit(False)
                )
            )
        )

        errores_identidad = revision.filter(
            ~F.col(
                "identidad_contable_evaluable"
            ).eqNullSafe(
                F.col("identidad_evaluable_esperada")
            )
            | ~F.col(
                "identidad_contable_valida"
            ).eqNullSafe(
                F.col("identidad_esperada")
            )
            | ~F.col(
                "diferencia_contable"
            ).eqNullSafe(
                F.col("diferencia_esperada")
            )
        ).count()

        print(f"Errores de identidad contable: {errores_identidad}")

        validar(
            "Validacion correcta de identidad contable",
            errores_identidad == 0
        )

        # 6. Normalizacion monetaria

        print("\n6. NORMALIZACION MONETARIA")

        etiqueta = F.upper(
            F.trim("moneda_silver")
        )

        revision = revision.withColumn(
            "moneda_esperada",
            F.when(
                etiqueta.isin("SOLES", "SOL", "PEN"),
                F.lit("PEN")
            ).when(
                etiqueta.isin(
                    "DOLARES",
                    "DÓLARES",
                    "D LARES",
                    "USD"
                ),
                F.lit("USD")
            )
        )

        errores_moneda = revision.filter(
            ~F.col("moneda").eqNullSafe(
                F.col("moneda_esperada")
            )
            | ~F.col("moneda_valida").eqNullSafe(
                F.col("moneda_esperada").isNotNull()
            )
        ).count()

        print(f"Monedas incorrectamente normalizadas: {errores_moneda}")

        validar(
            "Normalizacion monetaria correcta",
            errores_moneda == 0
        )

        # 7. Validacion de periodos

        print("\n7. PERIODOS FINANCIEROS")

        periodo_valido = F.coalesce(
            F.col("ejercicio_silver").between(1900, 9999)
            & F.col("trimestre_num_silver").between(1, 4),
            F.lit(False)
        )

        revision = (
            revision.withColumn(
                "periodo_valido_esperado",
                periodo_valido
            )
            .withColumn(
                "fecha_fin_esperada",
                F.when(
                    F.col("periodo_valido_esperado"),
                    F.last_day(
                        F.make_date(
                            F.col("ejercicio_silver"),
                            F.col("trimestre_num_silver") * 3,
                            F.lit(1)
                        )
                    )
                )
            )
        )

        errores_periodo = revision.filter(
            ~F.col("anio").eqNullSafe(
                F.col("ejercicio_silver")
            )
            | ~F.col("trimestre").eqNullSafe(
                F.col("trimestre_num_silver")
            )
            | ~F.col("periodo_valido").eqNullSafe(
                F.col("periodo_valido_esperado")
            )
            | ~F.col("fecha_fin_trimestre").eqNullSafe(
                F.col("fecha_fin_esperada")
            )
        ).count()

        print(f"Periodos incorrectos: {errores_periodo}")

        validar(
            "Periodos financieros correctos",
            errores_periodo == 0
        )

        # 8. Elegibilidad general

        print("\n8. ELEGIBILIDAD GENERAL")

        revision = (
            revision.withColumn(
                "sin_incidencias_esperado",
                ~F.col("colision_esperada")
                & F.col("identidad_esperada")
            )
            .withColumn(
                "apto_esperado",
                F.col("sin_incidencias_esperado")
                & F.col("periodo_valido_esperado")
                & F.col("moneda_esperada").isNotNull()
            )
        )

        errores_elegibilidad = revision.filter(
            ~F.col(
                "sin_incidencias_estructurales"
            ).eqNullSafe(
                F.col("sin_incidencias_esperado")
            )
            | ~F.col("apto_kpi_general").eqNullSafe(
                F.col("apto_esperado")
            )
        ).count()

        print(f"Errores de elegibilidad: {errores_elegibilidad}")

        validar(
            "Elegibilidad financiera correcta",
            errores_elegibilidad == 0
        )

        # 9. Recalculo de indicadores financieros

        print("\n9. INDICADORES FINANCIEROS")

        activo_positivo = (
            numero_valido("activo_total_silver")
            & (F.col("activo_total_silver") > 0)
        )

        patrimonio_positivo = (
            numero_valido("patrimonio_total_silver")
            & (F.col("patrimonio_total_silver") > 0)
        )

        ingreso_positivo = (
            numero_valido("total_ingreso_silver")
            & (F.col("total_ingreso_silver") > 0)
        )

        pasivo_valido = (
            numero_valido("pasivo_total_silver")
            & (F.col("pasivo_total_silver") >= 0)
        )

        utilidad_valida = numero_valido(
            "utilidad_neta_silver"
        )

        patrimonio_valido = numero_valido(
            "patrimonio_total_silver"
        )

        apto = F.col("apto_esperado")

        indicadores = [
            (
                "roa_aproximado_pct",
                "roa_calculable",
                apto & activo_positivo & utilidad_valida,
                "utilidad_neta_silver",
                "activo_total_silver"
            ),
            (
                "roe_aproximado_pct",
                "roe_calculable",
                apto & patrimonio_positivo & utilidad_valida,
                "utilidad_neta_silver",
                "patrimonio_total_silver"
            ),
            (
                "endeudamiento_pct",
                "endeudamiento_calculable",
                apto & activo_positivo & pasivo_valido,
                "pasivo_total_silver",
                "activo_total_silver"
            ),
            (
                "margen_neto_pct",
                "margen_neto_calculable",
                apto & ingreso_positivo & utilidad_valida,
                "utilidad_neta_silver",
                "total_ingreso_silver"
            ),
            (
                "autonomia_financiera_pct",
                "autonomia_calculable",
                apto & activo_positivo & patrimonio_valido,
                "patrimonio_total_silver",
                "activo_total_silver"
            )
        ]

        revision = revision.cache()

        for (
            kpi,
            indicador_calculable,
            condicion,
            numerador,
            denominador
        ) in indicadores:

            esperado = F.when(
                condicion,
                F.col(numerador)
                / F.col(denominador) * 100
            )

            actual = F.col(kpi)

            errores = revision.filter(
                ~F.col(indicador_calculable).eqNullSafe(
                    F.coalesce(condicion, F.lit(False))
                )
                | (actual.isNull() != esperado.isNull())
                | (
                    actual.isNotNull()
                    & ~numero_valido(kpi)
                )
                | (
                    F.abs(actual - esperado)
                    > 0.000001
                )
            ).count()

            print(f"{kpi} incorrectos: {errores}")

            validar(
                f"Calculo correcto de {kpi}",
                errores == 0
            )

        # 10. Registros excluidos

        print("\n10. CONTROL DE REGISTROS EXCLUIDOS")

        tiene_kpi = F.lit(False)

        for kpi, _, _, _, _ in indicadores:

            tiene_kpi = (
                tiene_kpi
                | F.col(kpi).isNotNull()
            )

        errores_exclusion = revision.filter(
            ~F.col("apto_esperado")
            & tiene_kpi
        ).count()

        print(
            "Registros no aptos con KPIs calculados: "
            f"{errores_exclusion}"
        )

        validar(
            "Exclusion correcta de registros no aptos",
            errores_exclusion == 0
        )

        # 11. Clasificacion financiera

        print("\n11. CLASIFICACION FINANCIERA")

        resultado_esperado = (
            F.when(
                ~F.col("apto_esperado")
                | ~numero_valido("utilidad_neta_silver"),
                F.lit("NO_EVALUABLE")
            )
            .when(
                F.col("utilidad_neta_silver") > 0,
                F.lit("GANANCIA")
            )
            .when(
                F.col("utilidad_neta_silver") < 0,
                F.lit("PERDIDA")
            )
            .otherwise(
                F.lit("EQUILIBRIO")
            )
        )

        errores_resultado = revision.filter(
            ~F.col("resultado_financiero").eqNullSafe(
                resultado_esperado
            )
        ).count()

        print(f"Clasificaciones incorrectas: {errores_resultado}")

        validar(
            "Clasificacion financiera correcta",
            errores_resultado == 0
        )

        # 12. Particionamiento y almacenamiento

        print("\n12. PARTICIONAMIENTO")

        errores_particion = gold.filter(
            ~F.col("anio").eqNullSafe(
                F.year("fecha_fin_trimestre")
            )
            | ~F.col("trimestre").eqNullSafe(
                F.quarter("fecha_fin_trimestre")
            )
        ).count()

        print(f"Particiones inconsistentes: {errores_particion}")
        print(f"Archivos Parquet: {archivos_parquet}")

        validar(
            "Particiones compatibles con el periodo",
            errores_particion == 0
        )

        validar(
            "Existencia de archivos Parquet",
            archivos_parquet > 0
        )

        # 13. Resumen de calidad

        print("\n13. RESUMEN DE CALIDAD FINANCIERA")

        gold.agg(
            F.count("*").alias("registros"),
            F.countDistinct("rpj").alias("empresas"),
            F.count(
                F.when(F.col("colision_financiera"), 1)
            ).alias("colisiones"),
            F.count(
                F.when(
                    ~F.col("identidad_contable_valida"),
                    1
                )
            ).alias("identidad_invalida"),
            F.count(
                F.when(F.col("apto_kpi_general"), 1)
            ).alias("aptos")
        ).show(truncate=False)

        # 14. Resultado final

        print("\n" + "=" * 80)
        print("RESULTADO FINAL")
        print("=" * 80)

        aprobados = sum(
            1 for _, estado in controles if estado
        )

        fallidos = len(controles) - aprobados

        print(f"Controles ejecutados: {len(controles)}")
        print(f"Controles aprobados: {aprobados}")
        print(f"Controles fallidos: {fallidos}")

        if fallidos > 0:
            raise ValueError(
                "Gold empresa_trimestre no aprobado"
            )

        print("RESULTADO: GOLD VALIDADO CORRECTAMENTE")

        spark.catalog.clearCache()

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
