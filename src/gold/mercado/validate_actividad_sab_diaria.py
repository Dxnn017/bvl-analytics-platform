
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main():

    parser = argparse.ArgumentParser(
        description="Validacion Gold actividad_sab_diaria"
    )

    parser.add_argument("--silver-root", required=True)
    parser.add_argument("--gold-root", required=True)

    args = parser.parse_args()

    silver_root = args.silver_root.rstrip("/")
    gold_root = args.gold_root.rstrip("/")

    spark = (
        SparkSession.builder
        .appName("BVL_Validacion_Gold_SAB")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

        silver = spark.read.parquet(
            f"{silver_root}/mercado_diario/montos_sab"
        )

        gold = spark.read.parquet(
            f"{gold_root}/mercado/actividad_sab_diaria"
        )

        archivos_parquet = len(gold.inputFiles())

        gold = gold.cache()

        controles = []

        def validar(nombre, condicion):

            estado = "OK" if condicion else "ERROR"

            controles.append((nombre, condicion))

            print(f"{nombre}: {estado}")

        print("\n" + "=" * 80)
        print("VALIDACION GOLD - ACTIVIDAD SAB DIARIA")
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

        # 2. Integridad de claves

        print("\n2. INTEGRIDAD DE CLAVES")

        claves_silver = silver.select(
            "codigo_sab",
            F.col("fecha_negociacion").alias("fecha")
        )

        claves_gold = gold.select(
            "codigo_sab",
            "fecha"
        )

        invalidas = gold.filter(
            F.col("fecha").isNull()
            | F.col("codigo_sab").isNull()
            | (F.trim("codigo_sab") == "")
        ).count()

        duplicadas = (
            claves_gold.groupBy(
                "codigo_sab", "fecha"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        faltantes = claves_silver.join(
            claves_gold,
            ["codigo_sab", "fecha"],
            "left_anti"
        ).count()

        adicionales = claves_gold.join(
            claves_silver,
            ["codigo_sab", "fecha"],
            "left_anti"
        ).count()

        print(f"Claves invalidas: {invalidas}")
        print(f"Claves duplicadas: {duplicadas}")
        print(f"Registros faltantes: {faltantes}")
        print(f"Registros adicionales: {adicionales}")

        validar("Claves completas", invalidas == 0)
        validar("Claves unicas", duplicadas == 0)
        validar("Sin registros perdidos", faltantes == 0)
        validar("Sin registros adicionales", adicionales == 0)

        # 3. Conservacion monetaria

        print("\n3. CONSERVACION DE MONTOS")

        originales = silver.select(
            "codigo_sab",
            F.col("fecha_negociacion").alias("fecha"),
            F.col("MontoCompras").cast(
                "decimal(24,2)"
            ).alias("compras_silver"),
            F.col("MontoVentas").cast(
                "decimal(24,2)"
            ).alias("ventas_silver"),
            F.col("MontoTotal").cast(
                "decimal(24,2)"
            ).alias("total_silver")
        )

        comparacion = gold.join(
            originales,
            ["codigo_sab", "fecha"],
            "inner"
        )

        diferencias = comparacion.filter(
            ~F.col("monto_compras").eqNullSafe(
                F.col("compras_silver")
            )
            | ~F.col("monto_ventas").eqNullSafe(
                F.col("ventas_silver")
            )
            | ~F.col("monto_total").eqNullSafe(
                F.col("total_silver")
            )
        ).count()

        print(f"Montos modificados: {diferencias}")

        validar(
            "Conservacion de montos originales",
            diferencias == 0
        )

        # 4. Consistencia monetaria

        print("\n4. CONSISTENCIA MONETARIA")

        inconsistencias = gold.filter(
            F.col("monto_compras").isNull()
            | F.col("monto_ventas").isNull()
            | F.col("monto_total").isNull()
            | (F.col("monto_compras") < 0)
            | (F.col("monto_ventas") < 0)
            | (F.col("monto_total") < 0)
            | (
                F.abs(
                    F.col("monto_total")
                    - F.col("monto_compras")
                    - F.col("monto_ventas")
                ) > F.lit("0.01").cast("decimal(24,2)")
            )
        ).count()

        errores_saldo = gold.filter(
            ~F.col("saldo_neto").eqNullSafe(
                F.col("monto_compras")
                - F.col("monto_ventas")
            )
        ).count()

        print(f"Totales inconsistentes: {inconsistencias}")
        print(f"Saldos netos incorrectos: {errores_saldo}")

        validar(
            "Consistencia de montos",
            inconsistencias == 0
        )

        validar(
            "Calculo correcto de saldo neto",
            errores_saldo == 0
        )

        # 5. Recalculo independiente de totales diarios

        print("\n5. TOTALES DIARIOS")

        totales = originales.groupBy("fecha").agg(
            F.sum("compras_silver").alias(
                "compras_esperadas"
            ),
            F.sum("ventas_silver").alias(
                "ventas_esperadas"
            ),
            F.sum("total_silver").alias(
                "actividad_esperada"
            ),
            F.count("*").alias("sab_esperadas")
        )

        revision = gold.join(
            totales,
            "fecha",
            "left"
        ).cache()

        errores_totales = revision.filter(
            ~F.col("total_compras_diario").eqNullSafe(
                F.col("compras_esperadas")
            )
            | ~F.col("total_ventas_diario").eqNullSafe(
                F.col("ventas_esperadas")
            )
            | ~F.col("total_actividad_diaria").eqNullSafe(
                F.col("actividad_esperada")
            )
            | ~F.col("cantidad_sab_diaria").eqNullSafe(
                F.col("sab_esperadas")
            )
        ).count()

        print(f"Totales diarios incorrectos: {errores_totales}")

        validar(
            "Calculo correcto de totales diarios",
            errores_totales == 0
        )

        # 6. Validacion de participaciones individuales

        print("\n6. PARTICIPACIONES INDIVIDUALES")

        def porcentaje_incorrecto(columna, esperado):

            actual = F.col(columna)

            return (
                (
                    actual.isNull()
                    != esperado.isNull()
                )
                | F.isnan(actual)
                | (
                    F.abs(actual - esperado)
                    > 0.000001
                )
            )

        compras_pct = F.when(
            F.col("compras_esperadas") > 0,
            F.col("monto_compras").cast("double")
            / F.col("compras_esperadas").cast("double")
            * 100
        )

        ventas_pct = F.when(
            F.col("ventas_esperadas") > 0,
            F.col("monto_ventas").cast("double")
            / F.col("ventas_esperadas").cast("double")
            * 100
        )

        total_pct = F.when(
            F.col("actividad_esperada") > 0,
            F.col("monto_total").cast("double")
            / F.col("actividad_esperada").cast("double")
            * 100
        )

        errores_participacion = revision.filter(
            porcentaje_incorrecto(
                "participacion_compras_pct",
                compras_pct
            )
            | porcentaje_incorrecto(
                "participacion_ventas_pct",
                ventas_pct
            )
            | porcentaje_incorrecto(
                "participacion_total_pct",
                total_pct
            )
        ).count()

        print(
            "Participaciones incorrectas: "
            f"{errores_participacion}"
        )

        validar(
            "Calculo correcto de participaciones",
            errores_participacion == 0
        )

        # 7. Suma diaria de participaciones

        print("\n7. SUMA DIARIA DE PARTICIPACIONES")

        participaciones = gold.groupBy("fecha").agg(
            F.sum("participacion_total_pct").alias(
                "suma_participacion"
            ),
            F.count("*").alias("registros"),
            F.count("participacion_total_pct").alias(
                "participaciones_validas"
            )
        )

        fechas_incorrectas = participaciones.filter(
            F.col("suma_participacion").isNull()
            | F.isnan("suma_participacion")
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

        print(f"Fechas inconsistentes: {fechas_incorrectas}")

        validar(
            "Participacion diaria igual a 100%",
            fechas_incorrectas == 0
        )

        # 8. Validacion independiente del ranking

        print("\n8. RANKING DIARIO")

        ventana = Window.partitionBy(
            "fecha"
        ).orderBy(
            F.col("total_silver").desc(),
            F.col("codigo_sab").asc()
        )

        ranking_esperado = originales.withColumn(
            "ranking_esperado",
            F.row_number().over(ventana)
        ).select(
            "fecha",
            "codigo_sab",
            "ranking_esperado"
        )

        errores_ranking = gold.join(
            ranking_esperado,
            ["fecha", "codigo_sab"],
            "left"
        ).filter(
            ~F.col("ranking_diario").eqNullSafe(
                F.col("ranking_esperado")
            )
        ).count()

        print(f"Rankings incorrectos: {errores_ranking}")

        validar(
            "Orden correcto del ranking",
            errores_ranking == 0
        )

        # 9. Validacion independiente del HHI

        print("\n9. CONCENTRACION HHI")

        aportes = revision.withColumn(
            "aporte_esperado",
            F.pow(total_pct, 2)
        )

        errores_aporte = aportes.filter(
            porcentaje_incorrecto(
                "aporte_hhi",
                F.col("aporte_esperado")
            )
        ).count()

        hhi_esperado = aportes.groupBy("fecha").agg(
            F.sum("aporte_esperado").alias(
                "hhi_esperado"
            )
        )

        errores_hhi = gold.join(
            hhi_esperado,
            "fecha",
            "left"
        ).filter(
            porcentaje_incorrecto(
                "hhi_diario",
                F.col("hhi_esperado")
            )
            | F.col("hhi_diario").isNull()
            | (F.col("hhi_diario") <= 0)
            | (F.col("hhi_diario") > 10000.000001)
        ).count()

        print(f"Aportes HHI incorrectos: {errores_aporte}")
        print(f"Registros con HHI incorrecto: {errores_hhi}")

        validar(
            "Calculo correcto de aportes HHI",
            errores_aporte == 0
        )

        validar(
            "Calculo correcto del HHI diario",
            errores_hhi == 0
        )

        # 10. Particionamiento y almacenamiento

        print("\n10. PARTICIONAMIENTO")

        errores_particion = gold.filter(
            ~F.col("anio").eqNullSafe(F.year("fecha"))
            | ~F.col("mes").eqNullSafe(F.month("fecha"))
        ).count()

        print(
            f"Particiones inconsistentes: {errores_particion}"
        )

        print(f"Archivos Parquet: {archivos_parquet}")

        validar(
            "Particiones compatibles con la fecha",
            errores_particion == 0
        )

        validar(
            "Existencia de archivos Parquet",
            archivos_parquet > 0
        )

        # 11. Resultado final

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
                "Gold actividad_sab_diaria no aprobado"
            )

        print("RESULTADO: GOLD VALIDADO CORRECTAMENTE")

        revision.unpersist()
        gold.unpersist()

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
