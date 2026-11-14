
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():

    parser = argparse.ArgumentParser(
        description="Validacion Gold mercado_valor_diario"
    )

    parser.add_argument("--silver-root", required=True)
    parser.add_argument("--gold-root", required=True)

    args = parser.parse_args()

    silver_root = args.silver_root.rstrip("/")
    gold_root = args.gold_root.rstrip("/")

    spark = (
        SparkSession.builder
        .appName("BVL_Validacion_Gold_Mercado")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

        silver = spark.read.parquet(
            f"{silver_root}/mercado_diario/cotizaciones"
        )

        gold = spark.read.parquet(
            f"{gold_root}/mercado/mercado_valor_diario"
        ).cache()

        print("\n" + "=" * 80)
        print("VALIDACION GOLD - MERCADO VALOR DIARIO")
        print("=" * 80)

        controles = []

        def validar(nombre, condicion):
            estado = "OK" if condicion else "ERROR"
            controles.append((nombre, condicion))
            print(f"{nombre}: {estado}")

        # 1. Conservacion de registros

        total_silver = silver.count()
        total_gold = gold.count()

        print("\n1. CONSERVACION DE REGISTROS")

        print(f"Registros Silver: {total_silver}")
        print(f"Registros Gold: {total_gold}")
        print(f"Columnas Gold: {len(gold.columns)}")

        validar(
            "Igualdad de cantidades",
            total_silver == total_gold
        )

        # 2. Validacion de claves

        print("\n2. INTEGRIDAD DE CLAVES")

        claves_silver = silver.select(
            F.col("valor").alias("nemonico"),
            F.col("fecha_cotizacion").alias("fecha")
        )

        claves_gold = gold.select(
            "nemonico",
            "fecha"
        )

        claves_invalidas = gold.filter(
            F.col("fecha").isNull()
            | F.col("nemonico").isNull()
            | (F.trim("nemonico") == "")
        ).count()

        duplicados = (
            claves_gold.groupBy("nemonico", "fecha")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        faltantes = claves_silver.join(
            claves_gold,
            ["nemonico", "fecha"],
            "left_anti"
        ).count()

        adicionales = claves_gold.join(
            claves_silver,
            ["nemonico", "fecha"],
            "left_anti"
        ).count()

        print(f"Claves invalidas: {claves_invalidas}")
        print(f"Claves duplicadas: {duplicados}")
        print(f"Cotizaciones faltantes: {faltantes}")
        print(f"Cotizaciones adicionales: {adicionales}")

        validar("Claves completas", claves_invalidas == 0)
        validar("Claves unicas", duplicados == 0)
        validar("Sin registros perdidos", faltantes == 0)
        validar("Sin registros adicionales", adicionales == 0)

        # 3. Verificacion de precios originales

        print("\n3. CONSERVACION DE VALORES ORIGINALES")

        original = silver.select(
            F.col("valor").alias("nemonico"),
            F.col("fecha_cotizacion").alias("fecha"),
            F.col("cierre_actual").alias("cierre_silver"),
            F.col("cierre_anterior").alias("anterior_silver"),
            F.col("monto_negociado").alias("monto_silver")
        )

        comparacion = gold.join(
            original,
            ["nemonico", "fecha"],
            "inner"
        )

        diferencias = comparacion.filter(
            ~F.col("precio_cierre").eqNullSafe(
                F.col("cierre_silver")
            )
            | ~F.col("precio_anterior").eqNullSafe(
                F.col("anterior_silver")
            )
            | ~F.col("monto_negociado").eqNullSafe(
                F.col("monto_silver")
            )
        ).count()

        print(f"Registros con diferencias: {diferencias}")

        validar(
            "Conservacion de precios y montos",
            diferencias == 0
        )

        # 4. Validacion del retorno

        print("\n4. VALIDACION DE RETORNOS")

        retorno_esperado = F.when(
            (F.col("precio_anterior") > 0)
            & (F.col("precio_cierre") > 0),
            (
                F.col("precio_cierre")
                - F.col("precio_anterior")
            ) / F.col("precio_anterior") * 100
        )

        retornos = gold.withColumn(
            "retorno_esperado",
            retorno_esperado
        )

        errores_retorno = retornos.filter(
            (
                F.col("retorno_diario_pct").isNull()
                != F.col("retorno_esperado").isNull()
            )
            | (
                F.abs(
                    F.col("retorno_diario_pct")
                    - F.col("retorno_esperado")
                ) > 0.000001
            )
        ).count()

        print(f"Retornos inconsistentes: {errores_retorno}")

        validar(
            "Calculo correcto de retornos",
            errores_retorno == 0
        )

        # 5. Validacion de particiones

        print("\n5. PARTICIONAMIENTO")

        errores_particion = gold.filter(
            ~F.col("anio").eqNullSafe(F.year("fecha"))
            | ~F.col("mes").eqNullSafe(F.month("fecha"))
        ).count()

        print(f"Particiones inconsistentes: {errores_particion}")
        print(f"Archivos Parquet: {len(gold.inputFiles())}")

        validar(
            "Particiones compatibles con la fecha",
            errores_particion == 0
        )

        # 6. Resumen final

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
                "Gold mercado_valor_diario no aprobado"
            )

        print("RESULTADO: GOLD VALIDADO CORRECTAMENTE")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
