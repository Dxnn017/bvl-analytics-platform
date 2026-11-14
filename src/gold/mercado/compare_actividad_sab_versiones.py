
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType


CLAVES = ["fecha", "codigo_sab"]

TOLERANCIA_ABSOLUTA = 1e-9
TOLERANCIA_RELATIVA = 1e-12


def esquema_normalizado(df):

    return {
        campo.name: campo.dataType.simpleString()
        for campo in df.schema.fields
    }


def validar_claves(df):

    invalidas = df.filter(
        F.col("fecha").isNull()
        | F.col("codigo_sab").isNull()
        | (F.trim("codigo_sab") == "")
    ).count()

    duplicadas = (
        df.groupBy(*CLAVES)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    return invalidas, duplicadas


def comparar_numeros(actual, esperado):

    tolerancia = (
        F.lit(TOLERANCIA_ABSOLUTA)
        + F.greatest(
            F.abs(actual),
            F.abs(esperado)
        ) * F.lit(TOLERANCIA_RELATIVA)
    )

    ambos_finitos = (
        actual.isNotNull()
        & esperado.isNotNull()
        & ~F.isnan(actual)
        & ~F.isnan(esperado)
        & (F.abs(actual) != float("inf"))
        & (F.abs(esperado) != float("inf"))
    )

    diferencia_aceptable = (
        ambos_finitos
        & (
            F.abs(actual - esperado)
            <= tolerancia
        )
    )

    return ~(
        actual.eqNullSafe(esperado)
        | F.coalesce(
            diferencia_aceptable,
            F.lit(False)
        )
    )


def main():

    parser = argparse.ArgumentParser(
        description="Comparacion de versiones Gold SAB"
    )

    parser.add_argument(
        "--original-root",
        required=True
    )

    parser.add_argument(
        "--optimized-root",
        required=True
    )

    args = parser.parse_args()

    ruta_original = (
        args.original_root.rstrip("/")
        + "/mercado/actividad_sab_diaria"
    )

    ruta_optimizada = (
        args.optimized_root.rstrip("/")
        + "/mercado/actividad_sab_diaria"
    )

    spark = (
        SparkSession.builder
        .appName("BVL_Comparacion_Versiones_Gold_SAB")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    controles = []

    def validar(nombre, condicion):

        controles.append(
            (nombre, condicion)
        )

        estado = "OK" if condicion else "ERROR"

        print(f"{nombre}: {estado}")

    try:

        print("\n" + "=" * 80)
        print("COMPARACION DE VERSIONES GOLD - ACTIVIDAD SAB")
        print("=" * 80)

        # 1. Lectura de ambas versiones

        original = spark.read.parquet(
            ruta_original
        )

        optimizado = spark.read.parquet(
            ruta_optimizada
        )

        archivos_original = len(
            original.inputFiles()
        )

        archivos_optimizado = len(
            optimizado.inputFiles()
        )

        original = original.cache()
        optimizado = optimizado.cache()

        total_original = original.count()
        total_optimizado = optimizado.count()

        print("\n1. INVENTARIO DE VERSIONES")

        print(f"Ruta original: {ruta_original}")
        print(f"Ruta optimizada: {ruta_optimizada}")

        print(f"Registros originales: {total_original}")
        print(f"Registros optimizados: {total_optimizado}")

        print(f"Columnas originales: {len(original.columns)}")
        print(f"Columnas optimizadas: {len(optimizado.columns)}")

        print(f"Archivos originales: {archivos_original}")
        print(f"Archivos optimizados: {archivos_optimizado}")

        validar(
            "Igualdad de cantidades",
            total_original == total_optimizado
        )

        # 2. Comparacion de esquemas

        print("\n2. COMPARACION DE ESQUEMAS")

        esquema_original = esquema_normalizado(
            original
        )

        esquema_optimizado = esquema_normalizado(
            optimizado
        )

        mismos_esquemas = (
            esquema_original
            == esquema_optimizado
        )

        validar(
            "Igualdad de columnas y tipos",
            mismos_esquemas
        )

        if not mismos_esquemas:

            print("\nEsquema original:")
            original.printSchema()

            print("\nEsquema optimizado:")
            optimizado.printSchema()

            raise ValueError(
                "Los esquemas no son equivalentes"
            )

        # Alinear el orden de las columnas
        # antes de utilizar exceptAll.

        optimizado = optimizado.select(
            *original.columns
        )

        # 3. Integridad de claves

        print("\n3. INTEGRIDAD DE CLAVES")

        invalidas_original, duplicadas_original = (
            validar_claves(original)
        )

        invalidas_optimizado, duplicadas_optimizado = (
            validar_claves(optimizado)
        )

        print(
            "Claves invalidas originales: "
            f"{invalidas_original}"
        )

        print(
            "Claves invalidas optimizadas: "
            f"{invalidas_optimizado}"
        )

        print(
            "Claves duplicadas originales: "
            f"{duplicadas_original}"
        )

        print(
            "Claves duplicadas optimizadas: "
            f"{duplicadas_optimizado}"
        )

        validar(
            "Claves originales completas",
            invalidas_original == 0
        )

        validar(
            "Claves optimizadas completas",
            invalidas_optimizado == 0
        )

        validar(
            "Claves originales unicas",
            duplicadas_original == 0
        )

        validar(
            "Claves optimizadas unicas",
            duplicadas_optimizado == 0
        )

        if any([
            invalidas_original,
            invalidas_optimizado,
            duplicadas_original,
            duplicadas_optimizado
        ]):

            raise ValueError(
                "No se puede comparar con claves invalidas"
            )

        # 4. Conservacion de registros

        print("\n4. CONSERVACION DE CLAVES")

        faltantes = original.select(
            *CLAVES
        ).join(
            optimizado.select(*CLAVES),
            CLAVES,
            "left_anti"
        ).count()

        adicionales = optimizado.select(
            *CLAVES
        ).join(
            original.select(*CLAVES),
            CLAVES,
            "left_anti"
        ).count()

        print(f"Registros faltantes: {faltantes}")
        print(f"Registros adicionales: {adicionales}")

        validar(
            "Sin registros faltantes",
            faltantes == 0
        )

        validar(
            "Sin registros adicionales",
            adicionales == 0
        )

        # 5. Comparacion exacta en ambos sentidos

        print("\n5. COMPARACION EXACTA EXCEPTALL")

        diferencias_original = original.exceptAll(
            optimizado
        ).count()

        diferencias_optimizado = optimizado.exceptAll(
            original
        ).count()

        print(
            "Filas diferentes original -> optimizado: "
            f"{diferencias_original}"
        )

        print(
            "Filas diferentes optimizado -> original: "
            f"{diferencias_optimizado}"
        )

        equivalencia_exacta = (
            diferencias_original == 0
            and diferencias_optimizado == 0
        )

        print(
            "Equivalencia exacta: "
            + ("SI" if equivalencia_exacta else "NO")
        )

        # La igualdad exacta se informa por separado.
        # Una diferencia de precision flotante no debe
        # confundirse con una modificacion monetaria.

        # 6. Comparacion detallada de columnas

        print("\n6. COMPARACION DE TODAS LAS COLUMNAS")

        print(
            "Tolerancia absoluta para flotantes: "
            f"{TOLERANCIA_ABSOLUTA}"
        )

        print(
            "Tolerancia relativa para flotantes: "
            f"{TOLERANCIA_RELATIVA}"
        )

        izquierda = original.alias("o")
        derecha = optimizado.alias("n")

        revision = izquierda.join(
            derecha,
            CLAVES,
            "inner"
        )

        expresiones = []
        condiciones = []
        nombres = []

        for campo in original.schema.fields:

            nombre = campo.name

            if nombre in CLAVES:
                continue

            actual = F.col(f"o.{nombre}")
            esperado = F.col(f"n.{nombre}")

            # Los tipos decimales, enteros, fechas,
            # cadenas y booleanos se comparan
            # exactamente.

            if isinstance(
                campo.dataType,
                (DoubleType, FloatType)
            ):

                diferencia = comparar_numeros(
                    actual,
                    esperado
                )

            else:

                diferencia = ~actual.eqNullSafe(
                    esperado
                )

            condiciones.append(diferencia)

            expresiones.append(
                F.coalesce(
                    F.sum(
                        F.when(
                            diferencia, 1
                        ).otherwise(0)
                    ),
                    F.lit(0)
                ).alias(nombre)
            )

            nombres.append(nombre)

        resultado = revision.agg(
            *expresiones
        ).first().asDict()

        columnas_con_errores = []

        for nombre in nombres:

            errores = resultado[nombre]

            print(
                f"{nombre}: "
                f"{errores} diferencias"
            )

            if errores > 0:

                columnas_con_errores.append(
                    nombre
                )

        condicion_general = F.lit(False)

        for condicion in condiciones:

            condicion_general = (
                condicion_general | condicion
            )

        filas_diferentes = revision.filter(
            condicion_general
        ).count()

        print(
            "\nColumnas con diferencias significativas: "
            f"{len(columnas_con_errores)}"
        )

        print(
            "Filas con diferencias significativas: "
            f"{filas_diferentes}"
        )

        validar(
            "Equivalencia de todas las columnas",
            len(columnas_con_errores) == 0
        )

        validar(
            "Sin diferencias significativas por fila",
            filas_diferentes == 0
        )

        if columnas_con_errores:

            print("\nMUESTRA DE DIFERENCIAS")

            revision.filter(
                condicion_general
            ).select(
                *CLAVES,
                *[
                    F.col(f"o.{nombre}").alias(
                        f"{nombre}_original"
                    )
                    for nombre in columnas_con_errores
                ],
                *[
                    F.col(f"n.{nombre}").alias(
                        f"{nombre}_optimizado"
                    )
                    for nombre in columnas_con_errores
                ]
            ).show(10, truncate=False)

        # 7. Comparacion de particiones mensuales

        print("\n7. PARTICIONES MENSUALES")

        particiones_original = original.groupBy(
            "anio",
            "mes"
        ).count()

        particiones_optimizado = optimizado.groupBy(
            "anio",
            "mes"
        ).count()

        total_particiones_original = (
            particiones_original.count()
        )

        total_particiones_optimizado = (
            particiones_optimizado.count()
        )

        diferencias_particiones = (
            particiones_original.exceptAll(
                particiones_optimizado
            ).count()
            +
            particiones_optimizado.exceptAll(
                particiones_original
            ).count()
        )

        print(
            "Particiones originales: "
            f"{total_particiones_original}"
        )

        print(
            "Particiones optimizadas: "
            f"{total_particiones_optimizado}"
        )

        print(
            "Diferencias en particiones: "
            f"{diferencias_particiones}"
        )

        validar(
            "Igualdad de cantidad de particiones",
            total_particiones_original
            == total_particiones_optimizado
        )

        validar(
            "Igualdad de registros por particion",
            diferencias_particiones == 0
        )

        # 8. Verificacion fisica de la optimizacion

        print("\n8. OPTIMIZACION FISICA")

        reduccion_archivos = (
            (
                archivos_original
                - archivos_optimizado
            )
            / archivos_original
            * 100
            if archivos_original > 0
            else 0
        )

        print(
            f"Archivos originales: {archivos_original}"
        )

        print(
            f"Archivos optimizados: {archivos_optimizado}"
        )

        print(
            "Reduccion de archivos: "
            f"{reduccion_archivos:.2f}%"
        )

        validar(
            "Archivos optimizados existentes",
            archivos_optimizado > 0
        )

        validar(
            "Reduccion efectiva de archivos",
            archivos_optimizado < archivos_original
        )

        validar(
            "Un archivo por particion mensual",
            archivos_optimizado
            == total_particiones_optimizado
        )

        # 9. Resultado final

        print("\n" + "=" * 80)
        print("RESULTADO FINAL")
        print("=" * 80)

        aprobados = sum(
            1 for _, estado in controles
            if estado
        )

        fallidos = len(controles) - aprobados

        print(f"Controles ejecutados: {len(controles)}")
        print(f"Controles aprobados: {aprobados}")
        print(f"Controles fallidos: {fallidos}")

        print(
            "Equivalencia exacta: "
            + ("SI" if equivalencia_exacta else "NO")
        )

        if fallidos > 0:

            raise ValueError(
                "La version optimizada no ha sido aprobada"
            )

        if equivalencia_exacta:

            print(
                "RESULTADO: VERSIONES EXACTAMENTE EQUIVALENTES"
            )

        else:

            print(
                "RESULTADO: VERSIONES EQUIVALENTES "
                "DENTRO DE LA TOLERANCIA NUMERICA"
            )

        print("OPTIMIZACION GOLD SAB APROBADA")

    finally:

        spark.catalog.clearCache()
        spark.stop()


if __name__ == "__main__":
    main()
