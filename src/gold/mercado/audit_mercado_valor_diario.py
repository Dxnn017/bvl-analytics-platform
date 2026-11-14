
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


CLAVES = ["nemonico", "fecha"]

TOLERANCIA_ABSOLUTA = 1e-8
TOLERANCIA_RELATIVA = 1e-10


def numero_valido(columna):

    c = F.col(columna).cast("double")

    return F.coalesce(
        c.isNotNull()
        & ~F.isnan(c)
        & (F.abs(c) != float("inf")),
        F.lit(False)
    )


def contar_si(condicion):

    return F.sum(
        F.when(condicion, 1).otherwise(0)
    )


def comparar_exactos(df, campos):

    condicion = F.lit(False)

    for actual, esperado in campos:

        condicion = (
            condicion
            | ~F.col(actual).eqNullSafe(
                F.col(esperado)
            )
        )

    return df.filter(condicion).count()


def comparar_indicador(df, campo, esperado):

    actual = F.col(campo)

    tolerancia = (
        F.lit(TOLERANCIA_ABSOLUTA)
        + F.abs(esperado)
        * F.lit(TOLERANCIA_RELATIVA)
    )

    return df.filter(
        (actual.isNull() != esperado.isNull())
        | (
            actual.isNotNull()
            & ~numero_valido(campo)
        )
        | (
            F.abs(actual - esperado)
            > tolerancia
        )
    ).count()


def validar_claves(df, claves):

    condicion = F.lit(False)

    for clave in claves:

        condicion = condicion | (
            F.col(clave).isNull()
            | (
                F.trim(
                    F.col(clave).cast("string")
                ) == ""
            )
        )

    invalidas = df.filter(
        condicion
    ).count()

    duplicadas = (
        df.groupBy(*claves)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    return invalidas, duplicadas


def main():

    parser = argparse.ArgumentParser(
        description="Auditoria complementaria Gold Mercado"
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

    spark = (
        SparkSession.builder
        .appName("BVL_Auditoria_Complementaria_Mercado")
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
        print("AUDITORIA COMPLEMENTARIA GOLD - MERCADO")
        print("=" * 80)

        # 1. Lectura de fuentes Silver

        cotizaciones = spark.read.parquet(
            f"{silver_root}/mercado_diario/cotizaciones"
        )

        tipo_cambio = spark.read.parquet(
            f"{silver_root}/macroeconomia/tipo_cambio"
        )

        embig = spark.read.parquet(
            f"{silver_root}/macroeconomia/embig"
        )

        puente = spark.read.parquet(
            f"{silver_root}/referencias_smv/puente_ruc_nemonico"
        )

        gold = spark.read.parquet(
            f"{gold_root}/mercado/mercado_valor_diario"
        )

        archivos_parquet = len(
            gold.inputFiles()
        )

        gold = gold.cache()

        print("\n1. INVENTARIO DE FUENTES")

        total_silver = cotizaciones.count()
        total_gold = gold.count()

        print(f"Cotizaciones Silver: {total_silver}")
        print(f"Tipo de cambio Silver: {tipo_cambio.count()}")
        print(f"EMBIG Silver: {embig.count()}")
        print(f"Puente SMV Silver: {puente.count()}")
        print(f"Registros Gold: {total_gold}")
        print(f"Columnas Gold: {len(gold.columns)}")

        validar(
            "Conservacion global de registros",
            total_silver == total_gold
        )

        # 2. Integridad de las fuentes de entrada

        print("\n2. INTEGRIDAD DE FUENTES")

        fuentes = [
            (
                "cotizaciones",
                cotizaciones,
                ["valor", "fecha_cotizacion"]
            ),
            (
                "tipo_cambio",
                tipo_cambio,
                ["fecha"]
            ),
            (
                "embig",
                embig,
                ["fecha"]
            ),
            (
                "puente_smv",
                puente,
                ["nemonico_valor"]
            ),
            (
                "mercado_gold",
                gold,
                CLAVES
            )
        ]

        for nombre, df, claves in fuentes:

            invalidas, duplicadas = validar_claves(
                df,
                claves
            )

            print(
                f"{nombre}: "
                f"invalidas={invalidas}, "
                f"duplicadas={duplicadas}"
            )

            validar(
                f"Integridad de clave {nombre}",
                invalidas == 0
                and duplicadas == 0
            )

            if invalidas > 0 or duplicadas > 0:

                raise ValueError(
                    f"Clave invalida en {nombre}"
                )

        # 3. Preparacion de datos originales

        originales = cotizaciones.select(

            F.col("valor").alias(
                "nemonico"
            ),

            F.col("fecha_cotizacion").alias(
                "fecha"
            ),

            F.col("descripcion").alias(
                "descripcion_esperada"
            ),

            F.col("fecha_anterior").alias(
                "fecha_anterior_esperada"
            ),

            F.col("cierre_anterior").alias(
                "precio_anterior_esperado"
            ),

            F.col("cierre_actual").alias(
                "precio_cierre_esperado"
            ),

            F.col("apertura_actual").alias(
                "precio_apertura_esperado"
            ),

            F.col("maxima_actual").alias(
                "precio_maximo_esperado"
            ),

            F.col("minima_actual").alias(
                "precio_minimo_esperado"
            ),

            F.col("promedio_actual").alias(
                "precio_promedio_esperado"
            ),

            F.col("monto_negociado").alias(
                "monto_negociado_esperado"
            ),

            F.col("fuente").alias(
                "fuente_esperada"
            ),

            F.col("fecha_ingesta").alias(
                "fecha_ingesta_esperada"
            )
        )

        # 4. Conservacion de claves

        print("\n3. CONSERVACION DE CLAVES")

        faltantes = originales.select(
            *CLAVES
        ).join(
            gold.select(*CLAVES),
            CLAVES,
            "left_anti"
        ).count()

        adicionales = gold.select(
            *CLAVES
        ).join(
            originales.select(*CLAVES),
            CLAVES,
            "left_anti"
        ).count()

        print(f"Cotizaciones faltantes: {faltantes}")
        print(f"Cotizaciones adicionales: {adicionales}")

        validar(
            "Sin cotizaciones perdidas",
            faltantes == 0
        )

        validar(
            "Sin cotizaciones adicionales",
            adicionales == 0
        )

        revision = gold.join(
            originales,
            CLAVES,
            "inner"
        )

        # 5. Conservacion de todos los precios

        print("\n4. CONSERVACION DE PRECIOS")

        precios = [
            "precio_anterior",
            "precio_cierre",
            "precio_apertura",
            "precio_maximo",
            "precio_minimo",
            "precio_promedio",
            "monto_negociado"
        ]

        for campo in precios:

            errores = comparar_exactos(
                revision,
                [
                    (
                        campo,
                        f"{campo}_esperado"
                    )
                ]
            )

            print(f"{campo} modificados: {errores}")

            validar(
                f"Conservacion de {campo}",
                errores == 0
            )

        # 6. Conservacion de otros campos

        print("\n5. CONSERVACION DE METADATOS")

        errores_metadatos = comparar_exactos(
            revision,
            [
                (
                    "descripcion",
                    "descripcion_esperada"
                ),
                (
                    "fecha_anterior",
                    "fecha_anterior_esperada"
                ),
                (
                    "fuente",
                    "fuente_esperada"
                ),
                (
                    "fecha_ingesta",
                    "fecha_ingesta_esperada"
                )
            ]
        )

        print(
            "Metadatos originales modificados: "
            f"{errores_metadatos}"
        )

        validar(
            "Conservacion de metadatos Silver",
            errores_metadatos == 0
        )

        # 7. Integracion macroeconomica

        print("\n6. INTEGRACION MACROECONOMICA")

        tc_referencia = tipo_cambio.select(
            "fecha",
            F.col("tipo_cambio_venta").alias(
                "tipo_cambio_esperado"
            )
        )

        embig_referencia = embig.select(
            "fecha",
            F.col("embig_peru").alias(
                "embig_esperado"
            )
        )

        revision = (
            revision.join(
                tc_referencia,
                "fecha",
                "left"
            )
            .join(
                embig_referencia,
                "fecha",
                "left"
            )
        )

        errores_tc = comparar_exactos(
            revision,
            [
                (
                    "tipo_cambio_venta",
                    "tipo_cambio_esperado"
                )
            ]
        )

        errores_embig = comparar_exactos(
            revision,
            [
                (
                    "embig_peru",
                    "embig_esperado"
                )
            ]
        )

        print(f"Tipos de cambio incorrectos: {errores_tc}")
        print(f"EMBIG incorrectos: {errores_embig}")

        validar(
            "Integracion correcta de tipo de cambio",
            errores_tc == 0
        )

        validar(
            "Integracion correcta de EMBIG",
            errores_embig == 0
        )

        # 8. Integracion con el puente SMV

        print("\n7. IDENTIDAD EMPRESARIAL")

        puente_referencia = puente.select(

            F.col("nemonico_valor").alias(
                "nemonico"
            ),

            F.col("ruc").alias(
                "ruc_esperado"
            ),

            F.col("razon_social").alias(
                "razon_social_esperada"
            ),

            F.col("codigo_isin").alias(
                "codigo_isin_esperado"
            ),

            F.col("tipo_valor").alias(
                "tipo_valor_esperado"
            ),

            F.col("moneda").alias(
                "moneda_referencia_esperada"
            ),

            F.col("fecha_inscripcion").alias(
                "fecha_inscripcion_esperada"
            )
        )

        revision = revision.join(
            puente_referencia,
            "nemonico",
            "left"
        )

        errores_identidad = comparar_exactos(
            revision,
            [
                ("ruc", "ruc_esperado"),
                (
                    "razon_social",
                    "razon_social_esperada"
                ),
                (
                    "codigo_isin",
                    "codigo_isin_esperado"
                ),
                (
                    "tipo_valor",
                    "tipo_valor_esperado"
                )
            ]
        )

        errores_moneda = comparar_exactos(
            revision,
            [
                (
                    "moneda_referencia",
                    "moneda_referencia_esperada"
                )
            ]
        )

        errores_inscripcion = comparar_exactos(
            revision,
            [
                (
                    "fecha_inscripcion",
                    "fecha_inscripcion_esperada"
                )
            ]
        )

        print(
            f"Identidades incorrectas: {errores_identidad}"
        )

        print(
            f"Monedas de referencia incorrectas: {errores_moneda}"
        )

        print(
            f"Fechas de inscripcion incorrectas: {errores_inscripcion}"
        )

        validar(
            "Integracion correcta de identidad empresarial",
            errores_identidad == 0
        )

        validar(
            "Conservacion de moneda de referencia",
            errores_moneda == 0
        )

        validar(
            "Conservacion de fecha de inscripcion",
            errores_inscripcion == 0
        )

        revision = revision.cache()

        total_integrado = revision.count()

        print(
            "Registros despues de integrar referencias: "
            f"{total_integrado}"
        )

        validar(
            "Integracion sin multiplicacion de registros",
            total_integrado == total_gold
        )

        # 9. Disponibilidad macroeconomica

        print("\n8. DISPONIBILIDAD MACROECONOMICA")

        tc_disponible = (
            numero_valido("tipo_cambio_esperado")
            & (F.col("tipo_cambio_esperado") > 0)
        )

        embig_disponible = (
            numero_valido("embig_esperado")
            & (F.col("embig_esperado") >= 0)
        )

        errores_disponibilidad_macro = revision.filter(

            ~F.col(
                "tipo_cambio_disponible"
            ).eqNullSafe(
                tc_disponible
            )

            | ~F.col(
                "embig_disponible"
            ).eqNullSafe(
                embig_disponible
            )

            | ~F.col(
                "macro_disponible"
            ).eqNullSafe(
                tc_disponible
                & embig_disponible
            )

        ).count()

        print(
            "Banderas macroeconomicas incorrectas: "
            f"{errores_disponibilidad_macro}"
        )

        validar(
            "Disponibilidad macroeconomica correcta",
            errores_disponibilidad_macro == 0
        )

        # 10. Disponibilidad empresarial

        print("\n9. DISPONIBILIDAD EMPRESARIAL")

        empresa_vinculada = (
            F.col("ruc_esperado").isNotNull()
            & (
                F.trim("ruc_esperado") != ""
            )
        )

        errores_vinculacion = revision.filter(
            ~F.col(
                "empresa_vinculada"
            ).eqNullSafe(
                empresa_vinculada
            )
        ).count()

        print(
            "Banderas de vinculacion incorrectas: "
            f"{errores_vinculacion}"
        )

        validar(
            "Disponibilidad empresarial correcta",
            errores_vinculacion == 0
        )

        # La fecha de inscripcion es una
        # referencia del snapshot SMV.
        # No demuestra identidad historica.

        anteriores_inscripcion = revision.filter(
            F.col(
                "fecha_inscripcion_esperada"
            ).isNotNull()
            & (
                F.col("fecha")
                < F.col(
                    "fecha_inscripcion_esperada"
                )
            )
        ).count()

        print(
            "Cotizaciones anteriores a la inscripcion: "
            f"{anteriores_inscripcion}"
        )

        # 11. Calidad de precios

        print("\n10. CALIDAD DE PRECIOS")

        anterior_valido = (
            numero_valido("precio_anterior_esperado")
            & (
                F.col("precio_anterior_esperado") > 0
            )
        )

        cierre_valido = (
            numero_valido("precio_cierre_esperado")
            & (
                F.col("precio_cierre_esperado") > 0
            )
        )

        rango_valido = (
            numero_valido("precio_maximo_esperado")
            & numero_valido("precio_minimo_esperado")
            & (
                F.col("precio_maximo_esperado") >= 0
            )
            & (
                F.col("precio_minimo_esperado") >= 0
            )
            & (
                F.col("precio_maximo_esperado")
                >= F.col("precio_minimo_esperado")
            )
        )

        flags_precio = [

            (
                "precio_anterior_valido",
                anterior_valido
            ),

            (
                "precio_cierre_valido",
                cierre_valido
            ),

            (
                "rango_intradia_valido",
                rango_valido
            ),

            (
                "retorno_calculable",
                anterior_valido
                & cierre_valido
            )
        ]

        for campo, esperado in flags_precio:

            errores = revision.filter(
                ~F.col(campo).eqNullSafe(
                    esperado
                )
            ).count()

            print(f"{campo} incorrectos: {errores}")

            validar(
                f"Bandera correcta {campo}",
                errores == 0
            )

        # 12. Calidad de montos

        print("\n11. CALIDAD DE MONTOS NEGOCIADOS")

        monto_valido = (
            numero_valido("monto_negociado_esperado")
            & (
                F.col("monto_negociado_esperado") >= 0
            )
        )

        monto_positivo = (
            monto_valido
            & (
                F.col("monto_negociado_esperado") > 0
            )
        )

        errores_montos = revision.filter(

            ~F.col(
                "monto_negociado_valido"
            ).eqNullSafe(
                monto_valido
            )

            | ~F.col(
                "monto_negociado_positivo"
            ).eqNullSafe(
                monto_positivo
            )

        ).count()

        print(
            "Banderas de monto incorrectas: "
            f"{errores_montos}"
        )

        validar(
            "Calidad de montos negociados correcta",
            errores_montos == 0
        )

        # 13. Recalculo independiente de indicadores

        print("\n12. RECALCULO DE INDICADORES")

        retorno_valido = (
            anterior_valido
            & cierre_valido
        )

        indicadores = [

            (
                "variacion_precio",

                F.when(
                    retorno_valido,

                    F.col("precio_cierre_esperado")
                    - F.col("precio_anterior_esperado")
                )
            ),

            (
                "retorno_diario_pct",

                F.when(
                    retorno_valido,

                    (
                        (
                            F.col("precio_cierre_esperado")
                            - F.col("precio_anterior_esperado")
                        )
                        / F.col("precio_anterior_esperado")
                    ) * 100
                )
            ),

            (
                "rango_intradia",

                F.when(
                    rango_valido,

                    F.col("precio_maximo_esperado")
                    - F.col("precio_minimo_esperado")
                )
            ),

            (
                "rango_intradia_pct",

                F.when(
                    rango_valido
                    & anterior_valido,

                    (
                        (
                            F.col("precio_maximo_esperado")
                            - F.col("precio_minimo_esperado")
                        )
                        / F.col("precio_anterior_esperado")
                    ) * 100
                )
            )
        ]

        for campo, esperado in indicadores:

            errores = comparar_indicador(
                revision,
                campo,
                esperado
            )

            print(f"{campo} incorrectos: {errores}")

            validar(
                f"Calculo correcto de {campo}",
                errores == 0
            )

        # 14. Distancias temporales

        print("\n13. DISTANCIAS TEMPORALES")

        dias_esperados = F.datediff(
            "fecha",
            "fecha_anterior_esperada"
        )

        errores_dias = revision.filter(
            ~F.col(
                "dias_desde_cierre_anterior"
            ).eqNullSafe(
                dias_esperados
            )
        ).count()

        print(
            "Distancias temporales incorrectas: "
            f"{errores_dias}"
        )

        validar(
            "Distancia desde cotizacion anterior correcta",
            errores_dias == 0
        )

        print("\nDISTRIBUCION DE DISTANCIAS TEMPORALES")

        gold.groupBy(
            "dias_desde_cierre_anterior"
        ).count().orderBy(
            "dias_desde_cierre_anterior"
        ).show(30, truncate=False)

        # 15. Indicadores no finitos

        print("\n14. INDICADORES NO FINITOS")

        condicion_no_finita = F.lit(False)

        for campo in [

            "variacion_precio",
            "retorno_diario_pct",
            "rango_intradia",
            "rango_intradia_pct"

        ]:

            condicion_no_finita = (
                condicion_no_finita
                | (
                    F.col(campo).isNotNull()
                    & ~numero_valido(campo)
                )
            )

        no_finitos = gold.filter(
            condicion_no_finita
        ).count()

        print(
            f"Filas con indicadores no finitos: {no_finitos}"
        )

        validar(
            "Sin indicadores no finitos",
            no_finitos == 0
        )

        # 16. Particionamiento temporal

        print("\n15. PARTICIONAMIENTO")

        errores_particion = gold.filter(

            ~F.col("anio").eqNullSafe(
                F.year("fecha")
            )

            | ~F.col("mes").eqNullSafe(
                F.month("fecha")
            )

            | ~F.col("trimestre").eqNullSafe(
                F.quarter("fecha")
            )

        ).count()

        particiones = gold.select(
            "anio",
            "mes"
        ).distinct().count()

        print(f"Particiones mensuales: {particiones}")
        print(f"Archivos Parquet: {archivos_parquet}")
        print(f"Particiones incorrectas: {errores_particion}")

        validar(
            "Particiones compatibles con la fecha",
            errores_particion == 0
        )

        validar(
            "Existencia de archivos Parquet",
            archivos_parquet > 0
        )

        # 17. Auditoria de valores nulos

        print("\n" + "=" * 80)
        print("AUDITORIA DE COBERTURA Y VALORES NULOS")
        print("=" * 80)

        revision.agg(

            F.count("*").alias(
                "registros"
            ),

            contar_si(
                F.col("tipo_cambio_esperado").isNull()
            ).alias(
                "tc_nulo_silver"
            ),

            contar_si(
                F.col("tipo_cambio_venta").isNull()
            ).alias(
                "tc_nulo_gold"
            ),

            contar_si(
                F.col("embig_peru").isNull()
            ).alias(
                "embig_nulo"
            ),

            contar_si(
                F.col("ruc_esperado").isNull()
            ).alias(
                "sin_ruc_silver"
            ),

            contar_si(
                F.col("ruc").isNull()
            ).alias(
                "sin_ruc_gold"
            ),

            contar_si(
                F.col("retorno_diario_pct").isNull()
            ).alias(
                "retornos_nulos"
            )

        ).show(truncate=False)

        print("\nFECHAS SIN TIPO DE CAMBIO")

        gold.filter(
            F.col("tipo_cambio_venta").isNull()
        ).groupBy(
            "fecha"
        ).count().orderBy(
            "fecha"
        ).show(100, truncate=False)

        print("\nCOBERTURA EMPRESARIAL")

        gold.groupBy(
            "empresa_vinculada"
        ).agg(

            F.count("*").alias(
                "cotizaciones"
            ),

            F.countDistinct(
                "nemonico"
            ).alias(
                "nemonicos"
            )

        ).show(truncate=False)

        print("\nINSTRUMENTOS SIN VINCULACION")

        gold.filter(
            ~F.col("empresa_vinculada")
        ).groupBy(

            "nemonico",
            "descripcion"

        ).count().orderBy(
            F.desc("count")
        ).show(20, truncate=False)

        print("\nCOBERTURA POR ANIO")

        gold.groupBy(
            "anio"
        ).agg(

            F.count("*").alias(
                "registros"
            ),

            contar_si(
                F.col("empresa_vinculada")
            ).alias(
                "con_empresa"
            ),

            contar_si(
                ~F.col("empresa_vinculada")
            ).alias(
                "sin_empresa"
            ),

            contar_si(
                ~F.col("tipo_cambio_disponible")
            ).alias(
                "sin_tipo_cambio"
            ),

            contar_si(
                ~F.col("retorno_calculable")
            ).alias(
                "sin_retorno"
            )

        ).orderBy(
            "anio"
        ).show(truncate=False)

        # 18. Resumen final

        print("\n" + "=" * 80)
        print("RESULTADO FINAL")
        print("=" * 80)

        aprobados = sum(
            1
            for _, estado in controles
            if estado
        )

        fallidos = len(controles) - aprobados

        print(f"Controles ejecutados: {len(controles)}")
        print(f"Controles aprobados: {aprobados}")
        print(f"Controles fallidos: {fallidos}")

        if fallidos > 0:

            print("\nCONTROLES NO APROBADOS")

            for nombre, estado in controles:

                if not estado:
                    print(f"- {nombre}")

            raise ValueError(
                "Auditoria complementaria no aprobada"
            )

        print(
            "RESULTADO: AUDITORIA COMPLEMENTARIA APROBADA"
        )

    finally:

        spark.catalog.clearCache()
        spark.stop()


if __name__ == "__main__":
    main()
