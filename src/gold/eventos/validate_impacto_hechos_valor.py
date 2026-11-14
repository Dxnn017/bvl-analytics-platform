
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


VENTANA_DIAS = 7

CLAVES = [
    "numero_expediente",
    "nemonico"
]


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


def comparar(df, campos):

    condicion = F.lit(False)

    for actual, esperado in campos:

        condicion = (
            condicion
            | ~F.col(actual).eqNullSafe(
                F.col(esperado)
            )
        )

    return df.filter(condicion).count()


def main():

    parser = argparse.ArgumentParser(
        description="Validacion Gold impacto_hechos_valor"
    )

    parser.add_argument(
        "--gold-root",
        required=True
    )

    args = parser.parse_args()

    gold_root = args.gold_root.rstrip("/")

    spark = (
        SparkSession.builder
        .appName("BVL_Validacion_Gold_Impacto")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

        # Lectura de productos Gold

        hechos = spark.read.parquet(
            f"{gold_root}/eventos/hechos_empresa"
        )

        mercado = spark.read.parquet(
            f"{gold_root}/mercado/mercado_valor_diario"
        )

        gold = spark.read.parquet(
            f"{gold_root}/eventos/impacto_hechos_valor"
        )

        archivos_parquet = len(gold.inputFiles())

        gold = gold.cache()

        controles = []

        def validar(nombre, condicion):

            controles.append((nombre, condicion))

            estado = "OK" if condicion else "ERROR"

            print(f"{nombre}: {estado}")

        print("\n" + "=" * 80)
        print("VALIDACION GOLD - IMPACTO HECHOS VALOR")
        print("=" * 80)

        # 1. Reconstruccion de candidatos

        print("\n1. CONSERVACION DE CANDIDATOS")

        campos_eventos = [
            "fecha_hecho",
            "hora_presentacion",
            "fecha_acuerdo",
            "empresa",
            "ruc",
            "razon_social_smv",
            "tipo_empresa",
            "sector",
            "tipo_hecho",
            "descripcion",
            "metodo_vinculacion",
            "cantidad_nemonicos",
            "total_documentos",
            "textos_principales_suficientes",
            "evento_principal"
        ]

        candidatos = hechos.filter(
            F.col("evento_principal")
            & F.col("empresa_identificada")
            & F.col("fecha_hecho").isNotNull()
            & (F.size("nemonicos_cotizables") > 0)
        ).select(
            "numero_expediente",
            *campos_eventos,
            F.explode(
                "nemonicos_cotizables"
            ).alias("nemonico")
        ).cache()

        total_candidatos = candidatos.count()
        total_gold = gold.count()

        print(f"Candidatos esperados: {total_candidatos}")
        print(f"Registros Gold: {total_gold}")
        print(f"Columnas Gold: {len(gold.columns)}")

        validar(
            "Igualdad de cantidades",
            total_candidatos == total_gold
        )

        # 2. Integridad de claves

        print("\n2. INTEGRIDAD DE CLAVES")

        invalidas = gold.filter(
            F.col("numero_expediente").isNull()
            | (F.trim("numero_expediente") == "")
            | F.col("nemonico").isNull()
            | (F.trim("nemonico") == "")
        ).count()

        duplicadas = (
            gold.groupBy(*CLAVES)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        faltantes = candidatos.select(
            *CLAVES
        ).join(
            gold.select(*CLAVES),
            CLAVES,
            "left_anti"
        ).count()

        adicionales = gold.select(
            *CLAVES
        ).join(
            candidatos.select(*CLAVES),
            CLAVES,
            "left_anti"
        ).count()

        print(f"Claves invalidas: {invalidas}")
        print(f"Claves duplicadas: {duplicadas}")
        print(f"Pares faltantes: {faltantes}")
        print(f"Pares adicionales: {adicionales}")

        validar(
            "Claves completas",
            invalidas == 0
        )

        validar(
            "Claves unicas",
            duplicadas == 0
        )

        validar(
            "Sin candidatos perdidos",
            faltantes == 0
        )

        validar(
            "Sin candidatos adicionales",
            adicionales == 0
        )

        # 3. Conservacion de informacion empresarial

        print("\n3. CONSERVACION DE EVENTOS")

        originales = candidatos.select(
            *CLAVES,
            *[
                F.col(campo).alias(
                    f"{campo}_esperado"
                )
                for campo in campos_eventos
            ]
        )

        revision = gold.join(
            originales,
            CLAVES,
            "inner"
        )

        errores_eventos = comparar(
            revision,
            [
                (campo, f"{campo}_esperado")
                for campo in campos_eventos
            ]
        )

        print(f"Eventos modificados: {errores_eventos}")

        validar(
            "Conservacion de datos empresariales",
            errores_eventos == 0
        )

        # 4. Cotizaciones validas de origen

        print("\n4. COTIZACIONES DE REFERENCIA")

        cotizaciones = mercado.filter(
            F.col("fecha").isNotNull()
            & numero_valido("precio_cierre")
            & (F.col("precio_cierre") > 0)
        ).select(
            "nemonico",
            "fecha",
            "precio_cierre",
            "monto_negociado",
            "monto_negociado_valido",
            "moneda_referencia",
            "tipo_cambio_venta",
            "embig_peru"
        ).cache()

        eventos = candidatos.select(
            *CLAVES,
            "fecha_hecho"
        ).alias("e")

        precios = cotizaciones.alias("p")

        # Obtener todas las coincidencias
        # dentro de los siete dias por lado.
        # Excluir estrictamente el dia del evento.

        coincidencias = eventos.join(
            precios,
            (
                F.col("e.nemonico")
                == F.col("p.nemonico")
            )
            & (
                F.col("p.fecha")
                >= F.date_sub(
                    F.col("e.fecha_hecho"),
                    VENTANA_DIAS
                )
            )
            & (
                F.col("p.fecha")
                <= F.date_add(
                    F.col("e.fecha_hecho"),
                    VENTANA_DIAS
                )
            )
            & (
                F.col("p.fecha")
                != F.col("e.fecha_hecho")
            ),
            "inner"
        ).select(
            F.col("e.numero_expediente").alias(
                "numero_expediente"
            ),
            F.col("e.nemonico").alias(
                "nemonico"
            ),
            F.col("e.fecha_hecho").alias(
                "fecha_evento"
            ),
            F.col("p.fecha").alias(
                "fecha_cotizacion"
            )
        )

        # 5. Recalculo independiente de fechas

        print("\n5. SELECCION DE FECHAS MAS CERCANAS")

        anterior = (
            F.col("fecha_cotizacion")
            < F.col("fecha_evento")
        )

        posterior = (
            F.col("fecha_cotizacion")
            > F.col("fecha_evento")
        )

        referencias = coincidencias.groupBy(
            *CLAVES
        ).agg(
            F.max(
                F.when(
                    anterior,
                    F.col("fecha_cotizacion")
                )
            ).alias("fecha_anterior_esperada"),

            F.min(
                F.when(
                    posterior,
                    F.col("fecha_cotizacion")
                )
            ).alias("fecha_posterior_esperada"),

            contar_si(
                anterior
            ).alias("conteo_anterior_esperado"),

            contar_si(
                posterior
            ).alias("conteo_posterior_esperado")
        )

        revision = revision.join(
            referencias,
            CLAVES,
            "left"
        ).fillna(
            0,
            subset=[
                "conteo_anterior_esperado",
                "conteo_posterior_esperado"
            ]
        )

        errores_anterior = comparar(
            revision,
            [
                (
                    "fecha_anterior",
                    "fecha_anterior_esperada"
                )
            ]
        )

        errores_posterior = comparar(
            revision,
            [
                (
                    "fecha_posterior",
                    "fecha_posterior_esperada"
                )
            ]
        )

        errores_conteos = comparar(
            revision,
            [
                (
                    "cotizaciones_ventana_anterior",
                    "conteo_anterior_esperado"
                ),
                (
                    "cotizaciones_ventana_posterior",
                    "conteo_posterior_esperado"
                )
            ]
        )

        print(f"Fechas anteriores incorrectas: {errores_anterior}")
        print(f"Fechas posteriores incorrectas: {errores_posterior}")
        print(f"Conteos de ventana incorrectos: {errores_conteos}")

        validar(
            "Seleccion correcta de fecha anterior",
            errores_anterior == 0
        )

        validar(
            "Seleccion correcta de fecha posterior",
            errores_posterior == 0
        )

        validar(
            "Conteos de cotizaciones correctos",
            errores_conteos == 0
        )

        # 6. Recuperacion de valores originales

        print("\n6. CONSERVACION DE COTIZACIONES")

        campos_mercado = {
            "precio": "precio_cierre",
            "monto": "monto_negociado",
            "monto_valido": "monto_negociado_valido",
            "moneda": "moneda_referencia",
            "tipo_cambio": "tipo_cambio_venta",
            "embig": "embig_peru"
        }

        for direccion in ["anterior", "posterior"]:

            datos = cotizaciones.select(
                "nemonico",
                F.col("fecha").alias(
                    f"fecha_{direccion}_esperada"
                ),
                *[
                    F.col(origen).alias(
                        f"{destino}_{direccion}_esperado"
                    )
                    for destino, origen in
                    campos_mercado.items()
                ]
            )

            revision = revision.join(
                datos,
                [
                    "nemonico",
                    f"fecha_{direccion}_esperada"
                ],
                "left"
            )

            comparaciones = []

            for campo in campos_mercado:

                if campo == "monto_valido":

                    actual = (
                        f"monto_{direccion}_valido"
                    )

                else:

                    actual = f"{campo}_{direccion}"

                esperado = (
                    f"{campo}_{direccion}_esperado"
                )

                comparaciones.append(
                    (actual, esperado)
                )

            errores = comparar(
                revision,
                comparaciones
            )

            print(
                f"Cotizaciones {direccion} "
                f"modificadas: {errores}"
            )

            validar(
                f"Conservacion de cotizacion {direccion}",
                errores == 0
            )

        # 7. Control de ventanas temporales

        print("\n7. VENTANAS TEMPORALES")

        anteriores_invalidos = revision.filter(
            F.col("fecha_anterior").isNotNull()
            & ~F.coalesce(
                F.datediff(
                    "fecha_hecho",
                    "fecha_anterior"
                ).between(1, VENTANA_DIAS),
                F.lit(False)
            )
        ).count()

        posteriores_invalidos = revision.filter(
            F.col("fecha_posterior").isNotNull()
            & ~F.coalesce(
                F.datediff(
                    "fecha_posterior",
                    "fecha_hecho"
                ).between(1, VENTANA_DIAS),
                F.lit(False)
            )
        ).count()

        print(f"Anteriores fuera de ventana: {anteriores_invalidos}")
        print(f"Posteriores fuera de ventana: {posteriores_invalidos}")

        validar(
            "Ventana anterior correcta",
            anteriores_invalidos == 0
        )

        validar(
            "Ventana posterior correcta",
            posteriores_invalidos == 0
        )

        # 8. Cobertura de precios

        print("\n8. COBERTURA DE PRECIOS")

        revision = (
            revision.withColumn(
                "anterior_disponible_esperado",
                F.col(
                    "fecha_anterior_esperada"
                ).isNotNull()
            )
            .withColumn(
                "posterior_disponible_esperado",
                F.col(
                    "fecha_posterior_esperada"
                ).isNotNull()
            )
            .withColumn(
                "cobertura_esperada",
                F.col(
                    "anterior_disponible_esperado"
                )
                & F.col(
                    "posterior_disponible_esperado"
                )
            )
        )

        errores_cobertura = comparar(
            revision,
            [
                (
                    "tiene_precio_anterior",
                    "anterior_disponible_esperado"
                ),
                (
                    "tiene_precio_posterior",
                    "posterior_disponible_esperado"
                ),
                (
                    "cobertura_completa",
                    "cobertura_esperada"
                )
            ]
        )

        print(f"Coberturas incorrectas: {errores_cobertura}")

        validar(
            "Disponibilidad de precios correcta",
            errores_cobertura == 0
        )

        # 9. Distancias temporales

        print("\n9. DISTANCIAS TEMPORALES")

        revision = (
            revision.withColumn(
                "dias_anterior_esperados",
                F.datediff(
                    "fecha_hecho_esperado",
                    "fecha_anterior_esperada"
                )
            )
            .withColumn(
                "dias_posterior_esperados",
                F.datediff(
                    "fecha_posterior_esperada",
                    "fecha_hecho_esperado"
                )
            )
            .withColumn(
                "dias_totales_esperados",
                F.datediff(
                    "fecha_posterior_esperada",
                    "fecha_anterior_esperada"
                )
            )
            .withColumn(
                "ventana_valida_esperada",
                F.coalesce(
                    F.col(
                        "dias_anterior_esperados"
                    ).between(1, VENTANA_DIAS)
                    & F.col(
                        "dias_posterior_esperados"
                    ).between(1, VENTANA_DIAS),
                    F.lit(False)
                )
            )
        )

        errores_distancias = comparar(
            revision,
            [
                (
                    "dias_desde_precio_anterior",
                    "dias_anterior_esperados"
                ),
                (
                    "dias_hasta_precio_posterior",
                    "dias_posterior_esperados"
                ),
                (
                    "dias_entre_cotizaciones",
                    "dias_totales_esperados"
                ),
                (
                    "ventana_temporal_valida",
                    "ventana_valida_esperada"
                )
            ]
        )

        validar(
            "Distancias temporales correctas",
            errores_distancias == 0
        )

        # 10. Referencia monetaria

        print("\n10. REFERENCIA MONETARIA")

        moneda_disponible = (
            F.col("moneda_anterior_esperado").isNotNull()
            & F.col("moneda_posterior_esperado").isNotNull()
            & (
                F.trim("moneda_anterior_esperado") != ""
            )
            & (
                F.trim("moneda_posterior_esperado") != ""
            )
        )

        revision = (
            revision.withColumn(
                "moneda_disponible_esperada",
                moneda_disponible
            )
            .withColumn(
                "moneda_coincidente_esperada",
                F.col("moneda_disponible_esperada")
                & F.col(
                    "moneda_anterior_esperado"
                ).eqNullSafe(
                    F.col("moneda_posterior_esperado")
                )
            )
            .withColumn(
                "conflicto_moneda_esperado",
                F.col("moneda_disponible_esperada")
                & ~F.col("moneda_coincidente_esperada")
            )
        )

        errores_moneda = comparar(
            revision,
            [
                (
                    "moneda_referencia_disponible",
                    "moneda_disponible_esperada"
                ),
                (
                    "moneda_referencia_coincidente",
                    "moneda_coincidente_esperada"
                ),
                (
                    "conflicto_moneda_referencia",
                    "conflicto_moneda_esperado"
                )
            ]
        )

        validar(
            "Referencias monetarias correctas",
            errores_moneda == 0
        )

        # 11. Elegibilidad de retornos

        print("\n11. ELEGIBILIDAD DE RETORNOS")

        revision = revision.withColumn(
            "retorno_calculable_esperado",
            F.col("cobertura_esperada")
            & F.col("ventana_valida_esperada")
            & ~F.col("conflicto_moneda_esperado")
        )

        errores_elegibilidad = comparar(
            revision,
            [
                (
                    "retorno_calculable",
                    "retorno_calculable_esperado"
                )
            ]
        )

        validar(
            "Elegibilidad de retornos correcta",
            errores_elegibilidad == 0
        )

        # 12. Elegibilidad de montos negociados

        print("\n12. ELEGIBILIDAD DE MONTOS")

        montos_disponibles = (
            F.coalesce(
                F.col("monto_valido_anterior_esperado"),
                F.lit(False)
            )
            & F.coalesce(
                F.col("monto_valido_posterior_esperado"),
                F.lit(False)
            )
            & numero_valido("monto_anterior_esperado")
            & numero_valido("monto_posterior_esperado")
            & (F.col("monto_anterior_esperado") >= 0)
            & (F.col("monto_posterior_esperado") >= 0)
        )

        revision = (
            revision.withColumn(
                "montos_disponibles_esperados",
                montos_disponibles
            )
            .withColumn(
                "variacion_monto_esperada_valida",
                F.col("cobertura_esperada")
                & F.col("ventana_valida_esperada")
                & F.col("montos_disponibles_esperados")
                & F.col("moneda_coincidente_esperada")
                & (F.col("monto_anterior_esperado") > 0)
            )
        )

        errores_montos = comparar(
            revision,
            [
                (
                    "montos_disponibles",
                    "montos_disponibles_esperados"
                ),
                (
                    "variacion_monto_calculable",
                    "variacion_monto_esperada_valida"
                )
            ]
        )

        validar(
            "Elegibilidad de montos correcta",
            errores_montos == 0
        )

        # 13. Recalculo de indicadores

        print("\n13. RECALCULO DE INDICADORES")

        retorno_valido = F.col(
            "retorno_calculable_esperado"
        )

        monto_valido = F.col(
            "variacion_monto_esperada_valida"
        )

        tipo_cambio_valido = (
            F.col("cobertura_esperada")
            & numero_valido(
                "tipo_cambio_anterior_esperado"
            )
            & numero_valido(
                "tipo_cambio_posterior_esperado"
            )
            & (
                F.col("tipo_cambio_anterior_esperado") > 0
            )
            & (
                F.col("tipo_cambio_posterior_esperado") > 0
            )
        )

        embig_valido = (
            F.col("cobertura_esperada")
            & numero_valido("embig_anterior_esperado")
            & numero_valido("embig_posterior_esperado")
            & (F.col("embig_anterior_esperado") >= 0)
            & (F.col("embig_posterior_esperado") >= 0)
        )

        indicadores = [
            (
                "variacion_precio",
                F.when(
                    retorno_valido,
                    F.col("precio_posterior_esperado")
                    - F.col("precio_anterior_esperado")
                )
            ),
            (
                "retorno_observado_pct",
                F.when(
                    retorno_valido,
                    (
                        (
                            F.col("precio_posterior_esperado")
                            - F.col("precio_anterior_esperado")
                        )
                        / F.col("precio_anterior_esperado")
                    ) * 100
                )
            ),
            (
                "variacion_monto_negociado_pct",
                F.when(
                    monto_valido,
                    (
                        (
                            F.col("monto_posterior_esperado")
                            - F.col("monto_anterior_esperado")
                        )
                        / F.col("monto_anterior_esperado")
                    ) * 100
                )
            ),
            (
                "variacion_tipo_cambio_pct",
                F.when(
                    tipo_cambio_valido,
                    (
                        (
                            F.col("tipo_cambio_posterior_esperado")
                            - F.col("tipo_cambio_anterior_esperado")
                        )
                        / F.col("tipo_cambio_anterior_esperado")
                    ) * 100
                )
            ),
            (
                "variacion_embig",
                F.when(
                    embig_valido,
                    F.col("embig_posterior_esperado")
                    - F.col("embig_anterior_esperado")
                )
            )
        ]

        revision = revision.cache()

        for campo, esperado in indicadores:

            actual = F.col(campo)

            tolerancia = (
                F.lit(0.000001)
                + F.abs(esperado) * F.lit(0.000000001)
            )

            errores = revision.filter(
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

            print(f"{campo} incorrectos: {errores}")

            validar(
                f"Calculo correcto de {campo}",
                errores == 0
            )

        # 14. Control de pares no evaluables

        print("\n14. CONTROL DE PARES NO EVALUABLES")

        retornos_invalidos = revision.filter(
            ~F.col("retorno_calculable_esperado")
            & (
                F.col("retorno_observado_pct").isNotNull()
                | F.col("variacion_precio").isNotNull()
            )
        ).count()

        montos_invalidos = revision.filter(
            ~F.col("variacion_monto_esperada_valida")
            & F.col(
                "variacion_monto_negociado_pct"
            ).isNotNull()
        ).count()

        print(f"Retornos no elegibles: {retornos_invalidos}")
        print(f"Montos no elegibles: {montos_invalidos}")

        validar(
            "Exclusion correcta de retornos",
            retornos_invalidos == 0
        )

        validar(
            "Exclusion correcta de montos",
            montos_invalidos == 0
        )

        # 15. Direccion del movimiento

        print("\n15. DIRECCION DEL MOVIMIENTO")

        direccion_esperada = (
            F.when(
                ~F.col("retorno_calculable_esperado"),
                F.lit("NO_EVALUABLE")
            )
            .when(
                F.col("precio_posterior_esperado")
                > F.col("precio_anterior_esperado"),
                F.lit("SUBIDA")
            )
            .when(
                F.col("precio_posterior_esperado")
                < F.col("precio_anterior_esperado"),
                F.lit("BAJADA")
            )
            .otherwise(
                F.lit("SIN_VARIACION")
            )
        )

        errores_direccion = revision.filter(
            ~F.col("direccion_movimiento").eqNullSafe(
                direccion_esperada
            )
        ).count()

        validar(
            "Direccion del movimiento correcta",
            errores_direccion == 0
        )

        # 16. Estado de cobertura

        print("\n16. ESTADO DE COBERTURA")

        estado_esperado = (
            F.when(
                F.col("cobertura_esperada"),
                F.lit("COMPLETA")
            )
            .when(
                F.col("anterior_disponible_esperado"),
                F.lit("SOLO_ANTERIOR")
            )
            .when(
                F.col("posterior_disponible_esperado"),
                F.lit("SOLO_POSTERIOR")
            )
            .otherwise(
                F.lit("SIN_COTIZACIONES")
            )
        )

        errores_estado = revision.filter(
            ~F.col("estado_cobertura").eqNullSafe(
                estado_esperado
            )
        ).count()

        validar(
            "Estado de cobertura correcto",
            errores_estado == 0
        )

        # 17. Metadatos metodologicos

        print("\n17. METADATOS METODOLOGICOS")

        errores_metodologia = gold.filter(
            ~F.col("ventana_dias").eqNullSafe(
                F.lit(VENTANA_DIAS)
            )
            | ~F.col("metodologia").eqNullSafe(
                F.lit(
                    "CIERRES_CERCANOS_7D_SIN_DIA_EVENTO_V1"
                )
            )
            | ~F.col(
                "precio_ajustado_confirmado"
            ).eqNullSafe(
                F.lit(False)
            )
            | ~F.col(
                "moneda_historica_confirmada"
            ).eqNullSafe(
                F.lit(False)
            )
        ).count()

        validar(
            "Metadatos metodologicos correctos",
            errores_metodologia == 0
        )

        # 18. Particionamiento

        print("\n18. PARTICIONAMIENTO")

        errores_particion = gold.filter(
            ~F.col("anio").eqNullSafe(
                F.year("fecha_hecho")
            )
            | ~F.col("mes").eqNullSafe(
                F.month("fecha_hecho")
            )
            | ~F.col("trimestre").eqNullSafe(
                F.quarter("fecha_hecho")
            )
        ).count()

        print(f"Particiones incorrectas: {errores_particion}")
        print(f"Archivos Parquet: {archivos_parquet}")

        validar(
            "Particiones compatibles con el evento",
            errores_particion == 0
        )

        validar(
            "Existencia de archivos Parquet",
            archivos_parquet > 0
        )

        # 19. Resumen de cobertura

        print("\n" + "=" * 80)
        print("RESUMEN DE COBERTURA BURSATIL")
        print("=" * 80)

        gold.agg(
            F.count("*").alias("pares"),
            F.countDistinct(
                "numero_expediente"
            ).alias("eventos"),
            F.countDistinct(
                "nemonico"
            ).alias("nemonicos"),
            contar_si(
                F.col("cobertura_completa")
            ).alias("cobertura_completa"),
            contar_si(
                F.col("retorno_calculable")
            ).alias("retornos_calculables"),
            contar_si(
                F.col("variacion_monto_calculable")
            ).alias("montos_calculables")
        ).show(truncate=False)

        print("\nESTADOS DE COBERTURA")

        gold.groupBy(
            "estado_cobertura"
        ).count().orderBy(
            "estado_cobertura"
        ).show(truncate=False)

        print("\nDIRECCION DEL MOVIMIENTO")

        gold.groupBy(
            "direccion_movimiento"
        ).count().orderBy(
            "direccion_movimiento"
        ).show(truncate=False)

        # 20. Resultado final

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

        if fallidos > 0:

            raise ValueError(
                "Gold impacto_hechos_valor no aprobado"
            )

        print("RESULTADO: GOLD VALIDADO CORRECTAMENTE")

        spark.catalog.clearCache()

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
