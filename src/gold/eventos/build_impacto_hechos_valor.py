
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


VENTANA_DIAS = 7


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


def preparar_candidatos(hechos):

    # Una fila por expediente e instrumento.

    candidatos = hechos.filter(
        F.col("candidato_impacto_bursatil")
    ).select(
        "numero_expediente",
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
        "evento_principal",
        F.explode(
            "nemonicos_cotizables"
        ).alias("nemonico")
    )

    return candidatos


def preparar_cotizaciones(mercado):

    # Solo cotizaciones con fecha y precio de
    # cierre positivo, finito y valido.

    return mercado.filter(
        F.col("fecha").isNotNull()
        & numero_valido("precio_cierre")
        & (F.col("precio_cierre") > 0)
    ).select(
        "nemonico",
        F.col("fecha").alias(
            "fecha_cotizacion"
        ),
        "precio_cierre",
        "monto_negociado",
        "moneda_referencia",
        "monto_negociado_valido",
        "tipo_cambio_venta",
        "embig_peru"
    )


def buscar_cotizacion(
    candidatos,
    cotizaciones,
    direccion
):

    claves = [
        "numero_expediente",
        "nemonico"
    ]

    eventos = candidatos.select(
        *claves,
        "fecha_hecho"
    ).alias("e")

    precios = cotizaciones.alias("p")

    mismo_valor = (
        F.col("e.nemonico")
        == F.col("p.nemonico")
    )

    if direccion == "anterior":

        ventana_temporal = (
            (
                F.col("p.fecha_cotizacion")
                < F.col("e.fecha_hecho")
            )
            & (
                F.col("p.fecha_cotizacion")
                >= F.date_sub(
                    F.col("e.fecha_hecho"),
                    VENTANA_DIAS
                )
            )
        )

    elif direccion == "posterior":

        ventana_temporal = (
            (
                F.col("p.fecha_cotizacion")
                > F.col("e.fecha_hecho")
            )
            & (
                F.col("p.fecha_cotizacion")
                <= F.date_add(
                    F.col("e.fecha_hecho"),
                    VENTANA_DIAS
                )
            )
        )

    else:

        raise ValueError(
            f"Direccion invalida: {direccion}"
        )

    coincidencias = eventos.join(
        precios,
        mismo_valor & ventana_temporal,
        "inner"
    ).select(
        F.col("e.numero_expediente").alias(
            "numero_expediente"
        ),
        F.col("e.nemonico").alias(
            "nemonico"
        ),
        F.col("p.fecha_cotizacion").alias(
            f"fecha_{direccion}"
        ),
        F.col("p.precio_cierre").alias(
            f"precio_{direccion}"
        ),
        F.col("p.monto_negociado").alias(
            f"monto_{direccion}"
        ),
        F.col("p.monto_negociado_valido").alias(
            f"monto_{direccion}_valido"
        ),
        F.col("p.moneda_referencia").alias(
            f"moneda_{direccion}"
        ),
        F.col("p.tipo_cambio_venta").alias(
            f"tipo_cambio_{direccion}"
        ),
        F.col("p.embig_peru").alias(
            f"embig_{direccion}"
        )
    )

    # Seleccionar la cotizacion mas cercana
    # al acontecimiento.

    if direccion == "anterior":

        orden = F.col(
            f"fecha_{direccion}"
        ).desc()

    else:

        orden = F.col(
            f"fecha_{direccion}"
        ).asc()

    ventana = Window.partitionBy(
        *claves
    ).orderBy(orden)

    ventana_conteo = Window.partitionBy(
        *claves
    )

    coincidencias = (
        coincidencias.withColumn(
            f"cotizaciones_ventana_{direccion}",
            F.count("*").over(
                ventana_conteo
            )
        )
        .withColumn(
            "_posicion",
            F.row_number().over(ventana)
        )
        .filter(
            F.col("_posicion") == 1
        )
        .drop("_posicion")
    )

    return coincidencias


def construir_gold(candidatos, mercado):

    claves = [
        "numero_expediente",
        "nemonico"
    ]

    cotizaciones = preparar_cotizaciones(
        mercado
    )

    # 1. Cotizaciones anteriores y posteriores

    anteriores = buscar_cotizacion(
        candidatos,
        cotizaciones,
        "anterior"
    )

    posteriores = buscar_cotizacion(
        candidatos,
        cotizaciones,
        "posterior"
    )

    # 2. Integracion sin perder candidatos

    gold = (
        candidatos.join(
            anteriores,
            claves,
            "left"
        )
        .join(
            posteriores,
            claves,
            "left"
        )
    )

    gold = gold.fillna(
        0,
        subset=[
            "cotizaciones_ventana_anterior",
            "cotizaciones_ventana_posterior"
        ]
    )

    # 3. Disponibilidad de precios

    gold = (
        gold.withColumn(
            "tiene_precio_anterior",
            F.col("fecha_anterior").isNotNull()
            & numero_valido("precio_anterior")
            & (F.col("precio_anterior") > 0)
        )
        .withColumn(
            "tiene_precio_posterior",
            F.col("fecha_posterior").isNotNull()
            & numero_valido("precio_posterior")
            & (F.col("precio_posterior") > 0)
        )
        .withColumn(
            "cobertura_completa",
            F.col("tiene_precio_anterior")
            & F.col("tiene_precio_posterior")
        )
    )

    # 4. Distancias temporales

    gold = (
        gold.withColumn(
            "dias_desde_precio_anterior",
            F.datediff(
                "fecha_hecho",
                "fecha_anterior"
            )
        )
        .withColumn(
            "dias_hasta_precio_posterior",
            F.datediff(
                "fecha_posterior",
                "fecha_hecho"
            )
        )
        .withColumn(
            "dias_entre_cotizaciones",
            F.datediff(
                "fecha_posterior",
                "fecha_anterior"
            )
        )
        .withColumn(
            "ventana_dias",
            F.lit(VENTANA_DIAS)
        )
        .withColumn(
            "ventana_temporal_valida",
            F.coalesce(
                F.col(
                    "dias_desde_precio_anterior"
                ).between(1, VENTANA_DIAS)
                & F.col(
                    "dias_hasta_precio_posterior"
                ).between(1, VENTANA_DIAS),
                F.lit(False)
            )
        )
    )

    # 5. Referencia monetaria
    #
    # La moneda procede del puente de referencia.
    # No certifica la moneda historica de cotizacion.

    gold = (
        gold.withColumn(
            "moneda_referencia_disponible",
            F.col("moneda_anterior").isNotNull()
            & F.col("moneda_posterior").isNotNull()
            & (
                F.trim("moneda_anterior") != ""
            )
            & (
                F.trim("moneda_posterior") != ""
            )
        )
        .withColumn(
            "moneda_referencia_coincidente",
            F.col(
                "moneda_referencia_disponible"
            )
            & F.col(
                "moneda_anterior"
            ).eqNullSafe(
                F.col("moneda_posterior")
            )
        )
        .withColumn(
            "conflicto_moneda_referencia",
            F.col(
                "moneda_referencia_disponible"
            )
            & ~F.col(
                "moneda_referencia_coincidente"
            )
        )
    )

    # 6. Elegibilidad para retorno observado

    gold = gold.withColumn(
        "retorno_calculable",
        F.col("cobertura_completa")
        & F.col("ventana_temporal_valida")
        & ~F.col("conflicto_moneda_referencia")
    )

    # 7. Indicadores de precio

    gold = (
        gold.withColumn(
            "variacion_precio",
            F.when(
                F.col("retorno_calculable"),
                F.col("precio_posterior")
                - F.col("precio_anterior")
            )
        )
        .withColumn(
            "retorno_observado_pct",
            F.when(
                F.col("retorno_calculable"),
                (
                    F.col("precio_posterior")
                    / F.col("precio_anterior")
                    - 1
                ) * 100
            )
        )
        .withColumn(
            "direccion_movimiento",
            F.when(
                ~F.col("retorno_calculable"),
                F.lit("NO_EVALUABLE")
            )
            .when(
                F.col("precio_posterior")
                > F.col("precio_anterior"),
                F.lit("SUBIDA")
            )
            .when(
                F.col("precio_posterior")
                < F.col("precio_anterior"),
                F.lit("BAJADA")
            )
            .otherwise(
                F.lit("SIN_VARIACION")
            )
        )
    )

    # 8. Disponibilidad de montos negociados

    gold = gold.withColumn(
        "montos_disponibles",
        F.coalesce(
            F.col("monto_anterior_valido"),
            F.lit(False)
        )
        & F.coalesce(
            F.col("monto_posterior_valido"),
            F.lit(False)
        )
        & numero_valido("monto_anterior")
        & numero_valido("monto_posterior")
        & (F.col("monto_anterior") >= 0)
        & (F.col("monto_posterior") >= 0)
    )

    # No se ha confirmado la unidad monetaria
    # historica de los montos negociados.
    #
    # Se calcula una variacion exploratoria de
    # los valores reportados para el mismo
    # instrumento, si no hay conflicto conocido.

    gold = (
        gold.withColumn(
            "variacion_monto_calculable",
            F.col("cobertura_completa")
            & F.col("ventana_temporal_valida")
            & F.col("montos_disponibles")
            & F.col("moneda_referencia_coincidente")
            & (F.col("monto_anterior") > 0)
        )
        .withColumn(
            "variacion_monto_negociado_pct",
            F.when(
                F.col(
                    "variacion_monto_calculable"
                ),
                (
                    F.col("monto_posterior")
                    / F.col("monto_anterior")
                    - 1
                ) * 100
            )
        )
    )

    # 9. Contexto macroeconomico

    gold = (
        gold.withColumn(
            "variacion_tipo_cambio_pct",
            F.when(
                F.col("cobertura_completa")
                & numero_valido(
                    "tipo_cambio_anterior"
                )
                & numero_valido(
                    "tipo_cambio_posterior"
                )
                & (
                    F.col("tipo_cambio_anterior") > 0
                )
                & (
                    F.col("tipo_cambio_posterior") > 0
                ),
                (
                    F.col("tipo_cambio_posterior")
                    / F.col("tipo_cambio_anterior")
                    - 1
                ) * 100
            )
        )
        .withColumn(
            "variacion_embig",
            F.when(
                F.col("cobertura_completa")
                & numero_valido("embig_anterior")
                & numero_valido("embig_posterior")
                & (F.col("embig_anterior") >= 0)
                & (F.col("embig_posterior") >= 0),
                F.col("embig_posterior")
                - F.col("embig_anterior")
            )
        )
    )

    # 10. Clasificacion de cobertura

    gold = gold.withColumn(
        "estado_cobertura",
        F.when(
            F.col("cobertura_completa"),
            F.lit("COMPLETA")
        )
        .when(
            F.col("tiene_precio_anterior"),
            F.lit("SOLO_ANTERIOR")
        )
        .when(
            F.col("tiene_precio_posterior"),
            F.lit("SOLO_POSTERIOR")
        )
        .otherwise(
            F.lit("SIN_COTIZACIONES")
        )
    )

    # 11. Motivos de calidad

    gold = gold.withColumn(
        "motivos_calidad",
        F.concat_ws(
            ";",
            F.when(
                ~F.col("tiene_precio_anterior"),
                F.lit("SIN_PRECIO_ANTERIOR")
            ),
            F.when(
                ~F.col("tiene_precio_posterior"),
                F.lit("SIN_PRECIO_POSTERIOR")
            ),
            F.when(
                ~F.col("ventana_temporal_valida"),
                F.lit("VENTANA_INCOMPLETA_O_INVALIDA")
            ),
            F.when(
                ~F.col(
                    "moneda_referencia_disponible"
                ),
                F.lit("MONEDA_REFERENCIA_NO_DISPONIBLE")
            ),
            F.when(
                F.col("conflicto_moneda_referencia"),
                F.lit("CONFLICTO_MONEDA_REFERENCIA")
            ),
            F.when(
                ~F.col("montos_disponibles"),
                F.lit("MONTOS_NO_DISPONIBLES")
            ),
            F.when(
                ~F.col(
                    "variacion_monto_calculable"
                ),
                F.lit("VARIACION_MONTO_NO_CALCULABLE")
            )
        )
    )

    # 12. Metadatos de la metodologia

    gold = (
        gold.withColumn(
            "metodologia",
            F.lit("CIERRES_CERCANOS_7D_SIN_DIA_EVENTO_V1")
        )
        .withColumn(
            "precio_ajustado_confirmado",
            F.lit(False)
        )
        .withColumn(
            "moneda_historica_confirmada",
            F.lit(False)
        )
        .withColumn(
            "anio",
            F.year("fecha_hecho")
        )
        .withColumn(
            "mes",
            F.month("fecha_hecho")
        )
        .withColumn(
            "trimestre",
            F.quarter("fecha_hecho")
        )
    )

    return gold


def main():

    parser = argparse.ArgumentParser(
        description="Construccion Gold impacto_hechos_valor"
    )

    parser.add_argument(
        "--gold-root",
        required=True
    )

    args = parser.parse_args()

    gold_root = args.gold_root.rstrip("/")

    ruta_salida = (
        f"{gold_root}/eventos/impacto_hechos_valor"
    )

    spark = (
        SparkSession.builder
        .appName("BVL_Gold_Impacto_Hechos_Valor")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    gold = None
    candidatos = None

    try:

        print("\n" + "=" * 80)
        print("BVL ANALYTICS - GOLD IMPACTO HECHOS VALOR")
        print("=" * 80)

        # 1. Lectura de productos Gold

        hechos = spark.read.parquet(
            f"{gold_root}/eventos/hechos_empresa"
        )

        mercado = spark.read.parquet(
            f"{gold_root}/mercado/mercado_valor_diario"
        )

        print(f"\nEventos Gold: {hechos.count()}")
        print(f"Cotizaciones Gold: {mercado.count()}")
        print(f"Ventana por lado: {VENTANA_DIAS} dias")

        # 2. Validacion de entradas

        validar_clave(
            hechos,
            ["numero_expediente"],
            "hechos_empresa"
        )

        validar_clave(
            mercado,
            ["nemonico", "fecha"],
            "mercado_valor_diario"
        )

        # 3. Preparacion de candidatos

        candidatos = preparar_candidatos(
            hechos
        ).cache()

        total_candidatos = candidatos.count()

        validar_clave(
            candidatos,
            ["numero_expediente", "nemonico"],
            "candidatos_impacto"
        )

        print("\nCANDIDATOS A IMPACTO")

        candidatos.agg(
            F.count("*").alias(
                "pares_evento_valor"
            ),
            F.countDistinct(
                "numero_expediente"
            ).alias("eventos"),
            F.countDistinct(
                "nemonico"
            ).alias("nemonicos")
        ).show(truncate=False)

        # 4. Construccion Gold

        gold = construir_gold(
            candidatos,
            mercado
        ).cache()

        total_gold = gold.count()

        validar_clave(
            gold,
            ["numero_expediente", "nemonico"],
            "impacto_hechos_valor"
        )

        if total_candidatos != total_gold:

            raise ValueError(
                "La transformacion modifico la "
                "cantidad de pares candidatos"
            )

        # 5. Cobertura de precios

        print("\n" + "=" * 80)
        print("COBERTURA TEMPORAL DE COTIZACIONES")
        print("=" * 80)

        gold.agg(
            F.count("*").alias("pares"),
            contar_si(
                F.col("tiene_precio_anterior")
            ).alias("con_anterior"),
            contar_si(
                F.col("tiene_precio_posterior")
            ).alias("con_posterior"),
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

        # 6. Control de ventanas temporales

        print("\nCONTROL TEMPORAL")

        anteriores_invalidos = gold.filter(
            F.col("fecha_anterior").isNotNull()
            & (
                (
                    F.col("fecha_anterior")
                    >= F.col("fecha_hecho")
                )
                | (
                    F.col("dias_desde_precio_anterior")
                    > VENTANA_DIAS
                )
            )
        ).count()

        posteriores_invalidos = gold.filter(
            F.col("fecha_posterior").isNotNull()
            & (
                (
                    F.col("fecha_posterior")
                    <= F.col("fecha_hecho")
                )
                | (
                    F.col("dias_hasta_precio_posterior")
                    > VENTANA_DIAS
                )
            )
        ).count()

        print(
            "Cotizaciones anteriores invalidas: "
            f"{anteriores_invalidos}"
        )

        print(
            "Cotizaciones posteriores invalidas: "
            f"{posteriores_invalidos}"
        )

        if anteriores_invalidos > 0 or posteriores_invalidos > 0:

            raise ValueError(
                "Existen cotizaciones fuera de la ventana"
            )

        # 7. Control de retornos no elegibles

        retornos_invalidos = gold.filter(
            ~F.col("retorno_calculable")
            & (
                F.col("retorno_observado_pct").isNotNull()
                | F.col("variacion_precio").isNotNull()
            )
        ).count()

        montos_invalidos = gold.filter(
            ~F.col("variacion_monto_calculable")
            & F.col(
                "variacion_monto_negociado_pct"
            ).isNotNull()
        ).count()

        print(
            "Retornos calculados sin elegibilidad: "
            f"{retornos_invalidos}"
        )

        print(
            "Variaciones de monto sin elegibilidad: "
            f"{montos_invalidos}"
        )

        if retornos_invalidos > 0 or montos_invalidos > 0:

            raise ValueError(
                "Existen indicadores no elegibles"
            )

        # 8. Control de indicadores no finitos

        print("\nCONTROL DE INDICADORES NO FINITOS")

        indicadores = [
            "variacion_precio",
            "retorno_observado_pct",
            "variacion_monto_negociado_pct",
            "variacion_tipo_cambio_pct",
            "variacion_embig"
        ]

        for campo in indicadores:

            invalidos = gold.filter(
                F.col(campo).isNotNull()
                & ~numero_valido(campo)
            ).count()

            print(f"{campo}: {invalidos}")

            if invalidos > 0:

                raise ValueError(
                    f"Indicador no finito: {campo}"
                )

        # 9. Distribucion del retorno observado

        print("\nDIRECCION DEL MOVIMIENTO")

        gold.groupBy(
            "direccion_movimiento"
        ).count().orderBy(
            "direccion_movimiento"
        ).show(truncate=False)

        print("\nESTADISTICAS DE RETORNO OBSERVADO")

        gold.filter(
            F.col("retorno_calculable")
        ).agg(
            F.count("*").alias("registros"),
            F.min("retorno_observado_pct").alias(
                "retorno_min"
            ),
            F.max("retorno_observado_pct").alias(
                "retorno_max"
            ),
            F.avg("retorno_observado_pct").alias(
                "retorno_promedio"
            ),
            F.expr(
                "percentile_approx(retorno_observado_pct, 0.5)"
            ).alias("retorno_mediano")
        ).show(truncate=False)

        # 10. Distancias temporales

        print("\nDISTANCIAS TEMPORALES")

        gold.filter(
            F.col("cobertura_completa")
        ).agg(
            F.min(
                "dias_desde_precio_anterior"
            ).alias("anterior_min"),
            F.max(
                "dias_desde_precio_anterior"
            ).alias("anterior_max"),
            F.avg(
                "dias_desde_precio_anterior"
            ).alias("anterior_promedio"),
            F.min(
                "dias_hasta_precio_posterior"
            ).alias("posterior_min"),
            F.max(
                "dias_hasta_precio_posterior"
            ).alias("posterior_max"),
            F.avg(
                "dias_hasta_precio_posterior"
            ).alias("posterior_promedio")
        ).show(truncate=False)

        # 11. Cobertura monetaria de referencia

        print("\nCOBERTURA MONETARIA")

        gold.agg(
            contar_si(
                F.col("moneda_referencia_disponible")
            ).alias("moneda_disponible"),
            contar_si(
                F.col("moneda_referencia_coincidente")
            ).alias("moneda_coincidente"),
            contar_si(
                F.col("conflicto_moneda_referencia")
            ).alias("conflictos")
        ).show(truncate=False)

        # 12. Pares sin cobertura completa

        print("\nPARES SIN COBERTURA COMPLETA")

        gold.filter(
            ~F.col("cobertura_completa")
        ).select(
            "numero_expediente",
            "fecha_hecho",
            "empresa",
            "nemonico",
            "fecha_anterior",
            "fecha_posterior",
            "estado_cobertura",
            "motivos_calidad"
        ).orderBy(
            "fecha_hecho",
            "nemonico"
        ).show(50, truncate=False)

        # 13. Muestra de movimientos observados

        print("\nMUESTRA GOLD IMPACTO")

        gold.filter(
            F.col("retorno_calculable")
        ).select(
            "numero_expediente",
            "fecha_hecho",
            "nemonico",
            "fecha_anterior",
            "precio_anterior",
            "fecha_posterior",
            "precio_posterior",
            "retorno_observado_pct",
            "direccion_movimiento"
        ).orderBy(
            "fecha_hecho",
            "numero_expediente",
            "nemonico"
        ).show(20, truncate=False)

        # 14. Esquema final

        print("\nESQUEMA GOLD")

        gold.printSchema()

        # 15. Escritura segura

        print(f"\nRuta de salida: {ruta_salida}")

        (
            gold.repartition("anio")
            .write
            .mode("errorifexists")
            .option("compression", "snappy")
            .partitionBy("anio")
            .parquet(ruta_salida)
        )

        print("\n" + "=" * 80)
        print("CONSTRUCCION GOLD IMPACTO FINALIZADA")
        print("=" * 80)

        print(f"Pares candidatos: {total_candidatos}")
        print(f"Registros Gold: {total_gold}")
        print(f"Columnas Gold: {len(gold.columns)}")
        print(f"Ruta Gold: {ruta_salida}")

    finally:

        if gold is not None:
            gold.unpersist()

        if candidatos is not None:
            candidatos.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
