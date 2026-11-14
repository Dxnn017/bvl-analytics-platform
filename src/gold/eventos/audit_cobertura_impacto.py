
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


CLAVES = [
    "numero_expediente",
    "nemonico"
]

VENTANAS = [7, 15, 30]


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


def validar_claves(df, claves, nombre):

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

    print(
        f"{nombre}: "
        f"invalidas={invalidas}, "
        f"duplicadas={duplicadas}"
    )

    if invalidas > 0 or duplicadas > 0:

        raise ValueError(
            f"Clave invalida en {nombre}"
        )


def main():

    parser = argparse.ArgumentParser(
        description="Auditoria de cobertura bursatil Gold"
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
        .appName("BVL_Auditoria_Cobertura_Impacto")
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
        print("AUDITORIA DE COBERTURA - IMPACTO BURSATIL")
        print("=" * 80)

        # 1. Lectura de fuentes

        silver = spark.read.parquet(
            f"{silver_root}/mercado_diario/cotizaciones"
        )

        hechos = spark.read.parquet(
            f"{gold_root}/eventos/hechos_empresa"
        )

        mercado = spark.read.parquet(
            f"{gold_root}/mercado/mercado_valor_diario"
        )

        impacto = spark.read.parquet(
            f"{gold_root}/eventos/impacto_hechos_valor"
        ).cache()

        print("\n1. INVENTARIO DE FUENTES")

        print(f"Cotizaciones Silver: {silver.count()}")
        print(f"Cotizaciones Gold: {mercado.count()}")
        print(f"Eventos Gold: {hechos.count()}")
        print(f"Pares impacto Gold: {impacto.count()}")

        # 2. Integridad de claves

        print("\n2. INTEGRIDAD DE CLAVES")

        validar_claves(
            silver,
            ["valor", "fecha_cotizacion"],
            "cotizaciones_silver"
        )

        validar_claves(
            mercado,
            ["nemonico", "fecha"],
            "mercado_gold"
        )

        validar_claves(
            hechos,
            ["numero_expediente"],
            "hechos_gold"
        )

        validar_claves(
            impacto,
            CLAVES,
            "impacto_gold"
        )

        # 3. Reconstruccion de candidatos

        print("\n3. RECONSTRUCCION DE CANDIDATOS")

        candidatos = hechos.filter(
            F.col("candidato_impacto_bursatil")
        ).select(
            "numero_expediente",
            "fecha_hecho",
            "empresa",
            "ruc",
            "tipo_hecho",
            "sector",
            F.explode(
                "nemonicos_cotizables"
            ).alias("nemonico")
        ).cache()

        validar_claves(
            candidatos,
            CLAVES,
            "candidatos"
        )

        total_candidatos = candidatos.count()

        faltantes = candidatos.select(
            *CLAVES
        ).join(
            impacto.select(*CLAVES),
            CLAVES,
            "left_anti"
        ).count()

        adicionales = impacto.select(
            *CLAVES
        ).join(
            candidatos.select(*CLAVES),
            CLAVES,
            "left_anti"
        ).count()

        print(f"Pares candidatos: {total_candidatos}")
        print(f"Pares faltantes: {faltantes}")
        print(f"Pares adicionales: {adicionales}")

        validar(
            "Conservacion de candidatos",
            total_candidatos == impacto.count()
            and faltantes == 0
            and adicionales == 0
        )

        candidatos.agg(
            F.count("*").alias("pares"),
            F.countDistinct(
                "numero_expediente"
            ).alias("eventos"),
            F.countDistinct(
                "nemonico"
            ).alias("nemonicos"),
            F.min(
                "fecha_hecho"
            ).alias("fecha_min"),
            F.max(
                "fecha_hecho"
            ).alias("fecha_max")
        ).show(truncate=False)

        # 4. Cotizaciones Silver originales

        print("\n4. COTIZACIONES SILVER")

        precios_silver = silver.select(
            F.col("valor").alias("nemonico"),
            F.col(
                "fecha_cotizacion"
            ).alias("fecha"),
            F.col(
                "cierre_actual"
            ).alias("precio_cierre"),
            "monto_negociado"
        )

        precios_gold = mercado.select(
            "nemonico",
            "fecha",
            "precio_cierre",
            "monto_negociado"
        )

        # Comparacion de todas las cotizaciones
        # utilizadas en esta auditoria.

        columnas = [
            "nemonico",
            "fecha",
            "precio_cierre",
            "monto_negociado"
        ]

        diferencias_silver = (
            precios_silver.select(*columnas)
            .exceptAll(
                precios_gold.select(*columnas)
            )
            .count()
        )

        diferencias_gold = (
            precios_gold.select(*columnas)
            .exceptAll(
                precios_silver.select(*columnas)
            )
            .count()
        )

        print(
            "Diferencias Silver -> Gold: "
            f"{diferencias_silver}"
        )

        print(
            "Diferencias Gold -> Silver: "
            f"{diferencias_gold}"
        )

        validar(
            "Equivalencia de cotizaciones Silver y Gold",
            diferencias_silver == 0
            and diferencias_gold == 0
        )

        # 5. Cobertura historica por instrumento

        print("\n5. COBERTURA HISTORICA POR NEMONICO")

        precios_silver = precios_silver.withColumn(
            "cierre_valido",
            F.col("fecha").isNotNull()
            & numero_valido("precio_cierre")
            & (F.col("precio_cierre") > 0)
        ).cache()

        historico = precios_silver.groupBy(
            "nemonico"
        ).agg(

            F.count("*").alias(
                "cotizaciones_historicas"
            ),

            contar_si(
                F.col("cierre_valido")
            ).alias(
                "cierres_validos_historicos"
            ),

            F.min(
                "fecha"
            ).alias(
                "primera_cotizacion"
            ),

            F.max(
                "fecha"
            ).alias(
                "ultima_cotizacion"
            ),

            contar_si(
                (
                    F.col("fecha")
                    >= F.lit("2025-01-01").cast("date")
                )
                & (
                    F.col("fecha")
                    < F.lit("2025-02-01").cast("date")
                )
            ).alias(
                "cotizaciones_enero_2025"
            ),

            contar_si(
                F.col("cierre_valido")
                & (
                    F.col("fecha")
                    >= F.lit("2025-01-01").cast("date")
                )
                & (
                    F.col("fecha")
                    < F.lit("2025-02-01").cast("date")
                )
            ).alias(
                "cierres_validos_enero_2025"
            )
        )

        # 6. Buscar cotizaciones anteriores y
        # posteriores sin restringir la ventana.
        #
        # Esto permite conocer las distancias
        # reales respecto a cada evento.

        print("\n6. DISTANCIAS HISTORICAS")

        eventos = candidatos.select(
            *CLAVES,
            "fecha_hecho"
        ).alias("e")

        cotizaciones_validas = precios_silver.filter(
            F.col("cierre_valido")
        ).alias("p")

        coincidencias = eventos.join(
            cotizaciones_validas,

            F.col("e.nemonico")
            == F.col("p.nemonico"),

            "inner"
        ).select(

            F.col("e.numero_expediente").alias(
                "numero_expediente"
            ),

            F.col("e.nemonico").alias(
                "nemonico"
            ),

            F.col("e.fecha_hecho").alias(
                "fecha_hecho"
            ),

            F.col("p.fecha").alias(
                "fecha_cotizacion"
            ),

            F.col("p.monto_negociado").alias(
                "monto_negociado"
            )
        )

        anterior = (
            F.col("fecha_cotizacion")
            < F.col("fecha_hecho")
        )

        posterior = (
            F.col("fecha_cotizacion")
            > F.col("fecha_hecho")
        )

        mismo_dia = (
            F.col("fecha_cotizacion")
            == F.col("fecha_hecho")
        )

        referencias = coincidencias.groupBy(
            *CLAVES
        ).agg(

            F.max(
                F.when(
                    anterior,
                    F.col("fecha_cotizacion")
                )
            ).alias(
                "anterior_historica"
            ),

            F.min(
                F.when(
                    posterior,
                    F.col("fecha_cotizacion")
                )
            ).alias(
                "posterior_historica"
            ),

            contar_si(
                anterior
            ).alias(
                "cotizaciones_antes"
            ),

            contar_si(
                posterior
            ).alias(
                "cotizaciones_despues"
            ),

            contar_si(
                mismo_dia
            ).alias(
                "cotizaciones_dia_evento"
            )
        )

        revision = (
            candidatos.join(
                historico,
                "nemonico",
                "left"
            )
            .join(
                referencias,
                CLAVES,
                "left"
            )
            .join(
                impacto.select(
                    *CLAVES,
                    "fecha_anterior",
                    "fecha_posterior",
                    "estado_cobertura",
                    "cobertura_completa",
                    "retorno_calculable",
                    "retorno_observado_pct",
                    "monto_anterior",
                    "monto_posterior"
                ),
                CLAVES,
                "left"
            )
        )

        revision = revision.fillna(
            0,
            subset=[
                "cotizaciones_historicas",
                "cierres_validos_historicos",
                "cotizaciones_enero_2025",
                "cierres_validos_enero_2025",
                "cotizaciones_antes",
                "cotizaciones_despues",
                "cotizaciones_dia_evento"
            ]
        )

        revision = (
            revision.withColumn(

                "distancia_anterior_historica",

                F.datediff(
                    "fecha_hecho",
                    "anterior_historica"
                )
            )
            .withColumn(

                "distancia_posterior_historica",

                F.datediff(
                    "posterior_historica",
                    "fecha_hecho"
                )
            )
        )

        # 7. Analisis de sensibilidad.
        #
        # Las ventanas de 15 y 30 dias son
        # exclusivamente diagnosticas.
        #
        # No reemplazan la ventana oficial.

        print("\n7. ANALISIS DE SENSIBILIDAD")

        for dias in VENTANAS:

            revision = (
                revision.withColumn(

                    f"anterior_{dias}d",

                    F.coalesce(
                        F.col(
                            "distancia_anterior_historica"
                        ).between(1, dias),
                        F.lit(False)
                    )
                )
                .withColumn(

                    f"posterior_{dias}d",

                    F.coalesce(
                        F.col(
                            "distancia_posterior_historica"
                        ).between(1, dias),
                        F.lit(False)
                    )
                )
                .withColumn(

                    f"completa_{dias}d",

                    F.col(f"anterior_{dias}d")
                    & F.col(f"posterior_{dias}d")
                )
            )

        revision = revision.cache()

        validar(
            "Auditoria sin multiplicacion de pares",
            revision.count() == total_candidatos
        )

        resultados_ventanas = []

        for dias in VENTANAS:

            resumen = revision.agg(

                F.count("*").alias("pares"),

                contar_si(
                    F.col(f"anterior_{dias}d")
                ).alias("con_anterior"),

                contar_si(
                    F.col(f"posterior_{dias}d")
                ).alias("con_posterior"),

                contar_si(
                    F.col(f"completa_{dias}d")
                ).alias("completos"),

                contar_si(
                    ~F.col(f"completa_{dias}d")
                ).alias("incompletos")

            ).first()

            resultados_ventanas.append(
                (
                    dias,
                    int(resumen["pares"]),
                    int(resumen["con_anterior"]),
                    int(resumen["con_posterior"]),
                    int(resumen["completos"]),
                    int(resumen["incompletos"]),
                    round(
                        resumen["completos"]
                        / resumen["pares"] * 100,
                        2
                    )
                    if resumen["pares"] > 0
                    else 0.0
                )
            )

        sensibilidad = spark.createDataFrame(
            resultados_ventanas,
            [
                "ventana_dias",
                "pares",
                "con_anterior",
                "con_posterior",
                "cobertura_completa",
                "cobertura_incompleta",
                "cobertura_pct"
            ]
        )

        sensibilidad.orderBy(
            "ventana_dias"
        ).show(truncate=False)

        # 8. Comparacion con metodologia actual

        print("\n8. CONSISTENCIA DE VENTANA OFICIAL")

        errores_cobertura = revision.filter(
            ~F.col(
                "cobertura_completa"
            ).eqNullSafe(
                F.col("completa_7d")
            )
        ).count()

        errores_anterior = revision.filter(

            ~F.col("fecha_anterior").eqNullSafe(

                F.when(
                    F.col("anterior_7d"),
                    F.col("anterior_historica")
                )
            )
        ).count()

        errores_posterior = revision.filter(

            ~F.col("fecha_posterior").eqNullSafe(

                F.when(
                    F.col("posterior_7d"),
                    F.col("posterior_historica")
                )
            )
        ).count()

        print(
            f"Coberturas diferentes: {errores_cobertura}"
        )

        print(
            f"Fechas anteriores diferentes: {errores_anterior}"
        )

        print(
            f"Fechas posteriores diferentes: {errores_posterior}"
        )

        validar(
            "Cobertura oficial reproducible desde Silver",
            errores_cobertura == 0
        )

        validar(
            "Fecha anterior reproducible desde Silver",
            errores_anterior == 0
        )

        validar(
            "Fecha posterior reproducible desde Silver",
            errores_posterior == 0
        )

        # 9. Clasificar motivos observables.
        #
        # Estas categorias describen los datos.
        # No diagnostican por si solas una causa
        # externa como iliquidez o deslistado.

        print("\n9. DIAGNOSTICO DE COBERTURA")

        revision = revision.withColumn(

            "diagnostico_cobertura",

            F.when(
                F.col("completa_7d"),
                F.lit("COBERTURA_COMPLETA_7D")
            )

            .when(
                F.col("cotizaciones_historicas") == 0,
                F.lit("SIN_COTIZACIONES_HISTORICAS")
            )

            .when(
                F.col("cierres_validos_historicos") == 0,
                F.lit("SIN_CIERRES_VALIDOS_HISTORICOS")
            )

            .when(
                F.col("completa_15d"),
                F.lit("RECUPERABLE_EN_15D")
            )

            .when(
                F.col("completa_30d"),
                F.lit("RECUPERABLE_EN_30D")
            )

            .when(
                F.col("anterior_historica").isNull()
                & F.col("posterior_historica").isNull(),
                F.lit("SIN_CIERRES_ANTES_NI_DESPUES")
            )

            .when(
                F.col("anterior_historica").isNull(),
                F.lit("SIN_CIERRE_ANTERIOR_HISTORICO")
            )

            .when(
                F.col("posterior_historica").isNull(),
                F.lit("SIN_CIERRE_POSTERIOR_HISTORICO")
            )

            .otherwise(
                F.lit("CIERRES_FUERA_DE_30D")
            )
        )

        revision.groupBy(
            "diagnostico_cobertura"
        ).agg(

            F.count("*").alias("pares"),

            F.countDistinct(
                "numero_expediente"
            ).alias("eventos"),

            F.countDistinct(
                "nemonico"
            ).alias("nemonicos")

        ).orderBy(
            F.desc("pares"),
            "diagnostico_cobertura"
        ).show(truncate=False)

        # 10. Validar monotonicidad de ventanas

        print("\n10. CONSISTENCIA DE SENSIBILIDAD")

        errores_monotonicidad = revision.filter(

            (
                F.col("completa_7d")
                & ~F.col("completa_15d")
            )

            | (
                F.col("completa_15d")
                & ~F.col("completa_30d")
            )

        ).count()

        print(
            "Errores de monotonicidad: "
            f"{errores_monotonicidad}"
        )

        validar(
            "Cobertura no decreciente por ventana",
            errores_monotonicidad == 0
        )

        # 11. Disponibilidad durante enero de 2025

        print("\n11. COTIZACIONES EN ENERO DE 2025")

        revision.agg(

            F.countDistinct(
                "nemonico"
            ).alias("nemonicos_candidatos"),

            F.countDistinct(
                F.when(
                    F.col(
                        "cotizaciones_enero_2025"
                    ) > 0,
                    F.col("nemonico")
                )
            ).alias("nemonicos_con_cotizacion"),

            F.countDistinct(
                F.when(
                    F.col(
                        "cotizaciones_enero_2025"
                    ) == 0,
                    F.col("nemonico")
                )
            ).alias("nemonicos_sin_cotizacion"),

            contar_si(
                F.col(
                    "cotizaciones_enero_2025"
                ) == 0
            ).alias("pares_sin_cotizacion_en_enero"),

            contar_si(
                F.col(
                    "cotizaciones_dia_evento"
                ) > 0
            ).alias("pares_con_cotizacion_mismo_dia")

        ).show(truncate=False)

        print("\nNEMONICOS SIN COTIZACION EN ENERO")

        revision.filter(
            F.col("cotizaciones_enero_2025") == 0
        ).select(
            "nemonico",
            "empresa",
            "cotizaciones_historicas",
            "primera_cotizacion",
            "ultima_cotizacion"
        ).distinct().orderBy(
            "nemonico"
        ).show(100, truncate=False)

        # 12. Cobertura por instrumento

        print("\n12. COBERTURA POR NEMONICO")

        revision.groupBy(
            "nemonico"
        ).agg(

            F.count("*").alias("pares"),

            contar_si(
                F.col("completa_7d")
            ).alias("completos_7d"),

            contar_si(
                F.col("completa_15d")
            ).alias("completos_15d"),

            contar_si(
                F.col("completa_30d")
            ).alias("completos_30d"),

            F.max(
                "cotizaciones_enero_2025"
            ).alias("cotizaciones_enero"),

            F.max(
                "ultima_cotizacion"
            ).alias("ultima_cotizacion")

        ).orderBy(
            F.desc("pares"),
            "nemonico"
        ).show(100, truncate=False)

        # 13. Cobertura a nivel de evento
        #
        # Un evento puede tener varios nemonicos.
        # No confundir pares con eventos.

        print("\n13. COBERTURA POR EVENTO")

        cobertura_eventos = revision.groupBy(
            "numero_expediente"
        ).agg(

            F.count("*").alias(
                "instrumentos_candidatos"
            ),

            contar_si(
                F.col("completa_7d")
            ).alias(
                "instrumentos_evaluables"
            )
        )

        cobertura_eventos.agg(

            F.count("*").alias(
                "eventos_candidatos"
            ),

            contar_si(
                F.col("instrumentos_evaluables") > 0
            ).alias(
                "eventos_con_algun_instrumento_evaluable"
            ),

            contar_si(
                F.col("instrumentos_evaluables") == 0
            ).alias(
                "eventos_sin_instrumentos_evaluables"
            ),

            contar_si(
                F.col("instrumentos_evaluables")
                == F.col("instrumentos_candidatos")
            ).alias(
                "eventos_con_todos_los_instrumentos"
            )

        ).show(truncate=False)

        # 14. Cobertura por tipo de hecho

        print("\n14. COBERTURA POR TIPO DE HECHO")

        revision.groupBy(
            "tipo_hecho"
        ).agg(

            F.count("*").alias(
                "pares"
            ),

            F.countDistinct(
                "numero_expediente"
            ).alias(
                "eventos"
            ),

            contar_si(
                F.col("completa_7d")
            ).alias(
                "pares_evaluables"
            )

        ).withColumn(

            "cobertura_pct",

            F.round(
                F.col("pares_evaluables")
                / F.col("pares")
                * 100,
                2
            )

        ).orderBy(
            F.desc("pares")
        ).show(100, truncate=False)

        # 15. Actividad negociada en los cierres
        # seleccionados oficialmente.
        #
        # Un precio disponible no garantiza
        # que haya existido una transaccion.

        print("\n15. ACTIVIDAD NEGOCIADA")

        evaluables = revision.filter(
            F.col("completa_7d")
        )

        anterior_positivo = (
            numero_valido("monto_anterior")
            & (F.col("monto_anterior") > 0)
        )

        posterior_positivo = (
            numero_valido("monto_posterior")
            & (F.col("monto_posterior") > 0)
        )

        evaluables.agg(

            F.count("*").alias(
                "pares_con_cobertura"
            ),

            contar_si(
                anterior_positivo
            ).alias(
                "monto_anterior_positivo"
            ),

            contar_si(
                posterior_positivo
            ).alias(
                "monto_posterior_positivo"
            ),

            contar_si(
                anterior_positivo
                & posterior_positivo
            ).alias(
                "actividad_positiva_ambos_lados"
            ),

            contar_si(
                (F.col("monto_anterior") == 0)
                | (F.col("monto_posterior") == 0)
            ).alias(
                "algun_monto_cero"
            )

        ).show(truncate=False)

        # 16. Pares incompletos

        print("\n16. DETALLE DE PARES INCOMPLETOS")

        revision.filter(
            ~F.col("completa_7d")
        ).select(

            "numero_expediente",
            "fecha_hecho",
            "nemonico",
            "estado_cobertura",
            "anterior_historica",
            "posterior_historica",
            "distancia_anterior_historica",
            "distancia_posterior_historica",
            "diagnostico_cobertura"

        ).orderBy(
            "fecha_hecho",
            "numero_expediente",
            "nemonico"
        ).show(120, truncate=False)

        # 17. Control de retornos faltantes

        print("\n17. RETORNOS SIN COBERTURA")

        retornos_invalidos = revision.filter(

            ~F.col("completa_7d")

            & (
                F.col("retorno_observado_pct").isNotNull()
                | F.coalesce(
                    F.col("retorno_calculable"),
                    F.lit(False)
                )
            )
        ).count()

        print(
            "Retornos calculados sin cobertura: "
            f"{retornos_invalidos}"
        )

        validar(
            "Sin retornos inventados",
            retornos_invalidos == 0
        )

        # 18. Resultado final

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

            raise ValueError(
                "Auditoria de cobertura no aprobada"
            )

        print(
            "RESULTADO: AUDITORIA DE COBERTURA APROBADA"
        )

        print(
            "La metodologia oficial de 7 dias "
            "permanece sin modificaciones"
        )

    finally:

        spark.catalog.clearCache()
        spark.stop()


if __name__ == "__main__":
    main()
