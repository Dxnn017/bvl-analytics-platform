
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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


def contar_si(condicion):

    return F.sum(
        F.when(condicion, 1).otherwise(0)
    )


def construir_gold(eventos, identidad, documentos, textos):

    # 1. Seleccion de acontecimientos empresariales

    hechos = eventos.select(
        "numero_expediente",
        "empresa",
        F.col("fecha_presentacion_date").alias(
            "fecha_hecho"
        ),
        "fecha_presentacion",
        "hora_presentacion",
        F.col("fecha_acuerdo_date").alias(
            "fecha_acuerdo"
        ),
        F.col("fecha_consulta_portal_date").alias(
            "fecha_consulta"
        ),
        "tipo_hecho",
        "tipo_empresa",
        "sector",
        "descripcion",
        "cantidad_documentos_pdf",
        "cantidad_documentos_portal",
        "usar_silver_principal",
        "motivo_calidad",
        "coherencia_temporal"
    )

    # 2. Seleccion de identidad empresarial

    identidad_empresa = identidad.select(
        "numero_expediente",
        "empresa_norm",
        "ruc",
        "razon_social_smv",
        "nemonicos_cotizables",
        "metodo_vinculacion",
        F.col("identificado").alias(
            "identificacion_original"
        )
    )

    # 3. Integracion empresarial

    hechos = hechos.join(
        identidad_empresa,
        "numero_expediente",
        "left"
    )

    # Normalizar lista de nemonicos.
    # No se inventan relaciones empresariales.

    hechos = hechos.withColumn(
        "nemonicos_cotizables",
        F.array_sort(
            F.array_distinct(
                F.filter(
                    F.coalesce(
                        F.col("nemonicos_cotizables"),
                        F.array().cast("array<string>")
                    ),
                    lambda x: (
                        x.isNotNull()
                        & (F.trim(x) != "")
                    )
                )
            )
        )
    )

    hechos = (
        hechos.withColumn(
            "empresa_identificada",
            F.coalesce(
                F.col("identificacion_original"),
                F.lit(False)
            )
        )
        .withColumn(
            "cantidad_nemonicos",
            F.size("nemonicos_cotizables")
        )
        .withColumn(
            "tiene_nemonicos",
            F.col("cantidad_nemonicos") > 0
        )
    )

    # 4. Preparacion documental

    documentos_base = documentos.select(
        "guid_documento",
        "numero_expediente",
        "duplicado",
        "estado_extraccion",
        "tamano_bytes",
        "caracteres_extraidos",
        F.col("usar_silver_principal").alias(
            "documento_principal"
        ),
        F.col("coherencia_temporal").alias(
            "coherencia_documento"
        )
    )

    textos_base = textos.select(
        "guid_documento",
        F.col("numero_expediente").alias(
            "expediente_texto"
        ),
        "texto_suficiente",
        "caracteres_texto_final",
        F.col("usar_silver_principal").alias(
            "texto_principal"
        ),
        F.col("coherencia_temporal").alias(
            "coherencia_texto"
        ),
        F.lit(True).alias("texto_asociado")
    )

    # 5. Integracion de documentos y textos

    documental = documentos_base.join(
        textos_base,
        "guid_documento",
        "left"
    )

    documental = (
        documental.withColumn(
            "texto_asociado",
            F.coalesce(
                F.col("texto_asociado"),
                F.lit(False)
            )
        )
        .withColumn(
            "texto_suficiente_valido",
            F.coalesce(
                F.col("texto_suficiente"),
                F.lit(False)
            )
        )
        .withColumn(
            "texto_principal_suficiente",
            F.coalesce(
                F.col("texto_principal"),
                F.lit(False)
            )
            & F.col("texto_suficiente_valido")
        )
        .withColumn(
            "documento_texto_principal",
            F.coalesce(
                F.col("documento_principal"),
                F.lit(False)
            )
            & F.coalesce(
                F.col("texto_principal"),
                F.lit(False)
            )
        )
        .withColumn(
            "expediente_documental_consistente",
            F.col("texto_asociado")
            & F.col("numero_expediente").eqNullSafe(
                F.col("expediente_texto")
            )
        )
    )

    # 6. Agregacion documental por expediente
    #
    # Se agregan documentos antes de unirlos
    # con los acontecimientos empresariales.

    resumen_documental = documental.groupBy(
        "numero_expediente"
    ).agg(
        F.count("*").alias(
            "total_documentos"
        ),
        contar_si(
            F.col("duplicado") == True
        ).alias(
            "documentos_duplicados"
        ),
        contar_si(
            F.col("duplicado") == False
        ).alias(
            "documentos_no_duplicados"
        ),
        contar_si(
            F.col("documento_principal") == True
        ).alias(
            "documentos_principales"
        ),
        contar_si(
            F.col("texto_asociado")
        ).alias(
            "documentos_con_texto"
        ),
        contar_si(
            F.col("texto_suficiente_valido")
        ).alias(
            "textos_suficientes"
        ),
        contar_si(
            F.col("texto_asociado")
            & ~F.col("texto_suficiente_valido")
        ).alias(
            "textos_insuficientes"
        ),
        contar_si(
            F.col("texto_principal") == True
        ).alias(
            "textos_principales"
        ),
        contar_si(
            F.col("texto_principal_suficiente")
        ).alias(
            "textos_principales_suficientes"
        ),
        contar_si(
            F.col("documento_texto_principal")
            & F.col("texto_suficiente_valido")
        ).alias(
            "documentos_principales_con_texto_suficiente"
        ),
        contar_si(
            ~F.col("expediente_documental_consistente")
        ).alias(
            "documentos_con_incidencia_referencial"
        ),
        F.sum("tamano_bytes").alias(
            "tamano_documental_bytes"
        ),
        F.sum("caracteres_texto_final").alias(
            "caracteres_textuales_totales"
        )
    )

    # 7. Integracion del resumen documental

    hechos = hechos.join(
        resumen_documental,
        "numero_expediente",
        "left"
    )

    campos_conteo = [
        "total_documentos",
        "documentos_duplicados",
        "documentos_no_duplicados",
        "documentos_principales",
        "documentos_con_texto",
        "textos_suficientes",
        "textos_insuficientes",
        "textos_principales",
        "textos_principales_suficientes",
        "documentos_principales_con_texto_suficiente",
        "documentos_con_incidencia_referencial"
    ]

    hechos = hechos.fillna(
        0,
        subset=campos_conteo
    )

    # 8. Indicadores documentales

    hechos = (
        hechos.withColumn(
            "tiene_documentos",
            F.col("total_documentos") > 0
        )
        .withColumn(
            "tiene_texto_suficiente",
            F.col("textos_suficientes") > 0
        )
        .withColumn(
            "tiene_texto_principal_suficiente",
            F.col(
                "textos_principales_suficientes"
            ) > 0
        )
        .withColumn(
            "cobertura_textual_pct",
            F.when(
                F.col("total_documentos") > 0,
                F.col("documentos_con_texto")
                / F.col("total_documentos") * 100
            )
        )
        .withColumn(
            "suficiencia_textual_pct",
            F.when(
                F.col("total_documentos") > 0,
                F.col("textos_suficientes")
                / F.col("total_documentos") * 100
            )
        )
        .withColumn(
            "suficiencia_textual_principal_pct",
            F.when(
                F.col("textos_principales") > 0,
                F.col("textos_principales_suficientes")
                / F.col("textos_principales") * 100
            )
        )
        .withColumn(
            "duplicidad_documental_pct",
            F.when(
                F.col("total_documentos") > 0,
                F.col("documentos_duplicados")
                / F.col("total_documentos") * 100
            )
        )
    )

    # 9. Calidad temporal y elegibilidad

    hechos = (
        hechos.withColumn(
            "evento_principal",
            F.coalesce(
                F.col("usar_silver_principal"),
                F.lit(False)
            )
            & F.coalesce(
                F.upper(
                    F.trim("coherencia_temporal")
                ) == "COHERENTE",
                F.lit(False)
            )
        )
        .withColumn(
            "fecha_hecho_valida",
            F.col("fecha_hecho").isNotNull()
        )
        .withColumn(
            "integridad_documental_valida",
            F.col(
                "documentos_con_incidencia_referencial"
            ) == 0
        )
        .withColumn(
            "candidato_impacto_bursatil",
            F.col("evento_principal")
            & F.col("fecha_hecho_valida")
            & F.col("empresa_identificada")
            & F.col("tiene_nemonicos")
        )
    )

    # 10. Indicadores temporales

    hechos = (
        hechos.withColumn(
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
        .withColumn(
            "dias_acuerdo_presentacion",
            F.datediff(
                "fecha_hecho",
                "fecha_acuerdo"
            )
        )
    )

    # 11. Motivos analiticos de calidad

    hechos = hechos.withColumn(
        "motivos_calidad_gold",
        F.concat_ws(
            ";",
            F.when(
                ~F.col("evento_principal"),
                F.lit("EVENTO_NO_PRINCIPAL")
            ),
            F.when(
                ~F.col("fecha_hecho_valida"),
                F.lit("FECHA_HECHO_INVALIDA")
            ),
            F.when(
                ~F.col("empresa_identificada"),
                F.lit("EMPRESA_NO_IDENTIFICADA")
            ),
            F.when(
                ~F.col("tiene_nemonicos"),
                F.lit("SIN_NEMONICOS_VINCULADOS")
            ),
            F.when(
                ~F.col("tiene_documentos"),
                F.lit("SIN_DOCUMENTOS")
            ),
            F.when(
                ~F.col("tiene_texto_principal_suficiente"),
                F.lit("SIN_TEXTO_PRINCIPAL_SUFICIENTE")
            ),
            F.when(
                ~F.col("integridad_documental_valida"),
                F.lit("INCIDENCIA_REFERENCIAL_DOCUMENTAL")
            )
        )
    )

    return hechos


def main():

    parser = argparse.ArgumentParser(
        description="Construccion Gold hechos_empresa"
    )

    parser.add_argument("--silver-root", required=True)
    parser.add_argument("--gold-root", required=True)

    args = parser.parse_args()

    silver_root = args.silver_root.rstrip("/")
    gold_root = args.gold_root.rstrip("/")

    ruta_salida = f"{gold_root}/eventos/hechos_empresa"

    spark = (
        SparkSession.builder
        .appName("BVL_Gold_Hechos_Empresa")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    gold = None

    try:

        print("\n" + "=" * 80)
        print("BVL ANALYTICS - GOLD HECHOS EMPRESA")
        print("=" * 80)

        # 1. Lectura Silver

        base = f"{silver_root}/hechos_importancia"

        eventos = spark.read.parquet(
            f"{base}/eventos"
        )

        identidad = spark.read.parquet(
            f"{base}/identidad_empresarial"
        )

        documentos = spark.read.parquet(
            f"{base}/documentos"
        )

        textos = spark.read.parquet(
            f"{base}/contenido_textual"
        )

        total_eventos = eventos.count()
        total_identidad = identidad.count()
        total_documentos = documentos.count()
        total_textos = textos.count()

        print(f"\nEventos Silver: {total_eventos}")
        print(f"Identidades Silver: {total_identidad}")
        print(f"Documentos Silver: {total_documentos}")
        print(f"Textos Silver: {total_textos}")

        # 2. Validacion de claves

        validar_clave(
            eventos,
            ["numero_expediente"],
            "eventos"
        )

        validar_clave(
            identidad,
            ["numero_expediente"],
            "identidad_empresarial"
        )

        validar_clave(
            documentos,
            ["guid_documento"],
            "documentos"
        )

        validar_clave(
            textos,
            ["guid_documento"],
            "contenido_textual"
        )

        # 3. Integridad referencial de entrada

        print("\nINTEGRIDAD REFERENCIAL SILVER")

        documentos_huerfanos = documentos.join(
            eventos.select("numero_expediente"),
            "numero_expediente",
            "left_anti"
        ).count()

        textos_huerfanos = textos.join(
            documentos.select("guid_documento"),
            "guid_documento",
            "left_anti"
        ).count()

        identidad_huerfana = identidad.join(
            eventos.select("numero_expediente"),
            "numero_expediente",
            "left_anti"
        ).count()

        print(f"Documentos huerfanos: {documentos_huerfanos}")
        print(f"Textos huerfanos: {textos_huerfanos}")
        print(f"Identidades huerfanas: {identidad_huerfana}")

        if (
            documentos_huerfanos > 0
            or textos_huerfanos > 0
            or identidad_huerfana > 0
        ):
            raise ValueError(
                "Integridad referencial Silver invalida"
            )

        # 4. Construccion Gold

        gold = construir_gold(
            eventos,
            identidad,
            documentos,
            textos
        ).cache()

        total_gold = gold.count()

        validar_clave(
            gold,
            ["numero_expediente"],
            "hechos_empresa"
        )

        if total_eventos != total_gold:
            raise ValueError(
                "La integracion modifico la cantidad "
                "de eventos"
            )

        # 5. Metricas generales

        print("\n" + "=" * 80)
        print("METRICAS GENERALES GOLD")
        print("=" * 80)

        gold.agg(
            F.count("*").alias("eventos"),
            contar_si(
                F.col("evento_principal")
            ).alias("eventos_principales"),
            contar_si(
                ~F.col("evento_principal")
            ).alias("eventos_no_principales"),
            contar_si(
                F.col("empresa_identificada")
            ).alias("empresas_identificadas"),
            contar_si(
                ~F.col("empresa_identificada")
            ).alias("empresas_no_identificadas"),
            contar_si(
                F.col("candidato_impacto_bursatil")
            ).alias("candidatos_impacto")
        ).show(truncate=False)

        # 6. Metricas documentales

        print("\nMETRICAS DOCUMENTALES")

        resumen = gold.agg(
            F.sum("total_documentos").alias(
                "documentos"
            ),
            F.sum("documentos_duplicados").alias(
                "duplicados"
            ),
            F.sum("documentos_con_texto").alias(
                "documentos_con_texto"
            ),
            F.sum("textos_suficientes").alias(
                "textos_suficientes"
            ),
            F.sum("textos_insuficientes").alias(
                "textos_insuficientes"
            ),
            F.sum("textos_principales").alias(
                "textos_principales"
            ),
            F.sum(
                "textos_principales_suficientes"
            ).alias(
                "principales_suficientes"
            ),
            F.sum(
                "documentos_con_incidencia_referencial"
            ).alias(
                "incidencias_referenciales"
            )
        )

        resumen.show(truncate=False)

        control_documentos = resumen.first()

        if control_documentos["documentos"] != total_documentos:
            raise ValueError(
                "La agregacion no conserva los documentos"
            )

        if (
            control_documentos["documentos_con_texto"]
            != total_textos
        ):
            raise ValueError(
                "La agregacion no conserva los textos"
            )

        if control_documentos["incidencias_referenciales"] > 0:
            raise ValueError(
                "Expedientes documentales inconsistentes"
            )

        # 7. Validacion de porcentajes

        print("\nVALIDACION DE PORCENTAJES")

        porcentajes = [
            "cobertura_textual_pct",
            "suficiencia_textual_pct",
            "suficiencia_textual_principal_pct",
            "duplicidad_documental_pct"
        ]

        for campo in porcentajes:

            invalidos = gold.filter(
                F.isnan(F.col(campo))
                | (F.col(campo) < 0)
                | (F.col(campo) > 100.000001)
            ).count()

            print(f"{campo}: {invalidos}")

            if invalidos > 0:
                raise ValueError(
                    f"Porcentaje invalido: {campo}"
                )

        # 8. Clasificacion temporal

        print("\nCLASIFICACION TEMPORAL")

        gold.groupBy(
            "coherencia_temporal",
            "evento_principal"
        ).count().show(truncate=False)

        # 9. Eventos candidatos a impacto

        print("\nCANDIDATOS A IMPACTO BURSATIL")

        candidatos = gold.filter(
            F.col("candidato_impacto_bursatil")
        ).select(
            "numero_expediente",
            F.explode(
                "nemonicos_cotizables"
            ).alias("nemonico")
        ).distinct()

        candidatos.agg(
            F.count("*").alias(
                "pares_evento_valor"
            ),
            F.countDistinct(
                "numero_expediente"
            ).alias(
                "eventos_distintos"
            ),
            F.countDistinct(
                "nemonico"
            ).alias(
                "valores_distintos"
            )
        ).show(truncate=False)

        # 10. Distribucion por tipo de hecho

        print("\nEVENTOS PRINCIPALES POR TIPO")

        gold.filter(
            F.col("evento_principal")
        ).groupBy(
            "tipo_hecho"
        ).count().orderBy(
            F.desc("count")
        ).show(20, truncate=False)

        # 11. Distribucion por sector

        print("\nEVENTOS PRINCIPALES POR SECTOR")

        gold.filter(
            F.col("evento_principal")
        ).groupBy(
            "sector"
        ).count().orderBy(
            F.desc("count")
        ).show(20, truncate=False)

        # 12. Cobertura temporal

        print("\nCOBERTURA TEMPORAL")

        gold.agg(
            F.min("fecha_hecho").alias("fecha_min"),
            F.max("fecha_hecho").alias("fecha_max")
        ).show(truncate=False)

        print("\nCOBERTURA EVENTOS PRINCIPALES")

        gold.filter(
            F.col("evento_principal")
        ).agg(
            F.min("fecha_hecho").alias("fecha_min"),
            F.max("fecha_hecho").alias("fecha_max")
        ).show(truncate=False)

        # 13. Esquema Gold

        print("\nESQUEMA GOLD")

        gold.printSchema()

        # 14. Muestra Gold

        print("\nMUESTRA GOLD")

        gold.filter(
            F.col("evento_principal")
        ).select(
            "numero_expediente",
            "fecha_hecho",
            "empresa",
            "tipo_hecho",
            "empresa_identificada",
            "cantidad_nemonicos",
            "total_documentos",
            "textos_principales_suficientes",
            "candidato_impacto_bursatil"
        ).orderBy(
            "fecha_hecho",
            "numero_expediente"
        ).show(15, truncate=False)

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
        print("CONSTRUCCION GOLD HECHOS FINALIZADA")
        print("=" * 80)

        print(f"Eventos Silver: {total_eventos}")
        print(f"Eventos Gold: {total_gold}")
        print(f"Ruta Gold: {ruta_salida}")

    finally:

        if gold is not None:
            gold.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
