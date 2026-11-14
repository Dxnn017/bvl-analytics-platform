
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def contar_si(condicion):
    return F.sum(
        F.when(condicion, 1).otherwise(0)
    )


def main():

    parser = argparse.ArgumentParser(
        description="Validacion Gold hechos_empresa"
    )

    parser.add_argument("--silver-root", required=True)
    parser.add_argument("--gold-root", required=True)

    args = parser.parse_args()

    silver_root = args.silver_root.rstrip("/")
    gold_root = args.gold_root.rstrip("/")

    spark = (
        SparkSession.builder
        .appName("BVL_Validacion_Gold_Hechos")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

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

        gold = spark.read.parquet(
            f"{gold_root}/eventos/hechos_empresa"
        )

        archivos_parquet = len(gold.inputFiles())

        gold = gold.cache()

        controles = []

        def validar(nombre, condicion):

            controles.append((nombre, condicion))

            estado = "OK" if condicion else "ERROR"

            print(f"{nombre}: {estado}")

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

        print("\n" + "=" * 80)
        print("VALIDACION GOLD - HECHOS EMPRESA")
        print("=" * 80)

        # 1. Conservacion de eventos

        print("\n1. CONSERVACION DE EVENTOS")

        total_silver = eventos.count()
        total_gold = gold.count()

        print(f"Eventos Silver: {total_silver}")
        print(f"Eventos Gold: {total_gold}")
        print(f"Columnas Gold: {len(gold.columns)}")

        validar(
            "Igualdad de cantidades",
            total_silver == total_gold
        )

        # 2. Integridad de claves

        print("\n2. INTEGRIDAD DE CLAVES")

        claves_silver = eventos.select(
            "numero_expediente"
        )

        claves_gold = gold.select(
            "numero_expediente"
        )

        invalidas = gold.filter(
            F.col("numero_expediente").isNull()
            | (F.trim("numero_expediente") == "")
        ).count()

        duplicadas = (
            claves_gold.groupBy("numero_expediente")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        faltantes = claves_silver.join(
            claves_gold,
            "numero_expediente",
            "left_anti"
        ).count()

        adicionales = claves_gold.join(
            claves_silver,
            "numero_expediente",
            "left_anti"
        ).count()

        print(f"Claves invalidas: {invalidas}")
        print(f"Claves duplicadas: {duplicadas}")
        print(f"Eventos faltantes: {faltantes}")
        print(f"Eventos adicionales: {adicionales}")

        validar("Claves completas", invalidas == 0)
        validar("Claves unicas", duplicadas == 0)
        validar("Sin eventos perdidos", faltantes == 0)
        validar("Sin eventos adicionales", adicionales == 0)

        # 3. Conservacion de campos originales

        print("\n3. CONSERVACION DE CAMPOS ORIGINALES")

        campos_eventos = {
            "empresa": "empresa",
            "fecha_hecho": "fecha_presentacion_date",
            "fecha_presentacion": "fecha_presentacion",
            "hora_presentacion": "hora_presentacion",
            "fecha_acuerdo": "fecha_acuerdo_date",
            "fecha_consulta": "fecha_consulta_portal_date",
            "tipo_hecho": "tipo_hecho",
            "tipo_empresa": "tipo_empresa",
            "sector": "sector",
            "descripcion": "descripcion",
            "cantidad_documentos_pdf":
                "cantidad_documentos_pdf",
            "cantidad_documentos_portal":
                "cantidad_documentos_portal",
            "usar_silver_principal":
                "usar_silver_principal",
            "motivo_calidad": "motivo_calidad",
            "coherencia_temporal": "coherencia_temporal"
        }

        originales = eventos.select(
            "numero_expediente",
            *[
                F.col(origen).alias(f"{destino}_silver")
                for destino, origen in campos_eventos.items()
            ]
        )

        revision = gold.join(
            originales,
            "numero_expediente",
            "inner"
        )

        modificados = comparar(
            revision,
            [
                (campo, f"{campo}_silver")
                for campo in campos_eventos
            ]
        )

        print(f"Eventos originales modificados: {modificados}")

        validar(
            "Conservacion de campos originales",
            modificados == 0
        )

        # 4. Clasificacion temporal

        print("\n4. CLASIFICACION TEMPORAL")

        principal_esperado = (
            F.coalesce(
                F.col("usar_silver_principal_silver"),
                F.lit(False)
            )
            & F.coalesce(
                F.upper(
                    F.trim("coherencia_temporal_silver")
                ) == "COHERENTE",
                F.lit(False)
            )
        )

        revision = revision.withColumn(
            "principal_esperado",
            principal_esperado
        )

        errores_temporales = revision.filter(
            ~F.col("evento_principal").eqNullSafe(
                F.col("principal_esperado")
            )
            | ~F.col("fecha_hecho_valida").eqNullSafe(
                F.col("fecha_hecho_silver").isNotNull()
            )
        ).count()

        print(f"Errores temporales: {errores_temporales}")

        validar(
            "Clasificacion temporal correcta",
            errores_temporales == 0
        )

        # 5. Identidad empresarial

        print("\n5. IDENTIDAD EMPRESARIAL")

        identidad_esperada = identidad.select(
            "numero_expediente",
            F.col("identificado").alias(
                "identificado_silver"
            ),
            F.col("empresa_norm").alias(
                "empresa_norm_silver"
            ),
            F.col("ruc").alias("ruc_silver"),
            F.col("razon_social_smv").alias(
                "razon_social_silver"
            ),
            F.col("metodo_vinculacion").alias(
                "metodo_silver"
            ),
            F.col("nemonicos_cotizables").alias(
                "nemonicos_silver"
            )
        )

        revision = revision.join(
            identidad_esperada,
            "numero_expediente",
            "left"
        )

        errores_identidad = revision.filter(
            ~F.col("empresa_identificada").eqNullSafe(
                F.coalesce(
                    F.col("identificado_silver"),
                    F.lit(False)
                )
            )
            | ~F.col("identificacion_original").eqNullSafe(
                F.col("identificado_silver")
            )
            | ~F.col("empresa_norm").eqNullSafe(
                F.col("empresa_norm_silver")
            )
            | ~F.col("ruc").eqNullSafe(
                F.col("ruc_silver")
            )
            | ~F.col("razon_social_smv").eqNullSafe(
                F.col("razon_social_silver")
            )
            | ~F.col("metodo_vinculacion").eqNullSafe(
                F.col("metodo_silver")
            )
        ).count()

        print(f"Identidades incorrectas: {errores_identidad}")

        validar(
            "Identificacion empresarial correcta",
            errores_identidad == 0
        )

        # 6. Nemonicos vinculados

        print("\n6. NEMONICOS VINCULADOS")

        nemonicos_esperados = F.array_sort(
            F.array_distinct(
                F.filter(
                    F.coalesce(
                        F.col("nemonicos_silver"),
                        F.array().cast("array<string>")
                    ),
                    lambda x: (
                        x.isNotNull()
                        & (F.trim(x) != "")
                    )
                )
            )
        )

        revision = revision.withColumn(
            "nemonicos_esperados",
            nemonicos_esperados
        )

        errores_nemonicos = revision.filter(
            ~F.col("nemonicos_cotizables").eqNullSafe(
                F.col("nemonicos_esperados")
            )
            | ~F.col("cantidad_nemonicos").eqNullSafe(
                F.size("nemonicos_esperados")
            )
            | ~F.col("tiene_nemonicos").eqNullSafe(
                F.size("nemonicos_esperados") > 0
            )
        ).count()

        print(f"Vinculos incorrectos: {errores_nemonicos}")

        validar(
            "Vinculacion de nemonicos correcta",
            errores_nemonicos == 0
        )

        # 7. Integridad documental

        print("\n7. INTEGRIDAD DOCUMENTAL")

        documentos_huerfanos = documentos.join(
            claves_silver,
            "numero_expediente",
            "left_anti"
        ).count()

        textos_huerfanos = textos.join(
            documentos.select("guid_documento"),
            "guid_documento",
            "left_anti"
        ).count()

        documentos_sin_texto = documentos.join(
            textos.select("guid_documento"),
            "guid_documento",
            "left_anti"
        ).count()

        identidad_huerfana = identidad.join(
            claves_silver,
            "numero_expediente",
            "left_anti"
        ).count()

        print(f"Documentos huerfanos: {documentos_huerfanos}")
        print(f"Textos huerfanos: {textos_huerfanos}")
        print(f"Documentos sin texto: {documentos_sin_texto}")
        print(f"Identidades huerfanas: {identidad_huerfana}")

        validar(
            "Integridad referencial Silver",
            documentos_huerfanos == 0
            and textos_huerfanos == 0
            and documentos_sin_texto == 0
            and identidad_huerfana == 0
        )

        # 8. Recalculo de conteos documentales

        print("\n8. CONTEOS DOCUMENTALES")

        docs = documentos.select(
            "guid_documento",
            "numero_expediente",
            "duplicado",
            "tamano_bytes",
            F.col("usar_silver_principal").alias(
                "doc_principal"
            )
        )

        contenido = textos.select(
            "guid_documento",
            F.col("numero_expediente").alias(
                "expediente_texto"
            ),
            "texto_suficiente",
            "caracteres_texto_final",
            F.col("usar_silver_principal").alias(
                "texto_principal"
            ),
            F.lit(True).alias("asociado")
        )

        documental = docs.join(
            contenido,
            "guid_documento",
            "left"
        )

        asociado = F.coalesce(
            F.col("asociado"),
            F.lit(False)
        )

        suficiente = F.coalesce(
            F.col("texto_suficiente"),
            F.lit(False)
        )

        principal = F.coalesce(
            F.col("texto_principal"),
            F.lit(False)
        )

        doc_principal = F.coalesce(
            F.col("doc_principal"),
            F.lit(False)
        )

        incidencia = ~(
            asociado
            & F.col("numero_expediente").eqNullSafe(
                F.col("expediente_texto")
            )
        )

        conteos = {
            "total_documentos": F.count("*"),
            "documentos_duplicados":
                contar_si(F.col("duplicado") == True),
            "documentos_no_duplicados":
                contar_si(F.col("duplicado") == False),
            "documentos_principales":
                contar_si(doc_principal),
            "documentos_con_texto":
                contar_si(asociado),
            "textos_suficientes":
                contar_si(suficiente),
            "textos_insuficientes":
                contar_si(asociado & ~suficiente),
            "textos_principales":
                contar_si(principal),
            "textos_principales_suficientes":
                contar_si(principal & suficiente),
            "documentos_principales_con_texto_suficiente":
                contar_si(
                    doc_principal & principal & suficiente
                ),
            "documentos_con_incidencia_referencial":
                contar_si(incidencia)
        }

        resumen = documental.groupBy(
            "numero_expediente"
        ).agg(
            *[
                expresion.alias(f"{campo}_esperado")
                for campo, expresion in conteos.items()
            ],
            F.sum("tamano_bytes").alias(
                "tamano_esperado"
            ),
            F.sum("caracteres_texto_final").alias(
                "caracteres_esperados"
            )
        )

        revision = revision.join(
            resumen,
            "numero_expediente",
            "left"
        )

        revision = revision.fillna(
            0,
            subset=[
                f"{campo}_esperado"
                for campo in conteos
            ]
        ).cache()

        errores_conteos = comparar(
            revision,
            [
                (campo, f"{campo}_esperado")
                for campo in conteos
            ]
        )

        print(f"Eventos con conteos incorrectos: {errores_conteos}")

        validar(
            "Conteos documentales correctos",
            errores_conteos == 0
        )

        errores_volumen = comparar(
            revision,
            [
                ("tamano_documental_bytes", "tamano_esperado"),
                (
                    "caracteres_textuales_totales",
                    "caracteres_esperados"
                )
            ]
        )

        validar(
            "Volumen documental correcto",
            errores_volumen == 0
        )

        # 9. Conservacion global de documentos

        print("\n9. CONSERVACION DOCUMENTAL")

        totales = gold.agg(
            F.sum("total_documentos").alias("documentos"),
            F.sum("documentos_con_texto").alias("textos"),
            F.sum(
                "documentos_con_incidencia_referencial"
            ).alias("incidencias")
        ).first()

        print(f"Documentos Gold: {totales['documentos']}")
        print(f"Textos asociados: {totales['textos']}")
        print(f"Incidencias: {totales['incidencias']}")

        validar(
            "Conservacion global de documentos",
            totales["documentos"] == documentos.count()
        )

        validar(
            "Conservacion global de textos",
            totales["textos"] == textos.count()
        )

        validar(
            "Sin incidencias documentales",
            totales["incidencias"] == 0
        )

        # 10. Indicadores porcentuales

        print("\n10. INDICADORES PORCENTUALES")

        indicadores = [
            (
                "cobertura_textual_pct",
                "documentos_con_texto",
                "total_documentos"
            ),
            (
                "suficiencia_textual_pct",
                "textos_suficientes",
                "total_documentos"
            ),
            (
                "suficiencia_textual_principal_pct",
                "textos_principales_suficientes",
                "textos_principales"
            ),
            (
                "duplicidad_documental_pct",
                "documentos_duplicados",
                "total_documentos"
            )
        ]

        for campo, numerador, denominador in indicadores:

            num = F.col(f"{numerador}_esperado")
            den = F.col(f"{denominador}_esperado")

            esperado = F.when(
                den > 0,
                num / den * 100
            )

            actual = F.col(campo)

            errores = revision.filter(
                (actual.isNull() != esperado.isNull())
                | F.isnan(actual)
                | (actual < 0)
                | (actual > 100.000001)
                | (F.abs(actual - esperado) > 0.000001)
            ).count()

            print(f"{campo} incorrectos: {errores}")

            validar(
                f"Calculo correcto de {campo}",
                errores == 0
            )

        # 11. Indicadores booleanos documentales

        print("\n11. DISPONIBILIDAD DOCUMENTAL")

        errores_disponibilidad = revision.filter(
            ~F.col("tiene_documentos").eqNullSafe(
                F.col("total_documentos_esperado") > 0
            )
            | ~F.col("tiene_texto_suficiente").eqNullSafe(
                F.col("textos_suficientes_esperado") > 0
            )
            | ~F.col(
                "tiene_texto_principal_suficiente"
            ).eqNullSafe(
                F.col(
                    "textos_principales_suficientes_esperado"
                ) > 0
            )
            | ~F.col(
                "integridad_documental_valida"
            ).eqNullSafe(
                F.col(
                    "documentos_con_incidencia_referencial_esperado"
                ) == 0
            )
        ).count()

        validar(
            "Disponibilidad documental correcta",
            errores_disponibilidad == 0
        )

        # 12. Candidatos a impacto bursatil

        print("\n12. CANDIDATOS A IMPACTO BURSATIL")

        candidato_esperado = (
            F.col("principal_esperado")
            & F.col("fecha_hecho_silver").isNotNull()
            & F.coalesce(
                F.col("identificado_silver"),
                F.lit(False)
            )
            & (F.size("nemonicos_esperados") > 0)
        )

        errores_candidatos = revision.filter(
            ~F.col("candidato_impacto_bursatil").eqNullSafe(
                candidato_esperado
            )
        ).count()

        print(f"Candidatos incorrectos: {errores_candidatos}")

        validar(
            "Elegibilidad de candidatos correcta",
            errores_candidatos == 0
        )

        pares_gold = gold.filter(
            F.col("candidato_impacto_bursatil")
        ).select(
            "numero_expediente",
            F.explode(
                "nemonicos_cotizables"
            ).alias("nemonico")
        ).distinct()

        pares_esperados = revision.filter(
            candidato_esperado
        ).select(
            "numero_expediente",
            F.explode(
                "nemonicos_esperados"
            ).alias("nemonico")
        ).distinct()

        claves_pares = [
            "numero_expediente",
            "nemonico"
        ]

        pares_faltantes = pares_esperados.join(
            pares_gold,
            claves_pares,
            "left_anti"
        ).count()

        pares_adicionales = pares_gold.join(
            pares_esperados,
            claves_pares,
            "left_anti"
        ).count()

        print(f"Pares faltantes: {pares_faltantes}")
        print(f"Pares adicionales: {pares_adicionales}")

        validar(
            "Integridad de pares evento-valor",
            pares_faltantes == 0
            and pares_adicionales == 0
        )

        pares_gold.agg(
            F.count("*").alias("pares_evento_valor"),
            F.countDistinct(
                "numero_expediente"
            ).alias("eventos"),
            F.countDistinct(
                "nemonico"
            ).alias("nemonicos")
        ).show(truncate=False)

        # 13. Indicadores temporales y particiones

        print("\n13. INDICADORES TEMPORALES")

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
            | ~F.col(
                "dias_acuerdo_presentacion"
            ).eqNullSafe(
                F.datediff(
                    "fecha_hecho",
                    "fecha_acuerdo"
                )
            )
        ).count()

        print(f"Errores temporales: {errores_particion}")
        print(f"Archivos Parquet: {archivos_parquet}")

        validar(
            "Indicadores temporales y particiones correctos",
            errores_particion == 0
        )

        validar(
            "Existencia de archivos Parquet",
            archivos_parquet > 0
        )

        # 14. Resumen final

        print("\nRESUMEN DE CALIDAD")

        gold.agg(
            F.count("*").alias("eventos"),
            contar_si(
                F.col("evento_principal")
            ).alias("principales"),
            contar_si(
                F.col("empresa_identificada")
            ).alias("identificados"),
            contar_si(
                F.col("candidato_impacto_bursatil")
            ).alias("candidatos"),
            F.sum("total_documentos").alias("documentos"),
            F.sum("textos_suficientes").alias(
                "textos_suficientes"
            )
        ).show(truncate=False)

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
                "Gold hechos_empresa no aprobado"
            )

        print("RESULTADO: GOLD VALIDADO CORRECTAMENTE")

        spark.catalog.clearCache()

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
