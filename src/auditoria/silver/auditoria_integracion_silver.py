
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser(
        description="Auditoria de integracion Silver"
    )

    parser.add_argument("--silver-root", required=True)

    args = parser.parse_args()
    root = args.silver_root.rstrip("/")

    spark = (
        SparkSession.builder
        .appName("BVL_Integracion_Silver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    def leer(dataset):
        return spark.read.parquet(f"{root}/{dataset}")

    try:
        cotizaciones = leer(
            "mercado_diario/cotizaciones"
        )

        sab = leer(
            "mercado_diario/montos_sab"
        )

        tc = leer(
            "macroeconomia/tipo_cambio"
        )

        embig = leer(
            "macroeconomia/embig"
        )

        puente = leer(
            "referencias_smv/puente_ruc_nemonico"
        )

        eventos = leer(
            "hechos_importancia/eventos"
        )

        identidad = leer(
            "hechos_importancia/identidad_empresarial"
        )

        documentos = leer(
            "hechos_importancia/documentos"
        )

        textos = leer(
            "hechos_importancia/contenido_textual"
        )

        print("\n" + "=" * 80)
        print("AUDITORIA DE INTEGRACION SILVER")
        print("=" * 80)

        print("\n1. INTEGRACION MERCADO Y MACROECONOMIA")

        mercado = (
            cotizaciones
            .withColumnRenamed(
                "fecha_cotizacion", "fecha"
            )
            .join(
                tc.select(
                    "fecha",
                    "tipo_cambio_venta"
                ),
                "fecha",
                "left"
            )
            .join(
                embig.select(
                    "fecha",
                    "embig_peru"
                ),
                "fecha",
                "left"
            )
        )

        print(f"Cotizaciones originales: {cotizaciones.count()}")
        print(f"Registros integrados: {mercado.count()}")

        mercado.agg(
            F.count(
                F.when(
                    F.col("tipo_cambio_venta").isNull(),
                    1
                )
            ).alias("tc_nulos"),

            F.count(
                F.when(
                    F.col("embig_peru").isNull(),
                    1
                )
            ).alias("embig_nulos"),

            F.count(
                F.when(
                    F.isnan("tipo_cambio_venta")
                    | (F.col("tipo_cambio_venta") <= 0),
                    1
                )
            ).alias("tc_no_validos"),

            F.count(
                F.when(
                    F.isnan("embig_peru")
                    | (F.col("embig_peru") < 0),
                    1
                )
            ).alias("embig_no_validos")
        ).show(truncate=False)

        print("\n2. INTEGRACION MERCADO Y PUENTE SMV")

        mercado_empresa = mercado.join(
            puente.select(
                F.col("nemonico_valor").alias("valor"),
                "ruc",
                "razon_social",
                "fecha_inscripcion"
            ),
            "valor",
            "left"
        )

        print(
            f"Registros despues del join: "
            f"{mercado_empresa.count()}"
        )

        mercado_empresa.agg(
            F.count("*").alias("total"),

            F.count("ruc").alias("con_ruc"),

            F.count(
                F.when(
                    F.col("ruc").isNull(),
                    1
                )
            ).alias("sin_ruc"),

            F.count(
                F.when(
                    F.col("fecha")
                    < F.col("fecha_inscripcion"),
                    1
                )
            ).alias("anteriores_inscripcion")
        ).show(truncate=False)

        print("\n3. COBERTURA TEMPORAL MERCADO Y SAB")

        fechas_mercado = cotizaciones.select(
            F.col("fecha_cotizacion").alias("fecha")
        ).distinct()

        fechas_sab = sab.select(
            F.col("fecha_negociacion").alias("fecha")
        ).distinct()

        print(
            f"Fechas mercado sin SAB: "
            f"{fechas_mercado.join(fechas_sab, 'fecha', 'left_anti').count()}"
        )

        print(
            f"Fechas SAB sin mercado: "
            f"{fechas_sab.join(fechas_mercado, 'fecha', 'left_anti').count()}"
        )

        print("\n4. EVENTOS E IDENTIDAD EMPRESARIAL")

        eventos_principales = eventos.filter(
            (F.col("usar_silver_principal") == True)
            & (F.col("coherencia_temporal") == "COHERENTE")
        )

        eventos_identidad = eventos_principales.join(
            identidad.select(
                "numero_expediente",
                "ruc",
                "identificado",
                "nemonicos_cotizables"
            ),
            "numero_expediente",
            "left"
        )

        print(f"Eventos totales: {eventos.count()}")
        print(
            f"Eventos principales: "
            f"{eventos_principales.count()}"
        )
        print(
            f"Eventos despues del join: "
            f"{eventos_identidad.count()}"
        )

        eventos_identidad.groupBy(
            "identificado"
        ).count().show(truncate=False)

        print("\n5. EVENTOS Y NEMONICOS COTIZADOS")

        eventos_valores = (
            eventos_identidad
            .filter(F.col("identificado") == True)
            .select(
                "numero_expediente",
                "fecha_presentacion_date",
                F.explode(
                    "nemonicos_cotizables"
                ).alias("valor")
            )
            .filter(
                F.col("valor").isNotNull()
                & (F.trim("valor") != "")
            )
            .distinct()
        )

        valores_cotizados = cotizaciones.select(
            "valor"
        ).distinct()

        vinculados = eventos_valores.join(
            valores_cotizados,
            "valor",
            "left_semi"
        )

        print(
            f"Pares evento-valor candidatos: "
            f"{eventos_valores.count()}"
        )

        print(
            f"Pares con cotizaciones historicas: "
            f"{vinculados.count()}"
        )

        print(
            f"Eventos distintos con cotizaciones: "
            f"{vinculados.select('numero_expediente').distinct().count()}"
        )

        print(
            f"Valores distintos vinculados: "
            f"{vinculados.select('valor').distinct().count()}"
        )

        print("\n6. INTEGRIDAD DOCUMENTAL")

        expedientes = eventos.select(
            "numero_expediente"
        ).distinct()

        print(
            f"Documentos sin evento: "
            f"{documentos.join(expedientes, 'numero_expediente', 'left_anti').count()}"
        )

        guids_documentos = documentos.select(
            "guid_documento"
        ).distinct()

        guids_textos = textos.select(
            "guid_documento"
        ).distinct()

        print(
            f"Textos sin documento: "
            f"{textos.join(guids_documentos, 'guid_documento', 'left_anti').count()}"
        )

        print(
            f"Documentos sin texto asociado: "
            f"{documentos.join(guids_textos, 'guid_documento', 'left_anti').count()}"
        )

        print("\n7. CALIDAD DEL CONTENIDO TEXTUAL")

        textos.groupBy(
            "texto_suficiente",
            "usar_silver_principal"
        ).count().show(truncate=False)

        print("\nAUDITORIA FINALIZADA")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
