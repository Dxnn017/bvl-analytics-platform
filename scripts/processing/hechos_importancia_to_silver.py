import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURACIÓN
# ============================================================

FECHA_INGESTA = "2026-10-04"

BASE_METADATA = (
    "/datalake/bvl/metadata/smv/hechos_importancia/"
    f"ingestion_date={FECHA_INGESTA}"
)

EVENTOS_PATH = (
    f"{BASE_METADATA}/hechos_eventos_2025.csv"
)

DOCUMENTOS_PATH = (
    f"{BASE_METADATA}/hechos_documentos_2025.csv"
)

TXT_PATH = (
    "/datalake/bvl/bronze/smv/hechos_importancia/"
    f"texto_extraido/ingestion_date={FECHA_INGESTA}"
)

SILVER_BASE = (
    "/datalake/bvl/silver/hechos_importancia"
)

SILVER_EVENTOS = (
    f"{SILVER_BASE}/eventos"
)

SILVER_DOCUMENTOS = (
    f"{SILVER_BASE}/documentos"
)

SILVER_TEXTO = (
    f"{SILVER_BASE}/contenido_textual"
)

SUMMARY_LOCAL = Path(
    "outputs/quality/hechos_importancia_silver_summary.json"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("BVL-HechosImportancia-Silver")
    .config("spark.sql.shuffle.partitions", "4")
    .config(
        "spark.sql.parquet.compression.codec",
        "snappy"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# FUNCIONES
# ============================================================

def limpiar_columnas(df):
    """
    Elimina espacios y BOM de encabezados CSV.
    """

    for columna in df.columns:

        limpia = (
            columna
            .lstrip("\ufeff")
            .strip()
        )

        if limpia != columna:

            df = df.withColumnRenamed(
                columna,
                limpia
            )

    return df


def leer_csv(path):

    df = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("multiLine", "false")
        .option("escape", '"')
        .csv(path)
    )

    return limpiar_columnas(df)


# ============================================================
# 1. EVENTOS
# ============================================================

print("=" * 75)
print("LEYENDO EVENTOS")
print("=" * 75)

eventos = leer_csv(
    EVENTOS_PATH
)

eventos = (
    eventos

    .withColumn(
        "fecha_presentacion_date",
        F.to_date(
            "fecha_presentacion",
            "dd/MM/yyyy"
        )
    )

    .withColumn(
        "fecha_acuerdo_date",
        F.to_date(
            "fecha_acuerdo",
            "dd/MM/yyyy"
        )
    )

    .withColumn(
        "fecha_consulta_portal_date",
        F.to_date(
            "fecha_consulta_portal",
            "dd/MM/yyyy"
        )
    )

    .withColumn(
        "cantidad_documentos_pdf",
        F.col(
            "cantidad_documentos_pdf"
        ).cast("integer")
    )

    .withColumn(
        "cantidad_documentos_portal",
        F.col(
            "cantidad_documentos_portal"
        ).cast("integer")
    )

    .withColumn(
        "usar_silver_principal",
        F.col(
            "usar_silver_principal"
        ).cast("boolean")
    )

    .withColumn(
        "anio_presentacion",
        F.year(
            "fecha_presentacion_date"
        )
    )
)


# ============================================================
# 2. DOCUMENTOS
# ============================================================

print("=" * 75)
print("LEYENDO DOCUMENTOS")
print("=" * 75)

documentos = leer_csv(
    DOCUMENTOS_PATH
)

documentos = (
    documentos

    .withColumn(
        "tamano_bytes",
        F.col(
            "tamano_bytes"
        ).cast("long")
    )

    .withColumn(
        "caracteres_extraidos",
        F.col(
            "caracteres_extraidos"
        ).cast("long")
    )

    .withColumn(
        "duplicado",
        F.col(
            "duplicado"
        ).cast("boolean")
    )

    .withColumn(
        "usar_silver_principal",
        F.col(
            "usar_silver_principal"
        ).cast("boolean")
    )

    .withColumn(
        "fecha_consulta_portal_date",
        F.to_date(
            "fecha_consulta_portal",
            "dd/MM/yyyy"
        )
    )

    .withColumn(
        "archivo_txt_esperado",
        F.regexp_replace(
            F.col("archivo_pdf"),
            r"(?i)\.pdf$",
            ".txt"
        )
    )

    .withColumn(
        "archivo_txt_canonico",
        F.regexp_replace(
            F.col("archivo_canonico"),
            r"(?i)\.pdf$",
            ".txt"
        )
    )
)


# ============================================================
# 3. LEER TXT NO ESTRUCTURADOS DESDE HDFS
# ============================================================

print("=" * 75)
print("LEYENDO CONTENIDO TEXTUAL DESDE BRONZE")
print("=" * 75)

txt_rdd = (
    spark.sparkContext
    .wholeTextFiles(
        TXT_PATH,
        minPartitions=4
    )
)

txt_df = (
    txt_rdd

    .map(
        lambda x: (
            x[0].rsplit("/", 1)[-1],
            x[0],
            x[1]
        )
    )

    .toDF(
        [
            "archivo_txt",
            "ruta_txt_hdfs",
            "contenido_texto"
        ]
    )

    .withColumn(
        "caracteres_texto_real",
        F.length(
            "contenido_texto"
        )
    )
)


# ============================================================
# 4. RELACIONAR DOCUMENTOS CON TXT
# ============================================================

# TXT directo
txt_directo = (
    txt_df
    .select(
        F.col(
            "archivo_txt"
        ).alias(
            "txt_directo_nombre"
        ),

        F.col(
            "ruta_txt_hdfs"
        ).alias(
            "txt_directo_ruta"
        ),

        F.col(
            "contenido_texto"
        ).alias(
            "txt_directo_contenido"
        ),

        F.col(
            "caracteres_texto_real"
        ).alias(
            "txt_directo_caracteres"
        )
    )
)

# TXT del archivo canónico, utilizado solamente
# cuando el PDF corresponde al duplicado exacto.
txt_canonico = (
    txt_df
    .select(
        F.col(
            "archivo_txt"
        ).alias(
            "txt_canonico_nombre"
        ),

        F.col(
            "ruta_txt_hdfs"
        ).alias(
            "txt_canonico_ruta"
        ),

        F.col(
            "contenido_texto"
        ).alias(
            "txt_canonico_contenido"
        ),

        F.col(
            "caracteres_texto_real"
        ).alias(
            "txt_canonico_caracteres"
        )
    )
)


contenido = (
    documentos

    .join(
        txt_directo,
        documentos[
            "archivo_txt_esperado"
        ]
        ==
        txt_directo[
            "txt_directo_nombre"
        ],
        "left"
    )

    .join(
        txt_canonico,
        documentos[
            "archivo_txt_canonico"
        ]
        ==
        txt_canonico[
            "txt_canonico_nombre"
        ],
        "left"
    )

    .withColumn(
        "contenido_texto_final",

        F.coalesce(
            F.col(
                "txt_directo_contenido"
            ),
            F.col(
                "txt_canonico_contenido"
            )
        )
    )

    .withColumn(
        "ruta_texto_final",

        F.coalesce(
            F.col(
                "txt_directo_ruta"
            ),
            F.col(
                "txt_canonico_ruta"
            )
        )
    )

    .withColumn(
        "caracteres_texto_final",

        F.coalesce(
            F.col(
                "txt_directo_caracteres"
            ),
            F.col(
                "txt_canonico_caracteres"
            )
        )
    )

    .withColumn(
        "fuente_texto",

        F.when(
            F.col(
                "txt_directo_contenido"
            ).isNotNull(),
            F.lit("DIRECTO")
        )

        .when(
            F.col(
                "txt_canonico_contenido"
            ).isNotNull(),
            F.lit(
                "CANONICO_DUPLICADO"
            )
        )

        .otherwise(
            F.lit(
                "NO_DISPONIBLE"
            )
        )
    )

    .withColumn(
        "texto_suficiente",

        F.col(
            "estado_extraccion"
        )
        ==
        F.lit("OK")
    )
)


# ============================================================
# 5. VALIDACIÓN PREVIA
# ============================================================

print("=" * 75)
print("VALIDACION PREVIA A SILVER")
print("=" * 75)

eventos_total = eventos.count()

eventos_coherentes = (
    eventos
    .filter(
        F.col(
            "coherencia_temporal"
        )
        ==
        "COHERENTE"
    )
    .count()
)

eventos_historicos = (
    eventos
    .filter(
        F.col(
            "coherencia_temporal"
        )
        ==
        "HISTORICO_REGULARIZACION"
    )
    .count()
)


documentos_total = documentos.count()

documentos_coherentes = (
    documentos
    .filter(
        F.col(
            "coherencia_temporal"
        )
        ==
        "COHERENTE"
    )
    .count()
)

documentos_historicos = (
    documentos
    .filter(
        F.col(
            "coherencia_temporal"
        )
        ==
        "HISTORICO_REGULARIZACION"
    )
    .count()
)


txt_total = txt_df.count()

contenido_total = contenido.count()

contenido_disponible = (
    contenido
    .filter(
        F.col(
            "contenido_texto_final"
        ).isNotNull()
    )
    .count()
)

texto_canonico = (
    contenido
    .filter(
        F.col(
            "fuente_texto"
        )
        ==
        "CANONICO_DUPLICADO"
    )
    .count()
)


print(
    "Eventos totales              :",
    eventos_total
)

print(
    "Eventos COHERENTE            :",
    eventos_coherentes
)

print(
    "Eventos HISTORICO            :",
    eventos_historicos
)

print()

print(
    "Documentos totales           :",
    documentos_total
)

print(
    "Documentos COHERENTE         :",
    documentos_coherentes
)

print(
    "Documentos HISTORICO         :",
    documentos_historicos
)

print()

print(
    "TXT físicos                  :",
    txt_total
)

print(
    "Filas contenido              :",
    contenido_total
)

print(
    "Contenido disponible         :",
    contenido_disponible
)

print(
    "Usando TXT canónico duplicado:",
    texto_canonico
)


# ============================================================
# VALIDACIONES CRÍTICAS
# ============================================================

errores = []

if eventos_total != 346:

    errores.append(
        f"Eventos esperados 346, obtenidos {eventos_total}"
    )

if eventos_coherentes != 338:

    errores.append(
        f"Eventos coherentes esperados 338, "
        f"obtenidos {eventos_coherentes}"
    )

if eventos_historicos != 8:

    errores.append(
        f"Eventos históricos esperados 8, "
        f"obtenidos {eventos_historicos}"
    )

if documentos_total != 990:

    errores.append(
        f"Documentos esperados 990, "
        f"obtenidos {documentos_total}"
    )

if documentos_coherentes != 428:

    errores.append(
        f"Documentos coherentes esperados 428, "
        f"obtenidos {documentos_coherentes}"
    )

if documentos_historicos != 562:

    errores.append(
        f"Documentos históricos esperados 562, "
        f"obtenidos {documentos_historicos}"
    )

if txt_total != 989:

    errores.append(
        f"TXT esperados 989, obtenidos {txt_total}"
    )

if contenido_total != 990:

    errores.append(
        f"Filas de contenido esperadas 990, "
        f"obtenidas {contenido_total}"
    )


if errores:

    print("\n" + "!" * 75)
    print("VALIDACION FALLIDA - SILVER NO SERA ESCRITO")
    print("!" * 75)

    for error in errores:
        print(" -", error)

    spark.stop()

    raise SystemExit(1)


# ============================================================
# 6. PREPARAR CONTENIDO TEXTUAL FINAL
# ============================================================

contenido_silver = (
    contenido
    .select(
        "guid_documento",
        "numero_expediente",
        "archivo_pdf",
        "archivo_txt_esperado",
        "archivo_canonico",
        "fuente_texto",
        "ruta_texto_final",
        "contenido_texto_final",
        "caracteres_texto_final",
        "estado_extraccion",
        "texto_suficiente",
        "coherencia_temporal",
        "usar_silver_principal",
        "fecha_consulta_portal_date"
    )
)


# ============================================================
# 7. ESCRIBIR SILVER PARQUET + SNAPPY
# ============================================================

print("\n" + "=" * 75)
print("ESCRIBIENDO SILVER")
print("=" * 75)


(
    eventos
    .write
    .mode("overwrite")
    .partitionBy(
        "coherencia_temporal"
    )
    .parquet(
        SILVER_EVENTOS
    )
)


(
    documentos
    .write
    .mode("overwrite")
    .partitionBy(
        "coherencia_temporal"
    )
    .parquet(
        SILVER_DOCUMENTOS
    )
)


(
    contenido_silver
    .write
    .mode("overwrite")
    .partitionBy(
        "coherencia_temporal"
    )
    .parquet(
        SILVER_TEXTO
    )
)


# ============================================================
# 8. VALIDAR PARQUET GENERADO
# ============================================================

eventos_silver = (
    spark.read.parquet(
        SILVER_EVENTOS
    )
)

documentos_silver = (
    spark.read.parquet(
        SILVER_DOCUMENTOS
    )
)

texto_silver = (
    spark.read.parquet(
        SILVER_TEXTO
    )
)


validacion_eventos = (
    eventos_silver.count()
)

validacion_documentos = (
    documentos_silver.count()
)

validacion_texto = (
    texto_silver.count()
)


print()
print(
    "Eventos Silver    :",
    validacion_eventos
)

print(
    "Documentos Silver :",
    validacion_documentos
)

print(
    "Contenido Silver  :",
    validacion_texto
)


if (
    validacion_eventos != 346
    or
    validacion_documentos != 990
    or
    validacion_texto != 990
):

    spark.stop()

    raise RuntimeError(
        "La validación posterior a escritura Silver falló."
    )


# ============================================================
# 9. RESUMEN LOCAL PARA GITHUB
# ============================================================

SUMMARY_LOCAL.parent.mkdir(
    parents=True,
    exist_ok=True
)

summary = {
    "fecha_ingesta": FECHA_INGESTA,

    "bronze": {
        "pdf_originales": 990,
        "txt_fisicos": txt_total
    },

    "silver": {
        "eventos_total":
            validacion_eventos,

        "eventos_coherentes":
            eventos_coherentes,

        "eventos_historicos_regularizacion":
            eventos_historicos,

        "documentos_total":
            validacion_documentos,

        "documentos_coherentes":
            documentos_coherentes,

        "documentos_historicos_regularizacion":
            documentos_historicos,

        "contenido_textual_total":
            validacion_texto,

        "contenido_disponible":
            contenido_disponible,

        "contenido_desde_canonico_duplicado":
            texto_canonico
    },

    "format": "Parquet",
    "compression": "Snappy",

    "hdfs": {
        "eventos": SILVER_EVENTOS,
        "documentos": SILVER_DOCUMENTOS,
        "contenido_textual": SILVER_TEXTO
    }
}


with open(
    SUMMARY_LOCAL,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        summary,
        f,
        ensure_ascii=False,
        indent=2
    )


print("\n" + "=" * 75)
print("SILVER HECHOS DE IMPORTANCIA COMPLETADO")
print("=" * 75)

print(
    "Eventos:",
    SILVER_EVENTOS
)

print(
    "Documentos:",
    SILVER_DOCUMENTOS
)

print(
    "Contenido:",
    SILVER_TEXTO
)

print(
    "Resumen:",
    SUMMARY_LOCAL
)

print("=" * 75)


spark.stop()
