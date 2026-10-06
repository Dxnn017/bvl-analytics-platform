import json
import re
import unicodedata
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


# ============================================================
# RUTAS
# ============================================================

HECHOS = (
    "/datalake/bvl/silver/"
    "hechos_importancia/eventos"
)

FIN = (
    "/datalake/bvl/silver/"
    "finanzas_empresariales/principales_cuentas"
)

PUENTE = (
    "/datalake/bvl/silver/"
    "referencias_smv/puente_ruc_nemonico"
)

COT = (
    "/datalake/bvl/silver/"
    "mercado_diario/cotizaciones"
)

SOAP_BRONZE = (
    "/datalake/bvl/bronze/smv/"
    "hechos_valores_consulta/json/"
    "ingestion_date=2026-10-05/"
    "hechos_empresas_valores_smv.json"
)

DESTINO = (
    "/datalake/bvl/silver/"
    "hechos_importancia/identidad_empresarial"
)

SUMMARY = Path(
    "outputs/quality/"
    "hechos_identidad_silver_summary.json"
)


# ============================================================
# NORMALIZACION
# ============================================================

def normalizar_nombre(columna):

    x = F.upper(
        F.trim(
            columna.cast("string")
        )
    )

    x = F.translate(
        x,
        "ÁÉÍÓÚÜÑ",
        "AEIOUUN"
    )

    x = F.regexp_replace(
        x,
        r"[^A-Z0-9]+",
        " "
    )

    x = F.regexp_replace(
        x,
        r"\s+",
        " "
    )

    return F.trim(x)


def norm_python(texto):

    if texto is None:
        return ""

    texto = str(texto).strip().upper()

    texto = unicodedata.normalize(
        "NFD",
        texto
    )

    texto = "".join(
        c
        for c in texto
        if unicodedata.category(c) != "Mn"
    )

    texto = re.sub(
        r"[^A-Z0-9]+",
        " ",
        texto
    )

    return re.sub(
        r"\s+",
        " ",
        texto
    ).strip()


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName(
        "BVL-Hechos-Identidad-Silver"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 90)
print("HECHOS DE IMPORTANCIA - IDENTIDAD EMPRESARIAL -> SILVER")
print("=" * 90)


# ============================================================
# CARGA BASE
# ============================================================

hechos = spark.read.parquet(HECHOS)
fin = spark.read.parquet(FIN)
puente = spark.read.parquet(PUENTE)
cot = spark.read.parquet(COT)


# Solo eventos principales de Silver
if "usar_silver_principal" in hechos.columns:

    hechos = (
        hechos
        .filter(
            F.col(
                "usar_silver_principal"
            ) == True
        )
    )


hechos = (
    hechos
    .withColumn(
        "empresa_norm",
        normalizar_nombre(
            F.col("empresa")
        )
    )
)


print()
print("Eventos principales:", hechos.count())


# ============================================================
# MAESTRO EMPRESA -> RUC
# ============================================================

fin_nombres = (
    fin
    .withColumn(
        "ruc_norm",
        F.trim(
            F.col("ruc").cast("string")
        )
    )
    .filter(
        F.col("ruc_norm").rlike(r"^\d{11}$")
    )
    .filter(
        F.col("nombre_empresa").isNotNull()
    )
    .select(
        "ruc_norm",
        normalizar_nombre(
            F.col("nombre_empresa")
        ).alias(
            "empresa_norm"
        )
    )
)


puente_nombres = (
    puente
    .withColumn(
        "ruc_norm",
        F.trim(
            F.col("ruc").cast("string")
        )
    )
    .filter(
        F.col("ruc_norm").rlike(r"^\d{11}$")
    )
    .filter(
        F.col("razon_social").isNotNull()
    )
    .select(
        "ruc_norm",
        normalizar_nombre(
            F.col("razon_social")
        ).alias(
            "empresa_norm"
        )
    )
)


maestro = (
    fin_nombres
    .unionByName(
        puente_nombres
    )
    .filter(
        F.col("empresa_norm") != ""
    )
    .dropDuplicates(
        [
            "ruc_norm",
            "empresa_norm"
        ]
    )
)


maestro_stats = (
    maestro
    .groupBy(
        "empresa_norm"
    )
    .agg(
        F.countDistinct(
            "ruc_norm"
        ).alias(
            "cantidad_ruc"
        ),

        F.collect_set(
            "ruc_norm"
        ).alias(
            "rucs"
        )
    )
)


mapa_ruc = (
    maestro_stats
    .filter(
        F.col("cantidad_ruc") == 1
    )
    .select(
        "empresa_norm",

        F.element_at(
            "rucs",
            1
        ).alias(
            "ruc"
        )
    )
)


# ============================================================
# NEMONICOS OFICIALES POR RUC QUE EXISTEN EN COTIZACIONES
# ============================================================

cot_nemonicos = (
    cot
    .select(
        F.upper(
            F.trim(
                F.col("valor")
            )
        ).alias(
            "nemonico_valor"
        )
    )
    .distinct()
)


puente_cotizable = (
    puente
    .withColumn(
        "ruc",
        F.trim(
            F.col("ruc").cast("string")
        )
    )
    .withColumn(
        "nemonico_valor",
        F.upper(
            F.trim(
                F.col("nemonico_valor")
            )
        )
    )
    .join(
        cot_nemonicos,
        "nemonico_valor",
        "inner"
    )
    .select(
        "ruc",
        "nemonico_valor"
    )
    .dropDuplicates()
)


nemonicos_por_ruc = (
    puente_cotizable
    .groupBy(
        "ruc"
    )
    .agg(
        F.sort_array(
            F.collect_set(
                "nemonico_valor"
            )
        ).alias(
            "nemonicos_ruc"
        )
    )
)


# ============================================================
# LEER SEGUNDA VIA DESDE BRONZE HDFS
# ============================================================

soap_text = (
    spark.read.text(
        SOAP_BRONZE
    )
    .agg(
        F.concat_ws(
            "",
            F.collect_list(
                "value"
            )
        ).alias(
            "json_text"
        )
    )
    .first()["json_text"]
)


soap_data = json.loads(
    soap_text
)


soap_rows = []

for r in soap_data:

    empresa = (
        r.get(
            "empresa_consultada"
        )
        or ""
    ).strip()

    nemonico = (
        r.get(
            "NemonicoValor"
        )
        or ""
    ).strip().upper()

    razon_smv = (
        r.get(
            "RazonSocial"
        )
        or ""
    ).strip()

    if empresa and nemonico:

        soap_rows.append({
            "empresa_norm":
                norm_python(
                    empresa
                ),

            "nemonico_smv":
                nemonico,

            "razon_social_smv":
                razon_smv
        })


soap_schema = StructType([
    StructField(
        "empresa_norm",
        StringType(),
        True
    ),

    StructField(
        "nemonico_smv",
        StringType(),
        True
    ),

    StructField(
        "razon_social_smv",
        StringType(),
        True
    )
])


soap = spark.createDataFrame(
    soap_rows,
    schema=soap_schema
)


# Solo conservar nemonicos que realmente existen
# en nuestro histórico de Cotizaciones.
soap_validos = (
    soap
    .join(
        cot_nemonicos,
        soap.nemonico_smv
        == cot_nemonicos.nemonico_valor,
        "inner"
    )
    .select(
        soap.empresa_norm,
        soap.nemonico_smv,
        soap.razon_social_smv
    )
    .dropDuplicates(
        [
            "empresa_norm",
            "nemonico_smv"
        ]
    )
)


soap_empresa = (
    soap_validos
    .groupBy(
        "empresa_norm"
    )
    .agg(
        F.sort_array(
            F.collect_set(
                "nemonico_smv"
            )
        ).alias(
            "nemonicos_smv"
        ),

        F.first(
            "razon_social_smv",
            ignorenulls=True
        ).alias(
            "razon_social_smv"
        )
    )
)


# ============================================================
# CONSTRUIR IDENTIDAD
# ============================================================

identidad = (
    hechos.alias("h")

    .join(
        mapa_ruc.alias("r"),
        "empresa_norm",
        "left"
    )

    .join(
        nemonicos_por_ruc.alias("nr"),
        "ruc",
        "left"
    )

    .join(
        soap_empresa.alias("s"),
        "empresa_norm",
        "left"
    )
)


identidad = (
    identidad

    .withColumn(
        "metodo_vinculacion",

        F.when(
            F.col("ruc").isNotNull(),
            F.lit("RUC_EXACTO")
        )

        .when(
            F.size(
                F.col("nemonicos_smv")
            ) > 0,
            F.lit("NEMONICO_SMV")
        )

        .otherwise(
            F.lit("SIN_PUENTE")
        )
    )

    .withColumn(
        "nemonicos_cotizables",

        F.when(
            F.col(
                "metodo_vinculacion"
            ) == "RUC_EXACTO",
            F.col(
                "nemonicos_ruc"
            )
        )

        .when(
            F.col(
                "metodo_vinculacion"
            ) == "NEMONICO_SMV",
            F.col(
                "nemonicos_smv"
            )
        )
    )

    .withColumn(
        "identificado",
        F.col(
            "metodo_vinculacion"
        ) != "SIN_PUENTE"
    )
)


# ============================================================
# SELECCION FINAL
# ============================================================

columnas_base = [
    "numero_expediente",
    "empresa",
    "empresa_norm",
    "fecha_presentacion_date",
    "anio_presentacion",
    "tipo_hecho",
    "sector",
    "coherencia_temporal"
]

columnas_disponibles = [
    c
    for c in columnas_base
    if c in identidad.columns
]


silver = (
    identidad
    .select(
        *columnas_disponibles,

        "ruc",

        "razon_social_smv",

        "nemonicos_cotizables",

        "metodo_vinculacion",

        "identificado"
    )
)


# ============================================================
# VALIDACION PREVIA
# ============================================================

print()
print("=" * 90)
print("VALIDACION PREVIA A SILVER")
print("=" * 90)


total = silver.count()

expedientes = (
    silver
    .select(
        "numero_expediente"
    )
    .distinct()
    .count()
)

duplicados = (
    total
    - expedientes
)


print(
    "Registros:",
    total
)

print(
    "Expedientes distintos:",
    expedientes
)

print(
    "Duplicados adicionales:",
    duplicados
)


por_metodo = (
    silver
    .groupBy(
        "metodo_vinculacion"
    )
    .count()
    .orderBy(
        F.desc(
            "count"
        )
    )
)


por_metodo.show(
    20,
    truncate=False
)


conteos = {
    r["metodo_vinculacion"]:
        int(r["count"])
    for r in por_metodo.collect()
}


ruc_exacto = (
    conteos.get(
        "RUC_EXACTO",
        0
    )
)

nemonico_smv = (
    conteos.get(
        "NEMONICO_SMV",
        0
    )
)

sin_puente = (
    conteos.get(
        "SIN_PUENTE",
        0
    )
)


# ============================================================
# REGLAS DE SEGURIDAD
# ============================================================

if total != 338:
    raise RuntimeError(
        f"Se esperaban 338 eventos y se obtuvieron {total}"
    )

if expedientes != 338:
    raise RuntimeError(
        "numero_expediente no es unico."
    )

if duplicados != 0:
    raise RuntimeError(
        "Se generaron duplicados."
    )

if ruc_exacto != 277:
    raise RuntimeError(
        f"RUC_EXACTO esperado=277 obtenido={ruc_exacto}"
    )

if nemonico_smv != 15:
    raise RuntimeError(
        f"NEMONICO_SMV esperado=15 obtenido={nemonico_smv}"
    )

if sin_puente != 46:
    raise RuntimeError(
        f"SIN_PUENTE esperado=46 obtenido={sin_puente}"
    )


print("Validaciones de identidad: OK")


# ============================================================
# ESCRIBIR SILVER
# ============================================================

print()
print("=" * 90)
print("ESCRIBIENDO SILVER")
print("=" * 90)


(
    silver
    .write
    .mode(
        "overwrite"
    )
    .option(
        "compression",
        "snappy"
    )
    .parquet(
        DESTINO
    )
)


# ============================================================
# VALIDAR ESCRITURA
# ============================================================

check = (
    spark
    .read
    .parquet(
        DESTINO
    )
)


if check.count() != 338:
    raise RuntimeError(
        "Conteo Silver escrito incorrecto."
    )


print()
print("=" * 90)
print("SILVER IDENTIDAD EMPRESARIAL COMPLETADO")
print("=" * 90)

print(
    "Ruta:",
    DESTINO
)

print(
    "Registros:",
    check.count()
)


# ============================================================
# RESUMEN
# ============================================================

summary = {
    "dataset":
        "hechos_importancia_identidad_empresarial",

    "ruta_hdfs":
        DESTINO,

    "registros":
        total,

    "expedientes_distintos":
        expedientes,

    "metodos": {
        "RUC_EXACTO":
            ruc_exacto,

        "NEMONICO_SMV":
            nemonico_smv,

        "SIN_PUENTE":
            sin_puente
    },

    "identificados":
        ruc_exacto
        + nemonico_smv,

    "sin_identificar":
        sin_puente,

    "cobertura_pct":
        round(
            (
                ruc_exacto
                + nemonico_smv
            )
            * 100.0
            / total,
            4
        )
}


SUMMARY.parent.mkdir(
    parents=True,
    exist_ok=True
)

SUMMARY.write_text(
    json.dumps(
        summary,
        ensure_ascii=False,
        indent=2
    ),
    encoding="utf-8"
)


print(
    "Resumen:",
    SUMMARY
)

print()
print("=" * 90)

spark.stop()
