
# BVL_HECHOS_PREFLIGHT_OFFLINE_INTEGRADO_V1
# SOLO permite comprobar bytes locales; nunca habilita Spark, HDFS o GCS.
import sys as _bvl_sys
if _bvl_sys.argv[1:2] == ['--preflight-offline']:
    import importlib.util as _bvl_importlib
    from pathlib import Path as _BvlPath
    _root = _BvlPath(__file__).resolve().parents[2]
    _path = _root / 'scripts/pipeline/puerta_hechos_silver_preparacion.py'
    _spec = _bvl_importlib.spec_from_file_location('bvl_hechos_preflight', _path)
    _module = _bvl_importlib.module_from_spec(_spec)
    _spec.loader.exec_module(_module)
    raise SystemExit(_module.main(_bvl_sys.argv[2:]))

SILVER_EJECUCION_HABILITADA = False
if not SILVER_EJECUCION_HABILITADA:
    raise SystemExit(
        "BLOQUEADO: faltan manifest oficial, generaciones GCS "
        "y autorizacion operativa de Quality."
    )

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

# BVL_CANDIDATO_PARAMETRIZADO_V1
# Contrato oficial, manifest y snapshot de generaciones: PENDIENTES.
# El bloqueo SILVER_EJECUCION_HABILITADA=False se mantiene.
import argparse
p = argparse.ArgumentParser(description="Preparar candidato Silver, sin publicacion")
p.add_argument("--hechos", required=True)
p.add_argument("--fin", required=True)
p.add_argument("--puente", required=True)
p.add_argument("--cot", required=True)
p.add_argument("--bronze-json", required=True)
p.add_argument("--staging", required=True)
args = p.parse_args()

# Solo staging HDFS para este paso local. GCS y produccion siguen bloqueados.
# Entradas Silver existentes o candidatos de staging.
for nombre in ("hechos", "fin", "puente", "cot"):
    ruta = getattr(args, nombre)
    if ".." in Path(ruta).parts:
        p.error(f"{nombre}: ruta insegura")
    if not ruta.startswith(("/datalake/bvl/silver/", "/datalake/bvl/staging/")):
        p.error(f"{nombre}: entrada no autorizada")

# JSON procedente de Bronze: solo snapshot preparado en staging.
# Pendiente validar manifest, generation y SHA-256 oficiales.
if ".." in Path(args.bronze_json).parts:
    p.error("bronze_json: ruta insegura")
if not args.bronze_json.startswith("/datalake/bvl/staging/"):
    p.error("bronze_json requiere snapshot verificado en staging")

if ".." in Path(args.staging).parts:
    p.error("staging: ruta insegura")
if not args.staging.startswith("/datalake/bvl/staging/"):
    p.error("staging debe estar fuera de Silver oficial")

HECHOS = args.hechos
FIN = args.fin
PUENTE = args.puente
COT = args.cot
SOAP_BRONZE = args.bronze_json
DESTINO = args.staging
SUMMARY = Path("outputs/quality/hechos_identidad_gcp_candidato_summary.json")

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

# Leer exactamente los bytes del unico JSON consolidado.
# La integridad debe verificarse previamente contra el manifest autorizado.
archivos_json = (
    spark.read.format("binaryFile")
    .load(SOAP_BRONZE)
    .select("content")
    .collect()
)

if len(archivos_json) != 1:
    raise RuntimeError("Se requiere exactamente un JSON consolidado")

soap_data = json.loads(
    bytes(archivos_json[0]["content"]).decode("utf-8")
)

if not isinstance(soap_data, list):
    raise RuntimeError("El JSON consolidado debe contener un arreglo")

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

# Validar el lote actual sin fijar cifras historicas (338/277/15/46).
if total <= 0:
    raise RuntimeError("Candidato vacio, bloqueado")
if expedientes != total or duplicados != 0:
    raise RuntimeError("numero_expediente duplicado o ausente")
if any(n < 0 for n in (ruc_exacto, nemonico_smv, sin_puente)):
    raise RuntimeError("Conteos de vinculacion invalidos")
if ruc_exacto + nemonico_smv + sin_puente != total:
    raise RuntimeError("No se conservaron todos los eventos, incluidos SIN_PUENTE")

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
        "errorifexists"
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


if check.count() != total:
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

    "ruta_candidata":
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
