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

SOAP_JSON = Path(
    "/media/sf_bvl_shared/"
    "puentes_smv/hechos_valores_smv/"
    "hechos_empresas_valores_smv.json"
)

OUT_JSON = Path(
    "outputs/quality/"
    "hechos_bridge_final_audit.json"
)

OUT_CSV = Path(
    "outputs/quality/"
    "hechos_sin_puente_final.csv"
)


def norm_python(texto):
    if texto is None:
        return ""

    texto = str(texto).strip().upper()

    texto = unicodedata.normalize(
        "NFD",
        texto
    )

    texto = "".join(
        c for c in texto
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


def norm_spark(columna):

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


spark = (
    SparkSession.builder
    .appName("BVL-Hechos-Bridge-Final-Audit")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 90)
print("AUDITORIA FINAL - IDENTIDAD DE HECHOS DE IMPORTANCIA")
print("=" * 90)


# ============================================================
# CARGA
# ============================================================

hechos = spark.read.parquet(HECHOS)
fin = spark.read.parquet(FIN)
puente = spark.read.parquet(PUENTE)
cot = spark.read.parquet(COT)


# ============================================================
# 1. PUENTE EXACTO EMPRESA -> RUC
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
        norm_spark(
            F.col("nombre_empresa")
        ).alias("empresa_norm")
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
        norm_spark(
            F.col("razon_social")
        ).alias("empresa_norm")
    )
)

maestro = (
    fin_nombres
    .select(
        "ruc_norm",
        "empresa_norm"
    )
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

stats = (
    maestro
    .groupBy("empresa_norm")
    .agg(
        F.countDistinct(
            "ruc_norm"
        ).alias("cantidad_ruc"),

        F.collect_set(
            "ruc_norm"
        ).alias("rucs")
    )
)

mapa_ruc = (
    stats
    .filter(
        F.col("cantidad_ruc") == 1
    )
    .select(
        "empresa_norm",

        F.element_at(
            "rucs",
            1
        ).alias("ruc_match")
    )
)


# ============================================================
# 2. PUENTE EMPRESA -> NEMONICO POR SOAP
# ============================================================

if not SOAP_JSON.exists():
    raise FileNotFoundError(
        f"No existe {SOAP_JSON}"
    )

soap_data = json.loads(
    SOAP_JSON.read_text(
        encoding="utf-8"
    )
)

soap_rows = []

for r in soap_data:

    empresa = (
        r.get("empresa_consultada")
        or ""
    ).strip()

    nemonico = (
        r.get("NemonicoValor")
        or ""
    ).strip().upper()

    razon_smv = (
        r.get("RazonSocial")
        or ""
    ).strip()

    if empresa and nemonico:

        soap_rows.append({
            "empresa_norm":
                norm_python(empresa),

            "empresa_consultada":
                empresa,

            "razon_social_smv":
                razon_smv,

            "nemonico_smv":
                nemonico
        })


schema = StructType([
    StructField(
        "empresa_norm",
        StringType(),
        True
    ),
    StructField(
        "empresa_consultada",
        StringType(),
        True
    ),
    StructField(
        "razon_social_smv",
        StringType(),
        True
    ),
    StructField(
        "nemonico_smv",
        StringType(),
        True
    )
])

soap = spark.createDataFrame(
    soap_rows,
    schema=schema
)


# ============================================================
# SOLO NEMONICOS QUE EXISTEN EN COTIZACIONES
# ============================================================

valores_cot = (
    cot
    .select(
        F.upper(
            F.trim("valor")
        ).alias("nemonico_smv")
    )
    .distinct()
)

soap_validos = (
    soap
    .join(
        valores_cot,
        "nemonico_smv",
        "inner"
    )
    .dropDuplicates(
        [
            "empresa_norm",
            "nemonico_smv"
        ]
    )
)

mapa_nemonicos = (
    soap_validos
    .groupBy(
        "empresa_norm"
    )
    .agg(
        F.collect_set(
            "nemonico_smv"
        ).alias(
            "nemonicos_smv"
        ),

        F.countDistinct(
            "nemonico_smv"
        ).alias(
            "cantidad_nemonicos_smv"
        )
    )
)


# ============================================================
# 3. HECHOS
# ============================================================

hechos_norm = (
    hechos
    .withColumn(
        "empresa_norm",
        norm_spark(
            F.col("empresa")
        )
    )
)

if "usar_silver_principal" in hechos_norm.columns:

    principales = (
        hechos_norm
        .filter(
            F.col(
                "usar_silver_principal"
            ) == True
        )
    )

else:
    principales = hechos_norm


# ============================================================
# 4. COMBINAR AMBAS VIAS
# ============================================================

resultado = (
    principales
    .join(
        mapa_ruc,
        "empresa_norm",
        "left"
    )
    .join(
        mapa_nemonicos,
        "empresa_norm",
        "left"
    )
    .withColumn(
        "metodo_vinculacion",

        F.when(
            F.col("ruc_match").isNotNull(),
            F.lit("RUC_EXACTO")
        )
        .when(
            F.col(
                "cantidad_nemonicos_smv"
            ) > 0,
            F.lit("NEMONICO_SMV")
        )
        .otherwise(
            F.lit("SIN_PUENTE")
        )
    )
)


# ============================================================
# 5. RESUMEN
# ============================================================

total = resultado.count()

por_metodo = (
    resultado
    .groupBy(
        "metodo_vinculacion"
    )
    .count()
    .orderBy(
        F.desc("count")
    )
)

print()
print("=" * 90)
print("1. COBERTURA FINAL POR METODO")
print("=" * 90)

por_metodo.show(
    20,
    truncate=False
)

ruc_count = (
    resultado
    .filter(
        F.col(
            "metodo_vinculacion"
        ) == "RUC_EXACTO"
    )
    .count()
)

nemonico_count = (
    resultado
    .filter(
        F.col(
            "metodo_vinculacion"
        ) == "NEMONICO_SMV"
    )
    .count()
)

sin_count = (
    resultado
    .filter(
        F.col(
            "metodo_vinculacion"
        ) == "SIN_PUENTE"
    )
    .count()
)

identificados = (
    ruc_count
    + nemonico_count
)

pct = (
    100.0
    * identificados
    / total
    if total
    else 0
)

print(
    "Eventos principales       :",
    total
)

print(
    "Por RUC exacto            :",
    ruc_count
)

print(
    "Por nemonico oficial SMV  :",
    nemonico_count
)

print(
    "Sin puente                :",
    sin_count
)

print(
    "Identificados             :",
    identificados
)

print(
    "Cobertura final           :",
    f"{pct:.2f}%"
)


# ============================================================
# 6. COBERTURA POR EMPRESA
# ============================================================

empresas = (
    resultado
    .select(
        "empresa",
        "empresa_norm",
        "metodo_vinculacion"
    )
    .distinct()
)

empresas_total = (
    empresas
    .select("empresa_norm")
    .distinct()
    .count()
)

empresas_resueltas = (
    empresas
    .filter(
        F.col(
            "metodo_vinculacion"
        ) != "SIN_PUENTE"
    )
    .select("empresa_norm")
    .distinct()
    .count()
)

empresas_sin = (
    empresas
    .filter(
        F.col(
            "metodo_vinculacion"
        ) == "SIN_PUENTE"
    )
    .select("empresa_norm")
    .distinct()
    .count()
)

print()
print("=" * 90)
print("2. COBERTURA FINAL POR EMPRESA")
print("=" * 90)

print(
    "Empresas totales    :",
    empresas_total
)

print(
    "Empresas resueltas  :",
    empresas_resueltas
)

print(
    "Empresas sin puente :",
    empresas_sin
)

if empresas_total:

    print(
        "Cobertura empresas  :",
        f"{100*empresas_resueltas/empresas_total:.2f}%"
    )


# ============================================================
# 7. LISTADO DEFINITIVO SIN PUENTE
# ============================================================

sin_puente = (
    resultado
    .filter(
        F.col(
            "metodo_vinculacion"
        ) == "SIN_PUENTE"
    )
    .groupBy(
        "empresa",
        "empresa_norm"
    )
    .count()
    .orderBy(
        F.desc("count"),
        "empresa"
    )
)

print()
print("=" * 90)
print("3. EMPRESAS DEFINITIVAMENTE SIN PUENTE")
print("=" * 90)

sin_puente.show(
    100,
    truncate=False
)


# ============================================================
# 8. EMPRESAS RESUELTAS POR SEGUNDA VIA
# ============================================================

segunda_via = (
    resultado
    .filter(
        F.col(
            "metodo_vinculacion"
        ) == "NEMONICO_SMV"
    )
    .groupBy(
        "empresa",
        "empresa_norm",
        "nemonicos_smv"
    )
    .count()
    .orderBy(
        F.desc("count")
    )
)

print()
print("=" * 90)
print("4. EMPRESAS RECUPERADAS POR NEMONICO SMV")
print("=" * 90)

segunda_via.show(
    100,
    truncate=False
)


# ============================================================
# 9. GUARDAR CSV DE FALTANTES
# ============================================================

OUT_CSV.parent.mkdir(
    parents=True,
    exist_ok=True
)

(
    sin_puente
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(
        str(OUT_CSV) + "_tmp"
    )
)


# ============================================================
# 10. JSON
# ============================================================

faltantes = [
    {
        "empresa":
            r["empresa"],

        "empresa_norm":
            r["empresa_norm"],

        "eventos":
            int(r["count"])
    }
    for r in sin_puente.collect()
]

segunda = [
    {
        "empresa":
            r["empresa"],

        "empresa_norm":
            r["empresa_norm"],

        "nemonicos":
            list(
                r["nemonicos_smv"]
                or []
            ),

        "eventos":
            int(r["count"])
    }
    for r in segunda_via.collect()
]

summary = {
    "eventos_principales": total,

    "ruc_exacto": ruc_count,

    "nemonico_smv": nemonico_count,

    "sin_puente": sin_count,

    "identificados": identificados,

    "cobertura_pct": round(
        pct,
        4
    ),

    "empresas": {
        "total": empresas_total,
        "resueltas": empresas_resueltas,
        "sin_puente": empresas_sin,

        "cobertura_pct": (
            round(
                100
                * empresas_resueltas
                / empresas_total,
                4
            )
            if empresas_total
            else 0
        )
    },

    "recuperadas_por_nemonico":
        segunda,

    "empresas_sin_puente":
        faltantes
}

OUT_JSON.write_text(
    json.dumps(
        summary,
        ensure_ascii=False,
        indent=2
    ),
    encoding="utf-8"
)


print()
print("=" * 90)
print("ARCHIVO GENERADO")
print("=" * 90)

print(OUT_JSON)

print()
print("=" * 90)
print("FIN AUDITORIA FINAL")
print("=" * 90)

spark.stop()
