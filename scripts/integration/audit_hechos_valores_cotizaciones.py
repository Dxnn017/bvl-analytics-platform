import json
import re
import unicodedata
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


HECHOS = (
    "/datalake/bvl/silver/"
    "hechos_importancia/eventos"
)

COTIZACIONES = (
    "/datalake/bvl/silver/"
    "mercado_diario/cotizaciones"
)

VALORES_JSON = Path(
    "/media/sf_bvl_shared/"
    "puentes_smv/hechos_valores_smv/"
    "hechos_empresas_valores_smv.json"
)

OUT = Path(
    "outputs/quality/"
    "hechos_valores_cotizaciones_audit.json"
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

    texto = re.sub(
        r"\s+",
        " ",
        texto
    ).strip()

    return texto


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
    .appName(
        "BVL-Hechos-Valores-Cotizaciones-Audit"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 85)
print("AUDITORIA HECHOS -> VALORES SMV -> COTIZACIONES")
print("=" * 85)


# ============================================================
# LEER RESULTADO SOAP LOCAL
# ============================================================

if not VALORES_JSON.exists():
    raise FileNotFoundError(
        f"No existe {VALORES_JSON}"
    )

datos = json.loads(
    VALORES_JSON.read_text(
        encoding="utf-8"
    )
)

filas = []

for r in datos:

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

    tipo_valor = (
        r.get("TipoValor")
        or ""
    ).strip()

    if empresa and nemonico:

        filas.append({
            "empresa_consultada":
                empresa,

            "empresa_norm":
                norm_python(empresa),

            "nemonico_smv":
                nemonico,

            "razon_social_smv":
                razon_smv,

            "tipo_valor":
                tipo_valor
        })


schema = StructType([
    StructField(
        "empresa_consultada",
        StringType(),
        True
    ),
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
    ),
    StructField(
        "tipo_valor",
        StringType(),
        True
    )
])

valores = (
    spark
    .createDataFrame(
        filas,
        schema=schema
    )
    .dropDuplicates(
        [
            "empresa_norm",
            "nemonico_smv"
        ]
    )
)


# ============================================================
# COTIZACIONES
# ============================================================

cot = (
    spark.read.parquet(
        COTIZACIONES
    )
    .withColumn(
        "valor_norm",
        F.upper(
            F.trim("valor")
        )
    )
)

valores_cot = (
    cot
    .select("valor_norm")
    .distinct()
)


# ============================================================
# 1. NEMONICOS RECUPERADOS
# ============================================================

print()
print("=" * 85)
print("1. NEMONICOS DEVUELTOS POR SMV")
print("=" * 85)

print(
    "Relaciones empresa-nemonico:",
    valores.count()
)

print(
    "Empresas:",
    valores
    .select("empresa_norm")
    .distinct()
    .count()
)

print(
    "Nemonicos:",
    valores
    .select("nemonico_smv")
    .distinct()
    .count()
)


# ============================================================
# 2. MATCH CON COTIZACIONES
# ============================================================

valores_match = (
    valores.alias("v")
    .join(
        valores_cot.alias("c"),
        F.col("v.nemonico_smv")
        == F.col("c.valor_norm"),
        "left"
    )
    .withColumn(
        "match_cotizaciones",
        F.col("c.valor_norm").isNotNull()
    )
)

print()
print("=" * 85)
print("2. NEMONICOS SMV VS COTIZACIONES")
print("=" * 85)

(
    valores_match
    .select(
        "empresa_consultada",
        "razon_social_smv",
        "nemonico_smv",
        "tipo_valor",
        "match_cotizaciones"
    )
    .orderBy(
        "empresa_consultada",
        "nemonico_smv"
    )
    .show(
        100,
        truncate=False
    )
)

nemonicos_match = (
    valores_match
    .filter(
        F.col("match_cotizaciones")
    )
    .select("nemonico_smv")
    .distinct()
    .count()
)

print(
    "Nemonicos que existen en Cotizaciones:",
    nemonicos_match
)


# ============================================================
# EMPRESAS CON AL MENOS UN NEMONICO COTIZADO
# ============================================================

empresas_validas = (
    valores_match
    .filter(
        F.col("match_cotizaciones")
    )
    .select(
        "empresa_norm"
    )
    .distinct()
)

print(
    "Empresas recuperadas con cotizacion:",
    empresas_validas.count()
)


# ============================================================
# 3. HECHOS RECUPERADOS
# ============================================================

hechos = (
    spark.read.parquet(
        HECHOS
    )
    .withColumn(
        "empresa_norm",
        norm_spark(
            F.col("empresa")
        )
    )
)

hechos_recuperados = (
    hechos.alias("h")
    .join(
        empresas_validas.alias("e"),
        "empresa_norm",
        "inner"
    )
)


print()
print("=" * 85)
print("3. EVENTOS RECUPERADOS MEDIANTE VALORES SMV")
print("=" * 85)

print(
    "Eventos totales recuperables:",
    hechos_recuperados.count()
)

if "usar_silver_principal" in hechos.columns:

    hechos_principales_recuperados = (
        hechos_recuperados
        .filter(
            F.col(
                "usar_silver_principal"
            ) == True
        )
    )

else:
    hechos_principales_recuperados = (
        hechos_recuperados
    )

print(
    "Eventos principales recuperables:",
    hechos_principales_recuperados.count()
)

print(
    "Empresas recuperadas:",
    hechos_principales_recuperados
    .select("empresa_norm")
    .distinct()
    .count()
)


print()
print("DETALLE POR EMPRESA")

(
    hechos_principales_recuperados
    .groupBy(
        "empresa",
        "empresa_norm"
    )
    .count()
    .orderBy(
        F.desc("count")
    )
    .show(
        50,
        truncate=False
    )
)


# ============================================================
# 4. CALCULAR NUEVA COBERTURA POTENCIAL
# ============================================================

TOTAL_PRINCIPALES = 338
MATCH_RUC_PREVIO = 277

adicionales = (
    hechos_principales_recuperados.count()
)

nuevo_total = (
    MATCH_RUC_PREVIO
    + adicionales
)

nueva_cobertura = (
    100.0
    * nuevo_total
    / TOTAL_PRINCIPALES
)


print()
print("=" * 85)
print("4. COBERTURA POTENCIAL COMBINADA")
print("=" * 85)

print(
    "Eventos principales totales :",
    TOTAL_PRINCIPALES
)

print(
    "Resueltos previamente por RUC:",
    MATCH_RUC_PREVIO
)

print(
    "Recuperados por SMV valores :",
    adicionales
)

print(
    "Total potencial identificado:",
    nuevo_total
)

print(
    "Cobertura potencial:",
    f"{nueva_cobertura:.2f}%"
)


# ============================================================
# 5. EVENTOS QUE SIGUEN SIN PUENTE
# ============================================================

empresas_resueltas = (
    empresas_validas
    .select(
        "empresa_norm"
    )
)

restantes = (
    hechos
    .filter(
        F.col(
            "usar_silver_principal"
        ) == True
    )
    .join(
        empresas_resueltas,
        "empresa_norm",
        "left_anti"
    )
)

print()
print("=" * 85)
print("5. EMPRESAS AUN SIN PUENTE POR ESTA VIA")
print("=" * 85)

(
    restantes
    .groupBy(
        "empresa",
        "empresa_norm"
    )
    .count()
    .orderBy(
        F.desc("count")
    )
    .show(
        50,
        truncate=False
    )
)


# ============================================================
# RESUMEN JSON
# ============================================================

detalle_match = [
    {
        "empresa_consultada":
            r["empresa_consultada"],

        "razon_social_smv":
            r["razon_social_smv"],

        "nemonico":
            r["nemonico_smv"],

        "tipo_valor":
            r["tipo_valor"],

        "match_cotizaciones":
            bool(
                r["match_cotizaciones"]
            )
    }
    for r in (
        valores_match
        .select(
            "empresa_consultada",
            "razon_social_smv",
            "nemonico_smv",
            "tipo_valor",
            "match_cotizaciones"
        )
        .collect()
    )
]

resultado = {
    "relaciones_empresa_nemonico":
        valores.count(),

    "nemonicos_smv":
        valores
        .select("nemonico_smv")
        .distinct()
        .count(),

    "nemonicos_en_cotizaciones":
        nemonicos_match,

    "empresas_recuperadas":
        empresas_validas.count(),

    "eventos_principales_recuperados":
        adicionales,

    "cobertura_previa_ruc": {
        "resueltos": 277,
        "total": 338,
        "pct": round(
            277 * 100 / 338,
            4
        )
    },

    "cobertura_combinada_potencial": {
        "resueltos":
            nuevo_total,

        "total":
            TOTAL_PRINCIPALES,

        "pct":
            round(
                nueva_cobertura,
                4
            )
    },

    "detalle_nemonicos":
        detalle_match
}

OUT.parent.mkdir(
    parents=True,
    exist_ok=True
)

OUT.write_text(
    json.dumps(
        resultado,
        ensure_ascii=False,
        indent=2
    ),
    encoding="utf-8"
)


print()
print("=" * 85)
print("ARCHIVO GENERADO")
print("=" * 85)

print(OUT)

print()
print("=" * 85)
print("FIN AUDITORIA")
print("=" * 85)

spark.stop()
