import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURACION
# ============================================================

BRONZE = (
    "/datalake/bvl/bronze/smv/"
    "valores_inscritos/json/"
    "ingestion_date=2026-10-05/"
    "valores_inscritos_smv.json"
)

SILVER_VALORES = (
    "/datalake/bvl/silver/"
    "referencias_smv/valores_inscritos"
)

SILVER_PUENTE = (
    "/datalake/bvl/silver/"
    "referencias_smv/puente_ruc_nemonico"
)

SUMMARY = Path(
    "outputs/quality/"
    "valores_smv_silver_summary.json"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("BVL-Valores-SMV-Silver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 80)
print("VALORES INSCRITOS SMV - BRONZE -> SILVER")
print("=" * 80)


# ============================================================
# LEER JSON BRONZE
# ============================================================

root = (
    spark.read
    .option("multiline", "true")
    .json(BRONZE)
)

print()
print("Columnas raíz:", root.columns)

if "Resultado" not in root.columns:
    raise RuntimeError(
        "No existe el array Resultado en el JSON Bronze."
    )

raw = (
    root
    .select(
        F.explode(
            F.col("Resultado")
        ).alias("registro")
    )
    .select("registro.*")
)

raw_count = raw.count()

print(
    "Registros Bronze :",
    f"{raw_count:,}"
)

if raw_count != 1008:
    raise RuntimeError(
        f"Se esperaban 1,008 registros y se obtuvieron {raw_count:,}"
    )

print()
print("COLUMNAS BRONZE")

for c in raw.columns:
    print(" -", c)


# ============================================================
# TRANSFORMACION
# ============================================================

def txt(nombre):
    return F.trim(
        F.col(nombre).cast("string")
    )


def numero(nombre):
    return (
        F.regexp_replace(
            txt(nombre),
            ",",
            ""
        )
        .cast("double")
    )


valores = (
    raw
    .select(
        txt("RUC").alias("ruc"),

        txt("RazonSocial").alias(
            "razon_social"
        ),

        txt("DenominacionValor").alias(
            "denominacion_valor"
        ),

        F.upper(
            txt("NemonicoValor")
        ).alias(
            "nemonico_valor"
        ),

        F.upper(
            txt("CodigoISIN")
        ).alias(
            "codigo_isin"
        ),

        txt("TipoValor").alias(
            "tipo_valor"
        ),

        F.to_date(
            txt("FechaInscripcion"),
            "dd/MM/yyyy"
        ).alias(
            "fecha_inscripcion"
        ),

        txt("ResolucionInscripcion").alias(
            "resolucion_inscripcion"
        ),

        txt("Moneda").alias(
            "moneda"
        ),

        numero("MontoInscrito").alias(
            "monto_inscrito"
        ),

        numero("Cotizacion").alias(
            "cotizacion"
        ),

        F.to_date(
            txt("FechaUltCot"),
            "dd/MM/yyyy"
        ).alias(
            "fecha_ultima_cotizacion"
        ),

        txt("_empresa_origen").alias(
            "empresa_origen"
        ),

        txt("_fuente").alias(
            "fuente"
        ),

        F.to_timestamp(
            txt("_fecha_extraccion")
        ).alias(
            "fecha_extraccion"
        )
    )
)


# ============================================================
# VALIDACIONES DEL DATASET OFICIAL
# ============================================================

silver_count = valores.count()

ruc_nulos = (
    valores
    .filter(
        F.col("ruc").isNull()
        | (F.col("ruc") == "")
    )
    .count()
)

nemonico_nulos = (
    valores
    .filter(
        F.col("nemonico_valor").isNull()
        | (F.col("nemonico_valor") == "")
    )
    .count()
)

pares = (
    valores
    .select(
        "ruc",
        "nemonico_valor"
    )
    .distinct()
    .count()
)

nemonicos = (
    valores
    .select(
        "nemonico_valor"
    )
    .distinct()
    .count()
)

print()
print("=" * 80)
print("VALIDACION SILVER - VALORES INSCRITOS")
print("=" * 80)

print(
    "Registros                    :",
    f"{silver_count:,}"
)

print(
    "RUC nulos/vacios             :",
    f"{ruc_nulos:,}"
)

print(
    "Nemonicos nulos/vacios       :",
    f"{nemonico_nulos:,}"
)

print(
    "Pares RUC+nemonico distintos :",
    f"{pares:,}"
)

print(
    "Nemonicos distintos          :",
    f"{nemonicos:,}"
)

if silver_count != 1008:
    raise RuntimeError(
        "Cantidad Silver incorrecta."
    )

if ruc_nulos != 0:
    raise RuntimeError(
        "Existen RUC nulos."
    )

if nemonico_nulos != 0:
    raise RuntimeError(
        "Existen nemonicos nulos."
    )

if pares != 831:
    raise RuntimeError(
        f"Se esperaban 831 pares RUC+nemonico y hay {pares:,}"
    )


# ============================================================
# VALIDAR QUE NEMONICO NO PERTENEZCA A MULTIPLES RUC
# ============================================================

ambiguos = (
    valores
    .groupBy(
        "nemonico_valor"
    )
    .agg(
        F.countDistinct(
            "ruc"
        ).alias(
            "cantidad_ruc"
        )
    )
    .filter(
        F.col("cantidad_ruc") > 1
    )
    .count()
)

print(
    "Nemonicos asociados >1 RUC   :",
    f"{ambiguos:,}"
)

if ambiguos != 0:
    raise RuntimeError(
        "Existen nemonicos asociados a multiples RUC."
    )


# ============================================================
# AUDITAR ATRIBUTOS DEL PUENTE
# ============================================================

atributos = [
    "razon_social",
    "codigo_isin",
    "denominacion_valor",
    "tipo_valor",
    "moneda",
    "fecha_inscripcion",
    "resolucion_inscripcion"
]

aggs = []

for campo in atributos:
    aggs.append(
        F.countDistinct(
            F.col(campo)
        ).alias(
            f"dist_{campo}"
        )
    )

variaciones = (
    valores
    .groupBy(
        "ruc",
        "nemonico_valor"
    )
    .agg(*aggs)
)

condicion = None

for campo in atributos:

    c = (
        F.col(
            f"dist_{campo}"
        ) > 1
    )

    condicion = (
        c
        if condicion is None
        else condicion | c
    )

pares_inconsistentes = (
    variaciones
    .filter(condicion)
    .count()
)

print(
    "Pares con atributos ambiguos :",
    f"{pares_inconsistentes:,}"
)

if pares_inconsistentes != 0:
    raise RuntimeError(
        "Hay pares RUC+nemonico con atributos de identidad inconsistentes."
    )


# ============================================================
# CREAR PUENTE RUC <-> NEMONICO
# ============================================================

puente = (
    valores
    .groupBy(
        "ruc",
        "nemonico_valor"
    )
    .agg(
        F.first(
            "razon_social",
            ignorenulls=True
        ).alias(
            "razon_social"
        ),

        F.first(
            "codigo_isin",
            ignorenulls=True
        ).alias(
            "codigo_isin"
        ),

        F.first(
            "denominacion_valor",
            ignorenulls=True
        ).alias(
            "denominacion_valor"
        ),

        F.first(
            "tipo_valor",
            ignorenulls=True
        ).alias(
            "tipo_valor"
        ),

        F.first(
            "moneda",
            ignorenulls=True
        ).alias(
            "moneda"
        ),

        F.first(
            "fecha_inscripcion",
            ignorenulls=True
        ).alias(
            "fecha_inscripcion"
        ),

        F.first(
            "resolucion_inscripcion",
            ignorenulls=True
        ).alias(
            "resolucion_inscripcion"
        )
    )
)

puente_count = puente.count()

print()
print("=" * 80)
print("PUENTE RUC <-> NEMONICO")
print("=" * 80)

print(
    "Relaciones únicas :",
    f"{puente_count:,}"
)

if puente_count != 831:
    raise RuntimeError(
        "Cantidad del puente incorrecta."
    )


# ============================================================
# ESCRIBIR SILVER
# ============================================================

print()
print("=" * 80)
print("ESCRIBIENDO SILVER")
print("=" * 80)

(
    valores
    .write
    .mode("overwrite")
    .option(
        "compression",
        "snappy"
    )
    .parquet(
        SILVER_VALORES
    )
)

(
    puente
    .write
    .mode("overwrite")
    .option(
        "compression",
        "snappy"
    )
    .parquet(
        SILVER_PUENTE
    )
)


# ============================================================
# VALIDACION POST-ESCRITURA
# ============================================================

check_valores = (
    spark.read
    .parquet(
        SILVER_VALORES
    )
)

check_puente = (
    spark.read
    .parquet(
        SILVER_PUENTE
    )
)

check_valores_count = (
    check_valores.count()
)

check_puente_count = (
    check_puente.count()
)

if check_valores_count != 1008:
    raise RuntimeError(
        "Error verificando Silver valores_inscritos."
    )

if check_puente_count != 831:
    raise RuntimeError(
        "Error verificando Silver puente."
    )


# ============================================================
# RESUMEN
# ============================================================

tipos = {
    str(r["tipo_valor"]): int(r["count"])
    for r in (
        valores
        .groupBy(
            "tipo_valor"
        )
        .count()
        .collect()
    )
}

summary = {
    "fuente": "SMV ServiceValores consultaValoresRUC",

    "bronze": {
        "registros": raw_count
    },

    "silver_valores_inscritos": {
        "registros": check_valores_count,
        "ruc_nulos": ruc_nulos,
        "nemonicos_nulos": nemonico_nulos,
        "pares_ruc_nemonico": pares,
        "nemonicos_distintos": nemonicos,
        "tipos_valor": tipos
    },

    "silver_puente": {
        "registros": check_puente_count,
        "nemonicos_multiples_ruc": ambiguos,
        "pares_atributos_ambiguos": pares_inconsistentes
    },

    "observacion_monto_inscrito": (
        "MontoInscrito se conserva en valores_inscritos "
        "pero se excluye del puente RUC+nemonico debido "
        "al patron de repeticion detectado en la auditoria."
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


print()
print("=" * 80)
print("SILVER VALORES SMV COMPLETADO CORRECTAMENTE")
print("=" * 80)

print(
    "Valores inscritos :",
    SILVER_VALORES
)

print(
    "Puente RUC-nemonico:",
    SILVER_PUENTE
)

print(
    "Resumen           :",
    SUMMARY
)

spark.stop()
