from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path
import json

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

OUT = Path(
    "outputs/quality/"
    "hechos_empresa_bridge_audit.json"
)

spark = (
    SparkSession.builder
    .appName("BVL-Hechos-Empresa-Bridge-Audit")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# UTILIDAD DE NORMALIZACION
# ============================================================

def normalizar_nombre(columna):

    x = F.upper(
        F.trim(
            columna.cast("string")
        )
    )

    # Eliminar diferencias de tildes sin alterar palabras
    x = F.translate(
        x,
        "ÁÉÍÓÚÜÑ",
        "AEIOUUN"
    )

    # Puntuacion -> espacio
    x = F.regexp_replace(
        x,
        r"[^A-Z0-9]+",
        " "
    )

    # Espacios multiples -> uno
    x = F.regexp_replace(
        x,
        r"\s+",
        " "
    )

    return F.trim(x)


print("=" * 85)
print("AUDITORIA HECHOS DE IMPORTANCIA -> EMPRESA -> RUC")
print("=" * 85)


# ============================================================
# CARGA
# ============================================================

hechos = spark.read.parquet(HECHOS)
fin = spark.read.parquet(FIN)
puente = spark.read.parquet(PUENTE)
cot = spark.read.parquet(COT)

print()
print("Columnas Hechos:")
print(hechos.columns)


# ============================================================
# NORMALIZAR FUENTES EMPRESARIALES
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

        F.col("nombre_empresa").alias(
            "nombre_oficial"
        ),

        normalizar_nombre(
            F.col("nombre_empresa")
        ).alias(
            "empresa_norm"
        )
    )
    .withColumn(
        "fuente_nombre",
        F.lit("PRINCIPALES_CUENTAS")
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

        F.col("razon_social").alias(
            "nombre_oficial"
        ),

        normalizar_nombre(
            F.col("razon_social")
        ).alias(
            "empresa_norm"
        )
    )
    .withColumn(
        "fuente_nombre",
        F.lit("VALORES_INSCRITOS")
    )
)

maestro_nombres = (
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


# ============================================================
# AUDITAR AMBIGUEDAD DEL NOMBRE
# ============================================================

mapa_stats = (
    maestro_nombres
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

nombres_ambiguos = (
    mapa_stats
    .filter(
        F.col("cantidad_ruc") > 1
    )
)

cantidad_ambiguos = (
    nombres_ambiguos.count()
)

print()
print("=" * 85)
print("1. MAESTRO DE IDENTIDAD EMPRESARIAL")
print("=" * 85)

print(
    "Pares RUC-nombre oficiales:",
    maestro_nombres.count()
)

print(
    "Nombres normalizados distintos:",
    mapa_stats.count()
)

print(
    "Nombres asociados a >1 RUC:",
    cantidad_ambiguos
)

if cantidad_ambiguos > 0:

    print()
    print("EJEMPLOS AMBIGUOS")

    nombres_ambiguos.show(
        30,
        truncate=False
    )


# Solo nombres que identifican exactamente un RUC
mapa_unico = (
    mapa_stats
    .filter(
        F.col("cantidad_ruc") == 1
    )
    .select(
        "empresa_norm",
        F.element_at(
            F.col("rucs"),
            1
        ).alias(
            "ruc_norm"
        )
    )
)


# ============================================================
# PREPARAR HECHOS
# ============================================================

hechos_norm = (
    hechos
    .withColumn(
        "empresa_norm",
        normalizar_nombre(
            F.col("empresa")
        )
    )
)

total_hechos = hechos_norm.count()

empresas_hechos = (
    hechos_norm
    .select(
        "empresa_norm"
    )
    .filter(
        F.col("empresa_norm") != ""
    )
    .distinct()
    .count()
)

if "usar_silver_principal" in hechos_norm.columns:

    hechos_principales = (
        hechos_norm
        .filter(
            F.col("usar_silver_principal") == True
        )
    )

else:

    hechos_principales = hechos_norm


print()
print("=" * 85)
print("2. UNIVERSO DE HECHOS")
print("=" * 85)

print(
    "Eventos totales:",
    total_hechos
)

print(
    "Eventos principales:",
    hechos_principales.count()
)

print(
    "Empresas distintas en eventos:",
    empresas_hechos
)


# ============================================================
# MATCH EXACTO NORMALIZADO
# ============================================================

match_todos = (
    hechos_norm.alias("h")
    .join(
        mapa_unico.alias("m"),
        "empresa_norm",
        "left"
    )
)

match_principal = (
    hechos_principales.alias("h")
    .join(
        mapa_unico.alias("m"),
        "empresa_norm",
        "left"
    )
)

eventos_match = (
    match_todos
    .filter(
        F.col("ruc_norm").isNotNull()
    )
    .count()
)

eventos_sin_match = (
    match_todos
    .filter(
        F.col("ruc_norm").isNull()
    )
    .count()
)

principales_match = (
    match_principal
    .filter(
        F.col("ruc_norm").isNotNull()
    )
    .count()
)

principales_sin_match = (
    match_principal
    .filter(
        F.col("ruc_norm").isNull()
    )
    .count()
)

pct_eventos = (
    100.0
    * eventos_match
    / total_hechos
    if total_hechos
    else 0
)

pct_principal = (
    100.0
    * principales_match
    / hechos_principales.count()
    if hechos_principales.count()
    else 0
)


print()
print("=" * 85)
print("3. COBERTURA HECHOS -> RUC")
print("=" * 85)

print(
    "Eventos con RUC:",
    eventos_match
)

print(
    "Eventos sin RUC:",
    eventos_sin_match
)

print(
    "Cobertura todos:",
    f"{pct_eventos:.2f}%"
)

print()
print(
    "Eventos principales con RUC:",
    principales_match
)

print(
    "Eventos principales sin RUC:",
    principales_sin_match
)

print(
    "Cobertura principales:",
    f"{pct_principal:.2f}%"
)


# ============================================================
# COBERTURA POR EMPRESA
# ============================================================

empresas_eventos_df = (
    hechos_norm
    .select(
        "empresa_norm"
    )
    .filter(
        F.col("empresa_norm") != ""
    )
    .distinct()
)

empresas_match = (
    empresas_eventos_df
    .join(
        mapa_unico,
        "empresa_norm",
        "inner"
    )
    .count()
)

empresas_sin_match = (
    empresas_eventos_df
    .join(
        mapa_unico,
        "empresa_norm",
        "left_anti"
    )
)

cantidad_empresas_sin = (
    empresas_sin_match.count()
)

print()
print("=" * 85)
print("4. COBERTURA POR EMPRESA")
print("=" * 85)

print(
    "Empresas eventos:",
    empresas_hechos
)

print(
    "Empresas con RUC:",
    empresas_match
)

print(
    "Empresas sin RUC:",
    cantidad_empresas_sin
)

if empresas_hechos:

    print(
        "Cobertura:",
        f"{100*empresas_match/empresas_hechos:.2f}%"
    )


# ============================================================
# EMPRESAS SIN MATCH
# ============================================================

print()
print("=" * 85)
print("5. PRIMERAS EMPRESAS SIN MATCH")
print("=" * 85)

sin_match_detalle = (
    hechos_norm.alias("h")
    .join(
        mapa_unico.alias("m"),
        "empresa_norm",
        "left_anti"
    )
    .groupBy(
        "empresa",
        "empresa_norm"
    )
    .count()
    .orderBy(
        F.desc("count")
    )
)

sin_match_detalle.show(
    50,
    truncate=False
)


# ============================================================
# RUC QUE TAMBIEN ESTAN EN COTIZACIONES
# ============================================================

puente_cot = (
    puente
    .withColumn(
        "nemonico_norm",
        F.upper(
            F.trim(
                F.col("nemonico_valor")
            )
        )
    )
    .withColumn(
        "ruc_norm",
        F.trim(
            F.col("ruc").cast("string")
        )
    )
)

cot_norm = (
    cot
    .withColumn(
        "valor_norm",
        F.upper(
            F.trim(
                F.col("valor")
            )
        )
    )
)

ruc_cotizados = (
    cot_norm.alias("c")
    .join(
        puente_cot.alias("p"),
        F.col("c.valor_norm")
        == F.col("p.nemonico_norm"),
        "inner"
    )
    .select(
        F.col("p.ruc_norm").alias(
            "ruc_norm"
        )
    )
    .distinct()
)

eventos_ruc_cotizados = (
    match_principal
    .filter(
        F.col("ruc_norm").isNotNull()
    )
    .join(
        ruc_cotizados,
        "ruc_norm",
        "inner"
    )
)

print()
print("=" * 85)
print("6. HECHOS DE EMPRESAS PRESENTES EN COTIZACIONES")
print("=" * 85)

print(
    "RUC cotizados:",
    ruc_cotizados.count()
)

print(
    "Eventos principales vinculados a RUC cotizado:",
    eventos_ruc_cotizados.count()
)

print(
    "RUC cotizados con al menos un hecho:",
    eventos_ruc_cotizados
    .select("ruc_norm")
    .distinct()
    .count()
)


# ============================================================
# RESUMEN JSON
# ============================================================

unmatched = [
    {
        "empresa": r["empresa"],
        "empresa_norm": r["empresa_norm"],
        "eventos": int(r["count"])
    }
    for r in (
        sin_match_detalle
        .limit(50)
        .collect()
    )
]

resultado = {
    "maestro_identidad": {
        "pares_ruc_nombre": maestro_nombres.count(),
        "nombres_distintos": mapa_stats.count(),
        "nombres_ambiguos": cantidad_ambiguos
    },

    "hechos": {
        "eventos_totales": total_hechos,
        "eventos_principales": hechos_principales.count(),
        "empresas_distintas": empresas_hechos
    },

    "cobertura_eventos": {
        "con_ruc": eventos_match,
        "sin_ruc": eventos_sin_match,
        "pct": round(pct_eventos, 4)
    },

    "cobertura_eventos_principales": {
        "con_ruc": principales_match,
        "sin_ruc": principales_sin_match,
        "pct": round(pct_principal, 4)
    },

    "cobertura_empresas": {
        "total": empresas_hechos,
        "con_ruc": empresas_match,
        "sin_ruc": cantidad_empresas_sin
    },

    "cotizaciones": {
        "ruc_cotizados": ruc_cotizados.count(),

        "eventos_principales_ruc_cotizado":
            eventos_ruc_cotizados.count(),

        "ruc_cotizados_con_hechos":
            eventos_ruc_cotizados
            .select("ruc_norm")
            .distinct()
            .count()
    },

    "primeras_empresas_sin_match": unmatched
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
