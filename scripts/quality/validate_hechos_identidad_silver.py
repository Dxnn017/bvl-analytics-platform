from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUTA = (
    "/datalake/bvl/silver/"
    "hechos_importancia/identidad_empresarial"
)

spark = (
    SparkSession.builder
    .appName("Validate-Hechos-Identidad-Silver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(RUTA)

print("=" * 85)
print("VALIDACION SILVER - IDENTIDAD EMPRESARIAL DE HECHOS")
print("=" * 85)

# ------------------------------------------------------------
# CONTEOS
# ------------------------------------------------------------

total = df.count()

expedientes = (
    df.select("numero_expediente")
    .distinct()
    .count()
)

identificados = (
    df.filter(
        F.col("identificado") == True
    )
    .count()
)

no_identificados = (
    df.filter(
        F.col("identificado") == False
    )
    .count()
)

print()
print("REGISTROS")
print("Registros totales       :", total)
print("Expedientes distintos   :", expedientes)
print("Identificados           :", identificados)
print("Sin identificar         :", no_identificados)

# ------------------------------------------------------------
# METODOS
# ------------------------------------------------------------

print()
print("=" * 85)
print("METODOS DE VINCULACION")
print("=" * 85)

df.groupBy(
    "metodo_vinculacion"
).count().orderBy(
    F.desc("count")
).show(
    truncate=False
)

# ------------------------------------------------------------
# CONSISTENCIA POR METODO
# ------------------------------------------------------------

ruc_exacto = df.filter(
    F.col("metodo_vinculacion") == "RUC_EXACTO"
)

nemonico_smv = df.filter(
    F.col("metodo_vinculacion") == "NEMONICO_SMV"
)

sin_puente = df.filter(
    F.col("metodo_vinculacion") == "SIN_PUENTE"
)

print()
print("=" * 85)
print("CONSISTENCIA DE IDENTIDAD")
print("=" * 85)

print(
    "RUC_EXACTO con RUC nulo:",
    ruc_exacto.filter(
        F.col("ruc").isNull()
    ).count()
)

print(
    "NEMONICO_SMV sin nemonico:",
    nemonico_smv.filter(
        F.col("nemonicos_cotizables").isNull()
        |
        (F.size("nemonicos_cotizables") == 0)
    ).count()
)

print(
    "SIN_PUENTE con RUC:",
    sin_puente.filter(
        F.col("ruc").isNotNull()
    ).count()
)

print(
    "SIN_PUENTE con nemonicos:",
    sin_puente.filter(
        F.col("nemonicos_cotizables").isNotNull()
        &
        (F.size("nemonicos_cotizables") > 0)
    ).count()
)

# ------------------------------------------------------------
# EMPRESAS
# ------------------------------------------------------------

print()
print("=" * 85)
print("COBERTURA EMPRESARIAL")
print("=" * 85)

empresas_total = (
    df.select("empresa_norm")
    .distinct()
    .count()
)

empresas_resueltas = (
    df.filter(
        F.col("identificado") == True
    )
    .select("empresa_norm")
    .distinct()
    .count()
)

empresas_sin = (
    df.filter(
        F.col("identificado") == False
    )
    .select("empresa_norm")
    .distinct()
    .count()
)

print("Empresas totales      :", empresas_total)
print("Empresas resueltas    :", empresas_resueltas)
print("Empresas sin puente   :", empresas_sin)

if empresas_total:
    print(
        "Cobertura empresarial:",
        f"{100 * empresas_resueltas / empresas_total:.2f}%"
    )

# ------------------------------------------------------------
# SCHEMA
# ------------------------------------------------------------

print()
print("=" * 85)
print("SCHEMA")
print("=" * 85)

df.printSchema()

# ------------------------------------------------------------
# MUESTRAS
# ------------------------------------------------------------

print()
print("=" * 85)
print("MUESTRA RUC_EXACTO")
print("=" * 85)

(
    ruc_exacto
    .select(
        "numero_expediente",
        "empresa",
        "ruc",
        "nemonicos_cotizables"
    )
    .show(5, truncate=False)
)

print()
print("=" * 85)
print("MUESTRA NEMONICO_SMV")
print("=" * 85)

(
    nemonico_smv
    .select(
        "numero_expediente",
        "empresa",
        "ruc",
        "razon_social_smv",
        "nemonicos_cotizables"
    )
    .show(20, truncate=False)
)

# ------------------------------------------------------------
# REGLAS FINALES
# ------------------------------------------------------------

errores = []

if total != 338:
    errores.append(
        f"Registros esperados 338, obtenidos {total}"
    )

if expedientes != 338:
    errores.append(
        f"Expedientes esperados 338, obtenidos {expedientes}"
    )

if ruc_exacto.count() != 277:
    errores.append(
        f"RUC_EXACTO esperado 277, obtenido {ruc_exacto.count()}"
    )

if nemonico_smv.count() != 15:
    errores.append(
        f"NEMONICO_SMV esperado 15, obtenido {nemonico_smv.count()}"
    )

if sin_puente.count() != 46:
    errores.append(
        f"SIN_PUENTE esperado 46, obtenido {sin_puente.count()}"
    )

if identificados != 292:
    errores.append(
        f"Identificados esperados 292, obtenidos {identificados}"
    )

if errores:
    print()
    print("RESULTADO: ERROR")

    for e in errores:
        print("-", e)

    raise RuntimeError(
        "Silver de identidad no supera las validaciones."
    )

print()
print("=" * 85)
print("RESULTADO: SILVER IDENTIDAD VALIDADO CORRECTAMENTE")
print("=" * 85)

spark.stop()
