from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = "/datalake/bvl/silver/hechos_importancia"

EVENTOS = f"{BASE}/eventos"
DOCUMENTOS = f"{BASE}/documentos"
TEXTO = f"{BASE}/contenido_textual"

spark = (
    SparkSession.builder
    .appName("Validate-Hechos-Importancia-Silver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

eventos = spark.read.parquet(EVENTOS)
documentos = spark.read.parquet(DOCUMENTOS)
texto = spark.read.parquet(TEXTO)

print("=" * 78)
print("VALIDACION SILVER - HECHOS DE IMPORTANCIA SMV")
print("=" * 78)

# ----------------------------------------------------------
# EVENTOS
# ----------------------------------------------------------

print("\nEVENTOS")

total_eventos = eventos.count()
exp_unicos = eventos.select("numero_expediente").distinct().count()

print(f"Registros totales             : {total_eventos:,}")
print(f"Expedientes distintos         : {exp_unicos:,}")

eventos.groupBy(
    "coherencia_temporal"
).count().orderBy(
    "coherencia_temporal"
).show(truncate=False)

# ----------------------------------------------------------
# DOCUMENTOS
# ----------------------------------------------------------

print("\nDOCUMENTOS")

total_documentos = documentos.count()
guid_unicos = documentos.select("guid_documento").distinct().count()

print(f"Registros totales             : {total_documentos:,}")
print(f"GUID distintos                : {guid_unicos:,}")

documentos.groupBy(
    "coherencia_temporal"
).count().orderBy(
    "coherencia_temporal"
).show(truncate=False)

# ----------------------------------------------------------
# CONTENIDO
# ----------------------------------------------------------

print("\nCONTENIDO TEXTUAL")

total_texto = texto.count()

contenido_disponible = texto.filter(
    F.col("contenido_texto_final").isNotNull()
).count()

fuente_directa = texto.filter(
    F.col("fuente_texto") == "DIRECTO"
).count()

fuente_canonica = texto.filter(
    F.col("fuente_texto") == "CANONICO_DUPLICADO"
).count()

print(f"Registros totales             : {total_texto:,}")
print(f"Contenido disponible          : {contenido_disponible:,}")
print(f"Texto DIRECTO                 : {fuente_directa:,}")
print(f"Texto CANONICO_DUPLICADO      : {fuente_canonica:,}")

print("\nESTADO DE EXTRACCION")

texto.groupBy(
    "estado_extraccion"
).count().orderBy(
    "estado_extraccion"
).show(truncate=False)

# ----------------------------------------------------------
# TIPOS
# ----------------------------------------------------------

print("\nSCHEMA EVENTOS")
eventos.printSchema()

print("\nSCHEMA DOCUMENTOS")
documentos.printSchema()

print("\nSCHEMA CONTENIDO")
texto.printSchema()

# ----------------------------------------------------------
# VALIDACIONES CRÍTICAS
# ----------------------------------------------------------

errores = []

if total_eventos != 346:
    errores.append(f"Eventos: {total_eventos} != 346")

if exp_unicos != 346:
    errores.append(f"Expedientes únicos: {exp_unicos} != 346")

if total_documentos != 990:
    errores.append(f"Documentos: {total_documentos} != 990")

if guid_unicos != 990:
    errores.append(f"GUID únicos: {guid_unicos} != 990")

if total_texto != 990:
    errores.append(f"Contenido: {total_texto} != 990")

if contenido_disponible != 990:
    errores.append(
        f"Contenido disponible: {contenido_disponible} != 990"
    )

if fuente_canonica != 1:
    errores.append(
        f"Texto canónico duplicado: {fuente_canonica} != 1"
    )

print("\n" + "=" * 78)

if errores:
    print("RESULTADO: FALLÓ")

    for e in errores:
        print(" -", e)

    spark.stop()
    raise SystemExit(1)

print("RESULTADO: SILVER VALIDADO CORRECTAMENTE")
print("=" * 78)

spark.stop()
