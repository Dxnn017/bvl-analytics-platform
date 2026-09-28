from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("BVL-Silver-Validation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

datasets = [
    (
        "cotizaciones",
        "/datalake/bvl/silver/mercado_diario/cotizaciones",
        "fecha_cotizacion"
    ),
    (
        "indices",
        "/datalake/bvl/silver/mercado_diario/indices",
        "fecha"
    ),
    (
        "tipo_cambio",
        "/datalake/bvl/silver/macroeconomia/tipo_cambio",
        "fecha"
    ),
    (
        "embig",
        "/datalake/bvl/silver/macroeconomia/embig",
        "fecha"
    ),
]

print("=" * 70)
print("VALIDACION FINAL SILVER")
print("=" * 70)

total = 0

for nombre, ruta, fecha_col in datasets:

    df = spark.read.parquet(ruta)

    registros = df.count()
    total += registros

    nulas = df.filter(
        F.col(fecha_col).isNull()
    ).count()

    rango = df.agg(
        F.min(fecha_col).alias("min"),
        F.max(fecha_col).alias("max")
    ).first()

    print(f"\n{nombre.upper()}")
    print("Registros:", registros)
    print("Fechas nulas:", nulas)
    print("Fecha minima:", rango["min"])
    print("Fecha maxima:", rango["max"])

    if nombre in ("tipo_cambio", "embig"):
        print(
            "Registros septiembre:",
            df.filter(F.month(fecha_col) == 9).count()
        )


for nombre in [
    "estado_resultados",
    "situacion_financiera",
    "flujo_efectivo",
    "cambios_patrimonio"
]:

    ruta = (
        "/datalake/bvl/silver/"
        f"finanzas_empresariales/{nombre}"
    )

    df = spark.read.parquet(ruta)

    registros = df.count()
    total += registros

    print(f"\n{nombre.upper()}")
    print("Registros:", registros)


print()
print("=" * 70)
print("TOTAL SILVER:", total)
print("=" * 70)

spark.stop()


