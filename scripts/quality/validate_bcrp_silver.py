from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Auditoria-BCRP-Silver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

datasets = {
    "TIPO_CAMBIO": {
        "ruta": "/datalake/bvl/silver/macroeconomia/tipo_cambio",
        "esperado": 7751,
        "fecha_min": "1997-01-02",
        "fecha_max": "2026-09-17"
    },

    "EMBIG": {
        "ruta": "/datalake/bvl/silver/macroeconomia/embig",
        "esperado": 7481,
        "fecha_min": "1998-01-01",
        "fecha_max": "2026-09-03"
    }
}

print("=" * 75)
print("AUDITORIA SILVER BCRP")
print("=" * 75)

for nombre, cfg in datasets.items():

    print()
    print("=" * 75)
    print(nombre)
    print("=" * 75)

    df = spark.read.parquet(cfg["ruta"])

    total = df.count()

    print("Registros        :", f"{total:,}")
    print("Columnas         :", df.columns)

    print("\nSCHEMA")
    df.printSchema()

    if "fecha" not in df.columns:
        print("\nERROR: no existe columna 'fecha'")
        continue

    resumen = (
        df
        .agg(
            F.min("fecha").alias("min_fecha"),
            F.max("fecha").alias("max_fecha"),
            F.countDistinct("fecha").alias("fechas_distintas"),
            F.sum(
                F.when(
                    F.col("fecha").isNull(),
                    1
                ).otherwise(0)
            ).alias("fechas_nulas")
        )
        .first()
    )

    min_fecha = str(resumen["min_fecha"])
    max_fecha = str(resumen["max_fecha"])
    fechas_distintas = resumen["fechas_distintas"]
    fechas_nulas = resumen["fechas_nulas"]

    duplicados_fecha = total - fechas_distintas

    print()
    print("Fecha mínima     :", min_fecha)
    print("Fecha máxima     :", max_fecha)
    print("Fechas distintas :", f"{fechas_distintas:,}")
    print("Fechas nulas     :", f"{fechas_nulas:,}")
    print("Duplicados fecha :", f"{duplicados_fecha:,}")

    coincide = (
        total == cfg["esperado"]
        and fechas_distintas == cfg["esperado"]
        and min_fecha == cfg["fecha_min"]
        and max_fecha == cfg["fecha_max"]
        and fechas_nulas == 0
        and duplicados_fecha == 0
    )

    print()
    print(
        "RESULTADO        :",
        "COINCIDE CON FUENTE LOCAL"
        if coincide
        else "REQUIERE REVISION"
    )

print()
print("=" * 75)
print("FIN AUDITORIA")
print("=" * 75)

spark.stop()
