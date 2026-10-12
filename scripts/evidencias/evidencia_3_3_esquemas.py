from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("Evidencia_3_3_Esquemas")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

datasets = [
    (
        "TIPO DE CAMBIO BCRP",
        "/datalake/bvl/silver/macroeconomia/tipo_cambio"
    ),
    (
        "EMBIG PERU BCRP",
        "/datalake/bvl/silver/macroeconomia/embig"
    ),
    (
        "COTIZACIONES HISTORICAS SMV",
        "/datalake/bvl/silver/mercado_diario/cotizaciones"
    )
]

print()
print("=" * 90)
print("3.3 - VERIFICACION DE ESQUEMAS DESPUES DEL PROCESAMIENTO")
print("=" * 90)

for nombre, ruta in datasets:

    print()
    print("=" * 90)
    print(nombre)
    print("=" * 90)
    print("Ruta Silver:", ruta)

    df = spark.read.parquet(ruta)

    print("Registros:", f"{df.count():,}")

    print()
    print("ESQUEMA RESULTANTE")
    df.printSchema()

    print("MUESTRA DE DATOS LIMPIOS")

    if nombre == "TIPO DE CAMBIO BCRP":
        df.select(
            "fecha",
            "tipo_cambio_venta",
            "anio",
            "mes"
        ).orderBy("fecha").show(5, False)

    elif nombre == "EMBIG PERU BCRP":
        df.select(
            "fecha",
            "embig_peru",
            "anio",
            "mes"
        ).orderBy("fecha").show(5, False)

    else:
        columnas = [
            c for c in [
                "fecha",
                "valor",
                "cierre_actual",
                "monto_negociado",
                "anio",
                "mes"
            ]
            if c in df.columns
        ]

        df.select(*columnas).show(5, False)

print()
print("=" * 90)
print("RESULTADO: ESQUEMAS SILVER VERIFICADOS")
print("=" * 90)

spark.stop()
