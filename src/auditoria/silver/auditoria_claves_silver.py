
import argparse
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


CLAVES = {
    "mercado_diario/cotizaciones": [
        ["valor", "fecha_cotizacion"]
    ],
    "mercado_diario/montos_sab": [
        ["codigo_sab", "fecha_negociacion"]
    ],
    "mercado_diario/indices": [
        ["indice", "fecha"],
        ["clase", "indice", "tipo", "fecha"]
    ],
    "macroeconomia/tipo_cambio": [
        ["fecha"]
    ],
    "macroeconomia/embig": [
        ["fecha"]
    ],
    "finanzas_empresariales/principales_cuentas": [
        ["rpj", "ejercicio", "trimestre_num"],
        ["clave_negocio"],
        ["id_registro"]
    ],
    "hechos_importancia/eventos": [
        ["numero_expediente"]
    ],
    "hechos_importancia/documentos": [
        ["guid_documento"]
    ],
    "hechos_importancia/contenido_textual": [
        ["guid_documento"]
    ],
    "hechos_importancia/identidad_empresarial": [
        ["numero_expediente"]
    ],
    "referencias_smv/puente_ruc_nemonico": [
        ["ruc", "nemonico_valor"]
    ],
    "referencias_smv/valores_inscritos": [
        ["codigo_isin"],
        ["ruc", "nemonico_valor"]
    ]
}


def auditar_clave(df, claves):
    condicion_invalida = reduce(
        lambda a, b: a | b,
        [
            F.col(c).isNull()
            | (F.trim(F.col(c).cast("string")) == "")
            for c in claves
        ]
    )

    total = df.count()

    invalidos = df.filter(
        condicion_invalida
    ).count()

    validos = df.filter(
        ~condicion_invalida
    )

    grupos = (
        validos
        .groupBy(*claves)
        .count()
    )

    repetidos = grupos.filter(
        F.col("count") > 1
    )

    metricas = repetidos.agg(
        F.count("*").alias("grupos_repetidos"),
        F.sum("count").alias("filas_repetidas"),
        F.sum(
            F.col("count") - 1
        ).alias("filas_excedentes")
    ).first()

    grupos_repetidos = metricas["grupos_repetidos"]
    filas_repetidas = metricas["filas_repetidas"] or 0
    excedentes = metricas["filas_excedentes"] or 0

    print(f"\nClave candidata: {claves}")
    print(f"Total registros: {total}")
    print(f"Registros con clave nula/vacia: {invalidos}")
    print(f"Grupos con clave repetida: {grupos_repetidos}")
    print(f"Filas en grupos repetidos: {filas_repetidas}")
    print(f"Filas excedentes: {excedentes}")

    if invalidos == 0 and grupos_repetidos == 0:
        print("RESULTADO: CLAVE CANDIDATA UNICA Y COMPLETA")
    else:
        print("RESULTADO: REQUIERE REVISION")

    if grupos_repetidos > 0:
        print("\nEjemplos de claves repetidas:")

        (
            repetidos
            .orderBy(F.desc("count"))
            .show(10, truncate=False)
        )


def main():
    parser = argparse.ArgumentParser(
        description="Auditoria de claves Silver"
    )

    parser.add_argument(
        "--silver-root",
        required=True
    )

    args = parser.parse_args()

    root = args.silver_root.rstrip("/")

    spark = (
        SparkSession.builder
        .appName("BVL_Auditoria_Claves_Silver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        for dataset, candidatos in CLAVES.items():

            print("\n" + "=" * 80)
            print(f"DATASET: {dataset}")
            print("=" * 80)

            df = (
                spark.read
                .option("mergeSchema", "true")
                .parquet(f"{root}/{dataset}")
                .cache()
            )

            try:
                for claves in candidatos:
                    auditar_clave(df, claves)
            finally:
                df.unpersist()

        print("\n" + "=" * 80)
        print("AUDITORIA FINALIZADA")
        print("=" * 80)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
