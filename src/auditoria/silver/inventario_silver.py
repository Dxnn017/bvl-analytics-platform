
import argparse
from pyspark.sql import SparkSession


DATASETS = {
    "mercado_diario": [
        "cotizaciones",
        "montos_sab",
        "indices"
    ],
    "macroeconomia": [
        "tipo_cambio",
        "embig"
    ],
    "finanzas_empresariales": [
        "principales_cuentas",
        "situacion_financiera",
        "estado_resultados",
        "flujo_efectivo",
        "cambios_patrimonio"
    ],
    "hechos_importancia": [
        "eventos",
        "documentos",
        "contenido_textual",
        "identidad_empresarial"
    ],
    "referencias_smv": [
        "puente_ruc_nemonico",
        "valores_inscritos"
    ]
}


def main():
    parser = argparse.ArgumentParser(
        description="Inventario de datasets Silver - BVL Analytics"
    )

    parser.add_argument(
        "--silver-root",
        required=True,
        help="Ruta raíz de Silver"
    )

    args = parser.parse_args()
    silver_root = args.silver_root.rstrip("/")

    spark = (
        SparkSession.builder
        .appName("BVL_Inventario_Silver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    resultados = []
    errores = []

    print("\n" + "=" * 80)
    print("BVL ANALYTICS - INVENTARIO DE SILVER")
    print("=" * 80)
    print(f"Ruta Silver: {silver_root}")
    print(f"Spark: {spark.version}")
    print(f"Master: {spark.sparkContext.master}")

    try:
        for dominio, datasets in DATASETS.items():
            for dataset in datasets:

                ruta = f"{silver_root}/{dominio}/{dataset}"

                print("\n" + "=" * 80)
                print(f"DOMINIO: {dominio}")
                print(f"DATASET: {dataset}")
                print(f"RUTA: {ruta}")
                print("=" * 80)

                try:
                    df = (
                        spark.read
                        .option("mergeSchema", "true")
                        .parquet(ruta)
                    )

                    registros = df.count()
                    columnas = len(df.columns)
                    archivos = len(df.inputFiles())

                    print(f"Registros: {registros:,}")
                    print(f"Columnas: {columnas}")
                    print(f"Archivos de datos: {archivos}")

                    print("\nESQUEMA:")
                    df.printSchema()

                    resultados.append(
                        (
                            dominio,
                            dataset,
                            registros,
                            columnas,
                            archivos
                        )
                    )

                except Exception as error:
                    errores.append(
                        (dominio, dataset, str(error))
                    )
                    print(f"ERROR: {error}")

        print("\n" + "=" * 80)
        print("RESUMEN GENERAL")
        print("=" * 80)

        if resultados:
            resumen = spark.createDataFrame(
                resultados,
                [
                    "dominio",
                    "dataset",
                    "registros",
                    "columnas",
                    "archivos"
                ]
            )

            resumen.show(50, truncate=False)

        total_esperado = sum(
            len(datasets)
            for datasets in DATASETS.values()
        )

        print(f"Datasets esperados: {total_esperado}")
        print(f"Datasets procesados: {len(resultados)}")
        print(f"Datasets con errores: {len(errores)}")

        if errores:
            print("\nDATASETS CON ERRORES:")

            for dominio, dataset, error in errores:
                print(f"- {dominio}/{dataset}: {error}")

    finally:
        spark.stop()

    if errores:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
