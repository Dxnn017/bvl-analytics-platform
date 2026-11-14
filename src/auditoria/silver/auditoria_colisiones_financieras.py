
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser(
        description="Auditoria de colisiones financieras"
    )

    parser.add_argument(
        "--silver-root",
        required=True
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("BVL_Colisiones_Financieras")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        ruta = (
            args.silver_root.rstrip("/")
            + "/finanzas_empresariales/principales_cuentas"
        )

        df = spark.read.parquet(ruta)

        claves = [
            "rpj",
            "ejercicio",
            "trimestre_num"
        ]

        repetidos = (
            df.groupBy(*claves)
            .count()
            .filter(F.col("count") > 1)
            .drop("count")
        )

        colisiones = df.join(
            repetidos,
            claves,
            "inner"
        )

        print("\n" + "=" * 80)
        print("AUDITORIA DE COLISIONES FINANCIERAS")
        print("=" * 80)

        print(f"Total registros: {df.count()}")
        print(f"Grupos repetidos: {repetidos.count()}")
        print(f"Filas involucradas: {colisiones.count()}")

        print("\n1. IDENTIDAD Y CONTEXTO FINANCIERO")

        colisiones.select(
            "rpj",
            "ruc",
            "nombre_empresa",
            "ejercicio",
            "trimestre_num",
            "tipo_empresa",
            "tipo_sector",
            "tipo_informacion",
            "metodo_flujo_efectivo",
            "moneda"
        ).orderBy(
            *claves,
            "id_registro"
        ).show(50, truncate=False)

        print("\n2. VALORES FINANCIEROS")

        colisiones.select(
            "rpj",
            "ejercicio",
            "trimestre_num",
            "id_registro",
            "activo_total",
            "pasivo_total",
            "patrimonio_total",
            "total_ingreso",
            "utilidad_neta"
        ).orderBy(
            *claves,
            "id_registro"
        ).show(50, truncate=False)

        print("\n3. TRAZABILIDAD DE LAS COLISIONES")

        colisiones.select(
            "rpj",
            "ejercicio",
            "trimestre_num",
            "clave_negocio",
            "id_registro",
            "cantidad_clave_negocio",
            "colision_clave_negocio",
            "fecha_ingesta"
        ).orderBy(
            *claves,
            "id_registro"
        ).show(50, truncate=False)

        print("\n4. COLUMNAS QUE VARIAN EN CADA GRUPO")

        columnas = [
            c for c in df.columns
            if c not in claves
        ]

        diferencias = colisiones.groupBy(*claves).agg(
            *[
                F.countDistinct(
                    F.struct(F.col(c))
                ).alias(c)
                for c in columnas
            ]
        )

        for fila in diferencias.collect():
            print(
                f"\nRPJ: {fila['rpj']} | "
                f"Ejercicio: {fila['ejercicio']} | "
                f"Trimestre: {fila['trimestre_num']}"
            )

            for columna in columnas:
                if fila[columna] > 1:
                    print(f"  - {columna}: presenta diferencias")

        print("\n5. VALORES DE TIPO DE INFORMACION")

        df.groupBy(
            "tipo_informacion",
            "moneda"
        ).count().orderBy(
            F.desc("count")
        ).show(50, truncate=False)

        print("\nAUDITORIA FINALIZADA")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
