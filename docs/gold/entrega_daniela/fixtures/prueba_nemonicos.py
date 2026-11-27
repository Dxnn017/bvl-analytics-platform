"""Diagnostico sintetico de normalizacion de nemonicos Gold."""

from datetime import date
from pyspark.sql import SparkSession
from detectar_alcance_mercado import detectar_claves_mercado


def main():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("BVL_Fixture_Nemonicos")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        esquema = """
            valor string,
            fecha_cotizacion date,
            cierre_actual double
        """

        fecha = date(2026, 1, 15)

        anterior = spark.createDataFrame(
            [("DEMO1", fecha, 10.0)], esquema
        )

        nuevo = spark.createDataFrame(
            [(" demo1 ", fecha, 12.0)], esquema
        )

        claves = detectar_claves_mercado(anterior, nuevo)

        print("=== FIXTURE SINTETICO ===")
        anterior.show(truncate=False)
        nuevo.show(truncate=False)

        print("=== CLAVES AFECTADAS REALES ===")
        claves.orderBy("nemonico").show(truncate=False)

        obtenidas = {
            (fila.nemonico, fila.fecha)
            for fila in claves.collect()
        }

        esperadas = {
            ("DEMO1", fecha),
            (" demo1 ", fecha)
        }

        assert obtenidas == esperadas, (
            "El comportamiento del detector ha cambiado"
        )

        print("CLAVES_AFECTADAS:", len(obtenidas))
        print("NORMALIZACION_CANONICA_OK: False")
        print("LIMITACION_NORMALIZACION_DETECTADA: True")
        print("PUBLICACION_AUTORIZADA: False")
        print("PRUEBA_DIAGNOSTICA_FINALIZADA_OK")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
