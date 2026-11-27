"""Diagnostico sintetico de cobertura parcial SAB."""

from datetime import date
from pyspark.sql import SparkSession
from build_actividad_sab_diaria import construir_gold
from politica_quality_gold import validar_cobertura_sab


def main():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("BVL_Fixture_Cuarentena_SAB")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        esquema = """
            fecha_negociacion date,
            codigo_sab string,
            SocAgenteBolsa string,
            MontoCompras double,
            MontoVentas double,
            MontoTotal double,
            PorcentajeCompras double,
            PorcentajeVentas double,
            PorcentajeTotal double,
            fuente_datos string,
            fecha_ingesta date
        """

        fecha = date(2026, 1, 15)

        filas = [
            (fecha, "SAB_DEMO_A", "Agente A",
             60.0, 40.0, 100.0, 50.0, 50.0, 50.0,
             "SINTETICO", fecha),

            (fecha, "SAB_DEMO_B", "Agente B",
             60.0, 40.0, 100.0, 50.0, 50.0, 50.0,
             "SINTETICO", fecha)
        ]

        completo = spark.createDataFrame(filas, esquema)
        parcial = completo.filter("codigo_sab = 'SAB_DEMO_A'")
        cuarentena = completo.filter("codigo_sab = 'SAB_DEMO_B'")

        gold_completo = construir_gold(completo)
        gold_parcial = construir_gold(parcial)

        columnas = [
            "codigo_sab",
            "monto_total",
            "total_actividad_diaria",
            "cantidad_sab_diaria",
            "participacion_total_pct",
            "hhi_diario",
            "actividad_diaria_valida"
        ]

        print("=== GOLD CON COBERTURA COMPLETA ===")
        gold_completo.select(*columnas).show(truncate=False)

        print("=== GOLD CON COBERTURA PARCIAL ===")
        gold_parcial.select(*columnas).show(truncate=False)

        fila_completa = gold_completo.filter(
            "codigo_sab = 'SAB_DEMO_A'"
        ).first()

        fila_parcial = gold_parcial.first()

        assert completo.count() == 2
        assert parcial.count() == 1
        assert cuarentena.count() == 1
        assert gold_parcial.count() == 1
        assert fila_completa.total_actividad_diaria == 200
        assert fila_completa.participacion_total_pct == 50.0
        assert fila_completa.hhi_diario == 5000.0
        assert fila_parcial.total_actividad_diaria == 100
        assert fila_parcial.participacion_total_pct == 100.0
        assert fila_parcial.hhi_diario == 10000.0
        assert fila_parcial.actividad_diaria_valida is True

        print("REGISTROS_VALIDOS:", parcial.count())
        print("REGISTROS_CUARENTENA:", cuarentena.count())
        print("REGISTROS_GOLD_PARCIAL:", gold_parcial.count())
        print("SIN_IMPUTACION_ARTIFICIAL: True")
        print("LIMITACION_COBERTURA_DETECTADA: True")

        try:
            validar_cobertura_sab(1, 1, 1, "PARCIAL")
        except ValueError as error:
            assert str(error) == (
                "Evidencia SAB incompatible con Quality."
            )
            print("POLITICA_ADMITE_FIXTURE_SINTETICO: False")
            print("MOTIVO:", str(error))
        else:
            raise AssertionError(
                "El comportamiento de la politica ha cambiado"
            )

        print("PUBLICACION_AUTORIZADA: False")
        print("PRUEBA_DIAGNOSTICA_SAB_FINALIZADA_OK")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
