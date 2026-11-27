"""Prueba sintetica de sustitucion financiera e idempotencia Gold."""

from datetime import date
from pyspark.sql import SparkSession
from build_empresa_trimestre import construir_gold
from recalcular_empresa_trimestre import recalcular_empresa_trimestre


def main():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("BVL_Fixture_Sustitucion_Financiera")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        esquema = """
            id_registro string,
            clave_negocio string,
            rpj string,
            ruc string,
            ruc_valido boolean,
            nombre_empresa string,
            tipo_empresa string,
            tipo_sector string,
            ciiu string,
            ejercicio int,
            trimestre_num int,
            trimestre string,
            tipo_informacion string,
            metodo_flujo_efectivo string,
            moneda string,
            activo_total double,
            pasivo_total double,
            patrimonio_total double,
            total_ingreso double,
            utilidad_neta double,
            cantidad_clave_negocio int,
            colision_clave_negocio boolean,
            fuente_datos string,
            fecha_ingesta date
        """

        def fila(id_registro, rpj, activo, pasivo, patrimonio, utilidad):
            return (
                id_registro, rpj + "-2026-T1",
                rpj, "RUC_SINTETICO_" + rpj, False,
                "Empresa sintetica " + rpj,
                "DEMO", "DEMO", "0000",
                2026, 1, "1",
                "INDIVIDUAL", "DIRECTO", "SOLES",
                activo, pasivo, patrimonio,
                500.0, utilidad,
                1, False, "SINTETICO",
                date(2026, 4, 15)
            )

        antigua = fila(
            "DEMO_A_V1", "RPJ_A", 1000.0, 400.0, 600.0, 100.0
        )
        nueva = fila(
            "DEMO_A_V2", "RPJ_A", 1200.0, 400.0, 800.0, 240.0
        )
        conservada = fila(
            "DEMO_B_V1", "RPJ_B", 2000.0, 800.0, 1200.0, 200.0
        )

        silver_anterior = spark.createDataFrame(
            [antigua, conservada], esquema
        )
        silver_nuevo = spark.createDataFrame(
            [nueva, conservada], esquema
        )

        gold_base = construir_gold(silver_anterior).cache()

        grupos = spark.createDataFrame(
            [("RPJ_A", 2026, 1)],
            "rpj string, ejercicio int, trimestre_num int"
        )

        candidato = recalcular_empresa_trimestre(
            silver_nuevo, gold_base, grupos
        ).cache()

        columnas = [
            "id_registro", "rpj", "activo_total",
            "utilidad_neta", "roa_aproximado_pct",
            "colision_financiera"
        ]

        print("=== GOLD ANTERIOR ===")
        gold_base.select(*columnas).show(truncate=False)

        print("=== GOLD CANDIDATO ===")
        candidato.select(*columnas).show(truncate=False)

        ids = {
            r.id_registro
            for r in candidato.select("id_registro").collect()
        }

        cols = gold_base.columns

        def diferencias(a, b):
            a = a.select(*cols)
            b = b.select(*cols)
            return (
                a.exceptAll(b).count()
                + b.exceptAll(a).count()
            )

        diferencias_b = diferencias(
            gold_base.filter("rpj = 'RPJ_B'"),
            candidato.filter("rpj = 'RPJ_B'")
        )

        segunda = recalcular_empresa_trimestre(
            silver_nuevo, candidato, grupos
        )

        diferencias_idempotencia = diferencias(
            candidato, segunda
        )

        registro = candidato.filter("rpj = 'RPJ_A'").first()

        assert ids == {"DEMO_A_V2", "DEMO_B_V1"}
        assert gold_base.count() == 2
        assert candidato.count() == 2
        assert diferencias_b == 0
        assert diferencias_idempotencia == 0
        assert abs(registro.roa_aproximado_pct - 20.0) < 0.000001
        assert registro.colision_financiera is False

        print("REGISTRO_ANTERIOR_ELIMINADO: True")
        print("REGISTRO_NUEVO_INCORPORADO: True")
        print("REGISTROS_BASE:", gold_base.count())
        print("REGISTROS_CANDIDATO:", candidato.count())
        print("DIFERENCIAS_NO_AFECTADOS:", diferencias_b)
        print("DIFERENCIAS_IDEMPOTENCIA:", diferencias_idempotencia)
        print("ROA_ACTUALIZADO:", registro.roa_aproximado_pct)
        print("PUBLICACION_AUTORIZADA: False")
        print("SUSTITUCION_FINANCIERA_APROBADA_OK")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
