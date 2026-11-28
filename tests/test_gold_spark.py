from datetime import date
from types import SimpleNamespace

from pyspark.sql import SparkSession, functions as F

from scripts.ingestion.muestra import generate
from scripts.processing.riesgo import features, inputs


def test_cardinalidad_cobertura_y_disponibilidad(tmp_path):
    generate(tmp_path, 1)
    spark = (SparkSession.builder.master("local[2]").appName("prueba-contrato")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    try:
        market, sab, firms = inputs(spark, SimpleNamespace(data=tmp_path, env="local"))
        duplicate = (firms.filter("id_registro = 'REG0'").withColumn("id_registro", F.lit("AMBIGUO"))
                     .withColumn("fecha_ingesta", F.lit("2024-02-01").cast("date")))
        sab = sab.withColumn("cobertura_completa", F.col("fecha") != F.lit("2024-01-09"))
        frame = features(market, sab, firms.unionByName(duplicate))
        rows = frame.filter("nemonico = 'SIM0'").orderBy("fecha").collect()
        assert len(rows) == market.filter("nemonico = 'SIM0'").count()
        assert all(r.roa_aproximado_pct is None for r in rows if r.fecha < date(2024,1,8))
        assert all(r.roa_aproximado_pct == 3 for r in rows if date(2024,1,8) <= r.fecha < date(2024,2,1))
        assert all(r.roa_aproximado_pct is None for r in rows if r.fecha >= date(2024,2,1))
        assert next(r for r in rows if r.fecha == date(2024,1,9)).hhi_mercado_fecha is None
    finally:
        spark.stop()
