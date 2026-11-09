#!/usr/bin/env python3
"""Auditoria de escalabilidad del benchmark historico de Cotizaciones.

No equivale al pipeline Bronze->Silver->Gold completo. Replica datos reales SOLO para
rendimiento. Verifica materializacion de todas las filas y reporta salida comprimida.
"""
import argparse
import time
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

COT = "/datalake/bvl/silver/mercado_diario/cotizaciones"
TC = "/datalake/bvl/silver/macroeconomia/tipo_cambio"
EMBIG = "/datalake/bvl/silver/macroeconomia/embig"


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("factor", type=int, choices=[1, 10, 100])
    p.add_argument("--output-root", default="/datalake/bvl/staging/benchmark_escalabilidad_auditado")
    a = p.parse_args()
    if not a.output_root.startswith("/datalake/bvl/staging/"):
        raise SystemExit("La salida de rendimiento debe estar en staging de BVL")
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex[:8]
    out = f"{a.output_root.rstrip('/')}/factor_{a.factor}_{run}"
    spark = (SparkSession.builder.appName(f"BVL-Auditoria-Escalabilidad-{a.factor}x")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.parquet.compression.codec", "snappy")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        cot, tc, embig = [spark.read.parquet(p) for p in (COT, TC, EMBIG)]
        base = cot.count()
        expected = base * a.factor
        start = time.perf_counter()
        replicas = spark.range(a.factor).select(F.col("id").alias("replica_id"))
        data = (cot.crossJoin(replicas)
             .withColumn("variacion_precio", F.col("cierre_actual") - F.col("cierre_anterior"))
             .withColumn("retorno_diario_pct", F.when(F.col("cierre_anterior") != 0,
                (F.col("cierre_actual")-F.col("cierre_anterior"))/F.col("cierre_anterior")*100))
             .withColumn("rango_intradia", F.col("maxima_actual")-F.col("minima_actual"))
             .withColumn("rango_intradia_pct", F.when(F.col("promedio_actual") != 0,
                (F.col("maxima_actual")-F.col("minima_actual"))/F.col("promedio_actual")*100)))
        joined = (data.join(tc.select(F.col("fecha").alias("fecha_tc"), "tipo_cambio_venta"),
                            F.col("fecha_cotizacion") == F.col("fecha_tc"), "left").drop("fecha_tc")
                 .join(embig.select(F.col("fecha").alias("fecha_embig"), "embig_peru"),
                       F.col("fecha_cotizacion") == F.col("fecha_embig"), "left").drop("fecha_embig"))
        (joined.write.mode("errorifexists").option("compression", "snappy")
         .partitionBy("anio", "mes").parquet(out))
        write_sec = time.perf_counter() - start
        check_start = time.perf_counter()
        materialized = spark.read.parquet(out)
        actual = materialized.count()
        replica_counts = materialized.groupBy("replica_id").count().agg(
            F.count("replica_id").alias("replicas"),
            F.min("count").alias("min_rows"),
            F.max("count").alias("max_rows")).first()
        verify_sec = time.perf_counter() - check_start
        jpath = spark._jvm.org.apache.hadoop.fs.Path(out)
        fs = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        compressed_bytes = fs.getContentSummary(jpath).getLength()
        if actual != expected:
            raise RuntimeError(f"Salida incompleta: {actual} filas vs {expected} esperadas")
        if (replica_counts["replicas"] != a.factor or replica_counts["min_rows"] != base
                or replica_counts["max_rows"] != base):
            raise RuntimeError(f"Replica incompleta: {replica_counts.asDict()}")
        throughput = actual / write_sec
        print(f"APPLICATION_ID|{spark.sparkContext.applicationId}")
        print(f"SALIDA_HDFS|{out}")
        print(f"INPUT_BASE|{base}")
        print(f"OUTPUT_ESPERADO|{expected}")
        print(f"OUTPUT_VERIFICADO|{actual}")
        print(f"REPLICAS_VERIFICADAS|{replica_counts['replicas']}")
        print(f"OUTPUT_COMPRIMIDO_MB|{compressed_bytes / (1024 ** 2):.3f}")
        print(f"TIEMPO_ESCRITURA_S|{write_sec:.3f}")
        print(f"TIEMPO_AUDITORIA_S|{verify_sec:.3f}")
        print(f"THROUGHPUT_FILAS_S|{throughput:.2f}")
        print("NOTA|La compresion Snappy y valores repetidos pueden hacer que los bytes no crezcan proporcionalmente")
        print("NOTA|Esta prueba mide el benchmark, NO toda la ingesta Bronze-Silver-Gold")
        print("SCALABILITY_RESULT_V2|{}|{}|{}|{:.4f}|{:.4f}|{}|{:.2f}|{}".format(
            a.factor, expected, actual, write_sec, verify_sec, compressed_bytes, throughput, out))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
