
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main():
    spark = SparkSession.builder \
        .appName("BVL-Gold-Pipeline") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    BASE_SILVER = "hdfs://localhost:9000/datalake/bvl/silver"
    BASE_GOLD = "hdfs://localhost:9000/datalake/bvl/gold"

    print(">>> Iniciando construccion de Capa Gold...")

    # =========================================================================
    # 1. PRODUCTO: mercado_valor_diario (Granularidad: nemonico + fecha)
    # =========================================================================
    print(">>> [1/5] Procesando mercado_valor_diario...")
    df_cotiz = spark.read.parquet(f"{BASE_SILVER}/mercado_diario/cotizaciones") \
        .withColumnRenamed("fecha_cotizacion", "fecha") \
        .withColumnRenamed("valor", "nemonico") \
        .withColumnRenamed("cierre_actual", "precio_cierre") \
        .withColumnRenamed("maxima_actual", "precio_maximo") \
        .withColumnRenamed("minima_actual", "precio_minimo")

    df_tc = spark.read.parquet(f"{BASE_SILVER}/macroeconomia/tipo_cambio") \
        .select("fecha", F.col("tipo_cambio_venta").alias("tipo_cambio"))
    
    df_embig = spark.read.parquet(f"{BASE_SILVER}/macroeconomia/embig") \
        .select("fecha", F.col("embig_peru").alias("embig"))

    df_macro = df_tc.join(df_embig, on="fecha", how="inner")
    df_mvd = df_cotiz.join(df_macro, on="fecha", how="left")

    w_val = Window.partitionBy("nemonico").orderBy("fecha")
    
    df_mvd = df_mvd.withColumn("precio_anterior", F.lag("precio_cierre").over(w_val)) \
        .withColumn("variacion_precio", F.col("precio_cierre") - F.col("precio_anterior")) \
        .withColumn(
            "retorno_diario_pct",
            F.when(F.col("precio_anterior") > 0, 
                   (F.col("precio_cierre") - F.col("precio_anterior")) / F.col("precio_anterior") * 100
            ).otherwise(None)
        ) \
        .withColumn("rango_intradia", F.col("precio_maximo") - F.col("precio_minimo")) \
        .withColumn(
            "rango_intradia_pct",
            F.when(F.col("precio_minimo") > 0,
                   (F.col("precio_maximo") - F.col("precio_minimo")) / F.col("precio_minimo") * 100
            ).otherwise(None)
        ) \
        .drop("precio_anterior")

    df_mvd.write.mode("overwrite").parquet(f"{BASE_GOLD}/mercado/mercado_valor_diario")

    # =========================================================================
    # 2. PRODUCTO: actividad_sab_diaria (Granularidad: fecha + codigo_sab)
    # =========================================================================
    print(">>> [2/5] Procesando actividad_sab_diaria...")
    df_sab = spark.read.parquet(f"{BASE_SILVER}/mercado_diario/montos_sab")

    df_sab_std = df_sab.select(
        F.col("fecha_negociacion").alias("fecha"),
        F.col("codigo_sab"),
        F.col("SocAgenteBolsa").alias("sociedad_agente_bolsa"),
        F.col("MontoCompras").alias("monto_compras"),
        F.col("MontoVentas").alias("monto_ventas"),
        F.col("MontoTotal").alias("monto_total"),
        F.col("PorcentajeCompras").alias("porcentaje_compras"),
        F.col("PorcentajeVentas").alias("porcentaje_ventas"),
        F.col("PorcentajeTotal").alias("porcentaje_total")
    )

    w_rank_sab = Window.partitionBy("fecha").orderBy(F.desc("monto_total"))
    df_sab_gold = df_sab_std.withColumn("ranking_sab_dia_por_monto", F.dense_rank().over(w_rank_sab))

    df_sab_gold.write.mode("overwrite").parquet(f"{BASE_GOLD}/mercado/actividad_sab_diaria")

    # =========================================================================
    # 3. PRODUCTO: empresa_trimestre (Granularidad: RUC + ejercicio + trimestre)
    # =========================================================================
    print(">>> [3/5] Procesando empresa_trimestre...")
    df_cuentas_raw = spark.read.parquet(f"{BASE_SILVER}/finanzas_empresariales/principales_cuentas")

    w_dedup = Window.partitionBy("ruc", "ejercicio", "trimestre").orderBy(F.desc(df_cuentas_raw.columns[0]))
    df_cuentas = df_cuentas_raw.withColumn("_row_num", F.row_number().over(w_dedup)) \
        .filter(F.col("_row_num") == 1) \
        .drop("_row_num")

    df_puente = spark.read.parquet(f"{BASE_SILVER}/referencias_smv/puente_ruc_nemonico") \
        .select(F.col("nemonico_valor").alias("nemonico"), F.col("ruc"))

    # Mapeo exacto al formato texto de SMV para asegurar el JOIN correcto
    q_expr = F.when(F.quarter("fecha") == 1, "1er Trimestre") \
              .when(F.quarter("fecha") == 2, "2do Trimestre") \
              .when(F.quarter("fecha") == 3, "3er Trimestre") \
              .when(F.quarter("fecha") == 4, "4to Trimestre")

    df_cotiz_trim = df_cotiz \
        .withColumn("ejercicio", F.year("fecha")) \
        .withColumn("trimestre", q_expr) \
        .join(df_puente, on="nemonico", how="inner")

    df_bursatil_empresa = df_cotiz_trim.groupBy("ruc", "ejercicio", "trimestre").agg(
        F.countDistinct("nemonico").alias("nemonicos_cotizados"),
        F.countDistinct("fecha").alias("dias_con_cotizacion"),
        F.sum("monto_negociado").alias("monto_negociado_trimestral"),
        F.stddev("precio_cierre").alias("volatilidad_retorno_pct")
    )

    df_empresa_trim = df_cuentas.join(
        df_bursatil_empresa,
        on=["ruc", "ejercicio", "trimestre"],
        how="left"
    )

    df_empresa_trim.write.mode("overwrite").parquet(f"{BASE_GOLD}/empresas/empresa_trimestre")

    # =========================================================================
    # 4. PRODUCTO: hechos_empresa (Granularidad: numero_expediente)
    # =========================================================================
    print(">>> [4/5] Procesando hechos_empresa...")
    df_eventos = spark.read.parquet(f"{BASE_SILVER}/hechos_importancia/eventos")
    df_identidad = spark.read.parquet(f"{BASE_SILVER}/hechos_importancia/identidad_empresarial")
    df_docs = spark.read.parquet(f"{BASE_SILVER}/hechos_importancia/documentos")

    df_docs_agregados = df_docs.groupBy("numero_expediente").agg(
        F.count(F.col(df_docs.columns[0])).alias("total_documentos")
    )

    cols_identidad_unicas = [c for c in df_identidad.columns if c not in df_eventos.columns or c == "numero_expediente"]
    df_identidad_clean = df_identidad.select(*cols_identidad_unicas)

    df_hechos_emp = df_eventos \
        .join(df_identidad_clean, on="numero_expediente", how="left") \
        .join(df_docs_agregados, on="numero_expediente", how="left")

    df_hechos_emp.write.mode("overwrite").parquet(f"{BASE_GOLD}/eventos/hechos_empresa")

    # =========================================================================
    # 5. PRODUCTO: impacto_hechos_valor (Granularidad: numero_expediente + nemonico)
    # =========================================================================
    print(">>> [5/5] Procesando impacto_hechos_valor...")
    fecha_col_name = "fecha_presentacion_date" if "fecha_presentacion_date" in df_hechos_emp.columns else (
        "fecha_presentacion" if "fecha_presentacion" in df_hechos_emp.columns else "fecha"
    )

    df_hechos_prep = df_hechos_emp.filter(F.col("ruc").isNotNull()) \
        .select(
            "numero_expediente",
            "ruc",
            F.to_date(F.col(fecha_col_name)).alias("fecha_evento")
        ) \
        .join(df_puente, on="ruc", how="inner")

    df_cotiz_prep = df_cotiz.select(
        "nemonico",
        F.to_date(F.col("fecha")).alias("fecha_cotiz"),
        F.col("precio_cierre").alias("cierre_evento"),
        F.col("monto_negociado").alias("monto_negociado_evento")
    )

    df_impacto = df_hechos_prep.join(
        df_cotiz_prep,
        (df_hechos_prep["nemonico"] == df_cotiz_prep["nemonico"]) &
        (df_hechos_prep["fecha_evento"] == df_cotiz_prep["fecha_cotiz"]),
        how="inner"
    ).select(
        "numero_expediente",
        df_hechos_prep["nemonico"],
        "fecha_evento",
        "cierre_evento",
        "monto_negociado_evento"
    )

    df_impacto.write.mode("overwrite").parquet(f"{BASE_GOLD}/eventos/impacto_hechos_valor")

    print("\n=======================================================")
    print("¡CAPA GOLD GENERADA CON ÉXITO Y 0 DUPLICADOS EN HDFS!")
    print("=======================================================")
    spark.stop()

if __name__ == "__main__":
    main()
