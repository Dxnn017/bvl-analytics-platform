#!/usr/bin/env python3
"""Reconstruccion CONTROLADA de los cinco Gold en un snapshot de staging.

Nunca modifica /datalake/bvl/gold. Util para validar el refresco de consumo tras
Silver incremental. ATENCION: esta version recalcula los 5 Gold completos, no es
optimizacion incremental por particiones. Publicacion de Gold NO incluida.
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

GOLD_PRODUCTS = {
    "mercado_valor_diario": ("mercado/mercado_valor_diario", ["fecha", "nemonico"]),
    "actividad_sab_diaria": ("mercado/actividad_sab_diaria", ["fecha", "codigo_sab"]),
    "empresa_trimestre": ("empresas/empresa_trimestre", ["ruc", "ejercicio", "trimestre"]),
    "hechos_empresa": ("eventos/hechos_empresa", ["numero_expediente"]),
    "impacto_hechos_valor": ("eventos/impacto_hechos_valor", ["numero_expediente", "nemonico"]),
}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--silver-root", default="/datalake/bvl/silver")
    ap.add_argument("--cotizaciones", help="Ruta Silver de cotizaciones alternativa para pruebas")
    ap.add_argument("--stage-root", default="/datalake/bvl/staging/gold_validacion")
    ap.add_argument("--report-root", default="outputs/quality/incremental")
    a = ap.parse_args()
    base = a.silver_root.rstrip("/")
    if not base.startswith("/datalake/bvl/"):
        raise SystemExit("Silver debe estar en /datalake/bvl/")
    stage_root = a.stage_root.rstrip("/")
    if not stage_root.startswith("/datalake/bvl/staging/"):
        raise SystemExit("Gold solo puede escribirse en /datalake/bvl/staging/")
    if stage_root.startswith(base + "/") or base.startswith(stage_root + "/"):
        raise SystemExit("La ruta stage no puede intersectar la ruta Silver")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex[:8]
    stage = f"{stage_root}/run_{run_id}"
    spark = (SparkSession.builder.appName("BVL-5-Gold-Stage-Sin-Sobrescribir")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.parquet.compression.codec", "snappy")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    audit = {"run": run_id, "stage": stage, "silver": base,
             "fecha_utc": datetime.now(timezone.utc).isoformat(),
             "nota": "Reconstruccion FULL Gold solo en staging. No es incremental fisico. Finanzas con RUC invalido o varios importes para un mismo periodo conservadas en excepciones.",
             "productos": {}, "alertas": []}
    try:
        cot_path = a.cotizaciones or f"{base}/mercado_diario/cotizaciones"
        df_cotiz = (spark.read.parquet(cot_path)
            .withColumnRenamed("fecha_cotizacion", "fecha")
            .withColumnRenamed("valor", "nemonico")
            .withColumnRenamed("cierre_actual", "precio_cierre")
            .withColumnRenamed("maxima_actual", "precio_maximo")
            .withColumnRenamed("minima_actual", "precio_minimo"))
        tc = spark.read.parquet(f"{base}/macroeconomia/tipo_cambio").select(
            "fecha", F.col("tipo_cambio_venta").alias("tipo_cambio"))
        embig = spark.read.parquet(f"{base}/macroeconomia/embig").select(
            "fecha", F.col("embig_peru").alias("embig"))
        # No forzar que ambas series macro tengan observacion: cotizaciones siguen presentes.
        macro = tc.join(embig, "fecha", "full_outer")
        mvd = df_cotiz.join(macro, "fecha", "left")
        w_val = Window.partitionBy("nemonico").orderBy("fecha")
        mvd = (mvd.withColumn("precio_anterior", F.lag("precio_cierre").over(w_val))
            .withColumn("variacion_precio", F.col("precio_cierre") - F.col("precio_anterior"))
            .withColumn("retorno_diario_pct", F.when(F.col("precio_anterior") > 0,
                       100 * (F.col("precio_cierre")-F.col("precio_anterior")) / F.col("precio_anterior")))
            .withColumn("rango_intradia", F.col("precio_maximo") - F.col("precio_minimo"))
            .withColumn("rango_intradia_pct", F.when(F.col("precio_minimo") > 0,
                100 * (F.col("precio_maximo")-F.col("precio_minimo"))/F.col("precio_minimo")))
            .drop("precio_anterior"))

        sab = spark.read.parquet(f"{base}/mercado_diario/montos_sab")
        sab_std = sab.select(F.col("fecha_negociacion").alias("fecha"), "codigo_sab",
            F.col("SocAgenteBolsa").alias("sociedad_agente_bolsa"),
            F.col("MontoCompras").alias("monto_compras"),
            F.col("MontoVentas").alias("monto_ventas"),
            F.col("MontoTotal").alias("monto_total"),
            F.col("PorcentajeCompras").alias("porcentaje_compras"),
            F.col("PorcentajeVentas").alias("porcentaje_ventas"),
            F.col("PorcentajeTotal").alias("porcentaje_total"))
        w_rank = Window.partitionBy("fecha").orderBy(F.desc("monto_total"))
        sab_gold = sab_std.withColumn("ranking_sab_dia_por_monto", F.dense_rank().over(w_rank))

        # FINANZAS: no colapsar RUC inexistentes (p. ej., "0").
        # El identificador RPJ distingue entidades diferentes: un RUC invalido
        # NO es una clave empresarial apta para deduplicar ni hacer JOIN.
        cuentas = spark.read.parquet(f"{base}/finanzas_empresariales/principales_cuentas")
        key_fin = ["ruc", "ejercicio", "trimestre"]
        ruc_ok = (F.col("ruc").rlike(r"^[0-9]{11}$") &
                  (F.col("ruc") != "00000000000"))
        if "ruc_valido" in cuentas.columns:
            ruc_ok = ruc_ok & F.coalesce(F.col("ruc_valido"), F.lit(False))
        cuentas_ok = cuentas.filter(ruc_ok)
        cuentas_sin_ruc = cuentas.filter(~F.coalesce(ruc_ok, F.lit(False)))
        total_fin = cuentas.count()
        total_valido = cuentas_ok.count()
        total_sin_ruc = cuentas_sin_ruc.count()
        if total_valido + total_sin_ruc != total_fin:
            raise RuntimeError("Control de particion de registros financieros fallido")
        ruta_excepcion = f"{stage}/calidad/finanzas_ruc_no_conciliado"
        (cuentas_sin_ruc.write.mode("errorifexists").option("compression", "snappy")
         .parquet(ruta_excepcion))
        audit["finanzas_ruc"] = {
            "registros_silver": total_fin,
            "registros_con_ruc_valido": total_valido,
            "registros_sin_ruc_valido": total_sin_ruc,
            "registros_excepciones": spark.read.parquet(ruta_excepcion).count(),
            "ruta_excepciones": ruta_excepcion,
            "politica": "No atribuir estados a una empresa usando RUC no valido; preservarlos en excepciones"
        }
        print(f"FINANZAS|silver={total_fin}|ruc_valido={total_valido}|sin_ruc_valido={total_sin_ruc}")
        print(f"FINANZAS_EXCEPCIONES|{ruta_excepcion}")
        # Granularidad empresarial declarada: RUC + ejercicio + trimestre.
        # Distinguir los registros financieramente contradictorios, incluso
        # si poseen el mismo RPJ/moneda/tipo. No elegir una fila arbitrariamente.
        colisiones_validas = (cuentas_ok.groupBy(*key_fin).count()
                             .filter(F.col("count") > 1))
        grupos_validos = colisiones_validas.count()
        ambiguas = cuentas_ok.join(colisiones_validas.select(*key_fin), key_fin, "inner")
        cuentas_1 = cuentas_ok.join(colisiones_validas.select(*key_fin), key_fin, "left_anti")
        n_ambiguas = ambiguas.count()
        n_cuentas_unicas = cuentas_1.count()
        if n_cuentas_unicas + n_ambiguas != total_valido:
            raise RuntimeError("No cierra reconciliacion de cuentas validas y ambiguas")
        n_claves = cuentas_1.select(*key_fin).distinct().count()
        if n_claves != n_cuentas_unicas:
            raise RuntimeError("Cuentas validas restantes aun presentan claves ambiguas")
        ruta_ambiguas = f"{stage}/calidad/finanzas_ruc_valido_ambiguo"
        (ambiguas.write.mode("errorifexists").option("compression", "snappy")
         .parquet(ruta_ambiguas))
        n_excepciones = spark.read.parquet(ruta_ambiguas).count()
        if n_excepciones != n_ambiguas:
            raise RuntimeError("Excepciones financieras no se conservaron completamente")
        audit["finanzas_ruc"].update({
            "grupos_duplicados_validos": grupos_validos,
            "registros_financieros_ambiguos": n_ambiguas,
            "registros_validos_no_ambiguos": n_cuentas_unicas,
            "ruta_ambiguos": ruta_ambiguas,
            "politica_ambiguos": "Conservar todas las versiones en excepciones; no publicar cifras contradictorias sin verificar SMV",
        })
        print(f"FINANZAS_AMBIGUAS|grupos={grupos_validos}|registros={n_ambiguas}|ruta={ruta_ambiguas}")
        print(f"FINANZAS_ELEGIBLES|{n_cuentas_unicas}")
        if grupos_validos:
            audit["alertas"].append(
                f"Finanzas: {n_ambiguas} registros en {grupos_validos} periodos ambiguos, excluidos temporalmente de empresa_trimestre y conservados en {ruta_ambiguas}. Requieren verificacion institucional antes de publicar Gold."
            )
        puente = spark.read.parquet(f"{base}/referencias_smv/puente_ruc_nemonico").select(
            F.col("nemonico_valor").alias("nemonico"), "ruc")
        trimestre = (F.when(F.quarter("fecha") == 1, F.lit("1er Trimestre"))
            .when(F.quarter("fecha") == 2, F.lit("2do Trimestre"))
            .when(F.quarter("fecha") == 3, F.lit("3er Trimestre"))
            .otherwise(F.lit("4to Trimestre")))
        cot_trim = (mvd.withColumn("ejercicio", F.year("fecha"))
                     .withColumn("trimestre", trimestre)
                     .join(puente, "nemonico", "inner"))
        bursatil = cot_trim.groupBy(*key_fin).agg(
            F.countDistinct("nemonico").alias("nemonicos_cotizados"),
            F.countDistinct("fecha").alias("dias_con_cotizacion"),
            F.sum("monto_negociado").alias("monto_negociado_trimestral"),
            # CORRECCION: desviacion estandar de retorno, no de precio de cierre.
            F.stddev("retorno_diario_pct").alias("volatilidad_retorno_pct"))
        empresa_trim = cuentas_1.join(bursatil, key_fin, "left")

        eventos = spark.read.parquet(f"{base}/hechos_importancia/eventos")
        identidad = spark.read.parquet(f"{base}/hechos_importancia/identidad_empresarial")
        docs = spark.read.parquet(f"{base}/hechos_importancia/documentos")
        docs_sum = docs.groupBy("numero_expediente").agg(
            F.count(F.col(docs.columns[0])).alias("total_documentos"))
        id_cols = [c for c in identidad.columns if c not in eventos.columns or c == "numero_expediente"]
        hechos = eventos.join(identidad.select(*id_cols), "numero_expediente", "left").join(
            docs_sum, "numero_expediente", "left")
        date_field = next((n for n in ("fecha_presentacion_date", "fecha_presentacion", "fecha")
                           if n in hechos.columns), None)
        if not date_field:
            raise RuntimeError("Hechos no contiene fecha de evento")
        hechos_prep = (hechos.filter(F.col("ruc").isNotNull())
            .select("numero_expediente", "ruc", F.to_date(date_field).alias("fecha_evento"))
            .join(puente, "ruc", "inner"))
        cot_prep = df_cotiz.select("nemonico", F.to_date("fecha").alias("fecha_cotiz"),
            F.col("precio_cierre").alias("cierre_evento"),
            F.col("monto_negociado").alias("monto_negociado_evento"))
        impacto = (hechos_prep.alias("h").join(cot_prep.alias("c"),
            (F.col("h.nemonico") == F.col("c.nemonico")) &
            (F.col("h.fecha_evento") == F.col("c.fecha_cotiz")), "inner")
            .select(F.col("h.numero_expediente").alias("numero_expediente"),
                    F.col("h.nemonico").alias("nemonico"),
                    "fecha_evento", "cierre_evento", "monto_negociado_evento"))

        products = {"mercado_valor_diario": mvd, "actividad_sab_diaria": sab_gold,
                    "empresa_trimestre": empresa_trim, "hechos_empresa": hechos,
                    "impacto_hechos_valor": impacto}
        for name, df in products.items():
            rel, keys = GOLD_PRODUCTS[name]
            path = f"{stage}/{rel}"
            (df.write.mode("errorifexists").option("compression", "snappy").parquet(path))
            saved = spark.read.parquet(path)
            n = saved.count()
            u = saved.select(*keys).distinct().count()
            audit["productos"][name] = {"ruta": path, "registros": n,
                                         "claves_unicas": u, "duplicados_por_clave": n-u,
                                         "columnas": saved.columns, "clave": keys}
            print(f"GOLD_STAGE|{name}|{n}|duplicados={n-u}|{path}")
            if n-u:
                audit["alertas"].append(f"Gold {name}: {n-u} duplicados por clave declarada")
        if audit["alertas"]:
            audit["estado"] = "REVISAR_ANTES_DE_PUBLICAR"
        else:
            audit["estado"] = "STAGING_VALIDADO_NO_PUBLICADO"
        out = Path(a.report_root) / f"gold_stage_{run_id}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(audit, indent=2, ensure_ascii=False), encoding="utf8")
        print(f"GOLD_MANIFEST|{out}")
        print(f"GOLD_STAGE_RAIZ|{stage}")
        print(f"GOLD_ESTADO|{audit['estado']}")
        for alert in audit["alertas"]:
            print(f"GOLD_ALERTA|{alert}")
        print("GOLD_OFICIAL|SIN_MODIFICACIONES")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
