"""Pipeline común para snapshots locales y tablas Gold de BigQuery."""
import argparse
import csv
import json
from pathlib import Path
from uuid import uuid4

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, Window, functions as F

from scripts.ingestion.muestra import TABLES, RATIOS
from scripts.integration.registro import persist

DATASET = "bvl-analytics-2026.gold_analytics"
FEATURES = ["retorno_diario_pct", "rango_intradia_pct", "dias_desde_cierre_anterior",
            "frecuencia_cotizacion_20d", "volatilidad_20d_pct", "monto_negociado",
            "tipo_cambio_venta", "embig_peru", "hhi_mercado_fecha", *RATIOS]
HORIZON = 5


def inputs(spark, args):
    if args.env == "local":
        with open("config/diccionario_gold.csv", encoding="utf8") as handle:
            dictionary = {(r["producto"], r["columna"]): r["spark_type"] for r in csv.DictReader(handle)}
    for table in TABLES:
        if args.env == "gcp":
            yield spark.read.format("bigquery").option("table", f"{DATASET}.{table}").load()
        else:
            path = args.data / f"{table}.csv"
            with path.open(encoding="utf8") as handle:
                names = next(csv.reader(handle))
            schema = ", ".join(f"{name} {dictionary[table, name]}" for name in names)
            yield spark.read.schema(schema).option("header", True).option("mode", "FAILFAST").csv(str(path.resolve()))


def unique(df, keys):
    invalid = df.groupBy(*keys).count().filter("count > 1 OR " + " OR ".join(f"{k} IS NULL" for k in keys))
    if invalid.take(1):
        raise ValueError(f"Clave Gold inválida: {keys}")


def features(market, sab, firms):
    unique(market, ["nemonico", "fecha"])
    unique(sab, ["fecha", "codigo_sab"])
    unique(firms, ["id_registro"])
    day = F.datediff("fecha", F.lit("1970-01-01"))
    order = Window.partitionBy("nemonico").orderBy(day)
    past = order.rangeBetween(-19, 0)
    future = order.rangeBetween(1, HORIZON)
    daily = sab.groupBy("fecha").agg(F.when(
        (F.count("hhi_diario") == F.count("*")) & (F.countDistinct("hhi_diario") == 1)
        & (F.min(F.when(F.expr("cobertura_completa AND trazabilidad_verificada AND estado_cobertura = 'COMPLETA'"), 1).otherwise(0)) == 1),
        F.max("hhi_diario")).alias("hhi_mercado_fecha"))
    market = (market.withColumn("frecuencia_cotizacion_20d", F.count(F.when(
        (F.col("precio_cierre") > 0) & (F.col("dias_desde_cierre_anterior").isNull() |
        (F.col("dias_desde_cierre_anterior") > 0)), 1)).over(past)/20)
        .withColumn("volatilidad_20d_pct", F.stddev_samp("retorno_diario_pct").over(past))
        .withColumn("movimiento_futuro", F.max(F.abs("retorno_diario_pct")).over(future))
        .withColumn("ultima_fecha", F.max("fecha").over(Window.partitionBy("nemonico")))
        .withColumn("label", F.when((F.date_add("fecha", HORIZON) <= F.col("ultima_fecha"))
                    & F.col("movimiento_futuro").isNotNull(),
                    F.when(F.col("movimiento_futuro") >= 2.5, 2.0)
                     .when(F.col("movimiento_futuro") >= 1, 1.0).otherwise(0.0)))
        .join(daily, "fecha", "left"))
    firms = firms.filter("apto_kpi_general AND fecha_ingesta IS NOT NULL").select(
        F.col("ruc").alias("empresa_ruc"), "fecha_fin_trimestre", "fecha_ingesta", "id_registro", *RATIOS)
    joined = market.join(firms, (market.ruc == firms.empresa_ruc) &
                         (firms.fecha_fin_trimestre <= market.fecha) &
                         (firms.fecha_ingesta <= market.fecha), "left")
    by_key = Window.partitionBy("nemonico", "fecha")
    latest = joined.withColumn("period_rank", F.dense_rank().over(by_key.orderBy(
        F.col("fecha_fin_trimestre").desc()))).filter("period_rank = 1")
    latest = latest.withColumn("matches", F.count("id_registro").over(by_key))
    for ratio in RATIOS:
        latest = latest.withColumn(ratio, F.when(F.col("matches") == 1, F.col(ratio)))
    return latest.select("nemonico", "ruc", "fecha", "label", *FEATURES).dropDuplicates(["nemonico", "fecha"])


def metrics(counts):
    matrix = [[0]*3 for _ in range(3)]
    for row in counts:
        matrix[int(row.label)][int(row.prediction)] = row["count"]
    tp = matrix[2][2]
    actual, predicted = sum(matrix[2]), sum(r[2] for r in matrix)
    return dict(matriz=matrix, soporte_alto=actual, precision_alto=tp/predicted if predicted else 0,
                recall_alto=tp/actual if actual else 0, f1_alto=2*tp/(actual+predicted) if actual+predicted else 0)


def run(spark, args):
    data = features(*inputs(spark, args)).cache()
    train = data.filter((F.date_add("fecha", HORIZON) <= F.lit(args.split)) & F.col("label").isNotNull())
    classes = train.groupBy("label").count().orderBy(F.desc("count"), "label").collect()
    if len(classes) != 3:
        raise ValueError("El entrenamiento requiere ejemplos de las tres clases")
    majority = classes[0].label
    imputed = [f"{c}_imputado" for c in FEATURES]
    pipeline = Pipeline(stages=[Imputer(inputCols=FEATURES, outputCols=imputed, strategy="median"),
        VectorAssembler(inputCols=imputed, outputCol="features"),
        RandomForestClassifier(seed=args.seed, numTrees=24, maxDepth=5)])
    if args.load_model:
        from pyspark.ml import PipelineModel
        from pyspark.ml.util import JavaMLReader
        model = JavaMLReader(PipelineModel).load(args.model)
    else:
        model = pipeline.fit(train.orderBy("fecha", "nemonico").coalesce(1))
        model.write().save(args.model)
    scored = model.transform(data.filter(F.col("fecha") > F.lit(args.split))).cache()
    test = scored.filter("label IS NOT NULL")
    model_metrics = metrics(test.groupBy("label", "prediction").count().collect())
    if not sum(map(sum, model_metrics["matriz"])):
        raise ValueError("Evaluación temporal vacía")
    baseline = metrics(test.withColumn("prediction", F.lit(majority)).groupBy("label", "prediction").count().collect())
    report = dict(seed=args.seed, corte=args.split, clase_baseline=majority, modelo=model_metrics, baseline=baseline)
    print(json.dumps(report))
    if args.env == "local":
        path = Path("outputs/performance/evaluacion.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2), encoding="utf8")
    probs = vector_to_array("probability")
    run_id = uuid4().hex
    preds = scored.select("nemonico", "ruc", F.col("fecha").alias("fecha_corte"),
        F.lit(HORIZON).alias("horizonte_dias"), F.col("prediction").cast("int").alias("clase_predicha"),
        F.array(*map(F.lit, ["BAJO", "MEDIO", "ALTO"]))[F.col("prediction").cast("int")].alias("etiqueta_riesgo"),
        *[probs[i].alias(f"probabilidad_clase_{i}") for i in range(3)],
        ((probs[1]+2*probs[2])/2).alias("score_riesgo"), "frecuencia_cotizacion_20d",
        "volatilidad_20d_pct", "hhi_mercado_fecha", F.lit("RandomForest").alias("modelo_nombre"),
        F.lit(args.version).alias("modelo_version"), F.lit(run_id).alias("run_id"),
        *[F.lit(v).alias("version_gold_"+t) for t,v in zip(("mercado","sab","empresa"),args.gold)],
        F.sha2(F.to_json(F.struct("ruc", *FEATURES), {"ignoreNullFields":"false"}),256).alias("hash_features_entrada"),
        F.current_timestamp().alias("fecha_prediccion_utc"))
    persist(preds, args, run_id, report)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--env", choices=["local","gcp"], default="local")
    p.add_argument("--data", type=Path, default=Path("data/sample"))
    p.add_argument("--db", default="outputs/riesgo.sqlite")
    p.add_argument("--split", default="2024-04-30")
    p.add_argument("--model", required=True)
    p.add_argument("--version", required=True)
    p.add_argument("--gold", nargs=3, required=True)
    p.add_argument("--load-model", action="store_true")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    builder = SparkSession.builder.appName("BVL riesgo").config("spark.sql.session.timeZone","UTC")
    if args.env == "local":
        builder = builder.master("local[2]").config("spark.sql.shuffle.partitions","4")
    spark = builder.getOrCreate()
    try:
        run(spark,args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
