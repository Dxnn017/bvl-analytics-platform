from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)

spark = (
    SparkSession.builder
    .appName("Modelo_Base_Volatilidad_BVL")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

COT = "/datalake/bvl/silver/mercado_diario/cotizaciones"
TC = "/datalake/bvl/silver/macroeconomia/tipo_cambio"
EMBIG = "/datalake/bvl/silver/macroeconomia/embig"

print()
print("=" * 90)
print("4.2 - MODELO BASE")
print("REGRESION LOGISTICA - ALTA VOLATILIDAD SIGUIENTE JORNADA")
print("=" * 90)

# ============================================================
# 1. CARGA
# ============================================================

cot = spark.read.parquet(COT)

tc = (
    spark.read.parquet(TC)
    .filter(
        F.col("fecha").isNotNull()
        & F.col("tipo_cambio_venta").isNotNull()
    )
    .select(
        F.col("fecha").alias("fecha_tc"),
        "tipo_cambio_venta"
    )
)

embig = (
    spark.read.parquet(EMBIG)
    .filter(
        F.col("fecha").isNotNull()
        & F.col("embig_peru").isNotNull()
    )
    .select(
        F.col("fecha").alias("fecha_embig"),
        "embig_peru"
    )
)

print(f"Cotizaciones cargadas : {cot.count():,}")

# ============================================================
# 2. INGENIERIA DE CARACTERISTICAS
# ============================================================

df = (
    cot
    .filter(
        F.col("fecha_cotizacion").isNotNull()
        & F.col("valor").isNotNull()
        & F.col("cierre_actual").isNotNull()
        & F.col("cierre_anterior").isNotNull()
        & (F.col("cierre_anterior") != 0)
    )
    .withColumn(
        "retorno_actual_pct",
        (
            (F.col("cierre_actual") - F.col("cierre_anterior"))
            / F.col("cierre_anterior")
        ) * 100.0
    )
    .withColumn(
        "rango_intradia_pct",
        F.when(
            F.col("promedio_actual").isNotNull()
            & (F.col("promedio_actual") != 0),
            (
                (F.col("maxima_actual") - F.col("minima_actual"))
                / F.col("promedio_actual")
            ) * 100.0
        )
    )
    .withColumn(
        "log_monto_negociado",
        F.log1p(
            F.greatest(
                F.coalesce(F.col("monto_negociado"), F.lit(0.0)),
                F.lit(0.0)
            )
        )
    )
)

# Join macroeconómico.
df = (
    df
    .join(
        tc,
        df["fecha_cotizacion"] == tc["fecha_tc"],
        "left"
    )
    .drop("fecha_tc")
    .join(
        embig,
        df["fecha_cotizacion"] == embig["fecha_embig"],
        "left"
    )
    .drop("fecha_embig")
)

# ============================================================
# 3. TARGET FUTURO
# ============================================================

w = (
    Window
    .partitionBy("valor")
    .orderBy("fecha_cotizacion")
)

df = (
    df
    .withColumn(
        "retorno_siguiente_pct",
        F.lead("retorno_actual_pct").over(w)
    )
    .withColumn(
        "abs_retorno_siguiente",
        F.abs(F.col("retorno_siguiente_pct"))
    )
)

features = [
    "retorno_actual_pct",
    "rango_intradia_pct",
    "log_monto_negociado",
    "tipo_cambio_venta",
    "embig_peru"
]

model_df = (
    df
    .select(
        "fecha_cotizacion",
        "valor",
        "anio",
        *features,
        "retorno_siguiente_pct",
        "abs_retorno_siguiente"
    )
    .dropna(
        subset=features + ["abs_retorno_siguiente"]
    )
)

# ============================================================
# 4. DIVISION TEMPORAL
# Entrenamiento: 2021-2024
# Prueba: 2025
# ============================================================

train_base = model_df.filter(
    F.col("anio") <= 2024
)

test_base = model_df.filter(
    F.col("anio") == 2025
)

n_train = train_base.count()
n_test = test_base.count()

print()
print("DIVISION TEMPORAL")
print("-" * 90)
print(f"Entrenamiento 2021-2024 : {n_train:,}")
print(f"Prueba 2025             : {n_test:,}")

if n_train == 0 or n_test == 0:
    raise RuntimeError(
        "No existen suficientes datos para la division temporal."
    )

# ============================================================
# 5. UMBRAL DEL TARGET
# Solo se calcula con TRAIN para evitar leakage.
# ============================================================

umbral = (
    train_base
    .approxQuantile(
        "abs_retorno_siguiente",
        [0.75],
        0.001
    )[0]
)

train = train_base.withColumn(
    "label",
    F.when(
        F.col("abs_retorno_siguiente") >= F.lit(umbral),
        F.lit(1.0)
    ).otherwise(F.lit(0.0))
)

test = test_base.withColumn(
    "label",
    F.when(
        F.col("abs_retorno_siguiente") >= F.lit(umbral),
        F.lit(1.0)
    ).otherwise(F.lit(0.0))
)

print()
print("VARIABLE OBJETIVO")
print("-" * 90)
print(
    f"Umbral P75 abs(retorno siguiente) : {umbral:.4f}%"
)

print()
print("Distribucion TRAIN:")
train.groupBy("label").count().orderBy("label").show()

print("Distribucion TEST:")
test.groupBy("label").count().orderBy("label").show()

# ============================================================
# 6. VECTOR DE VARIABLES
# ============================================================

assembler = VectorAssembler(
    inputCols=features,
    outputCol="features"
)

train_v = assembler.transform(train)
test_v = assembler.transform(test)

# ============================================================
# 7. REGRESION LOGISTICA BASE
# ============================================================

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=50,
    regParam=0.0,
    elasticNetParam=0.0
)

model = lr.fit(train_v)

pred = model.transform(test_v)

# ============================================================
# 8. METRICAS
# ============================================================

auc = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
).evaluate(pred)

f1 = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
).evaluate(pred)

accuracy = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
).evaluate(pred)

precision = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedPrecision"
).evaluate(pred)

recall = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedRecall"
).evaluate(pred)

# ============================================================
# 9. MATRIZ DE CONFUSION
# ============================================================

conf = (
    pred
    .groupBy("label", "prediction")
    .count()
    .orderBy("label", "prediction")
)

print()
print("=" * 90)
print("RESULTADOS DEL MODELO BASE")
print("=" * 90)

print(f"Modelo                 : Regresion Logistica")
print(f"Variables predictoras  : {', '.join(features)}")
print(f"Train                  : 2021-2024")
print(f"Test                   : 2025")
print(f"Umbral alta volatilidad: {umbral:.4f}%")

print()
print(f"ROC-AUC                : {auc:.4f}")
print(f"F1                     : {f1:.4f}")
print(f"Accuracy               : {accuracy:.4f}")
print(f"Precision ponderada    : {precision:.4f}")
print(f"Recall ponderado       : {recall:.4f}")

print()
print("MATRIZ DE CONFUSION")
print("-" * 90)

conf.show(truncate=False)

print()
print("=" * 90)
print(f"LINEA_BASE_TECNICA|ROC_AUC|{auc:.4f}|F1_PONDERADO|{f1:.4f}")
print("=" * 90)

spark.stop()
