#!/usr/bin/env python3

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession, functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--candidate", required=True)
args = parser.parse_args()

OFICIAL = "/datalake/bvl/silver/hechos_importancia"

if not args.candidate.startswith("/datalake/bvl/staging/"):
    parser.error("El candidato debe estar en staging")

CONFIG = {
    "eventos": "numero_expediente",
    "documentos": "guid_documento",
    "contenido_textual": "guid_documento",
    "identidad_empresarial": "numero_expediente"
}

spark = (
    SparkSession.builder
    .appName("BVL-Hechos-Incremental-Seguro")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

datos = {}
reporte = {}
errores = []

def con_hash(df, clave):
    columnas = [c for c in df.columns if c != clave]

    return df.select(
        F.col(clave),
        F.sha2(
            F.to_json(
                F.struct(*[F.col(c) for c in columnas]),
                {"ignoreNullFields": "false"}
            ),
            256
        ).alias("_hash")
    )

# 1. AUDITORIA ANTES DE MODIFICAR NADA
for nombre, clave in CONFIG.items():
    actual = spark.read.parquet(f"{OFICIAL}/{nombre}")
    candidato = spark.read.parquet(
        f"{args.candidate}/{nombre}"
    )

    if set(actual.columns) != set(candidato.columns):
        errores.append(f"{nombre}: esquema diferente")
        continue

    if any(
        actual.schema[c].dataType != candidato.schema[c].dataType
        for c in actual.columns
    ):
        errores.append(f"{nombre}: tipos diferentes")
        continue

    candidato = candidato.select(*actual.columns)

    n_actual = actual.count()
    n_candidato = candidato.count()

    unicas_actual = actual.select(clave).distinct().count()
    unicas_candidato = candidato.select(clave).distinct().count()

    nulas = (
        actual.filter(F.col(clave).isNull()).count()
        + candidato.filter(F.col(clave).isNull()).count()
    )

    if (
        unicas_actual != n_actual
        or unicas_candidato != n_candidato
        or nulas != 0
    ):
        errores.append(f"{nombre}: claves invalidas")
        continue

    claves_actual = actual.select(clave)
    claves_candidato = candidato.select(clave)

    nuevas_df = candidato.join(
        claves_actual, clave, "left_anti"
    )

    nuevos = nuevas_df.count()

    faltantes = claves_actual.join(
        claves_candidato, clave, "left_anti"
    ).count()

    ah = con_hash(actual, clave).alias("a")
    ch = con_hash(candidato, clave).alias("n")

    modificados = (
        ch.join(ah, clave, "inner")
        .filter(F.col("n._hash") != F.col("a._hash"))
        .count()
    )

    print(
        f"HECHOS_CHECK|{nombre}|"
        f"actual={n_actual}|"
        f"candidato={n_candidato}|"
        f"nuevos={nuevos}|"
        f"faltantes={faltantes}|"
        f"modificados={modificados}",
        flush=True
    )

    if faltantes or modificados:
        errores.append(
            f"{nombre}: revisar faltantes/correcciones"
        )

    datos[nombre] = (actual, nuevas_df, clave)
    reporte[nombre] = {
        "actual": n_actual,
        "candidato": n_candidato,
        "nuevos": nuevos,
        "faltantes": faltantes,
        "modificados": modificados
    }

if errores:
    for error in errores:
        print(f"HECHOS_ALERTA|{error}", flush=True)

    print("PUBLICACION|BLOQUEADA", flush=True)
    print("SILVER_OFICIAL|SIN_CAMBIOS", flush=True)
    spark.stop()
    sys.exit(2)

if all(x["nuevos"] == 0 for x in reporte.values()):
    print("HECHOS_INCREMENTAL|SIN_CAMBIOS", flush=True)
    print("SILVER_OFICIAL|SIN_CAMBIOS", flush=True)
    spark.stop()
    sys.exit(0)

# 2. PREPARAR SNAPSHOT COMPLETO SIN PUBLICAR
run = (
    datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    + "_" + uuid4().hex[:8]
)

raiz = f"/datalake/bvl/staging/hechos_merge/run_{run}"

for nombre, (actual, nuevos_df, clave) in datos.items():
    combinado = actual.unionByName(nuevos_df)

    esperado = (
        reporte[nombre]["actual"]
        + reporte[nombre]["nuevos"]
    )

    destino = f"{raiz}/{nombre}"

    writer = (
        combinado.write
        .mode("errorifexists")
        .option("compression", "snappy")
    )

    if "coherencia_temporal" in combinado.columns:
        writer = writer.partitionBy("coherencia_temporal")

    writer.parquet(destino)

    verificado = spark.read.parquet(destino)

    if (
        verificado.count() != esperado
        or verificado.select(clave).distinct().count() != esperado
    ):
        raise RuntimeError(
            f"Staging invalido: {nombre}"
        )

    print(
        f"HECHOS_STAGE|{nombre}|{esperado}|{destino}",
        flush=True
    )

salida = Path("outputs/quality/incremental")
salida.mkdir(parents=True, exist_ok=True)

manifest = salida / f"hechos_merge_{run}.json"

manifest.write_text(
    json.dumps(
        {
            "estado": "STAGING_VALIDADO_NO_PUBLICADO",
            "candidato": args.candidate,
            "oficial": OFICIAL,
            "staging": raiz,
            "datasets": reporte
        },
        ensure_ascii=False,
        indent=2
    ),
    encoding="utf-8"
)

print(f"STAGING_VALIDADO|{raiz}", flush=True)
print(f"MANIFEST|{manifest}", flush=True)
print("SILVER_OFICIAL|SIN_CAMBIOS", flush=True)

spark.stop()
