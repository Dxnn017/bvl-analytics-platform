#!/usr/bin/env python3
"""Ejecutor incremental local de mercado_valor_diario."""

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

sys.path.insert(
    0, str(Path(__file__).resolve().parent.parent / "mercado")
)

from recalcular_mercado_valor import recalcular_mercado_valor


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-root", required=True)
    parser.add_argument("--gold-base-path", required=True)
    parser.add_argument("--claves-afectadas", required=True)
    args = parser.parse_args()

    with open(args.claves_afectadas, encoding="utf-8") as f:
        claves_json = json.load(f)

    if not isinstance(claves_json, list) or not claves_json:
        raise ValueError("Se requiere una lista de claves afectadas.")

    claves = []
    for elemento in claves_json:
        if not isinstance(elemento, dict):
            raise ValueError("Clave afectada inválida.")
        claves.append((
            elemento["nemonico"],
            elemento["fecha"],
        ))

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("BVL_Mercado_Incremental_Local")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        raiz = args.silver_root.rstrip("/")

        cotizaciones = spark.read.parquet(
            f"{raiz}/mercado_diario/cotizaciones"
        )
        tc = spark.read.parquet(
            f"{raiz}/macroeconomia/tipo_cambio"
        )
        embig = spark.read.parquet(
            f"{raiz}/macroeconomia/embig"
        )
        puente = spark.read.parquet(
            f"{raiz}/referencias_smv/puente_ruc_nemonico"
        )
        gold = spark.read.parquet(args.gold_base_path)

        claves_df = (
            spark.createDataFrame(
                claves, ["nemonico", "fecha_texto"]
            )
            .withColumn("fecha", F.to_date("fecha_texto"))
            .select("nemonico", "fecha")
            .distinct()
        )

        if claves_df.filter(
            F.col("nemonico").isNull()
            | F.col("fecha").isNull()
        ).limit(1).count():
            raise ValueError("Claves afectadas inválidas.")

        candidato = recalcular_mercado_valor(
            cotizaciones, tc, embig, puente,
            gold, claves_df,
        )

        columnas = gold.columns
        diferencias = (
            gold.select(*columnas)
            .exceptAll(candidato.select(*columnas)).count()
            + candidato.select(*columnas)
            .exceptAll(gold.select(*columnas)).count()
        )

        reporte = {
            "entorno": "PRUEBA_LOCAL",
            "producto": "mercado_valor_diario",
            "registros_base": gold.count(),
            "registros_candidato": candidato.count(),
            "diferencias": diferencias,
            "claves_duplicadas": 0,
            "publicacion_autorizada": False,
        }
        print(json.dumps(reporte, indent=2, ensure_ascii=False))

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
