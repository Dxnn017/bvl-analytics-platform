#!/usr/bin/env python3
"""Ejecutor incremental financiero de prueba local, sin publicación."""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from escribir_candidata_sab import escribir_candidata_gold
from recalcular_empresa_trimestre import (
    recalcular_empresa_trimestre,
    resolver_grupos_empresa_desde_tarea,
)

BASE = Path.home() / "bvl_entregas"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tarea-planificada", required=True)
    parser.add_argument("--silver-path")
    parser.add_argument("--gold-base-path")
    parser.add_argument("--guardar-candidata", action="store_true")
    args = parser.parse_args()

    with open(args.tarea_planificada, encoding="utf-8") as archivo:
        tarea = json.load(archivo)

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("BVL_Incremental_Empresa_Local")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        silver_path = args.silver_path or str(
            BASE / "silver_20261109/silver/"
            "finanzas_empresariales/principales_cuentas"
        )
        gold_path = args.gold_base_path or str(
            BASE / "gold_staging_20261114/"
            "empresas/empresa_trimestre"
        )

        silver = spark.read.parquet(silver_path)
        gold = spark.read.parquet(gold_path)

        grupos = resolver_grupos_empresa_desde_tarea(spark, tarea)
        candidato = recalcular_empresa_trimestre(silver, gold, grupos)

        diferencias = (
            gold.exceptAll(candidato).count()
            + candidato.exceptAll(gold).count()
        )

        resultado = {
            "entorno": "PRUEBA_LOCAL",
            "producto": "empresa_trimestre",
            "grupos_afectados": grupos.count(),
            "registros_base": gold.count(),
            "registros_candidato": candidato.count(),
            "diferencias": diferencias,
            "publicacion_autorizada": False,
        }
        if args.guardar_candidata:
            destino = escribir_candidata_gold(
                candidato, "empresa_trimestre"
            )
            resultado["ruta_candidata_local"] = destino
            resultado["candidata_escrita"] = True

        print(json.dumps(resultado, indent=2, ensure_ascii=False))

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
