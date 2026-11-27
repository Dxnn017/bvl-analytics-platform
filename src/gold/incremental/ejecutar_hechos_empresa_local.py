#!/usr/bin/env python3
"""Ejecutor incremental local de hechos_empresa.

No escribe Parquet ni publica versiones oficiales.
"""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from recalcular_hechos_empresa import recalcular_hechos_empresa
from escribir_candidata_sab import escribir_candidata_gold


BASE = Path.home() / "bvl_entregas"

SILVER = (
    BASE
    / "silver_20261109"
    / "silver"
    / "hechos_importancia"
)

GOLD = (
    BASE
    / "gold_staging_20261114"
    / "eventos"
    / "hechos_empresa"
)


def main():
    parser = argparse.ArgumentParser(
        description="Recálculo incremental local de hechos_empresa"
    )

    parser.add_argument(
        "--expediente",
        action="append",
        required=True,
        help="Número de expediente afectado. Puede repetirse.",
    )

    parser.add_argument(
        "--guardar-candidata", action="store_true"
    )

    parser.add_argument("--silver-root")
    parser.add_argument("--gold-base-path")

    args = parser.parse_args()

    expedientes = list(dict.fromkeys(
        str(valor).strip()
        for valor in args.expediente
    ))

    if not expedientes or any(not valor for valor in expedientes):
        parser.error("Los expedientes no pueden estar vacíos.")

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("BVL_Incremental_Hechos_Empresa")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        silver_root = Path(args.silver_root) if args.silver_root else SILVER
        gold_base_path = args.gold_base_path or str(GOLD)

        eventos = spark.read.parquet(str(silver_root / "eventos"))

        identidad = spark.read.parquet(
            str(silver_root / "identidad_empresarial")
        )

        documentos = spark.read.parquet(
            str(silver_root / "documentos")
        )

        textos = spark.read.parquet(
            str(silver_root / "contenido_textual")
        )

        gold = spark.read.parquet(gold_base_path)

        afectados = spark.createDataFrame(
            [(numero,) for numero in expedientes],
            ["numero_expediente"],
        )

        candidato = recalcular_hechos_empresa(
            eventos,
            identidad,
            documentos,
            textos,
            gold,
            afectados,
        )

        diferencias = (
            gold.exceptAll(candidato).count()
            + candidato.exceptAll(gold).count()
        )

        duplicados = (
            candidato
            .groupBy("numero_expediente")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        resultado = {
            "entorno": "PRUEBA_LOCAL",
            "producto": "hechos_empresa",
            "expedientes_afectados": len(expedientes),
            "registros_base": gold.count(),
            "registros_candidato": candidato.count(),
            "diferencias": diferencias,
            "expedientes_duplicados": duplicados,
            "publicacion_autorizada": False,
        }

        if args.guardar_candidata:
            destino = escribir_candidata_gold(
                candidato, "hechos_empresa"
            )
            resultado["ruta_candidata_local"] = destino
            resultado["candidata_escrita"] = True

        print(json.dumps(
            resultado,
            indent=2,
            ensure_ascii=False,
        ))

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
