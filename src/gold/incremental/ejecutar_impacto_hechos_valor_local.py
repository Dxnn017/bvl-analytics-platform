#!/usr/bin/env python3
"""Ejecutor incremental local de impacto_hechos_valor."""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from recalcular_impacto_hechos_valor import (
    recalcular_impacto_hechos_valor,
    resolver_expedientes_por_cotizaciones,
)

from escribir_candidata_sab import escribir_candidata_gold


BASE = Path.home() / "bvl_entregas" / "gold_staging_20261114"


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--expediente",
        action="append",
        default=[],
    )

    parser.add_argument(
        "--cotizacion-afectada",
        action="append",
        nargs=2,
        metavar=("NEMONICO", "FECHA"),
        default=[],
    )

    parser.add_argument(
        "--guardar-candidata",
        action="store_true",
    )

    parser.add_argument("--hechos-path")
    parser.add_argument("--mercado-path")
    parser.add_argument("--impacto-base-path")

    args = parser.parse_args()

    if not args.expediente and not args.cotizacion_afectada:
        parser.error(
            "Debe indicar un expediente o una cotización afectada."
        )

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("BVL_Incremental_Impacto_Local")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        hechos = spark.read.parquet(
            args.hechos_path or str(BASE / "eventos/hechos_empresa")
        )
        mercado = spark.read.parquet(
            args.mercado_path or str(BASE / "mercado/mercado_valor_diario")
        )
        impacto = spark.read.parquet(
            args.impacto_base_path or str(BASE / "eventos/impacto_hechos_valor")
        )

        conjuntos = []

        if args.expediente:
            valores = sorted(set(args.expediente))
            conjuntos.append(
                spark.createDataFrame(
                    [(valor,) for valor in valores],
                    ["numero_expediente"],
                )
            )

        if args.cotizacion_afectada:
            cotizaciones = spark.createDataFrame(
                args.cotizacion_afectada,
                ["nemonico", "fecha_texto"],
            ).withColumn(
                "fecha",
                F.to_date("fecha_texto"),
            )

            if cotizaciones.filter(
                F.col("fecha").isNull()
            ).limit(1).count():
                raise ValueError("Fecha de cotización inválida.")

            conjuntos.append(
                resolver_expedientes_por_cotizaciones(
                    hechos,
                    cotizaciones,
                )
            )

        expedientes = conjuntos[0]

        for conjunto in conjuntos[1:]:
            expedientes = expedientes.unionByName(conjunto)

        expedientes = expedientes.distinct()

        candidato = recalcular_impacto_hechos_valor(
            hechos,
            mercado,
            impacto,
            expedientes,
        )

        candidato = candidato.select(*impacto.columns)

        diferencias = (
            impacto.exceptAll(candidato).count()
            + candidato.exceptAll(impacto).count()
        )

        duplicados = (
            candidato.groupBy(
                "numero_expediente", "nemonico"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        resultado = {
            "entorno": "PRUEBA_LOCAL",
            "producto": "impacto_hechos_valor",
            "expedientes_afectados": expedientes.count(),
            "registros_base": impacto.count(),
            "registros_candidato": candidato.count(),
            "diferencias": diferencias,
            "claves_duplicadas": duplicados,
            "publicacion_autorizada": False,
        }

        if args.guardar_candidata:
            destino = escribir_candidata_gold(
                candidato, "impacto_hechos_valor"
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
