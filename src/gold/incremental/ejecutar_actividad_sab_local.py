#!/usr/bin/env python3
"""Ejecutor incremental SAB para pruebas locales de solo lectura."""

import argparse
import json
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

import sys

DIRECTORIO_MERCADO = Path(__file__).resolve().parent.parent / "mercado"
sys.path.insert(0, str(DIRECTORIO_MERCADO))

from storage_config import SILVER_ORIGINAL, PRODUCTOS_GOLD_LOCALES
from recalcular_actividad_sab import recalcular_actividad_sab, resolver_fechas_sab, resolver_fechas_sab_desde_tarea
from escribir_candidata_sab import escribir_candidata_sab


GOLD_BASE = Path.home() / "bvl_entregas" / "gold_optimizado_20261114"


def main():
    parser = argparse.ArgumentParser()
    grupo = parser.add_mutually_exclusive_group(required=True)
    grupo.add_argument("--fecha")
    grupo.add_argument("--detalle-cambios")
    grupo.add_argument("--tarea-planificada")
    parser.add_argument("--guardar-candidata", action="store_true")
    parser.add_argument("--silver-path")
    parser.add_argument("--gold-base-path")
    parser.add_argument("--cobertura-json")
    parser.add_argument("--version-silver")
    args = parser.parse_args()

    cobertura = None
    if args.cobertura_json:
        with open(args.cobertura_json, encoding="utf-8") as archivo:
            cobertura = json.load(archivo)["registros"]

    fecha = date.fromisoformat(args.fecha) if args.fecha else None

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("BVL_Incremental_SAB_Local")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        silver = spark.read.parquet(
            args.silver_path or str(SILVER_ORIGINAL / "mercado_diario/montos_sab")
        )
        gold = spark.read.parquet(
            args.gold_base_path or str(GOLD_BASE / PRODUCTOS_GOLD_LOCALES["actividad_sab_diaria"])
        )

        if args.tarea_planificada:
            with open(args.tarea_planificada, encoding="utf-8") as archivo:
                tarea = json.load(archivo)
            fechas = resolver_fechas_sab_desde_tarea(spark, tarea)
        elif args.detalle_cambios:
            with open(args.detalle_cambios, encoding="utf-8") as archivo:
                dataset = json.load(archivo)
            fechas = resolver_fechas_sab(spark, dataset)
        else:
            fechas = spark.createDataFrame([(fecha,)], ["fecha"])

        candidato = recalcular_actividad_sab(
            silver, gold, fechas, cobertura, args.version_silver
        )

        columnas = gold.columns
        diferencias = (
            gold.select(*columnas)
            .exceptAll(candidato.select(*columnas)).count()
            + candidato.select(*columnas)
            .exceptAll(gold.select(*columnas)).count()
        )

        resultado = {
            "entorno": "PRUEBA_LOCAL",
            "producto": "actividad_sab_diaria",
            "fecha": fecha.isoformat() if fecha else None,
            "origen_fechas": (
                "TAREA_PLANIFICADA" if args.tarea_planificada
                else "DETALLE_SILVER" if args.detalle_cambios
                else "MANUAL"
            ),
            "registros_base": gold.count(),
            "registros_candidato": candidato.count(),
            "diferencias": diferencias,
            "equivalencia_historica": diferencias == 0,
            "publicacion_autorizada": False,
        }

        if args.guardar_candidata:
            destino = escribir_candidata_sab(candidato)
            resultado["ruta_candidata_local"] = destino
            resultado["candidata_escrita"] = True

        print(json.dumps(resultado, indent=2, ensure_ascii=False))

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
