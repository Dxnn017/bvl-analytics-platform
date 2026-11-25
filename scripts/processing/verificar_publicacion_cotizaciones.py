#!/usr/bin/env python3
"""Audita que un lote real se haya incorporado sin perder o alterar registros históricos.
Solo lectura; compara el destino final con el histórico y lote original.
Ejemplo: spark-submit ... --current /.../historico_2021_2024 --historical ... --incoming ...
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

KEYS = ["fecha_cotizacion", "valor"]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--current", required=True, help="Dataset despues de la publicacion")
    ap.add_argument("--historical", required=True, help="Copia INMUTABLE del historico original")
    ap.add_argument("--incoming", required=True, help="Lote nuevo, normalizado")
    ap.add_argument("--output", default="outputs/quality/incremental/auditoria_publicacion.json")
    args = ap.parse_args()
    assert args.current != args.historical, "Se requiere copia separada del historico anterior"
    spark = SparkSession.builder.appName("BVL-Auditoria-Post-Publicacion").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        current = spark.read.parquet(args.current)
        historical = spark.read.parquet(args.historical)
        incoming = spark.read.parquet(args.incoming)
        cols = sorted(current.columns)
        for label, frame in (("historico", historical), ("nuevo", incoming)):
            if sorted(frame.columns) != cols:
                raise RuntimeError(f"Esquema distinto para {label}")
        current = current.select(*cols)
        historical = historical.select(*cols)
        incoming = incoming.select(*cols)
        report = {
            "fecha_utc": datetime.now(timezone.utc).isoformat(),
            "ruta_evaluada": args.current,
            "historico_original": historical.count(),
            "lote_nuevo": incoming.count(),
            "final": current.count(),
            "claves_unicas": current.select(*KEYS).distinct().count(),
            "claves_nulas": current.filter(F.col("fecha_cotizacion").isNull() |
                        F.col("valor").isNull() | (F.trim("valor") == "")).count(),
            "filas_historicas_ausentes_o_cambiadas": historical.exceptAll(current).count(),
            "filas_nuevas_ausentes_o_cambiadas": incoming.exceptAll(current).count(),
            "filas_inesperadas": current.exceptAll(historical.unionByName(incoming)).count(),
            "fecha_minima": str(current.agg(F.min("fecha_cotizacion")).first()[0]),
            "fecha_maxima": str(current.agg(F.max("fecha_cotizacion")).first()[0]),
        }
        report["cumple"] = (report["final"] == report["historico_original"] + report["lote_nuevo"]
            and report["claves_unicas"] == report["final"] and report["claves_nulas"] == 0
            and report["filas_historicas_ausentes_o_cambiadas"] == 0
            and report["filas_nuevas_ausentes_o_cambiadas"] == 0
            and report["filas_inesperadas"] == 0)
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf8")
        print("AUDITORIA_POST_PUBLICACION_BVL")
        for key, val in report.items():
            print(f"{key}: {val}")
        if not report["cumple"]:
            raise RuntimeError("Validacion post-publicacion NO aprobada")
        print("VALIDACION_PUBLICACION|APROBADA")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
