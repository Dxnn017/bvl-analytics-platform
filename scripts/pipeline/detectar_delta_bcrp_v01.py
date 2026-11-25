#!/usr/bin/env python3

import json
import re
from pathlib import Path
from functools import reduce
from operator import and_
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CARPETA = Path("outputs/quality/incremental")

registros = sorted(
    CARPETA.glob("registro_publicacion_*.json"),
    key=lambda p: p.stat().st_mtime,
    reverse=True
)

if not registros:
    raise SystemExit("ERROR|Sin registro de publicacion")

registro = json.loads(registros[0].read_text())
run_id = registro["run_id"]

if not re.fullmatch(
    r"\d{8}T\d{6}Z_[0-9a-f]{8}", run_id
):
    raise SystemExit("ERROR|Run ID invalido")

base = (
    "/datalake/bvl/staging/"
    f"prueba_publicacion_bcrp_persistente_{run_id}"
)

anterior = f"{base}/backups/mes=9"
version = f"{base}/versions/v_{run_id}"

if (
    registro.get("tipo") != "LABORATORIO_AISLADO"
    or registro.get("backup") != anterior
    or registro.get("publicacion_aprobada") is not False
    or registro.get("activar_gold") is not False
):
    raise SystemExit("ERROR|Registro no autorizado")

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("BVL-Delta-BCRP-v01")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

def fecha_texto(valor):
    return valor.isoformat() if hasattr(valor, "isoformat") else str(valor)

try:
    viejo = spark.read.parquet(anterior)
    nuevo = spark.read.parquet(version)

    if viejo.schema != nuevo.schema:
        raise RuntimeError("ESQUEMAS_DIFERENTES")

    if "fecha" not in viejo.columns:
        raise RuntimeError("CLAVE_FECHA_AUSENTE")

    for nombre, df in [("anterior", viejo), ("nuevo", nuevo)]:
        if df.filter(F.col("fecha").isNull()).limit(1).count():
            raise RuntimeError(f"CLAVE_NULA|{nombre}")

        if df.groupBy("fecha").count().filter(
            F.col("count") > 1
        ).limit(1).count():
            raise RuntimeError(f"CLAVES_DUPLICADAS|{nombre}")

    inserts = (
        nuevo.join(viejo.select("fecha"), "fecha", "left_anti")
        .select("fecha")
        .collect()
    )

    deletes = (
        viejo.join(nuevo.select("fecha"), "fecha", "left_anti")
        .select("fecha")
        .collect()
    )

    campos = [c for c in viejo.columns if c != "fecha"]

    if not campos:
        raise RuntimeError("SIN_CAMPOS_COMPARABLES")

    iguales = reduce(
        and_,
        [
            F.col(f"a.`{c}`").eqNullSafe(F.col(f"b.`{c}`"))
            for c in campos
        ]
    )

    updates = (
        viejo.alias("a")
        .join(
            nuevo.alias("b"),
            F.col("a.fecha") == F.col("b.fecha"),
            "inner"
        )
        .filter(~iguales)
        .select(F.col("a.fecha").alias("fecha"))
        .collect()
    )

    cambios = []

    for fila in inserts:
        cambios.append({
            "operacion": "INSERT",
            "clave_anterior": None,
            "clave_nueva": {"fecha": fecha_texto(fila["fecha"])}
        })

    for fila in updates:
        clave = {"fecha": fecha_texto(fila["fecha"])}
        cambios.append({
            "operacion": "UPDATE",
            "clave_anterior": clave,
            "clave_nueva": clave
        })

    for fila in deletes:
        cambios.append({
            "operacion": "DELETE",
            "clave_anterior": {"fecha": fecha_texto(fila["fecha"])},
            "clave_nueva": None
        })

    antes = viejo.count()
    despues = nuevo.count()

    if despues != antes + len(inserts) - len(deletes):
        raise RuntimeError("CONTEOS_INCONSISTENTES")

    resultado = {
        "tipo": "DELTA_BCRP_LABORATORIO",
        "run_id": run_id,
        "dataset": "tipo_cambio",
        "registros_antes_particion": antes,
        "registros_despues_particion": despues,
        "registros_nuevos": len(inserts),
        "registros_corregidos": len(updates),
        "registros_eliminados": len(deletes),
        "particiones_afectadas": [
            {"anio": 2026, "mes": 9}
        ],
        "claves_afectadas": cambios,
        "publicacion_aprobada": False,
        "activar_gold": False
    }

    salida = (
        CARPETA / f"delta_bcrp_v01_{run_id}.json"
    )

    salida.write_text(
        json.dumps(resultado, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    print(f"REGISTROS_ANTES_PARTICION|{antes}")
    print(f"REGISTROS_DESPUES_PARTICION|{despues}")
    print(f"INSERCIONES|{len(inserts)}")
    print(f"CORRECCIONES|{len(updates)}")
    print(f"ELIMINACIONES|{len(deletes)}")
    print("CLAVES_AFECTADAS|IDENTIFICADAS")
    print(f"ARCHIVO|{salida}")
    print("PUBLICACION_OFICIAL|NO_EJECUTADA")
    print("GOLD|NO_ACTIVADO")

finally:
    spark.stop()
