#!/usr/bin/env python3
"""Simulacion persistente Bronze. Sin transferencias reales."""

import argparse
import hashlib
import json
import re
import sqlite3
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("--run-id", required=True)
p.add_argument("--fallar-primero", action="store_true")
args = p.parse_args()

run = args.run_id

if not re.fullmatch(
    r"bvl-bronze-inicial-\d{8}-\d{3}", run
):
    p.error("RUN_ID_INVALIDO")

base = Path("outputs/quality/bronze_runid_lab")
plan_path = base / f"PLAN_BRONZE_12_{run}.json"

if not plan_path.is_file():
    raise SystemExit("ERROR: PLAN_NO_ENCONTRADO")

contenido = plan_path.read_bytes()
huella = hashlib.sha256(contenido).hexdigest()
plan = json.loads(contenido)

operaciones = plan["operaciones"]

if (
    plan["run_id"] != run
    or plan["datasets"] != 12
    or plan["archivos"] != 3599
    or plan["bytes"] != 393623640
    or plan["transferencias"] != 0
    or plan["publicacion"] != "NO_PUBLICADO"
    or len(operaciones) != 3599
):
    raise SystemExit("ERROR: CONTRATO_INVALIDO")

datasets = set()
destinos = set()
total_bytes = 0

for op in operaciones:
    dataset = op["dataset"]
    origen = op["origen"]
    destino = op["destino"]

    if op["run_id"] != run:
        raise SystemExit("ERROR: RUN_ID_INCONSISTENTE")

    if not origen.startswith(
        f"/datalake/bvl/bronze/{dataset}/"
    ):
        raise SystemExit("ERROR: ORIGEN_INVALIDO")

    if not (
        destino.startswith("gs://")
        and f"/landing/{dataset}/run_id={run}/"
        in destino
    ):
        raise SystemExit("ERROR: DESTINO_INVALIDO")

    if (
        not re.fullmatch(r"[0-9a-f]{64}", op["sha256"])
        or op["size_bytes"] <= 0
        or op["estado"] != "PLANIFICADO_NO_EJECUTADO"
        or destino in destinos
    ):
        raise SystemExit("ERROR: OPERACION_INVALIDA")

    datasets.add(dataset)
    destinos.add(destino)
    total_bytes += op["size_bytes"]

if len(datasets) != 12 or total_bytes != plan["bytes"]:
    raise SystemExit("ERROR: TOTALES_INCONSISTENTES")

db_path = base / f"ESTADO_SIMULADO_{run}.sqlite3"
conexion = sqlite3.connect(db_path)

conexion.execute("""
CREATE TABLE IF NOT EXISTS lotes (
    run_id TEXT PRIMARY KEY,
    plan_sha256 TEXT NOT NULL
)
""")

conexion.execute("""
CREATE TABLE IF NOT EXISTS archivos (
    run_id TEXT NOT NULL,
    destino TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    estado TEXT NOT NULL,
    intentos INTEGER NOT NULL,
    PRIMARY KEY (run_id, destino)
)
""")

nuevos = 0
omitidos = 0
fallidos = 0
reintentos = 0

with conexion:
    previo = conexion.execute(
        "SELECT plan_sha256 FROM lotes WHERE run_id=?",
        (run,)
    ).fetchone()

    if previo and previo[0] != huella:
        raise SystemExit("CONFLICTO: MISMO_RUN_ID_PLAN_DIFERENTE")

    if not previo:
        conexion.execute(
            "INSERT INTO lotes VALUES (?, ?)",
            (run, huella)
        )

    for indice, op in enumerate(operaciones):
        destino = op["destino"]

        anterior = conexion.execute(
            """SELECT sha256, estado, intentos
               FROM archivos
               WHERE run_id=? AND destino=?""",
            (run, destino)
        ).fetchone()

        if anterior and anterior[0] != op["sha256"]:
            raise SystemExit("CONFLICTO: ARCHIVO_DIFERENTE")

        if anterior and anterior[1] == "SIMULADO_OK":
            omitidos += 1
            continue

        intentos = anterior[2] + 1 if anterior else 1

        if anterior:
            reintentos += 1
        else:
            nuevos += 1

        if (
            args.fallar_primero
            and indice == 0
            and intentos == 1
        ):
            estado = "FALLO_SIMULADO"
            fallidos += 1
        else:
            estado = "SIMULADO_OK"

        conexion.execute("""
        INSERT INTO archivos
            (run_id, destino, sha256, estado, intentos)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(run_id, destino)
        DO UPDATE SET
            estado=excluded.estado,
            intentos=excluded.intentos
        """, (
            run, destino, op["sha256"], estado, intentos
        ))

total_ok = conexion.execute(
    """SELECT COUNT(*) FROM archivos
       WHERE run_id=? AND estado='SIMULADO_OK'""",
    (run,)
).fetchone()[0]

conexion.close()

print("===== SIMULACION BRONZE =====")
print("RUN_ID:", run)
print("DATASETS:", len(datasets))
print("ARCHIVOS:", len(operaciones))
print("NUEVOS:", nuevos)
print("OMITIDOS:", omitidos)
print("REINTENTOS:", reintentos)
print("FALLOS_SIMULADOS:", fallidos)
print("SIMULADOS_OK:", total_ok)
print("TRANSFERENCIAS_REALES: 0")
print("ESTADO_LOCAL:", db_path)
print("RESULTADO: SIMULACION_PERSISTENTE_OK")
