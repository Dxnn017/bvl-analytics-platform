#!/usr/bin/env python3

import argparse
import fcntl
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE = Path("outputs/quality/incremental")
COLA = BASE / "cola_procesamiento.json"
HISTORIAL = BASE / "historial_ejecuciones.json"
LOCK = BASE / "historial_ejecuciones.lock"
LOGS = BASE / "logs"

parser = argparse.ArgumentParser()
parser.add_argument("--indice", type=int, required=True)
parser.add_argument("--ejecutar", action="store_true")
args = parser.parse_args()

def ahora():
    return datetime.now(timezone.utc).isoformat()

def guardar(datos):
    temporal = HISTORIAL.with_suffix(".tmp")
    temporal.write_text(
        json.dumps(datos, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )
    os.replace(temporal, HISTORIAL)

cola = json.loads(COLA.read_text(encoding="utf-8"))
trabajos = cola["trabajos"]

if not 1 <= args.indice <= len(trabajos):
    parser.error("Indice fuera de rango")

trabajo = trabajos[args.indice - 1]
clave = trabajo["clave"]
firma = trabajo["firma"]

BASE.mkdir(parents=True, exist_ok=True)

with LOCK.open("a+") as bloqueo:
    fcntl.flock(bloqueo, fcntl.LOCK_EX)

    historial = (
        json.loads(HISTORIAL.read_text(encoding="utf-8"))
        if HISTORIAL.exists()
        else {}
    )

    previo = historial.get(clave)

    if (
        previo
        and previo.get("firma") == firma
        and previo.get("estado") in (
            "VALIDADO_SIN_CAMBIOS",
            "PUBLICADO"
        )
    ):
        print(f"LOTE_OMITIDO|{clave}|YA_VALIDADO")
        sys.exit(0)

    print(f"LOTE_PENDIENTE|{clave}")

    if not args.ejecutar:
        print("MODO|SIMULACION")
        sys.exit(0)

    LOGS.mkdir(parents=True, exist_ok=True)

    nombre = (
        f"control_{args.indice}_"
        f"{firma[:12]}.log"
    )
    log = LOGS / nombre

    historial[clave] = {
        "firma": firma,
        "estado": "EN_EJECUCION",
        "inicio": ahora(),
        "log": str(log)
    }
    guardar(historial)

    comando = [
        sys.executable,
        "scripts/pipeline/ejecutar_cola_segura.py",
        "--indice", str(args.indice),
        "--ejecutar"
    ]

    resultado = subprocess.run(
        comando,
        capture_output=True,
        text=True
    )

    salida = resultado.stdout + "\n" + resultado.stderr
    log.write_text(salida, encoding="utf-8")

    if resultado.returncode != 0:
        estado = "ERROR"
    elif "RESULTADO|SIN_CAMBIOS" in salida:
        estado = "VALIDADO_SIN_CAMBIOS"
    elif "RESULTADO|STAGING_PENDIENTE_PUBLICACION" in salida:
        estado = "PENDIENTE_PUBLICACION"
    else:
        estado = "ERROR"

    historial[clave] = {
        "firma": firma,
        "estado": estado,
        "fin": ahora(),
        "codigo": resultado.returncode,
        "log": str(log)
    }
    guardar(historial)

    print(f"LOTE_RESULTADO|{clave}|{estado}")
    print(f"LOG|{log}")
    print("REGISTRO|ACTUALIZADO")
    print("PUBLICACION|NO_EJECUTADA")

    if estado == "ERROR":
        sys.exit(2)
