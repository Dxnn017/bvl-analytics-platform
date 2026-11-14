#!/usr/bin/env python3

import argparse
import json
import subprocess
import sys
from pathlib import Path

BASE = Path("outputs/quality/incremental")
COLA = BASE / "cola_procesamiento.json"
HISTORIAL = BASE / "historial_ejecuciones.json"

p = argparse.ArgumentParser()
p.add_argument("--ejecutar", action="store_true")
p.add_argument("--limite", type=int, default=0)
args = p.parse_args()

if args.limite < 0:
    p.error("El limite debe ser mayor o igual a cero")

cola = json.loads(COLA.read_text(encoding="utf-8"))
trabajos = cola["trabajos"]

def historial_actual():
    if HISTORIAL.exists():
        return json.loads(HISTORIAL.read_text(encoding="utf-8"))
    return {}

validados = 0
ejecutados = 0
pendientes = 0

print(f"TOTAL_TRABAJOS|{len(trabajos)}", flush=True)

for indice, trabajo in enumerate(trabajos, 1):

    clave = trabajo["clave"]
    firma = trabajo["firma"]
    anterior = historial_actual().get(clave, {})

    estado = anterior.get("estado")

    if (
        anterior.get("firma") == firma
        and estado in ("VALIDADO_SIN_CAMBIOS", "PUBLICADO")
    ):
        print(f"OMITIDO|{indice}|YA_VALIDADO", flush=True)
        validados += 1
        continue

    if (
        anterior.get("firma") == firma
        and estado in ("PENDIENTE_PUBLICACION", "EN_EJECUCION")
    ):
        print(f"BLOQUEADO|{indice}|{estado}", flush=True)
        sys.exit(2)

    pendientes += 1

    if not args.ejecutar:
        print(f"PENDIENTE|{indice}|{clave}", flush=True)
        continue

    if args.limite and ejecutados >= args.limite:
        print("LIMITE_DE_EJECUCIONES|ALCANZADO", flush=True)
        break

    print(f"EJECUTANDO|{indice}|{clave}", flush=True)

    resultado = subprocess.run(
        [
            sys.executable,
            "scripts/pipeline/registrar_ejecuciones.py",
            "--indice", str(indice),
            "--ejecutar"
        ],
        capture_output=True,
        text=True
    )

    print(resultado.stdout, flush=True)

    if resultado.returncode != 0:
        print(resultado.stderr[-1500:], flush=True)
        print(f"PIPELINE|DETENIDO|{indice}", flush=True)
        sys.exit(2)

    nuevo_estado = (
        historial_actual()
        .get(clave, {})
        .get("estado")
    )

    if nuevo_estado == "VALIDADO_SIN_CAMBIOS":
        ejecutados += 1
        print(f"APROBADO|{indice}", flush=True)

    elif nuevo_estado == "PENDIENTE_PUBLICACION":
        print(
            f"PUBLICACION_PENDIENTE|{indice}",
            flush=True
        )
        sys.exit(2)

    else:
        print(f"ESTADO_NO_VALIDADO|{indice}", flush=True)
        sys.exit(2)

print(f"TOTAL_OMITIDOS|{validados}")
print(f"TOTAL_EJECUTADOS|{ejecutados}")
print(f"TOTAL_PENDIENTES_DETECTADOS|{pendientes}")
print("PUBLICACION|NO_EJECUTADA")
print("PIPELINE|CONTROL_COMPLETADO")
