#!/usr/bin/env python3

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

COLA = Path(
    "outputs/quality/incremental/cola_procesamiento.json"
)

EJECUTOR = "./scripts/pipeline/ejecutar_incremental.sh"

p = argparse.ArgumentParser()
p.add_argument("--indice", type=int)
p.add_argument("--ejecutar", action="store_true")
args = p.parse_args()

if args.ejecutar and args.indice is None:
    p.error("Indica --indice para ejecutar un trabajo")

datos = json.loads(COLA.read_text(encoding="utf-8"))
trabajos = datos["trabajos"]

print(f"TOTAL_TRABAJOS|{len(trabajos)}")

for i, t in enumerate(trabajos, 1):
    bronze = t["comando_plan"][
        t["comando_plan"].index("--bronze") + 1
    ]
    print(
        f"TRABAJO|{i}|{t['dataset']}|"
        f"{t['fecha']}|{bronze}"
    )

if args.indice is None:
    print("MODO|SOLO_LISTADO")
    sys.exit(0)

if not 1 <= args.indice <= len(trabajos):
    p.error("Indice inexistente")

trabajo = trabajos[args.indice - 1]
comando = trabajo["comando_plan"]

if (
    comando[0] != EJECUTOR
    or comando[1] != "plan"
    or "--bronze" not in comando
):
    raise RuntimeError("Comando no autorizado")

# Comprobar que el lote no haya cambiado
# desde la creación del inventario.
bronze = comando[comando.index("--bronze") + 1]
carpeta = bronze.split("/ingestion_date=")[0]

fecha = trabajo["fecha"]
carpeta = f"{carpeta}/ingestion_date={fecha}"

r = subprocess.run(
    ["hdfs", "dfs", "-ls", "-R", carpeta],
    capture_output=True,
    text=True
)

if r.returncode != 0:
    raise RuntimeError("No se pudo consultar Bronze")

archivos = []

for linea in r.stdout.splitlines():
    x = linea.split()
    if len(x) >= 8 and x[0].startswith("-"):
        archivos.append((x[-1], x[4], x[5], x[6]))

firma = hashlib.sha256(
    json.dumps(sorted(archivos)).encode()
).hexdigest()

if firma != trabajo["firma"]:
    raise RuntimeError(
        "Lote modificado: regenerar inventario y cola"
    )

# Validar nuevamente el plan.
plan = subprocess.run(
    comando, capture_output=True, text=True
)

if (
    plan.returncode != 0
    or "ESTADO|PLANIFICADO_NO_EJECUTADO"
       not in plan.stdout
):
    raise RuntimeError("Plan no aprobado")

print(f"TRABAJO_SELECCIONADO|{args.indice}")
print("LOTE|FIRMA_VERIFICADA")
print("PLAN|APROBADO")

if not args.ejecutar:
    print("EJECUCION|NO_INICIADA")
    sys.exit(0)

real = comando.copy()
real[1] = "ejecutar"

resultado = subprocess.run(
    real, capture_output=True, text=True
)

print(resultado.stdout)

if resultado.returncode != 0:
    print(resultado.stderr[-1500:])
    print("EJECUCION|ERROR")
    sys.exit(resultado.returncode)

if "SIN_CAMBIOS|" in resultado.stdout:
    print("RESULTADO|SIN_CAMBIOS")
elif "STAGING_VALIDADO|" in resultado.stdout:
    print("RESULTADO|STAGING_PENDIENTE_PUBLICACION")
else:
    print("RESULTADO|REQUIERE_REVISION")
    sys.exit(2)

print("SILVER_OFICIAL|SIN_CAMBIOS")
