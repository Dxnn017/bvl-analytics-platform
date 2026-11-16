#!/usr/bin/env python3

import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

CARPETA = Path("outputs/quality/incremental")
SUFIJO = "/silver_copia/anio=2026/mes=9"

def hdfs(*args):
    r = subprocess.run(
        ["hdfs", "dfs", *args],
        capture_output=True,
        text=True
    )
    if r.returncode != 0:
        raise RuntimeError(
            f"ERROR_HDFS|{r.stderr[-300:]}"
        )
    return r.stdout

def existe(ruta):
    r = subprocess.run(
        ["hdfs", "dfs", "-test", "-d", ruta]
    )
    return r.returncode == 0

def huellas(ruta):
    resultado = []
    listado = hdfs("-ls", "-R", ruta)

    for linea in listado.splitlines():
        partes = linea.split()

        if (
            len(partes) >= 8
            and partes[0].startswith("-")
            and partes[-1].endswith(".parquet")
        ):
            archivo = partes[-1]
            firma = hdfs(
                "-checksum", archivo
            ).strip().split()[-1]

            resultado.append((
                archivo[len(ruta):],
                int(partes[4]),
                firma
            ))

    if not resultado:
        raise RuntimeError("SIN_PARQUET")

    return sorted(resultado)

reportes = sorted(
    CARPETA.glob("publicacion_bcrp_aislada_*.json"),
    key=lambda p: p.stat().st_mtime,
    reverse=True
)

seleccion = None

for archivo in reportes:
    datos = json.loads(archivo.read_text())

    destino = datos.get("destino_prueba", "")
    respaldo = datos.get("backup_prueba", "")

    if not destino.endswith(SUFIJO):
        continue

    origen_base = destino[:-len(SUFIJO)]

    if not re.fullmatch(
        r"/datalake/bvl/staging/"
        r"prueba_publicacion_bcrp_\d{8}T\d{6}_\d+",
        origen_base
    ):
        continue

    if not respaldo.startswith(origen_base + "/backups/"):
        continue

    if (
        datos.get("estado") != "PRUEBA_PUBLICADA_AISLADA"
        or datos.get("activar_gold") is not False
        or datos.get("publicacion_aprobada") is not False
    ):
        continue

    if existe(destino) and existe(respaldo):
        seleccion = (destino, respaldo)
        break

if seleccion is None:
    raise SystemExit("ERROR|Laboratorio original no disponible")

publicado, historico = seleccion

run_id = (
    datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    + "_" + uuid4().hex[:8]
)

base = (
    "/datalake/bvl/staging/"
    f"prueba_publicacion_bcrp_persistente_{run_id}"
)

current = f"{base}/silver_copia/anio=2026/mes=9"
candidate = f"{base}/staging_preparado/anio=2026/mes=9"
backup = f"{base}/backups/mes=9"

hdfs("-mkdir", "-p",
     f"{base}/silver_copia/anio=2026",
     f"{base}/staging_preparado/anio=2026",
     f"{base}/backups",
     f"{base}/metadata")

hdfs("-cp", historico, current)
hdfs("-cp", publicado, candidate)

if huellas(historico) != huellas(current):
    raise RuntimeError("HISTORICO_NO_COINCIDE")

if huellas(publicado) != huellas(candidate):
    raise RuntimeError("CANDIDATO_NO_COINCIDE")

registro = {
    "run_id": run_id,
    "tipo": "LABORATORIO_AISLADO",
    "estado": "PREPARADO",
    "dataset": "tipo_cambio",
    "current": current,
    "candidate": candidate,
    "backup": backup,
    "particion": {"anio": 2026, "mes": 9},
    "publicacion_aprobada": False,
    "activar_gold": False
}

local = CARPETA / f"registro_publicacion_{run_id}.json"
local.write_text(
    json.dumps(registro, indent=2),
    encoding="utf-8"
)

hdfs(
    "-put",
    str(local),
    f"{base}/metadata/estado.json"
)

print("LABORATORIO|PREPARADO")
print("COPIA_HISTORICA|VERIFICADA")
print("COPIA_CANDIDATA|VERIFICADA")
print("REGISTRO_PERSISTENTE|CREADO")
print(f"RUN_ID|{run_id}")
print(f"REGISTRO_HDFS|{base}/metadata/estado.json")
print("ESTADO|PREPARADO")
print("SILVER_OFICIAL|SIN_CAMBIOS")
print("GOLD|NO_ACTIVADO")
