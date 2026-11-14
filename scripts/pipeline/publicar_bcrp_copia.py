#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

RUTA_BASE = Path("/tmp/bvl_ruta_publicacion_prueba.txt")

if not RUTA_BASE.is_file():
    raise SystemExit("ERROR|Falta la ruta de prueba")

base = RUTA_BASE.read_text().strip()

patron = (
    r"/datalake/bvl/staging/"
    r"prueba_publicacion_bcrp_\d{8}T\d{6}_\d+"
)

if not re.fullmatch(patron, base):
    raise SystemExit("ERROR|Ruta no autorizada")

original = f"{base}/silver_copia/anio=2026/mes=9"
candidato = f"{base}/staging_preparado/anio=2026/mes=9"
lock = f"{base}/.publicacion_lock"

p = argparse.ArgumentParser()
p.add_argument("modo", choices=["plan", "publicar-prueba"])
args = p.parse_args()

def hdfs(*arg, check=True):
    r = subprocess.run(
        ["hdfs", "dfs", *arg],
        text=True,
        capture_output=True
    )
    if check and r.returncode != 0:
        raise RuntimeError(
            f"HDFS|{' '.join(arg)}|{r.stderr[-350:]}"
        )
    return r

def existe(path):
    return hdfs(
        "-test", "-d", path, check=False
    ).returncode == 0

def huellas(path):
    listado = hdfs("-ls", "-R", path).stdout.splitlines()
    archivos = []

    for linea in listado:
        partes = linea.split()
        if (
            len(partes) >= 8
            and partes[0].startswith("-")
            and partes[-1].endswith(".parquet")
        ):
            fichero = partes[-1]
            firma = hdfs(
                "-checksum", fichero
            ).stdout.strip().split()[-1]

            archivos.append((fichero[len(path):], firma))

    if not archivos:
        raise RuntimeError("SIN_ARCHIVOS_PARQUET")

    return sorted(archivos)

if not existe(original) or not existe(candidato):
    raise SystemExit("ERROR|Faltan copias de prueba")

if existe(lock):
    raise SystemExit("ERROR|Existe un bloqueo previo")

antes = huellas(original)
preparado = huellas(candidato)

if antes == preparado:
    raise SystemExit("ERROR|Candidato igual al historico")

print(f"PARQUET_HISTORICO|{len(antes)}")
print(f"PARQUET_CANDIDATO|{len(preparado)}")

if args.modo == "plan":
    print("PLAN|VALIDADO_SIN_EJECUTAR")
    print("SILVER_OFICIAL|SIN_CAMBIOS")
    raise SystemExit(0)

version = (
    datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    + "_" + uuid4().hex[:8]
)

backup = f"{base}/backups/publicacion_{version}"

if existe(backup):
    raise SystemExit("ERROR|Respaldo ya existente")

hdfs("-mkdir", lock)

seguro_para_liberar = False

try:
    hdfs("-mv", original, backup)
    print("RESPALDO|CREADO", flush=True)

    hdfs("-mv", candidato, original)
    print("PRUEBA|PUBLICADA", flush=True)

    if huellas(original) != preparado:
        raise RuntimeError("Verificacion fallida")

    reporte = {
        "modo": "LABORATORIO_AISLADO",
        "estado": "PRUEBA_PUBLICADA_AISLADA",
        "version_prueba": version,
        "dataset": "tipo_cambio",
        "particion": {"anio": 2026, "mes": 9},
        "destino_prueba": original,
        "backup_prueba": backup,
        "staging_prueba": candidato,
        "publicacion_aprobada": False,
        "activar_gold": False
    }

    salida = (
        Path("outputs/quality/incremental")
        / f"publicacion_bcrp_aislada_{version}.json"
    )

    salida.parent.mkdir(parents=True, exist_ok=True)
    salida.write_text(
        json.dumps(reporte, indent=2),
        encoding="utf-8"
    )

    print(f"REPORTE|{salida}", flush=True)
    print("PUBLICACION_REAL|NO_EJECUTADA", flush=True)
    print("GOLD|NO_ACTIVADO", flush=True)

    seguro_para_liberar = True

except Exception:
    print("INCIDENTE|INICIANDO_RECUPERACION", flush=True)

    try:
        if existe(backup):
            if existe(original):
                if existe(candidato):
                    raise RuntimeError(
                        "Staging ocupado: revision manual"
                    )
                hdfs("-mv", original, candidato)

            hdfs("-mv", backup, original)

        if huellas(original) != antes:
            raise RuntimeError("Integridad no recuperada")

        seguro_para_liberar = True
        print("RECUPERACION|VALIDADA", flush=True)

    except Exception as error:
        print(f"ALERTA|REVISION_MANUAL|{error}", flush=True)

    raise

finally:
    if seguro_para_liberar:
        hdfs("-rmdir", lock)
    else:
        print(f"BLOQUEO_CONSERVADO|{lock}", flush=True)
