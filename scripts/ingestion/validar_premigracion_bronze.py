#!/usr/bin/env python3
"""Validador local de premigracion BVL. Sin acceso Cloud."""

import argparse
import hashlib
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
parser.add_argument("--plan", required=True)
args = parser.parse_args()

def rechazar(motivo):
    raise SystemExit(f"ERROR|{motivo}")

run = args.run_id
patron = r"bvl-bronze-inicial-(\d{8})-(\d{3})"
coincidencia = re.fullmatch(patron, run)

if not coincidencia:
    rechazar("RUN_ID_INVALIDO")

try:
    datetime.strptime(coincidencia.group(1), "%Y%m%d")
except ValueError:
    rechazar("FECHA_RUN_ID_INVALIDA")

ruta_plan = Path(args.plan)

if not ruta_plan.is_file():
    rechazar("PLAN_INEXISTENTE")

contenido = ruta_plan.read_bytes()
sha_plan = hashlib.sha256(contenido).hexdigest()
plan = json.loads(contenido)

if (
    plan.get("run_id") != run
    or plan.get("tipo") != "PLAN_LOCAL_BRONZE"
    or plan.get("datasets") != 12
    or plan.get("archivos") != 3599
    or plan.get("bytes") != 393623640
    or plan.get("transferencias") != 0
    or plan.get("publicacion") != "NO_PUBLICADO"
):
    rechazar("CONTRATO_GLOBAL_INVALIDO")

operaciones = plan.get("operaciones")

if not isinstance(operaciones, list) or len(operaciones) != 3599:
    rechazar("OPERACIONES_INVALIDAS")

conteos = Counter()
destinos = set()
buckets = set()
bytes_total = 0

for indice, op in enumerate(operaciones):
    dataset = op["dataset"]
    origen = op["origen"]
    destino = op["destino"]

    if not re.fullmatch(r"(smv|bcrp)/[a-z0-9_]+", dataset):
        rechazar(f"DATASET_INVALIDO|{indice}")

    prefijo = f"/datalake/bvl/bronze/{dataset}/"

    if not origen.startswith(prefijo):
        rechazar(f"ORIGEN_INVALIDO|{indice}")

    relativo = origen[len(prefijo):]

    if not relativo or any(
        segmento in ("", ".", "..")
        for segmento in relativo.split("/")
    ):
        rechazar(f"RUTA_INSEGURA|{indice}")

    uri = urlsplit(destino)
    esperado = f"landing/{dataset}/run_id={run}/{relativo}"

    if (
        uri.scheme != "gs"
        or uri.netloc != "bvl26-bronze-7f3a9c"
        or uri.path.lstrip("/") != esperado
        or uri.query
        or uri.fragment
    ):
        rechazar(f"DESTINO_INVALIDO|{indice}")

    if op["run_id"] != run:
        rechazar(f"IDENTIDAD_INCONSISTENTE|{indice}")

    if op["estado"] != "PLANIFICADO_NO_EJECUTADO":
        rechazar(f"ESTADO_NO_AUTORIZADO|{indice}")

    if not re.fullmatch(r"[0-9a-f]{64}", op["sha256"]):
        rechazar(f"SHA256_INVALIDO|{indice}")

    tamano = op["size_bytes"]
    if type(tamano) is not int or tamano <= 0:
        rechazar(f"TAMANO_INVALIDO|{indice}")

    if destino in destinos:
        rechazar(f"DESTINO_DUPLICADO|{indice}")

    destinos.add(destino)
    buckets.add(uri.netloc)
    conteos[dataset] += 1
    bytes_total += tamano

if (
    len(conteos) != 12
    or len(destinos) != 3599
    or len(buckets) != 1
    or bytes_total != plan["bytes"]
):
    rechazar("INVENTARIO_INCONSISTENTE")

reporte = {
    "tipo": "PREFLIGHT_LOCAL_NO_OPERATIVO",
    "run_id_laboratorio": run,
    "plan_sha256": sha_plan,
    "bucket_destino_declarado": next(iter(buckets)),
    "datasets": dict(sorted(conteos.items())),
    "archivos": len(operaciones),
    "destinos_unicos": len(destinos),
    "bytes": bytes_total,
    "reservas_cloud": 0,
    "transferencias_cloud": 0,
    "publicacion": "NO_PUBLICADO",
    "resultado": "CONTRATO_LOCAL_VALIDADO"
}

salida = ruta_plan.parent / f"PREFLIGHT_MIGRACION_{run}.json"
texto = json.dumps(reporte, indent=2, ensure_ascii=False) + "\n"

if salida.exists():
    if salida.read_text(encoding="utf-8") != texto:
        rechazar("CONFLICTO_REPORTE_EXISTENTE")
else:
    salida.write_text(texto, encoding="utf-8")

print("===== PREFLIGHT MIGRACION BRONZE =====")
print("DATASETS:", len(conteos))
print("ARCHIVOS:", len(operaciones))
print("DESTINOS_UNICOS:", len(destinos))
print("BYTES:", bytes_total)
print("PLAN_SHA256: OK")
print("RESERVAS_CLOUD: 0")
print("TRANSFERENCIAS_CLOUD: 0")
print("EVIDENCIA:", salida)
print("RESULTADO: PREFLIGHT_MIGRACION_OK")
