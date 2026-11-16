#!/usr/bin/env python3

import hashlib
import json
import re
import subprocess
from pathlib import Path

CARPETA = Path("outputs/quality/incremental")

registros = sorted(
    CARPETA.glob("registro_publicacion_*.json"),
    key=lambda p: p.stat().st_mtime,
    reverse=True
)

if not registros:
    raise SystemExit("ERROR|Sin registro")

registro = json.loads(registros[0].read_text())
run_id = registro["run_id"]

if not re.fullmatch(
    r"\d{8}T\d{6}Z_[0-9a-f]{8}", run_id
):
    raise SystemExit("ERROR|RUN_ID_INVALIDO")

base = (
    "/datalake/bvl/staging/"
    f"prueba_publicacion_bcrp_persistente_{run_id}"
)

ruta_manifest = (
    f"{base}/metadata/version_{run_id}.json"
)

ruta_version = f"{base}/versions/v_{run_id}"

def hdfs(*argumentos):
    resultado = subprocess.run(
        ["hdfs", "dfs", *argumentos],
        capture_output=True,
        text=True
    )
    if resultado.returncode != 0:
        raise RuntimeError(resultado.stderr[-400:])
    return resultado.stdout

def calcular_sha256(ruta):
    proceso = subprocess.Popen(
        ["hdfs", "dfs", "-cat", ruta],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL
    )

    digest = hashlib.sha256()

    while True:
        bloque = proceso.stdout.read(1024 * 1024)
        if not bloque:
            break
        digest.update(bloque)

    if proceso.wait() != 0:
        raise RuntimeError("ERROR_LEYENDO_PARQUET")

    return digest.hexdigest()

manifest = json.loads(hdfs("-cat", ruta_manifest))

if (
    manifest.get("run_id") != run_id
    or manifest.get("dataset") != "tipo_cambio"
    or manifest.get("tipo") != "VERSION_INMUTABLE_LABORATORIO"
    or manifest.get("estado") != "VERIFICADA_SOLO_LABORATORIO"
    or manifest.get("ruta_version") != ruta_version
    or manifest.get("publicacion_aprobada") is not False
    or manifest.get("activar_gold") is not False
):
    raise SystemExit("ERROR|MANIFEST_INVALIDO")

esperados = manifest["archivos_sha256"]

listado = hdfs("-ls", "-R", ruta_version)
encontrados = {}

for linea in listado.splitlines():
    partes = linea.split()
    if len(partes) < 8 or not partes[0].startswith("-"):
        continue

    archivo = partes[-1]
    if archivo.endswith(".parquet"):
        relativo = archivo[len(ruta_version):]
        encontrados[relativo] = archivo

if set(encontrados) != set(esperados):
    raise SystemExit("ERROR|ARCHIVOS_NO_COINCIDEN")

for relativo, archivo in encontrados.items():
    if calcular_sha256(archivo) != esperados[relativo]:
        raise SystemExit("ERROR|SHA256_INVALIDO")

paquete = {
    "tipo": "PAQUETE_INTEGRACION_LABORATORIO",
    "run_id": run_id,
    "dataset": "tipo_cambio",
    "version_silver": f"v_{run_id}",
    "ruta_version": ruta_version,
    "manifest_hdfs": ruta_manifest,
    "particiones_afectadas": manifest["particiones"],
    "archivos_sha256": esperados,
    "productos_gold_candidatos": [
        "mercado_valor_diario",
        "empresa_trimestre"
    ],
    "estado": "PENDIENTE_CONTRATO_PIERO",
    "publicacion_aprobada": False,
    "activar_gold": False
}

salida = (
    CARPETA /
    f"paquete_integracion_bcrp_lab_{run_id}.json"
)

salida.write_text(
    json.dumps(paquete, indent=2, ensure_ascii=False),
    encoding="utf-8"
)

print("MANIFEST|VALIDADO")
print("VERSION_SILVER|VERIFICADA")
print(f"PARQUET_SHA256|{len(encontrados)}")
print("PRODUCTOS_GOLD_CANDIDATOS|2")
print("PAQUETE_INTEGRACION|GENERADO")
print(f"ARCHIVO|{salida}")
print("PUBLICACION_OFICIAL|NO_EJECUTADA")
print("GOLD|NO_ACTIVADO")
