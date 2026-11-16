#!/usr/bin/env python3
import hashlib
import json
import re
import subprocess
import tempfile
import os
from pathlib import Path

CARPETA = Path("outputs/quality/incremental")

def hdfs(*args):
    r = subprocess.run(
        ["hdfs", "dfs", *args],
        capture_output=True,
        text=True
    )
    if r.returncode:
        raise RuntimeError(r.stderr[-400:])
    return r.stdout

def existe(ruta):
    return subprocess.run(
        ["hdfs", "dfs", "-test", "-e", ruta],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    ).returncode == 0

def archivos(ruta):
    resultado = {}
    for linea in hdfs("-ls", "-R", ruta).splitlines():
        p = linea.split()
        if len(p) >= 8 and p[0].startswith("-"):
            archivo = p[-1]
            if archivo.endswith(".parquet"):
                resultado[archivo[len(ruta):]] = archivo
    if not resultado:
        raise RuntimeError("SIN_PARQUET")
    return resultado

def sha256(ruta):
    proceso = subprocess.Popen(
        ["hdfs", "dfs", "-cat", ruta],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    digest = hashlib.sha256()
    while True:
        bloque = proceso.stdout.read(1024 * 1024)
        if not bloque:
            break
        digest.update(bloque)
    error = proceso.stderr.read().decode(
        "utf-8", errors="replace"
    )
    if proceso.wait() != 0:
        raise RuntimeError(error[-400:])
    return digest.hexdigest()

registros = sorted(
    CARPETA.glob("registro_publicacion_*.json"),
    key=lambda p: p.stat().st_mtime,
    reverse=True
)

if not registros:
    raise SystemExit("ERROR|REGISTRO_NO_ENCONTRADO")

r = json.loads(registros[0].read_text(encoding="utf-8"))
run_id = r["run_id"]

if not re.fullmatch(r"\d{8}T\d{6}Z_[0-9a-f]{8}", run_id):
    raise SystemExit("ERROR|RUN_ID_INVALIDO")

base = (
    "/datalake/bvl/staging/"
    f"prueba_publicacion_bcrp_persistente_{run_id}"
)

current = f"{base}/silver_copia/anio=2026/mes=9"
backup = f"{base}/backups/mes=9"
versiones = f"{base}/versions"
version = f"{versiones}/v_{run_id}"
temporal = f"{versiones}/.temporal_{run_id}"
manifest = f"{base}/metadata/version_{run_id}.json"

if (
    r.get("tipo") != "LABORATORIO_AISLADO"
    or r.get("current") != current
    or r.get("backup") != backup
    or r.get("publicacion_aprobada") is not False
    or r.get("activar_gold") is not False
):
    raise SystemExit("ERROR|LABORATORIO_INVALIDO")

confirmado = json.loads(
    hdfs("-cat", f"{base}/metadata/publicacion_confirmada.json")
)

if (
    confirmado.get("run_id") != run_id
    or confirmado.get("estado") != "PUBLICADO_SOLO_LABORATORIO"
    or confirmado.get("activar_gold") is not False
):
    raise SystemExit("ERROR|PUBLICACION_NO_CONFIRMADA")

if not existe(current) or not existe(backup):
    raise SystemExit("ERROR|FALTAN_PARTICIONES")

if existe(version) or existe(temporal) or existe(manifest):
    raise SystemExit("ERROR|VERSION_PREVIA_O_INCOMPLETA")

origen = archivos(current)
huellas = {
    nombre: sha256(ruta)
    for nombre, ruta in origen.items()
}

hdfs("-mkdir", "-p", versiones)
hdfs("-cp", current, temporal)

copia = archivos(temporal)

if set(copia) != set(huellas):
    raise SystemExit("ERROR|ARCHIVOS_DIFERENTES")

for nombre, ruta in copia.items():
    if sha256(ruta) != huellas[nombre]:
        raise SystemExit("ERROR|SHA256_NO_COINCIDE")

hdfs("-mv", temporal, version)

datos = {
    "run_id": run_id,
    "tipo": "VERSION_INMUTABLE_LABORATORIO",
    "dataset": "tipo_cambio",
    "estado": "VERIFICADA_SOLO_LABORATORIO",
    "version": f"v_{run_id}",
    "ruta_version": version,
    "particiones": [{"anio": 2026, "mes": 9}],
    "archivos_sha256": huellas,
    "publicacion_aprobada": False,
    "activar_gold": False
}

with tempfile.NamedTemporaryFile(
    mode="w", encoding="utf-8",
    suffix=".json", delete=False
) as f:
    json.dump(datos, f, indent=2)
    archivo_local = f.name

try:
    temp_hdfs = manifest + ".tmp"
    hdfs("-put", archivo_local, temp_hdfs)
    hdfs("-mv", temp_hdfs, manifest)
finally:
    os.unlink(archivo_local)

print("VERSION|CREADA")
print(f"RUN_ID|{run_id}")
print(f"ARCHIVOS_VERIFICADOS|{len(huellas)}")
print("SHA256|APROBADO")
print("VERSION_INMUTABLE|DISPONIBLE")
print(f"MANIFEST_HDFS|{manifest}")
print("SILVER_OFICIAL|SIN_CAMBIOS")
print("GOLD|NO_ACTIVADO")
