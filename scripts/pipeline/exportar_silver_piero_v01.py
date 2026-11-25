#!/usr/bin/env python3
import hashlib
import json
import shutil
import subprocess
import tarfile
from pathlib import Path

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
args = parser.parse_args()

RUN = args.run_id

if not (
    1 <= len(RUN) <= 96
    and RUN.isascii()
    and RUN[0].isalnum()
    and all(c.isalnum() or c in "_-" for c in RUN)
):
    parser.error("run_id contiene caracteres no permitidos")


ROOT = Path("outputs/quality/incremental")
OUT = Path("entregas") / f"piero_silver_{RUN}"

manifest_file = ROOT / f"manifest_bcrp_v01_{RUN}.json"
versiones_file = ROOT / f"versiones_bcrp_v01_{RUN}.json"

manifest = json.loads(manifest_file.read_text())
versiones = json.loads(versiones_file.read_text())

anterior = versiones["version_anterior"]
nueva = versiones["version_publicada"]

if (
    manifest["run_id"] != RUN
    or manifest["entorno"] != "PRUEBA"
    or manifest["validacion_aprobada"] is not False
    or manifest["datasets"][0]["registros_antes"] != 7751
    or manifest["datasets"][0]["registros_despues"] != 7752
    or versiones["run_id"] != RUN
    or versiones["publicacion_oficial"] is not False
    or versiones["activar_gold"] is not False
):
    raise SystemExit("ERROR|METADATOS_INCOMPATIBLES")

if manifest["datasets"][0]["ruta_silver"] != nueva["ruta"]:
    raise SystemExit("ERROR|RUTA_VERSION_INCONSISTENTE")

if OUT.exists():
    raise SystemExit("ERROR|ENTREGA_EXISTENTE_REVISAR")

def ejecutar(*args):
    r = subprocess.run(
        list(args), capture_output=True, text=True
    )
    if r.returncode:
        raise RuntimeError(r.stderr[-500:])
    return r.stdout

rutas = [
    ("anterior", anterior),
    ("nueva", nueva)
]

total_bytes = 0

for nombre, datos in rutas:
    ruta = datos["ruta"]

    ejecutar("hdfs", "dfs", "-ls", ruta)
    resultado = ejecutar(
        "hdfs", "dfs", "-du", "-s", ruta
    )

    total_bytes += int(resultado.split()[0])

disco = shutil.disk_usage(".")

if disco.free < total_bytes * 3 + 100_000_000:
    raise SystemExit("ERROR|ESPACIO_INSUFICIENTE")

OUT.mkdir(parents=True)
(OUT / "silver_versiones").mkdir()
(OUT / "metadatos").mkdir()

for nombre, datos in rutas:
    destino = OUT / "silver_versiones" / nombre

    print(f"EXPORTANDO|{nombre}", flush=True)

    ejecutar(
        "hdfs", "dfs", "-get",
        datos["ruta"], str(destino)
    )

    esperados = datos["archivos_sha256"]
    encontrados = {}

    for archivo in destino.rglob("*.parquet"):
        relativo = "/" + archivo.relative_to(destino).as_posix()

        h = hashlib.sha256()

        with archivo.open("rb") as f:
            for bloque in iter(
                lambda: f.read(1024 * 1024), b""
            ):
                h.update(bloque)

        encontrados[relativo] = h.hexdigest()

    if encontrados != esperados:
        raise RuntimeError(
            f"ERROR|INTEGRIDAD_{nombre.upper()}"
        )

    print(
        f"SHA256_{nombre.upper()}|{len(encontrados)}",
        flush=True
    )

shutil.copy2(
    manifest_file,
    OUT / "metadatos" / manifest_file.name
)

shutil.copy2(
    versiones_file,
    OUT / "metadatos" / versiones_file.name
)

contrato = Path(
    "docs/integracion/piero_v01/"
    "contrato_manifest_silver_gold.md"
)

shutil.copy2(
    contrato,
    OUT / "metadatos" / contrato.name
)

mapa = {
    "run_id": RUN,
    "entorno": "PRUEBA",
    "version_anterior": "silver_versiones/anterior",
    "version_nueva": "silver_versiones/nueva",
    "nota": (
        "Las rutas HDFS del manifest original deben "
        "adaptarse al entorno de destino. "
        "No activar Gold oficial."
    )
}

(OUT / "metadatos" / "mapa_rutas.json").write_text(
    json.dumps(mapa, indent=2, ensure_ascii=False)
)

archivo_tar = OUT.with_suffix(".tar.gz")

with tarfile.open(archivo_tar, "w:gz") as tar:
    tar.add(OUT, arcname=OUT.name)

with tarfile.open(archivo_tar, "r:gz") as tar:
    if not tar.getmembers():
        raise RuntimeError("ARCHIVO_VACIO")

print("VERSION_ANTERIOR|EXPORTADA")
print("VERSION_NUEVA|EXPORTADA")
print("CONTRATO_V01|INCLUIDO")
print("MANIFEST|INCLUIDO")
print("INTEGRIDAD_LOCAL|VERIFICADA")
print(f"PAQUETE|{archivo_tar}")
print("EXPORTACION|COMPLETADA")
print("SILVER_OFICIAL|SIN_CAMBIOS")
print("GOLD|NO_ACTIVADO")
