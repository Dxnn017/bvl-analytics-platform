#!/usr/bin/env python3
import argparse
import json
import os
import re
import subprocess
import tempfile
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("fase", choices=["simular", "recuperar"])
args = parser.parse_args()

carpeta = Path("outputs/quality/incremental")
reportes = sorted(
    carpeta.glob("registro_publicacion_*.json"),
    key=lambda p: p.stat().st_mtime,
    reverse=True
)

if not reportes:
    raise SystemExit("ERROR|Registro local no encontrado")

registro = json.loads(reportes[0].read_text(encoding="utf-8"))
run_id = registro["run_id"]

if not re.fullmatch(
    r"\d{8}T\d{6}Z_[0-9a-f]{8}", run_id
):
    raise SystemExit("ERROR|RUN_ID_INVALIDO")

base = (
    "/datalake/bvl/staging/"
    f"prueba_publicacion_bcrp_persistente_{run_id}"
)

original = f"{base}/silver_copia/anio=2026/mes=9"
candidato = f"{base}/staging_preparado/anio=2026/mes=9"
backup = f"{base}/backups/mes=9"
pendiente = f"{base}/metadata/transaccion_pendiente.json"
recuperado = f"{base}/metadata/transaccion_recuperada.json"

if (
    registro.get("tipo") != "LABORATORIO_AISLADO"
    or registro.get("current") != original
    or registro.get("candidate") != candidato
    or registro.get("backup") != backup
    or registro.get("publicacion_aprobada") is not False
    or registro.get("activar_gold") is not False
):
    raise SystemExit("ERROR|Registro no autorizado")

def hdfs(*comando, check=True):
    r = subprocess.run(
        ["hdfs", "dfs", *comando],
        capture_output=True,
        text=True
    )
    if check and r.returncode != 0:
        raise RuntimeError(
            f"ERROR_HDFS|{r.stderr[-300:]}"
        )
    return r

def existe(ruta):
    return hdfs(
        "-test", "-e", ruta, check=False
    ).returncode == 0

def huellas(ruta):
    archivos = []
    listado = hdfs("-ls", "-R", ruta).stdout

    for linea in listado.splitlines():
        partes = linea.split()

        if (
            len(partes) >= 8
            and partes[0].startswith("-")
            and partes[-1].endswith(".parquet")
        ):
            archivo = partes[-1]
            checksum = hdfs(
                "-checksum", archivo
            ).stdout.strip().split()

            archivos.append([
                archivo[len(ruta):],
                int(partes[4]),
                checksum[-1]
            ])

    if not archivos:
        raise RuntimeError("SIN_ARCHIVOS_PARQUET")

    return sorted(archivos)

def guardar_hdfs(datos, destino):
    if existe(destino):
        raise RuntimeError("REGISTRO_YA_EXISTE")

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".json",
        delete=False
    ) as temp:
        json.dump(datos, temp, indent=2)
        temporal = temp.name

    try:
        hdfs("-put", temporal, destino)
    finally:
        os.unlink(temporal)

if args.fase == "simular":

    if existe(pendiente) or existe(recuperado):
        raise SystemExit("ERROR|Prueba ya iniciada")

    if not existe(original) or not existe(candidato):
        raise SystemExit("ERROR|Faltan copias")

    if existe(backup):
        raise SystemExit("ERROR|Backup ocupado")

    firma_original = huellas(original)

    guardar_hdfs({
        "run_id": run_id,
        "estado": "RESPALDO_PENDIENTE",
        "original": original,
        "backup": backup,
        "huellas_originales": firma_original,
        "modo": "LABORATORIO"
    }, pendiente)

    print("TRANSACCION|REGISTRADA_EN_HDFS", flush=True)

    hdfs("-mv", original, backup)

    print("BACKUP|CREADO", flush=True)
    print("FALLO_CONTROLADO|PROVOCADO", flush=True)

    os._exit(23)

elif args.fase == "recuperar":

    if not existe(pendiente):
        raise SystemExit("ERROR|Sin transaccion pendiente")

    if existe(recuperado):
        raise SystemExit("ERROR|Ya se recupero")

    datos = json.loads(
        hdfs("-cat", pendiente).stdout
    )

    if datos.get("run_id") != run_id:
        raise SystemExit("ERROR|RUN_ID_NO_COINCIDE")

    if not existe(backup) or existe(original):
        raise SystemExit(
            "ERROR|Estado inesperado: revisar manualmente"
        )

    if not existe(candidato):
        raise SystemExit("ERROR|Candidato perdido")

    if huellas(backup) != datos["huellas_originales"]:
        raise SystemExit("ERROR|Integridad backup incorrecta")

    hdfs("-mv", backup, original)

    if huellas(original) != datos["huellas_originales"]:
        raise SystemExit("ERROR|Integridad restaurada incorrecta")

    guardar_hdfs({
        "run_id": run_id,
        "estado": "RECUPERADO",
        "origen": pendiente,
        "silver_oficial_modificado": False,
        "activar_gold": False
    }, recuperado)

    print("RECUPERACION|COMPLETADA")
    print("INTEGRIDAD|CONSERVADA")
    print("CANDIDATO|INTACTO")
    print("REGISTRO_PERSISTENTE|ACTUALIZADO")
    print("SILVER_OFICIAL|SIN_CAMBIOS")
    print("GOLD|NO_ACTIVADO")
    print("PRUEBA|APROBADA")
