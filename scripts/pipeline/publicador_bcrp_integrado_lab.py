#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
import tempfile
import os
from pathlib import Path
from uuid import uuid4

p = argparse.ArgumentParser()
p.add_argument("accion", choices=["plan", "publicar", "recuperar"])
p.add_argument("--registro", required=True)
args = p.parse_args()

registro = json.loads(
    Path(args.registro).read_text(encoding="utf-8")
)

run_id = registro["run_id"]

if not re.fullmatch(r"\d{8}T\d{6}Z_[0-9a-f]{8}", run_id):
    raise SystemExit("ERROR|RUN_ID_INVALIDO")

base = (
    "/datalake/bvl/staging/"
    f"prueba_publicacion_bcrp_persistente_{run_id}"
)
original = f"{base}/silver_copia/anio=2026/mes=9"
candidato = f"{base}/staging_preparado/anio=2026/mes=9"
backup = f"{base}/backups/mes=9"
metadata = f"{base}/metadata"
lock = f"{metadata}/lock_integrado"
intento = f"{metadata}/publicacion_integrada.json"
confirmado = f"{metadata}/publicacion_confirmada.json"
recuperado = f"{metadata}/recuperacion_integrada.json"

def hdfs(*comando, check=True):
    r = subprocess.run(
        ["hdfs", "dfs", *comando],
        capture_output=True,
        text=True,
        timeout=90
    )
    if check and r.returncode != 0:
        raise RuntimeError(
            f"HDFS|{' '.join(comando)}|{r.stderr[-300:]}"
        )
    return r

def existe(ruta):
    return hdfs("-test", "-e", ruta, check=False).returncode == 0

def leer(ruta):
    return json.loads(hdfs("-cat", ruta).stdout)

def escribir(datos, destino):
    if existe(destino):
        raise RuntimeError("REGISTRO_EXISTENTE")

    temporal_hdfs = f"{destino}.tmp_{uuid4().hex}"

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        encoding="utf-8",
        delete=False
    ) as f:
        json.dump(datos, f, indent=2)
        temporal_local = f.name

    try:
        hdfs("-put", temporal_local, temporal_hdfs)
        hdfs("-mv", temporal_hdfs, destino)
    finally:
        os.unlink(temporal_local)

def huellas(ruta):
    archivos = []

    for linea in hdfs("-ls", "-R", ruta).stdout.splitlines():
        partes = linea.split()

        if len(partes) < 8 or not partes[0].startswith("-"):
            continue

        archivo = partes[-1]

        if not archivo.endswith(".parquet"):
            continue

        firma = hdfs("-checksum", archivo).stdout.split()

        if len(firma) < 3:
            raise RuntimeError("CHECKSUM_INVALIDO")

        archivos.append([
            archivo[len(ruta):],
            int(partes[4]),
            firma[1:]
        ])

    if not archivos:
        raise RuntimeError("SIN_ARCHIVOS_PARQUET")

    return sorted(archivos)

def validar():
    esperado = {
        "tipo": "LABORATORIO_AISLADO",
        "dataset": "tipo_cambio",
        "current": original,
        "candidate": candidato,
        "backup": backup,
        "publicacion_aprobada": False,
        "activar_gold": False
    }

    for campo, valor in esperado.items():
        if registro.get(campo) != valor:
            raise RuntimeError(f"REGISTRO_INVALIDO|{campo}")

    if not existe(f"{metadata}/estado.json"):
        raise RuntimeError("ESTADO_HDFS_INEXISTENTE")

    estado = leer(f"{metadata}/estado.json")

    if estado != registro:
        raise RuntimeError("REGISTRO_LOCAL_HDFS_DIFERENTE")

def plan():
    validar()

    if existe(lock) or existe(intento):
        raise RuntimeError("TRANSACCION_PREVIA_O_BLOQUEO")

    if existe(backup):
        raise RuntimeError("BACKUP_OCUPADO")

    if not existe(original) or not existe(candidato):
        raise RuntimeError("PARTICIONES_INCOMPLETAS")

    anterior = huellas(original)
    nuevo = huellas(candidato)

    if anterior == nuevo:
        raise RuntimeError("SIN_DIFERENCIAS")

    print("PLAN|APROBADO")
    print(f"RUN_ID|{run_id}")
    print(f"PARQUET_HISTORICO|{len(anterior)}")
    print(f"PARQUET_CANDIDATO|{len(nuevo)}")
    print("PUBLICACION|NO_EJECUTADA")
    print("SILVER_OFICIAL|SIN_CAMBIOS")

    return anterior, nuevo

def publicar():
    plan()

    resultado = hdfs("-mkdir", lock, check=False)

    if resultado.returncode != 0:
        raise RuntimeError("LOCK_NO_ADQUIRIDO")

    try:
        anterior, nuevo = plan_bajo_lock()

        escribir({
            "run_id": run_id,
            "estado": "INTENCION_REGISTRADA",
            "anterior": anterior,
            "candidato": nuevo,
            "publicacion_aprobada": False,
            "activar_gold": False
        }, intento)

        hdfs("-mv", original, backup)
        print("BACKUP|CREADO", flush=True)

        hdfs("-mv", candidato, original)
        print("CANDIDATO|MOVIDO", flush=True)

        if huellas(original) != nuevo:
            raise RuntimeError("VERIFICACION_FALLIDA")

        escribir({
            "run_id": run_id,
            "estado": "PUBLICADO_SOLO_LABORATORIO",
            "activar_gold": False
        }, confirmado)

        print("PUBLICACION_AISLADA|VERIFICADA")

        hdfs("-rmdir", lock)

    except Exception:
        print(f"TRANSACCION_PENDIENTE|{intento}")
        print("RECUPERACION|REQUIERE_EJECUCION")
        raise

def plan_bajo_lock():
    validar()

    if existe(intento) or existe(backup):
        raise RuntimeError("TRANSACCION_PREVIA")

    if not existe(original) or not existe(candidato):
        raise RuntimeError("PARTICIONES_INCOMPLETAS")

    return huellas(original), huellas(candidato)

def recuperar():
    validar()

    if not existe(intento):
        raise RuntimeError("SIN_TRANSACCION")

    if existe(recuperado):
        raise RuntimeError("YA_RECUPERADO")

    datos = leer(intento)

    if datos.get("run_id") != run_id:
        raise RuntimeError("RUN_ID_DIFERENTE")

    if existe(confirmado):
        raise RuntimeError(
            "PUBLICACION_CONFIRMADA_REQUIERE_ROLLBACK_EXPLICITO"
        )

    if existe(original) and existe(backup) and not existe(candidato):
        if huellas(original) != datos["candidato"]:
            raise RuntimeError("CANDIDATO_ALTERADO")
        if huellas(backup) != datos["anterior"]:
            raise RuntimeError("BACKUP_ALTERADO")

        hdfs("-mv", original, candidato)
        hdfs("-mv", backup, original)

    elif not existe(original) and existe(backup) and existe(candidato):
        if huellas(backup) != datos["anterior"]:
            raise RuntimeError("BACKUP_ALTERADO")

        hdfs("-mv", backup, original)

    elif existe(original) and not existe(backup) and existe(candidato):
        pass

    else:
        raise RuntimeError("ESTADO_AMBIGUO_REVISION_MANUAL")

    if huellas(original) != datos["anterior"]:
        raise RuntimeError("HISTORICO_NO_RECUPERADO")

    if huellas(candidato) != datos["candidato"]:
        raise RuntimeError("CANDIDATO_NO_RECUPERADO")

    escribir({
        "run_id": run_id,
        "estado": "RECUPERADO",
        "activar_gold": False
    }, recuperado)

    if existe(lock):
        hdfs("-rmdir", lock)

    print("RECUPERACION|APROBADA")
    print("HISTORICO|RESTAURADO")
    print("SILVER_OFICIAL|SIN_CAMBIOS")

try:
    if args.accion == "plan":
        plan()
    elif args.accion == "publicar":
        publicar()
    else:
        recuperar()

    print("CODIGO_PUBLICADOR|0")

except Exception as error:
    print(f"ERROR|{error}")
    raise SystemExit(1)
