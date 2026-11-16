#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

RUN = "20261116T003053Z_78127719"
BASE = (
    "/datalake/bvl/staging/"
    f"prueba_publicacion_bcrp_persistente_{RUN}"
)

CURRENT = f"{BASE}/silver_copia/anio=2026/mes=9"
CANDIDATE = f"{BASE}/staging_preparado/anio=2026/mes=9"
BACKUP = f"{BASE}/backups/mes=9"
LOCK = f"{BASE}/metadata/lock_post_publicacion"
INTENTO = f"{BASE}/metadata/post_publicacion_intento.json"
RECUPERADO = f"{BASE}/metadata/post_publicacion_recuperado.json"

def hdfs(*args, check=True):
    r = subprocess.run(
        ["hdfs", "dfs", *args],
        capture_output=True,
        text=True
    )
    if check and r.returncode != 0:
        raise RuntimeError(r.stderr[-400:])
    return r

def existe(ruta):
    return hdfs("-test", "-e", ruta, check=False).returncode == 0

def huellas(ruta):
    resultado = []
    listado = hdfs("-ls", "-R", ruta).stdout

    for linea in listado.splitlines():
        p = linea.split()
        if len(p) < 8 or not p[0].startswith("-"):
            continue
        if not p[-1].endswith(".parquet"):
            continue

        archivo = p[-1]
        checksum = hdfs("-checksum", archivo).stdout.split()
        if len(checksum) < 3:
            raise RuntimeError("Checksum invalido")

        resultado.append([
            archivo[len(ruta):],
            int(p[4]),
            checksum[1:]
        ])

    if not resultado:
        raise RuntimeError("Sin archivos Parquet")

    return sorted(resultado)

def guardar(datos, destino):
    if existe(destino):
        raise RuntimeError("Registro ya existente")

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json",
        encoding="utf-8", delete=False
    ) as f:
        json.dump(datos, f, indent=2)
        temporal = f.name

    try:
        hdfs("-put", temporal, destino)
    finally:
        os.unlink(temporal)

def validar_laboratorio():
    if not existe(f"{BASE}/metadata/estado.json"):
        raise RuntimeError("Registro base inexistente")

    datos = json.loads(
        hdfs("-cat", f"{BASE}/metadata/estado.json").stdout
    )

    if (
        datos.get("run_id") != RUN
        or datos.get("tipo") != "LABORATORIO_AISLADO"
        or datos.get("current") != CURRENT
        or datos.get("candidate") != CANDIDATE
        or datos.get("backup") != BACKUP
        or datos.get("activar_gold") is not False
        or datos.get("publicacion_aprobada") is not False
    ):
        raise RuntimeError("Laboratorio no autorizado")

def simular():
    validar_laboratorio()

    if existe(LOCK) or existe(INTENTO) or existe(RECUPERADO):
        raise RuntimeError("Prueba ya iniciada")

    if not existe(CURRENT) or not existe(CANDIDATE):
        raise RuntimeError("Faltan particiones")

    if existe(BACKUP):
        raise RuntimeError("Backup ocupado")

    anterior = huellas(CURRENT)
    candidato = huellas(CANDIDATE)

    hdfs("-mkdir", LOCK)

    guardar({
        "run_id": RUN,
        "estado": "INTENCION_PUBLICACION_LABORATORIO",
        "anterior": anterior,
        "candidato": candidato,
        "publicacion_aprobada": False,
        "activar_gold": False
    }, INTENTO)

    print("TRANSACCION|REGISTRADA", flush=True)

    hdfs("-mv", CURRENT, BACKUP)
    print("BACKUP|CREADO", flush=True)

    hdfs("-mv", CANDIDATE, CURRENT)
    print("CANDIDATO|MOVIDO", flush=True)

    print("INTERRUPCION|SIMULADA", flush=True)
    os._exit(24)

def recuperar():
    validar_laboratorio()

    if not existe(INTENTO) or not existe(LOCK):
        raise RuntimeError("Transaccion no encontrada")

    if existe(RECUPERADO):
        raise RuntimeError("Recuperacion ya registrada")

    datos = json.loads(hdfs("-cat", INTENTO).stdout)

    if (
        datos.get("run_id") != RUN
        or datos.get("estado") != "INTENCION_PUBLICACION_LABORATORIO"
        or datos.get("publicacion_aprobada") is not False
        or datos.get("activar_gold") is not False
    ):
        raise RuntimeError("Transaccion invalida")

    if not existe(CURRENT) or not existe(BACKUP):
        raise RuntimeError("Estado inesperado")

    if existe(CANDIDATE):
        raise RuntimeError("Candidato ya existe; revision manual")

    if huellas(BACKUP) != datos["anterior"]:
        raise RuntimeError("Backup alterado")

    if huellas(CURRENT) != datos["candidato"]:
        raise RuntimeError("Candidato alterado")

    hdfs("-mv", CURRENT, CANDIDATE)
    hdfs("-mv", BACKUP, CURRENT)

    if huellas(CURRENT) != datos["anterior"]:
        raise RuntimeError("Historico no restaurado")

    if huellas(CANDIDATE) != datos["candidato"]:
        raise RuntimeError("Candidato no conservado")

    guardar({
        "run_id": RUN,
        "estado": "RECUPERADO",
        "activar_gold": False
    }, RECUPERADO)

    hdfs("-rmdir", LOCK)

    print("ROLLBACK|COMPLETADO")
    print("HISTORICO|RESTAURADO")
    print("CANDIDATO|CONSERVADO")
    print("INTEGRIDAD|APROBADA")
    print("REGISTRO_HDFS|PERSISTENTE")
    print("SILVER_OFICIAL|SIN_CAMBIOS")
    print("GOLD|NO_ACTIVADO")
    print("PRUEBA|APROBADA")

if __name__ == "__main__":
    try:
        if sys.argv[1:] == ["simular"]:
            simular()
        elif sys.argv[1:] == ["recuperar"]:
            recuperar()
        else:
            raise RuntimeError("Usar simular o recuperar")
    except Exception as e:
        print(f"ERROR|{e}")
        sys.exit(1)
