#!/usr/bin/env python3

import hashlib
import json
import shlex
import subprocess
import sys
from pathlib import Path

DETECTOR = "scripts/pipeline/detectar_lotes.py"
EJECUTOR = "./scripts/pipeline/ejecutar_incremental.sh"
INVENTARIO = Path(
    "outputs/quality/incremental/inventario_bronze.json"
)

def ejecutar(comando):
    r = subprocess.run(
        comando,
        capture_output=True,
        text=True
    )
    if r.returncode != 0:
        raise RuntimeError(r.stderr or r.stdout)
    return r.stdout

def main():
    salida = ejecutar([sys.executable, DETECTOR])

    planes = [
        shlex.split(linea[5:])
        for linea in salida.splitlines()
        if linea.startswith("PLAN|")
    ]

    if not planes:
        raise RuntimeError("No se detectaron planes")

    listado = ejecutar([
        "hdfs", "dfs", "-ls", "-R",
        "/datalake/bvl/bronze"
    ])

    archivos = []

    for linea in listado.splitlines():
        p = linea.split()
        if len(p) >= 8 and p[0].startswith("-"):
            archivos.append((p[-1], p[4], p[5], p[6]))

    anterior = (
        json.loads(INVENTARIO.read_text())
        if INVENTARIO.exists() else {}
    )

    actual = dict(anterior)
    conteos = {"NUEVO": 0, "MODIFICADO": 0, "IGUAL": 0}

    for comando in planes:
        if comando[:2] != [EJECUTOR, "plan"]:
            raise RuntimeError("Comando no autorizado")

        respuesta = ejecutar(comando)

        if "ESTADO|PLANIFICADO_NO_EJECUTADO" not in respuesta:
            raise RuntimeError("Plan no aprobado")

        argumentos = comando[2:]
        fuente = argumentos[0]
        bronze = argumentos[
            argumentos.index("--bronze") + 1
        ]

        prefijo, fecha = bronze.split(
            "/ingestion_date=", 1
        )
        fecha = fecha.split("/")[0]

        carpeta = (
            prefijo + "/ingestion_date=" + fecha
        )

        lote = sorted(
            x for x in archivos
            if x[0].startswith(carpeta + "/")
        )

        if not lote:
            raise RuntimeError(
                "Lote sin archivos: " + carpeta
            )

        firma = hashlib.sha256(
            json.dumps(lote).encode()
        ).hexdigest()

        clave = fuente + "|" + carpeta
        previo = anterior.get(clave)

        if previo is None:
            estado = "NUEVO"
        elif previo["firma"] != firma:
            estado = "MODIFICADO"
        else:
            estado = "IGUAL"

        conteos[estado] += 1

        actual[clave] = {
            "firma": firma,
            "archivos": len(lote),
            "estado": "OBSERVADO_NO_PUBLICADO"
        }

        print(
            f"LOTE_CONTROL|{fuente}|{fecha}|"
            f"archivos={len(lote)}|{estado}"
        )

    INVENTARIO.parent.mkdir(
        parents=True, exist_ok=True
    )

    temporal = INVENTARIO.with_suffix(".tmp")
    temporal.write_text(
        json.dumps(actual, indent=2),
        encoding="utf-8"
    )
    temporal.replace(INVENTARIO)

    for estado, cantidad in conteos.items():
        print(f"TOTAL_{estado}|{cantidad}")

    print("CONTROLADOR|INVENTARIO_ACTUALIZADO")
    print("SILVER_OFICIAL|SIN_CAMBIOS")

if __name__ == "__main__":
    main()
