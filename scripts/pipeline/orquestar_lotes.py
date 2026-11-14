#!/usr/bin/env python3

import json
import shlex
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone

DETECTOR = "scripts/pipeline/detectar_lotes.py"
EJECUTOR = "./scripts/pipeline/ejecutar_incremental.sh"

BASE = Path("outputs/quality/incremental")
INVENTARIO = BASE / "inventario_bronze.json"
COLA = BASE / "cola_procesamiento.json"

def ejecutar(comando):
    resultado = subprocess.run(
        comando,
        capture_output=True,
        text=True
    )
    if resultado.returncode != 0:
        raise RuntimeError(
            resultado.stderr or resultado.stdout
        )
    return resultado.stdout

def main():
    if not INVENTARIO.exists():
        raise RuntimeError("Inventario no encontrado")

    inventario = json.loads(
        INVENTARIO.read_text(encoding="utf-8")
    )

    salida = ejecutar([sys.executable, DETECTOR])

    planes = [
        shlex.split(linea[5:])
        for linea in salida.splitlines()
        if linea.startswith("PLAN|")
    ]

    trabajos = []

    for comando in planes:
        if (
            len(comando) < 5
            or comando[0] != EJECUTOR
            or comando[1] != "plan"
            or "--bronze" not in comando
        ):
            raise RuntimeError("Plan no autorizado")

        respuesta = ejecutar(comando)

        if "ESTADO|PLANIFICADO_NO_EJECUTADO" not in respuesta:
            raise RuntimeError("Plan rechazado")

        dataset = comando[2]
        bronze = comando[
            comando.index("--bronze") + 1
        ]

        prefijo, resto = bronze.split(
            "/ingestion_date=", 1
        )

        fecha = resto.split("/")[0]

        carpeta = (
            prefijo + "/ingestion_date=" + fecha
        )

        clave = dataset + "|" + carpeta

        if clave not in inventario:
            raise RuntimeError(
                "Lote sin inventario: " + clave
            )

        trabajo = {
            "clave": clave,
            "dataset": dataset,
            "fecha": fecha,
            "firma": inventario[clave]["firma"],
            "comando_plan": comando,
            "estado": "PENDIENTE_EJECUCION"
        }

        trabajos.append(trabajo)

        print(
            f"COLA|{dataset}|{fecha}|"
            "PENDIENTE_EJECUCION"
        )

    if not trabajos:
        raise RuntimeError("No se encontraron lotes")

    claves = [t["clave"] for t in trabajos]

    if len(claves) != len(set(claves)):
        raise RuntimeError("Existen lotes duplicados")

    BASE.mkdir(parents=True, exist_ok=True)

    reporte = {
        "fecha_generacion": datetime.now(
            timezone.utc
        ).isoformat(),
        "modo": "PLANIFICACION",
        "total": len(trabajos),
        "trabajos": trabajos
    }

    temporal = COLA.with_suffix(".tmp")
    temporal.write_text(
        json.dumps(
            reporte, indent=2,
            ensure_ascii=False
        ),
        encoding="utf-8"
    )
    temporal.replace(COLA)

    print(f"TOTAL_PLANIFICADOS|{len(trabajos)}")
    print("ORQUESTADOR|COLA_GENERADA")
    print("EJECUCION|NO_INICIADA")
    print("SILVER_OFICIAL|SIN_CAMBIOS")

if __name__ == "__main__":
    main()
