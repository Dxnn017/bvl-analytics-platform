#!/usr/bin/env python3
"""Consolida resultados reales de transferencia para Quality."""

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--plan", required=True)
parser.add_argument("--resultados", required=True)
parser.add_argument("--salida", required=True)
args = parser.parse_args()

plan = json.loads(
    Path(args.plan).read_text(encoding="utf-8")
)

operaciones = plan["operaciones"]

esperados = {}

for op in operaciones:
    clave = op["destino"]
    if clave in esperados:
        raise SystemExit("ERROR|DESTINO_DUPLICADO_PLAN")
    esperados[clave] = op

recibidos = {}

with Path(args.resultados).open(encoding="utf-8") as archivo:
    for numero, linea in enumerate(archivo, 1):
        if not linea.strip():
            continue

        registro = json.loads(linea)
        destino = registro.get("destino_gcs")

        if destino not in esperados:
            raise SystemExit(
                f"ERROR|DESTINO_DESCONOCIDO|{numero}"
            )

        if destino in recibidos:
            raise SystemExit(
                f"ERROR|RESULTADO_DUPLICADO|{numero}"
            )

        op = esperados[destino]

        if (
            registro.get("run_id") != op["run_id"]
            or registro.get("dataset") != op["dataset"]
            or registro.get("origen_hdfs") != op["origen"]
        ):
            raise SystemExit(
                f"ERROR|IDENTIDAD_INCONSISTENTE|{numero}"
            )

        recibidos[destino] = registro

resumen = defaultdict(lambda: Counter({
    "esperados": 0,
    "transferidos": 0,
    "verificados": 0,
    "rechazados": 0,
    "pendientes": 0
}))
detalles = []

for destino, op in esperados.items():
    dato = recibidos.get(destino)
    dataset = op["dataset"]

    resumen[dataset]["esperados"] += 1

    estado = "PENDIENTE"
    generacion = None

    if dato is not None:
        estado_origen = dato.get("estado")

        if estado_origen in (
            "TRANSFERIDO",
            "REINTENTO_EXISTENTE"
        ):
            valido = (
                dato.get("validacion") == "INTEGRIDAD_VERIFICADA"
                and dato.get("bytes_esperados") == op["size_bytes"]
                and dato.get("bytes_observados") == op["size_bytes"]
                and dato.get("sha256_esperado") == op["sha256"]
                and dato.get("sha256_gcs") == op["sha256"]
                and type(dato.get("generacion_gcs")) is int
                and dato["generacion_gcs"] > 0
            )

            estado = "VERIFICADO" if valido else "RECHAZADO"
            if valido:
                resumen[dataset]["transferidos"] += 1
                resumen[dataset]["verificados"] += 1
                generacion = dato["generacion_gcs"]

        else:
            estado = "RECHAZADO"

    resumen[dataset][estado.lower() + "s"] += (
        1 if estado in ("PENDIENTE", "RECHAZADO") else 0
    )

    detalles.append({
        "run_id": op["run_id"],
        "dataset": dataset,
        "origen_hdfs": op["origen"],
        "destino_gcs": destino,
        "size_bytes_esperados": op["size_bytes"],
        "sha256_esperado": op["sha256"],
        "generacion_gcs": generacion,
        "estado": estado,
        "evidencia_transferencia": dato
    })

totales = Counter()
for valores in resumen.values():
    totales.update(valores)

reporte = {
    "run_id": plan["run_id"],
    "estado": "EVIDENCIA_PENDIENTE_APROBACION_QUALITY",
    "publicacion_bronze": "NO_AUTORIZADA_POR_ESTE_REPORTE",
    "datasets": {
        k: dict(v) for k, v in sorted(resumen.items())
    },
    "totales": dict(totales),
    "detalles": detalles
}

salida = Path(args.salida)
salida.parent.mkdir(parents=True, exist_ok=True)

if salida.exists():
    raise SystemExit("ERROR|REPORTE_EXISTENTE")

salida.write_text(
    json.dumps(reporte, indent=2, ensure_ascii=False),
    encoding="utf-8"
)

print("RESULTADO: REPORTE_QUALITY_GENERADO")
print("ARCHIVOS_ESPERADOS:", len(operaciones))
print("ARCHIVOS_VERIFICADOS:", totales["verificados"])
print("ARCHIVOS_PENDIENTES:", totales["pendientes"])
print("ARCHIVOS_RECHAZADOS:", totales["rechazados"])
print("PUBLICACION_BRONZE: NO_AUTORIZADA")
