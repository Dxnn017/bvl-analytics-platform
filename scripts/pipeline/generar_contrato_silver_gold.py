#!/usr/bin/env python3

import json
from datetime import datetime, timezone
from pathlib import Path

log = Path("/tmp/bcrp_optimizado.log").read_text(
    encoding="utf-8"
)

rutas = [
    linea.split("|", 1)[1].strip()
    for linea in log.splitlines()
    if linea.startswith("MANIFEST|")
]

if not rutas:
    raise SystemExit("ERROR: manifest no encontrado")

ruta_origen = Path(rutas[-1])
origen = json.loads(ruta_origen.read_text(encoding="utf-8"))

if origen["dataset"] != "tipo_cambio":
    raise SystemExit("ERROR: dataset incorrecto")

if origen["estado"] != "STAGING_VALIDADO_NO_PUBLICADO":
    raise SystemExit("ERROR: estado no autorizado")

if "/staging/prueba_bcrp_incremental/" not in origen["bronze"]:
    raise SystemExit("ERROR: origen no es prueba aislada")

if origen["current"] != "/datalake/bvl/silver/macroeconomia/tipo_cambio":
    raise SystemExit("ERROR: destino Silver inesperado")

particiones = [
    {
        "anio": int(p["anio"]),
        "mes": int(p["mes"]),
        "filas_candidato": int(p["filas"])
    }
    for p in origen["particiones"]
]

contrato = {
    "version_contrato": "1.0-propuesta",
    "fecha_generacion": datetime.now(timezone.utc).isoformat(),
    "tipo_evento": "SILVER_STAGING_VALIDADO",
    "estado": "PRUEBA_NO_PUBLICADA",
    "dataset": "tipo_cambio",
    "ruta_silver": origen["current"],
    "claves_negocio": ["fecha"],
    "particiones_afectadas": particiones,
    "registros_nuevos": origen["nuevas"],
    "registros_corregidos": origen["corregidas"],
    "manifest_origen": str(ruta_origen),
    "publicacion_aprobada": False,
    "activar_gold": False
}

destino = Path(
    "outputs/quality/incremental/"
    "contrato_silver_gold_prueba.json"
)

destino.parent.mkdir(parents=True, exist_ok=True)
destino.write_text(
    json.dumps(contrato, indent=2, ensure_ascii=False),
    encoding="utf-8"
)

print("CONTRATO|GENERADO")
print("DATASET|", contrato["dataset"])
print("PARTICIONES|", particiones)
print("PUBLICACION|NO_EJECUTADA")
print("GOLD|NO_ACTIVADO")
print("ARCHIVO|", destino)
