#!/usr/bin/env python3
import json
import subprocess
from pathlib import Path
from datetime import datetime, timezone

CARPETA = Path("outputs/quality/incremental")
RUTA_BASE = Path("/tmp/bvl_ruta_publicacion_prueba.txt")

if not RUTA_BASE.exists():
    raise SystemExit("ERROR|Falta entorno aislado")

base = RUTA_BASE.read_text().strip()

if not base.startswith(
    "/datalake/bvl/staging/prueba_publicacion_bcrp_"
):
    raise SystemExit("ERROR|Entorno no autorizado")

reportes = sorted(
    CARPETA.glob("publicacion_bcrp_aislada_*.json"),
    key=lambda p: p.stat().st_mtime,
    reverse=True
)

reporte = None
origen = None

for archivo in reportes:
    datos = json.loads(archivo.read_text())
    if datos.get("destino_prueba") == (
        base + "/silver_copia/anio=2026/mes=9"
    ):
        reporte = datos
        origen = archivo
        break

if reporte is None:
    raise SystemExit("ERROR|No existe publicacion aislada")

if (
    reporte.get("estado") != "PRUEBA_PUBLICADA_AISLADA"
    or reporte.get("publicacion_aprobada") is not False
    or reporte.get("activar_gold") is not False
):
    raise SystemExit("ERROR|Reporte incompatible")

for campo in ["destino_prueba", "backup_prueba"]:
    ruta = reporte[campo]
    if not ruta.startswith(base + "/"):
        raise SystemExit("ERROR|Ruta fuera del laboratorio")

    resultado = subprocess.run(
        ["hdfs", "dfs", "-test", "-d", ruta]
    )

    if resultado.returncode != 0:
        raise SystemExit(f"ERROR|No existe {campo}")

evento = {
    "version_contrato": "1.0-laboratorio",
    "fecha_utc": datetime.now(timezone.utc).isoformat(),
    "run_id": reporte["version_prueba"],
    "tipo_evento": "SILVER_PUBLICACION_SIMULADA",
    "estado": "LABORATORIO_NO_PUBLICADO",
    "dataset": "tipo_cambio",
    "ruta_silver_oficial": (
        "/datalake/bvl/silver/macroeconomia/tipo_cambio"
    ),
    "clave_negocio": ["fecha"],
    "particiones_afectadas": [
        {"anio": 2026, "mes": 9}
    ],
    "registros_nuevos": 1,
    "registros_corregidos": 0,
    "productos_gold_candidatos": [
        "mercado_valor_diario",
        "empresa_trimestre"
    ],
    "evidencia_laboratorio": str(origen),
    "publicacion_aprobada": False,
    "activar_gold": False
}

salida = CARPETA / "evento_silver_gold_laboratorio.json"

salida.write_text(
    json.dumps(evento, indent=2, ensure_ascii=False),
    encoding="utf-8"
)

print("EVENTO|GENERADO")
print("DATASET|tipo_cambio")
print("PARTICIONES|1")
print("PRODUCTOS_GOLD_CANDIDATOS|2")
print("PUBLICACION_OFICIAL|NO_EJECUTADA")
print("GOLD|BLOQUEADO")
print(f"ARCHIVO|{salida}")
