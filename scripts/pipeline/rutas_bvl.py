import json
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config/almacenamiento_bvl.json"

def cargar():
    return json.loads(CONFIG.read_text(encoding="utf-8"))

def ruta(zona, relativo="", entorno=None):
    cfg = cargar()
    modo = entorno or cfg["modo_actual"]

    if modo not in ("hdfs", "gcs"):
        raise ValueError("Entorno no permitido")

    bases = cfg[modo]

    if zona not in bases:
        raise ValueError("Zona desconocida")

    base = bases[zona]

    if not base:
        raise ValueError("Ruta GCS pendiente de Lizeth")

    if modo == "hdfs" and not base.startswith("/datalake/bvl/"):
        raise ValueError("Ruta HDFS no autorizada")

    if modo == "gcs" and not base.startswith("gs://"):
        raise ValueError("Ruta GCS invalida")

    if relativo:
        p = PurePosixPath(relativo)
        if p.is_absolute() or ".." in p.parts:
            raise ValueError("Ruta relativa no permitida")

    return base.rstrip("/") + (
        "/" + relativo.strip("/") if relativo else ""
    )

if __name__ == "__main__":
    cfg = cargar()

    print("PROYECTO|" + cfg["proyecto"])
    print("ENTORNO_ACTUAL|" + cfg["modo_actual"])

    print("SILVER_HDFS|" + ruta(
        "silver", "macroeconomia/tipo_cambio"
    ))

    print("BRONZE_HDFS|" + ruta(
        "bronze", "bcrp/tipo_cambio"
    ))

    if all(cfg["gcs"].values()):
        print("GCS|CONFIGURADO")
    else:
        print("GCS|PENDIENTE_LIZETH")

    print("CONFIGURACION|VALIDADA")
