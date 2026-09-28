#!/usr/bin/env python3

import hashlib
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path


# ============================================================
# CONFIGURACIÓN
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"

HDFS_ROOT = "/datalake/bvl"

DATASETS = [
    {
        "source": "smv",
        "dataset": "cotizaciones",
        "file": RAW_DIR / "smv" / "Cotizaciones.json",
        "extension": ".json",
    },
    {
        "source": "smv",
        "dataset": "indices",
        "file": RAW_DIR / "smv" / "Indices.json",
        "extension": ".json",
    },
    {
        "source": "smv",
        "dataset": "estado_resultados",
        "file": RAW_DIR / "smv" / "EstadoResultadosSMV.json",
        "extension": ".json",
    },
    {
        "source": "smv",
        "dataset": "cambios_patrimonio",
        "file": RAW_DIR / "smv" / "CambiosPatrimonioSMV.json",
        "extension": ".json",
    },
    {
        "source": "smv",
        "dataset": "situacion_financiera",
        "file": RAW_DIR / "smv" / "SituacionFinancieraSMV.json",
        "extension": ".json",
    },
    {
        "source": "smv",
        "dataset": "flujo_efectivo",
        "file": RAW_DIR / "smv" / "FlujoEfectivoSMV.json",
        "extension": ".json",
    },
    {
        "source": "bcrp",
        "dataset": "tipo_cambio",
        "file": RAW_DIR / "bcrp" / "BCRP_Tipo_Cambio_Venta.csv",
        "extension": ".csv",
    },
    {
        "source": "bcrp",
        "dataset": "embig",
        "file": RAW_DIR / "bcrp" / "BCRP_EMBIG_Peru.csv",
        "extension": ".csv",
    },
]


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def hdfs(*args, check=True):
    """Ejecuta un comando HDFS."""
    cmd = ["hdfs", "dfs", *args]

    result = subprocess.run(
        cmd,
        text=True,
        capture_output=True
    )

    if check and result.returncode != 0:
        raise RuntimeError(result.stderr.strip())

    return result


def calculate_sha256(file_path):
    """Calcula SHA-256 sin cargar todo el archivo en memoria."""
    sha = hashlib.sha256()

    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            sha.update(block)

    return sha.hexdigest()


def validate_file(file_path, expected_extension):
    """
    Validación técnica mínima.
    La validación semántica de registros se hará en Silver.
    """

    if not file_path.exists():
        return False, "Archivo no encontrado"

    if not file_path.is_file():
        return False, "La ruta no corresponde a un archivo"

    if file_path.stat().st_size == 0:
        return False, "Archivo vacío"

    if file_path.suffix.lower() != expected_extension:
        return False, "Extensión inesperada"

    # JSON: comprobar sintaxis.
    if expected_extension == ".json":
        try:
            with open(file_path, "r", encoding="utf-8-sig") as f:
                json.load(f)
        except Exception as exc:
            return False, f"JSON inválido: {exc}"

    # CSV: comprobar que sea legible.
    elif expected_extension == ".csv":
        try:
            with open(
                file_path,
                "r",
                encoding="utf-8-sig",
                errors="replace"
            ) as f:
                first_line = f.readline()

            if not first_line:
                return False, "CSV sin contenido legible"

        except Exception as exc:
            return False, f"CSV no legible: {exc}"

    return True, "OK"


def hdfs_exists(path):
    result = hdfs("-test", "-e", path, check=False)
    return result.returncode == 0


def write_json_to_hdfs(data, hdfs_path):
    """Escribe temporalmente JSON local y luego lo carga a HDFS."""

    local_tmp = Path("/tmp") / Path(hdfs_path).name

    with open(local_tmp, "w", encoding="utf-8") as f:
        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=2,
            default=str
        )

    hdfs("-put", "-f", str(local_tmp), hdfs_path)

    local_tmp.unlink(missing_ok=True)

def quarantine_file(file_path, source, dataset, run_id):
    """Copia un archivo inválido a la zona Quarantine de HDFS."""

    quarantine_dir = (
        f"{HDFS_ROOT}/quarantine/"
        f"{source}/{dataset}/"
        f"run_id={run_id}"
    )

    hdfs("-mkdir", "-p", quarantine_dir)

    destination = f"{quarantine_dir}/{file_path.name}"

    hdfs("-put", "-f", str(file_path), destination)

    return destination

# ============================================================
# INGESTA
# ============================================================

def process_dataset(config, run_id, ingestion_date):
    source = config["source"]
    dataset = config["dataset"]
    file_path = config["file"]

    print()
    print("=" * 65)
    print(f"Dataset : {dataset}")
    print(f"Fuente  : {source.upper()}")
    print(f"Archivo : {file_path.name}")
    print("=" * 65)

    timestamp = datetime.now().isoformat(timespec="seconds")

    audit = {
        "run_id": run_id,
        "source_system": source.upper(),
        "dataset_name": dataset,
        "file_name": file_path.name,
        "ingestion_timestamp": timestamp,
        "file_size_bytes": None,
        "checksum_sha256": None,
        "storage_path": None,
        "status": None,
        "error_message": None,
    }
      
    # --------------------------------------------------------
    # 1. VALIDACIÓN TÉCNICA
    # --------------------------------------------------------

    valid, message = validate_file(
        file_path,
        config["extension"]
    )

    if not valid:

        if not file_path.exists():
            audit["status"] = "FAILED"
            audit["error_message"] = message

            print(f"[FAILED] {message}")

        else:
            audit["status"] = "REJECTED"
            audit["error_message"] = message

            quarantine_path = quarantine_file(
                file_path,
                source,
                dataset,
                run_id
            )

            audit["storage_path"] = quarantine_path

            print(f"[REJECTED] {message}")
            print(f"[QUARANTINE] {quarantine_path}")

        audit_name = (
            f"{run_id}_{source}_{dataset}_{audit['status']}.json"
        )

        audit_path = (
            f"{HDFS_ROOT}/metadata/ingestion/{audit_name}"
        )

        write_json_to_hdfs(audit, audit_path)

        return audit



    # --------------------------------------------------------
    # 2. CHECKSUM
    # --------------------------------------------------------

    file_size = file_path.stat().st_size
    checksum = calculate_sha256(file_path)

    audit["file_size_bytes"] = file_size
    audit["checksum_sha256"] = checksum

    print(f"[OK] Tamaño   : {file_size:,} bytes")
    print(f"[OK] SHA-256  : {checksum}")

    # --------------------------------------------------------
    # 3. CONTROL DE DUPLICADOS
    # --------------------------------------------------------

    checksum_dir = (
        f"{HDFS_ROOT}/metadata/ingestion/checksums"
    )

    hdfs("-mkdir", "-p", checksum_dir)

    checksum_marker = (
        f"{checksum_dir}/{checksum}.json"
    )

    if hdfs_exists(checksum_marker):

        audit["status"] = "SKIPPED_DUPLICATE"
        audit["error_message"] = (
            "Archivo ya ingerido previamente "
            "con el mismo checksum"
        )

        print("[SKIPPED_DUPLICATE] Archivo ya procesado.")

        audit_name = (
            f"{run_id}_{source}_{dataset}_SKIPPED.json"
        )

        audit_path = (
            f"{HDFS_ROOT}/metadata/ingestion/{audit_name}"
        )

        write_json_to_hdfs(audit, audit_path)

        return audit

    # --------------------------------------------------------
    # 4. CARGA A BRONZE
    # --------------------------------------------------------

    bronze_dir = (
        f"{HDFS_ROOT}/bronze/"
        f"{source}/"
        f"{dataset}/"
        f"ingestion_date={ingestion_date}"
    )

    hdfs("-mkdir", "-p", bronze_dir)

    destination = f"{bronze_dir}/{file_path.name}"

    try:
        hdfs("-put", str(file_path), destination)

        audit["storage_path"] = destination
        audit["status"] = "SUCCESS"

        print(f"[SUCCESS] {destination}")

        # Crear marcador de checksum
        marker_data = {
            "checksum_sha256": checksum,
            "source_system": source.upper(),
            "dataset_name": dataset,
            "file_name": file_path.name,
            "first_ingestion_run": run_id,
            "storage_path": destination,
        }

        write_json_to_hdfs(
            marker_data,
            checksum_marker
        )

    except Exception as exc:

        audit["status"] = "FAILED"
        audit["error_message"] = str(exc)

        print(f"[FAILED] {exc}")

    # --------------------------------------------------------
    # 5. AUDITORÍA
    # --------------------------------------------------------

    audit_name = (
        f"{run_id}_{source}_{dataset}_{audit['status']}.json"
    )

    audit_path = (
        f"{HDFS_ROOT}/metadata/ingestion/{audit_name}"
    )

    write_json_to_hdfs(
        audit,
        audit_path
    )

    return audit


# ============================================================
# MAIN
# ============================================================

def main():

    now = datetime.now()

    run_id = now.strftime("RUN_%Y%m%d_%H%M%S")
    ingestion_date = now.strftime("%Y-%m-%d")

    print("=" * 65)
    print(" BVL ANALYTICS - INGESTA BRONZE")
    print("=" * 65)
    print(f"Run ID         : {run_id}")
    print(f"Fecha ingesta  : {ingestion_date}")
    print(f"Datasets       : {len(DATASETS)}")
    print("=" * 65)

    # Crear zonas necesarias
    hdfs("-mkdir", "-p", f"{HDFS_ROOT}/bronze")
    hdfs("-mkdir", "-p", f"{HDFS_ROOT}/quarantine")
    hdfs("-mkdir", "-p", f"{HDFS_ROOT}/metadata/ingestion")

    results = []

    for dataset in DATASETS:
        result = process_dataset(
            dataset,
            run_id,
            ingestion_date
        )
        results.append(result)

    # --------------------------------------------------------
    # RESUMEN
    # --------------------------------------------------------

    statuses = {}

    for result in results:
        status = result["status"]
        statuses[status] = statuses.get(status, 0) + 1

    print()
    print("=" * 65)
    print(" RESUMEN DE INGESTA")
    print("=" * 65)

    for status, count in statuses.items():
        print(f"{status:<20}: {count}")

    print("=" * 65)

    if statuses.get("FAILED", 0) > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()


