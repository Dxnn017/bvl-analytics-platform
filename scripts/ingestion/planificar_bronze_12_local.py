#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
import tarfile
from datetime import datetime
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
args = parser.parse_args()

run_id = args.run_id

match = re.fullmatch(
    r"bvl-bronze-inicial-(\d{8})-(\d{3})",
    run_id
)

if not match:
    parser.error("run_id invalido")

try:
    datetime.strptime(match.group(1), "%Y%m%d")
except ValueError:
    parser.error("Fecha invalida")

root = Path(__file__).resolve().parents[2]
base = root / "outputs/quality/bronze_runid_lab"

paquete = (
    Path.home() / "Descargas" /
    "BVL_CONTROL_BRONZE_10356.tar.gz"
)

checksum = Path(str(paquete) + ".sha256")

hash_paquete = hashlib.sha256(
    paquete.read_bytes()
).hexdigest()

assert hash_paquete == checksum.read_text().split()[0]

control = json.loads(
    (base / f"CONTROL_OPERATIVO_{run_id}.json")
    .read_text(encoding="utf-8")
)

assert control["run_id"] == run_id
assert control["status"] == "CANDIDATO"
assert control["validation_approved"] is False
assert control["publication_status"] == "NO_PUBLICADO"
assert len(control["datasets"]) == 12

operaciones = []
destinos = set()
total_bytes = 0

with tarfile.open(paquete, "r:gz") as tar:
    original = tar.extractfile(
        "CONTROL_LOTE_BRONZE_CANDIDATO.json"
    ).read()

    assert hashlib.sha256(original).hexdigest() == (
        control["control_original_sha256"]
    )

    for d in control["datasets"]:
        assert d["run_id_operativo"] == run_id

        contenido = tar.extractfile(
            d["manifest_file"]
        ).read()

        assert hashlib.sha256(contenido).hexdigest() == (
            d["manifest_sha256"]
        )

        manifest = json.loads(contenido)
        assert manifest["run_id"] is None
        assert len(manifest["files"]) == d["expected_file_count"]

        bytes_dataset = 0

        for archivo in manifest["files"]:
            origen = archivo["source_hdfs"]
            plantilla = archivo["landing_gcs_uri_template"]

            assert origen.startswith("/datalake/bvl/bronze/")
            assert plantilla.count("{run_id}") == 1

            destino = plantilla.replace("{run_id}", run_id)

            assert destino.startswith(
                d["landing_prefix_operativo"]
            )
            assert destino not in destinos
            assert re.fullmatch(
                r"[0-9a-f]{64}", archivo["sha256"]
            )
            assert archivo["size_bytes"] > 0

            destinos.add(destino)
            bytes_dataset += archivo["size_bytes"]

            operaciones.append({
                "run_id": run_id,
                "dataset": d["dataset"],
                "origen": origen,
                "destino": destino,
                "size_bytes": archivo["size_bytes"],
                "sha256": archivo["sha256"],
                "estado": "PLANIFICADO_NO_EJECUTADO"
            })

        assert bytes_dataset == d["expected_size_bytes"]
        total_bytes += bytes_dataset

assert len(operaciones) == control["file_count"] == 3599
assert len(destinos) == 3599
assert total_bytes == control["total_size_bytes"] == 393623640

plan = {
    "fase": "3.2.9",
    "tipo": "PLAN_LOCAL_BRONZE",
    "run_id": run_id,
    "paquete_sha256": hash_paquete,
    "datasets": 12,
    "archivos": len(operaciones),
    "bytes": total_bytes,
    "transferencias": 0,
    "publicacion": "NO_PUBLICADO",
    "operaciones": operaciones
}

salida = base / f"PLAN_BRONZE_12_{run_id}.json"

if salida.exists():
    anterior = json.loads(salida.read_text(encoding="utf-8"))
    assert anterior == plan, "CONFLICTO: plan diferente"
    print("REINTENTO_IDEMPOTENTE: OK")
else:
    with salida.open("x", encoding="utf-8") as f:
        json.dump(plan, f, indent=2, ensure_ascii=False)
    print("PLAN_CREADO: OK")

print("RUN_ID:", run_id)
print("DATASETS:", 12)
print("ARCHIVOS:", len(operaciones))
print("DESTINOS_UNICOS:", len(destinos))
print("TRANSFERENCIAS:", 0)
print("RESULTADO: PLAN_LOCAL_OK")
