#!/usr/bin/env python3
"""Nucleo de transferencia Bronze. Sin ejecutor Cloud habilitado."""

import hashlib
import re
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

BUCKET = "bvl26-bronze-7f3a9c"
IDENTIDAD = (
    "sa-bvl-ingestion@bvl-analytics-2026.iam.gserviceaccount.com"
)


def sha256_local(ruta):
    digest = hashlib.sha256()
    total = 0

    with Path(ruta).open("rb") as archivo:
        while True:
            bloque = archivo.read(1024 * 1024)
            if not bloque:
                break
            digest.update(bloque)
            total += len(bloque)

    return digest.hexdigest(), total


def validar_operacion(op):
    campos = (
        "run_id", "dataset", "origen",
        "destino", "size_bytes", "sha256"
    )

    if not isinstance(op, dict):
        raise ValueError("OPERACION_INVALIDA")

    if any(c not in op for c in campos):
        raise ValueError("CAMPOS_INCOMPLETOS")

    run = op["run_id"]
    dataset = op["dataset"]

    if not isinstance(run, str) or not re.fullmatch(
        r"bvl-bronze-inicial-\d{8}-\d{3}", run
    ):
        raise ValueError("RUN_ID_INVALIDO")

    if not isinstance(dataset, str) or not re.fullmatch(
        r"(bcrp|smv)/[a-z0-9_]+", dataset
    ):
        raise ValueError("DATASET_INVALIDO")

    prefijo_hdfs = f"/datalake/bvl/bronze/{dataset}/"
    origen = op["origen"]

    if not isinstance(origen, str) or not origen.startswith(prefijo_hdfs):
        raise ValueError("ORIGEN_INVALIDO")

    relativo = origen[len(prefijo_hdfs):]

    if not relativo or any(
        x in ("", ".", "..") for x in relativo.split("/")
    ):
        raise ValueError("RUTA_INSEGURA")

    destino = urlsplit(op["destino"])
    objeto = (
        f"landing/{dataset}/run_id={run}/{relativo}"
    )

    if (
        destino.scheme != "gs"
        or destino.netloc != BUCKET
        or destino.path.lstrip("/") != objeto
        or destino.query
        or destino.fragment
    ):
        raise ValueError("DESTINO_NO_AUTORIZADO")

    if (
        type(op["size_bytes"]) is not int
        or op["size_bytes"] <= 0
        or not isinstance(op["sha256"], str)
        or not re.fullmatch(r"[0-9a-f]{64}", op["sha256"])
    ):
        raise ValueError("INTEGRIDAD_DECLARADA_INVALIDA")

    return objeto


def transferir_archivo(op, cliente_gcs):
    """
    Funcion para el futuro ejecutor autorizado.

    Precondiciones externas obligatorias:
    - Reserva Quality verificada.
    - Run ID operativo autorizado.
    - Plan aprobado, inmutable y vinculado a la reserva.
    - Cliente GCS autenticado como sa-bvl-ingestion.

    No crea reservas ni publica Bronze oficial.
    """
    from google.api_core.exceptions import PreconditionFailed, NotFound

    objeto = validar_operacion(op)

    metadatos = {
        "bvl_run_id": op["run_id"],
        "bvl_dataset": op["dataset"],
        "bvl_sha256": op["sha256"],
    }

    with tempfile.TemporaryDirectory(prefix="bvl_transfer_") as tmp:
        original = Path(tmp) / "origen.bin"
        verificado = Path(tmp) / "verificado.bin"

        resultado = subprocess.run(
            ["hdfs", "dfs", "-get", op["origen"], str(original)],
            capture_output=True,
            text=True,
            timeout=900,
        )

        if resultado.returncode != 0:
            raise RuntimeError(
                "LECTURA_HDFS_FALLIDA: " + resultado.stderr[-300:]
            )

        sha, tamano = sha256_local(original)

        if sha != op["sha256"] or tamano != op["size_bytes"]:
            raise RuntimeError("INTEGRIDAD_HDFS_NO_COINCIDE")

        blob = cliente_gcs.bucket(BUCKET).blob(objeto)
        blob.metadata = metadatos

        estado = "TRANSFERIDO"

        try:
            blob.upload_from_filename(
                str(original),
                content_type="application/octet-stream",
                if_generation_match=0,
                checksum="auto",
                timeout=180,
            )
        except PreconditionFailed:
            # No sobrescribir: releer y conciliar.
            try:
                blob.reload(timeout=60)
            except NotFound as error:
                raise RuntimeError(
                    "CONFLICTO_412_REQUIERE_QUALITY"
                ) from error

            remoto_meta = blob.metadata or {}

            if any(
                remoto_meta.get(k) != v
                for k, v in metadatos.items()
            ):
                raise RuntimeError("CONFLICTO_IDENTIDAD_GCS")

            estado = "REINTENTO_EXISTENTE"

        if blob.generation is None:
            blob.reload(timeout=60)

        generacion = int(blob.generation)

        # Se verifica el contenido descargado, no solo metadata.
        remoto = cliente_gcs.bucket(BUCKET).blob(
            objeto, generation=generacion
        )

        remoto.download_to_filename(
            str(verificado),
            if_generation_match=generacion,
            timeout=180,
        )

        sha_gcs, bytes_gcs = sha256_local(verificado)

        if (
            sha_gcs != op["sha256"]
            or bytes_gcs != op["size_bytes"]
        ):
            raise RuntimeError("INTEGRIDAD_GCS_NO_COINCIDE")

        return {
            "run_id": op["run_id"],
            "dataset": op["dataset"],
            "origen_hdfs": op["origen"],
            "destino_gcs": op["destino"],
            "bytes_esperados": op["size_bytes"],
            "bytes_observados": bytes_gcs,
            "sha256_esperado": op["sha256"],
            "sha256_gcs": sha_gcs,
            "generacion_gcs": generacion,
            "identidad_esperada": IDENTIDAD,
            "estado": estado,
            "validacion": "INTEGRIDAD_VERIFICADA",
        }


if __name__ == "__main__":
    raise SystemExit(
        "BLOQUEADO: requiere orquestador Quality autorizado"
    )
