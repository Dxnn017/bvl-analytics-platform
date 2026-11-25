#!/usr/bin/env python3
"""Motor Bronze HDFS→GCS con integridad, no sobrescritura y trazabilidad.

SEGURIDAD: Paso 101.7 vigente. La entrada operativa SIEMPRE rechaza hasta
que Quality acuerde y autorice un verificador autenticado del release.
La función interna se prueba con dobles de HDFS/GCS; no es una vía de
liberación ni sustituye IAM, auditoría o firma de Quality.
"""

import hashlib
import re
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

TRANSFERENCIAS_HABILITADAS = False

BUCKET = "bvl26-bronze-7f3a9c"
CONTROL_BUCKET = "bvl26-control-7f3a9c"
IDENTIDAD = "sa-bvl-ingestion@bvl-analytics-2026.iam.gserviceaccount.com"
HEX = re.compile(r"[0-9a-f]{64}\Z")
RELEASE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{5,127}\Z")


def sha256_local(ruta):
    digest = hashlib.sha256()
    total = 0
    with Path(ruta).open("rb") as archivo:
        for bloque in iter(lambda: archivo.read(1024 * 1024), b""):
            digest.update(bloque)
            total += len(bloque)
    return digest.hexdigest(), total


def validar_operacion(op):
    campos = ("run_id", "dataset", "origen", "destino", "size_bytes", "sha256")
    if not isinstance(op, dict):
        raise ValueError("OPERACION_INVALIDA")
    if any(c not in op for c in campos):
        raise ValueError("CAMPOS_INCOMPLETOS")

    run, dataset = op["run_id"], op["dataset"]
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
    if not relativo or any(x in ("", ".", "..") for x in relativo.split("/")):
        raise ValueError("RUTA_INSEGURA")

    destino = urlsplit(op["destino"])
    objeto = f"landing/{dataset}/run_id={run}/{relativo}"
    if (destino.scheme != "gs" or destino.netloc != BUCKET
            or destino.path.lstrip("/") != objeto
            or destino.query or destino.fragment):
        raise ValueError("DESTINO_NO_AUTORIZADO")

    if (type(op["size_bytes"]) is not int or op["size_bytes"] <= 0
            or not isinstance(op["sha256"], str)
            or not HEX.fullmatch(op["sha256"])):
        raise ValueError("INTEGRIDAD_DECLARADA_INVALIDA")
    return objeto


def validar_trazabilidad(op, contexto):
    """Valida coherencia de referencias, NO autentica el release de Quality.

    El contexto debe provenir del verificador oficial de Quality (AÚN NO
    ACORDADO). Pasar un dict o hashes coincidentes no constituye permiso.
    """
    if not isinstance(contexto, dict):
        raise ValueError("CONTEXTO_CONTROL_AUSENTE")
    if contexto.get("run_id") != op["run_id"]:
        raise ValueError("CONTEXTO_RUN_DISTINTO")
    plan_sha = contexto.get("plan_sha256")
    if not isinstance(plan_sha, str) or not HEX.fullmatch(plan_sha):
        raise ValueError("CONTEXTO_PLAN_SHA")
    release_id = contexto.get("release_id")
    if not isinstance(release_id, str) or not RELEASE.fullmatch(release_id):
        raise ValueError("CONTEXTO_RELEASE_ID")
    alcance = contexto.get("alcance")
    if alcance not in ("piloto", "completo"):
        raise ValueError("CONTEXTO_ALCANCE")

    metadatos = {
        "bvl_run_id": op["run_id"],
        "bvl_dataset": op["dataset"],
        "bvl_sha256": op["sha256"],
        "bvl_plan_sha256": plan_sha,
        "bvl_release_id": release_id,
        "bvl_alcance": alcance,
    }
    referencias = {}
    for tipo, nombre in (("reservation", "reservations"),
                         ("approval", "approvals")):
        ref = contexto.get(tipo)
        if not isinstance(ref, dict):
            raise ValueError("CONTEXTO_REFERENCIA_" + tipo)
        uri = f"gs://{CONTROL_BUCKET}/{nombre}/bronze/{op['run_id']}.json"
        if ref.get("uri") != uri:
            raise ValueError("CONTEXTO_URI_" + tipo)
        gen, sha = ref.get("generation"), ref.get("sha256")
        if type(gen) is not int or gen <= 0:
            raise ValueError("CONTEXTO_GENERACION_" + tipo)
        if not isinstance(sha, str) or not HEX.fullmatch(sha):
            raise ValueError("CONTEXTO_SHA_" + tipo)
        metadatos[f"bvl_{tipo}_generation"] = str(gen)
        metadatos[f"bvl_{tipo}_sha256"] = sha
        referencias[tipo] = {"uri": uri, "generation": gen, "sha256": sha}
    return metadatos, referencias


def _transferir_motor_interno(op, cliente_gcs, contexto):
    """Motor probado con dobles; NO autentica el release ni expone CLI.

    No llamarlo desde código de producción sin un verificador de release
    aprobado y autenticado. El bloqueo de paso 101.7 sigue siendo obligatorio.
    """
    if not TRANSFERENCIAS_HABILITADAS:
        raise PermissionError("BLOQUEADO|PASO_101_7|QUALITY_NO_AUTORIZO")

    objeto = validar_operacion(op)
    metadatos, referencias = validar_trazabilidad(op, contexto)
    from google.api_core.exceptions import NotFound, PreconditionFailed

    with tempfile.TemporaryDirectory(prefix="bvl_transfer_") as tmp:
        original = Path(tmp) / "origen.bin"
        verificado = Path(tmp) / "verificado.bin"
        resultado = subprocess.run(
            ["hdfs", "dfs", "-get", op["origen"], str(original)],
            capture_output=True, text=True, timeout=900,
        )
        if resultado.returncode != 0:
            raise RuntimeError("LECTURA_HDFS_FALLIDA: " + resultado.stderr[-300:])
        sha, tamano = sha256_local(original)
        if sha != op["sha256"] or tamano != op["size_bytes"]:
            raise RuntimeError("INTEGRIDAD_HDFS_NO_COINCIDE")

        blob = cliente_gcs.bucket(BUCKET).blob(objeto)
        blob.metadata = metadatos
        estado = "TRANSFERIDO"
        try:
            blob.upload_from_filename(
                str(original), content_type="application/octet-stream",
                if_generation_match=0, checksum="auto", timeout=180,
            )
        except PreconditionFailed:
            try:
                blob.reload(timeout=60)
            except NotFound as error:
                raise RuntimeError("CONFLICTO_412_REQUIERE_QUALITY") from error
            remoto_meta = blob.metadata or {}
            if any(remoto_meta.get(k) != v for k, v in metadatos.items()):
                raise RuntimeError("CONFLICTO_IDENTIDAD_GCS")
            estado = "REINTENTO_EXISTENTE"

        if blob.generation is None:
            blob.reload(timeout=60)
        generacion = int(blob.generation)
        remoto = cliente_gcs.bucket(BUCKET).blob(objeto, generation=generacion)
        remoto.download_to_filename(
            str(verificado), if_generation_match=generacion, timeout=180,
        )
        sha_gcs, bytes_gcs = sha256_local(verificado)
        if sha_gcs != op["sha256"] or bytes_gcs != op["size_bytes"]:
            raise RuntimeError("INTEGRIDAD_GCS_NO_COINCIDE")

        return {
            "run_id": op["run_id"], "dataset": op["dataset"],
            "origen_hdfs": op["origen"], "destino_gcs": op["destino"],
            "bytes_esperados": op["size_bytes"],
            "bytes_observados": bytes_gcs,
            "sha256_esperado": op["sha256"], "sha256_gcs": sha_gcs,
            "generacion_gcs": generacion,
            "identidad_esperada": IDENTIDAD,
            "estado": estado, "validacion": "INTEGRIDAD_VERIFICADA",
            "plan_sha256": contexto["plan_sha256"],
            "quality_release_id": contexto["release_id"],
            "alcance": contexto["alcance"],
            "reserva": referencias["reservation"],
            "aprobacion": referencias["approval"],
            "emisor_quality_autenticado_por_motor": False,
            "autorizacion_operativa_validada_por_motor": False,
        }


def transferir_archivo(op, cliente_gcs, contexto=None):
    """Entrada pública: cerrada hasta integrar el verificador OFICIAL de Quality.

    Ni establecer TRANSFERENCIAS_HABILITADAS=True, ni aportar un dict con
    approved=True / release_id desbloquean esta interfaz.
    """
    if not TRANSFERENCIAS_HABILITADAS:
        raise PermissionError("BLOQUEADO|PASO_101_7|QUALITY_NO_AUTORIZO")
    raise PermissionError("BLOQUEADO|RELEASE_QUALITY_AUTENTICADO_NO_IMPLEMENTADO")


if __name__ == "__main__":
    raise SystemExit("BLOQUEADO: requiere orquestador Quality autorizado")
