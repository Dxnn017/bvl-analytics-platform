"""Validación local de contratos candidatos Silver–Gold."""

import hashlib
import json
from pathlib import Path

from consumidor_versiones_gold import VersionGoldError

PREFIJO = "gs://bvl26-silver-7f3a9c/versions/"


def validar_publicacion_local(
    ruta_current, ruta_manifest, sha256_confiable,
    generacion_confiable, version_previa=None
):
    """Valida consistencia local sin autorizar consumo oficial."""

    current = json.loads(Path(ruta_current).read_text())
    contenido = Path(ruta_manifest).read_bytes()
    manifest = json.loads(contenido)

    if not isinstance(sha256_confiable, str) or len(sha256_confiable) != 64:
        raise VersionGoldError("SHA-256 confiable inválido.")

    if hashlib.sha256(contenido).hexdigest() != sha256_confiable.lower():
        raise VersionGoldError("Hash del manifest incorrecto.")

    if not isinstance(generacion_confiable, str) or not generacion_confiable.isdigit():
        raise VersionGoldError("Generación confiable inválida.")

    if str(current.get("generacion_manifest")) != generacion_confiable:
        raise VersionGoldError("Generación del manifest incompatible.")

    version = current.get("version_id")

    if not isinstance(version, str) or not version:
        raise VersionGoldError("Versión ausente.")

    if manifest.get("version_publicada") != version:
        raise VersionGoldError("Versiones incompatibles.")

    if version_previa is not None:
        if manifest.get("version_anterior") != version_previa:
            raise VersionGoldError("Continuidad de versiones inválida.")

    uri = current.get("version_uri")

    if not isinstance(uri, str) or not uri.startswith(PREFIJO):
        raise VersionGoldError("Ruta fuera de Silver/versions.")

    if ".." in uri.split("/") or "/staging/" in uri:
        raise VersionGoldError("Ruta no permitida.")

    return {
        "version_id": version,
        "version_uri": uri,
        "hash_local_correcto": True,
        "continuidad_local_correcta": True,
        "firma_gcs_verificada": False,
        "consumo_productivo_autorizado": False,
        "publicacion_autorizada": False,
    }
