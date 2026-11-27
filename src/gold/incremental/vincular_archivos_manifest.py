"""Vinculación de archivos al manifest Silver oficial autenticado."""

from validar_archivos_silver import validar_archivos_silver


class VinculacionManifestError(ValueError):
    pass


def validar_archivos_del_manifest(cliente, manifest, version_autorizada):
    if not isinstance(manifest, dict):
        raise VinculacionManifestError("Manifest inválido")

    if (
        not isinstance(version_autorizada, str)
        or not version_autorizada.strip()
        or manifest.get("version_id") != version_autorizada
    ):
        raise VinculacionManifestError("Versión incompatible")

    files = manifest.get("files")
    version_prefix = manifest.get("version_prefix")

    if not isinstance(files, list) or not files:
        raise VinculacionManifestError("Manifest sin archivos")

    if (
        not isinstance(version_prefix, str)
        or not version_prefix.endswith("/")
        or "/staging/" in version_prefix
    ):
        raise VinculacionManifestError("Prefijo inválido")

    resultado = validar_archivos_silver(
        cliente, files, version_prefix
    )

    return {
        "version_id": version_autorizada,
        "archivos_validados": resultado["archivos_validados"],
        "integridad_verificada": True,
        "consumo_productivo_autorizado": False,
    }
