"""Vincula archivos Silver a un manifest previamente verificado."""

from validar_archivos_silver import validar_archivos_silver


class VinculacionManifestError(ValueError):
    pass


def validar_archivos_del_manifest(cliente, manifest, version_autorizada):
    if not isinstance(manifest, dict):
        raise VinculacionManifestError("Manifest inválido.")

    if not isinstance(version_autorizada, str) or not version_autorizada:
        raise VinculacionManifestError("Versión autorizada ausente.")

    if manifest.get("version_id") != version_autorizada:
        raise VinculacionManifestError("Versión incompatible.")

    archivos = manifest.get("archivos")

    if not isinstance(archivos, list) or not archivos:
        raise VinculacionManifestError("Manifest sin archivos.")

    rutas = [a.get("uri") for a in archivos if isinstance(a, dict)]

    if len(rutas) != len(archivos) or len(set(rutas)) != len(rutas):
        raise VinculacionManifestError("Archivos duplicados o inválidos.")

    resultado = validar_archivos_silver(cliente, archivos)

    return {
        "version_id": version_autorizada,
        "archivos_validados": resultado["archivos_validados"],
        "integridad_local_correcta": True,
        "consumo_productivo_autorizado": False,
    }
