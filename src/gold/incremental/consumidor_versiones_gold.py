"""Consumidor local Silver-Gold candidato. Sin acceso GCS ni publicación."""

from pathlib import Path
import hashlib
import json

BUCKET = "gs://bvl26-silver-7f3a9c/versions/"
BLOQUEADOS = {"hechos_empresa", "impacto_hechos_valor"}


class VersionGoldError(ValueError):
    pass


def cargar_json_local(ruta):
    return json.loads(Path(ruta).read_text(encoding="utf-8"))


def verificar_manifest_local(
    archivo_manifest,
    sha256_esperado,
    version_esperada,
    generacion_verificada,
):
    """Comprueba integridad local contra metadatos externos.

    No verifica firmas ni generaciones directamente en GCS.
    """
    if not isinstance(sha256_esperado, str) or len(sha256_esperado) != 64:
        raise VersionGoldError("SHA-256 esperado obligatorio.")

    if not isinstance(generacion_verificada, str) or not generacion_verificada.isdigit():
        raise VersionGoldError("Generación GCS verificada obligatoria.")

    contenido = Path(archivo_manifest).read_bytes()
    huella = hashlib.sha256(contenido).hexdigest()

    if huella != sha256_esperado.lower():
        raise VersionGoldError("SHA-256 del manifest no coincide.")

    manifest = json.loads(contenido)

    if manifest.get("version_publicada") != version_esperada:
        raise VersionGoldError("Versión Silver incompatible.")

    return manifest


def resolver_fuente_gold(producto_gold, fuente, habilitacion):
    """Resuelve únicamente versiones explícitamente habilitadas."""

    if producto_gold in BLOQUEADOS:
        raise VersionGoldError(
            f"Producto Gold bloqueado por Quality: {producto_gold}"
        )

    if not isinstance(habilitacion, dict):
        raise VersionGoldError("Habilitación inválida.")

    if habilitacion.get("autorizacion_gold") is not True:
        raise VersionGoldError("Consumo Gold no autorizado.")

    if habilitacion.get("producto_silver") != fuente:
        raise VersionGoldError("Fuente Silver incompatible.")

    uri = habilitacion.get("version_uri")
    version = habilitacion.get("version_id")

    if not isinstance(uri, str) or not uri.startswith(BUCKET):
        raise VersionGoldError("La versión debe estar en Silver/versions.")

    if not isinstance(version, str) or not version.strip():
        raise VersionGoldError("Identificador de versión obligatorio.")

    if "/staging/" in uri or "/../" in uri:
        raise VersionGoldError("Ruta Silver no permitida.")

    if not habilitacion.get("manifest_verificado"):
        raise VersionGoldError("Manifest oficial no verificado.")

    if not habilitacion.get("firma_verificada"):
        raise VersionGoldError("Firma oficial no verificada.")

    if not habilitacion.get("generacion_verificada"):
        raise VersionGoldError("Generación oficial no verificada.")

    return {
        "producto_gold": producto_gold,
        "producto_silver": fuente,
        "version_id": version,
        "version_uri": uri,
        "publicacion_gold_autorizada": False,
    }
