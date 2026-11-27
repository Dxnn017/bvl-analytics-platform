"""Flujo Silver–Gold candidato. Solo devuelve datos validados localmente."""

import json

from consumidor_gcs_seguro import consumir_objeto_verificado


class FlujoSilverGoldError(ValueError):
    pass


def preparar_consumo_silver(
    cliente,
    referencia_current,
    firma_current,
    referencia_manifest,
    firma_manifest,
    clave_publica_confiable,
    autorizacion,
):
    """Comprueba autorización, firma, integridad y vínculo de versión."""

    if not isinstance(autorizacion, dict):
        raise FlujoSilverGoldError("Autorización ausente.")

    if autorizacion.get("autorizacion_gold") is not True:
        raise FlujoSilverGoldError("Consumo Gold no autorizado.")

    producto = autorizacion.get("producto")
    version = autorizacion.get("version_id")

    if not producto or not version:
        raise FlujoSilverGoldError("Producto o versión no autorizados.")

    current_bytes = consumir_objeto_verificado(
        cliente, referencia_current, clave_publica_confiable, firma_current
    )
    manifest_bytes = consumir_objeto_verificado(
        cliente, referencia_manifest, clave_publica_confiable, firma_manifest
    )

    try:
        current = json.loads(current_bytes)
        manifest = json.loads(manifest_bytes)
    except (ValueError, UnicodeDecodeError) as error:
        raise FlujoSilverGoldError("JSON inválido.") from error

    if (
        current.get("producto") != producto
        or manifest.get("producto") != producto
    ):
        raise FlujoSilverGoldError("Producto incompatible.")

    if (
        current.get("version_id") != version
        or manifest.get("version_id") != version
    ):
        raise FlujoSilverGoldError("Versión incompatible.")

    if current.get("manifest_uri") != referencia_manifest["uri"]:
        raise FlujoSilverGoldError("Manifest no vinculado al current.")

    if current.get("manifest_generacion") != referencia_manifest["generation"]:
        raise FlujoSilverGoldError("Generación del manifest incompatible.")

    if current.get("manifest_sha256") != referencia_manifest["sha256"]:
        raise FlujoSilverGoldError("Hash del manifest incompatible.")

    return {
        "producto": producto,
        "version_id": version,
        "manifest": manifest,
        "current": current,
        "verificado_localmente": True,
        "consumo_productivo_autorizado": False,
    }
