"""Validador del marcador conjunto Silver (contrato candidato)."""

import json
from verificar_firma_quality import verificar_firma_ecdsa

PRODUCTOS = {
    "referencias_smv/puente_ruc_nemonico",
    "referencias_smv/valores_inscritos",
}


def validar_marcador_conjunto(contenido, firma, clave_publica):
    """Valida firma y correspondencia de ambas versiones."""

    verificar_firma_ecdsa(contenido, firma, clave_publica)
    marcador = json.loads(contenido)

    if marcador.get("estado") != "PUBLICACION_CONJUNTA_APROBADA":
        raise ValueError("Marcador no aprobado.")

    versiones = marcador.get("versiones")

    if not isinstance(versiones, list) or len(versiones) != 2:
        raise ValueError("Publicación conjunta incompleta.")

    productos = {v.get("producto") for v in versiones}

    if productos != PRODUCTOS:
        raise ValueError("Productos conjuntos incompatibles.")

    for version in versiones:
        if not version.get("version_id") or not version.get("manifest_sha256"):
            raise ValueError("Versión sin integridad declarada.")

    if not marcador.get("autorizacion_gold"):
        raise ValueError("Consumo Gold no autorizado.")

    return {
        "marcador_valido_local": True,
        "productos": sorted(productos),
        "publicacion_gold_autorizada": False,
    }
