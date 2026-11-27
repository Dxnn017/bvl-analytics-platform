"""Consumidor Silver GCS seguro para integración local supervisada.

No autoriza consumo productivo ni modifica registros.
"""

import hashlib
import re

from adaptador_gcs_quality import leer_objeto_gcs
from verificar_firma_quality import verificar_firma_ecdsa


class ConsumidorSeguroError(ValueError):
    pass


def consumir_objeto_verificado(cliente, referencia, clave_publica, firma):
    """Devuelve bytes únicamente después de verificar integridad y firma."""

    if not isinstance(referencia, dict):
        raise ConsumidorSeguroError("Referencia inválida.")

    requeridos = {"uri", "generation", "sha256", "size_bytes"}
    if not requeridos.issubset(referencia):
        raise ConsumidorSeguroError("Referencia incompleta.")

    uri = referencia["uri"]

    if not isinstance(uri, str) or "/staging/" in uri:
        raise ConsumidorSeguroError("Staging prohibido.")

    if not uri.startswith((
        "gs://bvl26-silver-7f3a9c/versions/",
        "gs://bvl26-control-7f3a9c/",
    )):
        raise ConsumidorSeguroError("Ruta no autorizada.")

    sha = referencia["sha256"]
    tamano = referencia["size_bytes"]

    if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{64}", sha):
        raise ConsumidorSeguroError("SHA-256 inválido.")

    if type(tamano) is not int or tamano < 0:
        raise ConsumidorSeguroError("Tamaño inválido.")

    if not clave_publica or not firma:
        raise ConsumidorSeguroError("Firma o clave de confianza ausente.")

    contenido = leer_objeto_gcs(
        cliente, uri, referencia["generation"]
    )

    if len(contenido) != tamano:
        raise ConsumidorSeguroError("Tamaño incompatible.")

    if hashlib.sha256(contenido).hexdigest() != sha:
        raise ConsumidorSeguroError("SHA-256 incompatible.")

    verificar_firma_ecdsa(contenido, firma, clave_publica)

    return contenido
