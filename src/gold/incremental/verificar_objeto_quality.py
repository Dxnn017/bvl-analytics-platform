"""Verificación local de integridad de objetos Silver.

No verifica generaciones contra GCS ni autoriza consumo productivo.
"""

import hashlib
from pathlib import Path


class IntegridadObjetoError(ValueError):
    pass


def verificar_objeto_local(
    archivo,
    tamano_esperado,
    sha256_esperado,
    generacion_esperada,
    generacion_observada,
):
    if type(tamano_esperado) is not int or tamano_esperado < 0:
        raise IntegridadObjetoError("Tamaño esperado inválido.")

    if (
        not isinstance(sha256_esperado, str)
        or len(sha256_esperado) != 64
    ):
        raise IntegridadObjetoError("SHA-256 esperado inválido.")

    if not all(
        isinstance(g, str) and g.isdigit() and int(g) > 0
        for g in (generacion_esperada, generacion_observada)
    ):
        raise IntegridadObjetoError("Generación inválida.")

    if generacion_esperada != generacion_observada:
        raise IntegridadObjetoError("Generación incompatible.")

    contenido = Path(archivo).read_bytes()

    if len(contenido) != tamano_esperado:
        raise IntegridadObjetoError("Tamaño incompatible.")

    if hashlib.sha256(contenido).hexdigest() != sha256_esperado.lower():
        raise IntegridadObjetoError("SHA-256 incompatible.")

    return {
        "integridad_local_correcta": True,
        "generacion_declarada_coincide": True,
        "generacion_gcs_verificada": False,
        "consumo_productivo_autorizado": False,
    }
