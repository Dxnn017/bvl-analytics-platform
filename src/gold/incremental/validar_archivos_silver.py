"""Validación de archivos Silver sin ejecutar publicaciones."""

import hashlib
import re

from adaptador_gcs_quality import leer_objeto_gcs


class ArchivoSilverError(ValueError):
    pass


def validar_archivos_silver(cliente, archivos):
    if not isinstance(archivos, list) or not archivos:
        raise ArchivoSilverError("Lista de archivos vacía o inválida.")

    resultados = []

    for archivo in archivos:
        if not isinstance(archivo, dict):
            raise ArchivoSilverError("Referencia inválida.")

        uri = archivo.get("uri")
        generacion = archivo.get("generacion")
        tamano = archivo.get("tamano_bytes")
        sha = archivo.get("sha256")

        if not isinstance(uri, str) or not uri.startswith(
            "gs://bvl26-silver-7f3a9c/versions/"
        ):
            raise ArchivoSilverError("Archivo fuera de Silver/versions.")

        if type(tamano) is not int or tamano < 0:
            raise ArchivoSilverError("Tamaño inválido.")

        if not isinstance(sha, str) or not re.fullmatch(
            r"[0-9a-f]{64}", sha
        ):
            raise ArchivoSilverError("SHA-256 inválido.")

        contenido = leer_objeto_gcs(cliente, uri, generacion)

        if len(contenido) != tamano:
            raise ArchivoSilverError("Tamaño incompatible.")

        if hashlib.sha256(contenido).hexdigest() != sha:
            raise ArchivoSilverError("SHA-256 incompatible.")

        resultados.append({
            "uri": uri,
            "generacion": generacion,
            "integridad_verificada": True,
        })

    return {
        "archivos_validados": len(resultados),
        "resultados": resultados,
        "consumo_productivo_autorizado": False,
    }
