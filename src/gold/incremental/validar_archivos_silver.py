"""Verificación de archivos Silver con referencias oficiales v1.

Cliente GCS inyectado. No concede autorización productiva.
"""

import hashlib
import re

from adaptador_gcs_quality import leer_objeto_gcs


class ArchivoSilverError(ValueError):
    pass


def validar_archivos_silver(cliente, files, version_prefix):
    if not isinstance(files, list) or not files:
        raise ArchivoSilverError("Lista de archivos inválida")

    if (
        not isinstance(version_prefix, str)
        or not version_prefix.startswith(
            "gs://bvl26-silver-7f3a9c/versions/"
        )
        or "/staging/" in version_prefix
        or not version_prefix.endswith("/")
    ):
        raise ArchivoSilverError("Prefijo de versión inválido")

    rutas = set()
    resultados = []

    for ref in files:
        if not isinstance(ref, dict):
            raise ArchivoSilverError("Referencia inválida")

        uri = ref.get("uri")
        generation = ref.get("generation")
        size = ref.get("size_bytes")
        sha = ref.get("sha256")

        if (
            not isinstance(uri, str)
            or not uri.startswith(version_prefix)
            or uri == version_prefix
            or ".." in uri.split("/")
            or uri in rutas
        ):
            raise ArchivoSilverError(
                "Archivo fuera de versión o duplicado"
            )

        if (
            not isinstance(generation, str)
            or not re.fullmatch(r"[1-9][0-9]*", generation)
        ):
            raise ArchivoSilverError("Generation inválida")

        if type(size) is not int or size <= 0:
            raise ArchivoSilverError("Size_bytes inválido")

        if (
            not isinstance(sha, str)
            or not re.fullmatch(r"[0-9a-f]{64}", sha)
        ):
            raise ArchivoSilverError("SHA-256 inválido")

        rutas.add(uri)

        contenido = leer_objeto_gcs(
            cliente, uri, generation
        )

        if len(contenido) != size:
            raise ArchivoSilverError("Tamaño incompatible")

        if hashlib.sha256(contenido).hexdigest() != sha:
            raise ArchivoSilverError("SHA-256 incompatible")

        resultados.append({
            "uri": uri,
            "generation": generation,
            "integridad_verificada": True,
        })

    return {
        "archivos_validados": len(resultados),
        "resultados": resultados,
        "consumo_productivo_autorizado": False,
    }
