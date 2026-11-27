"""Lectura GCS por generación exacta, sin publicación ni escrituras."""

import re


class LecturaGCSQualityError(RuntimeError):
    pass


def leer_objeto_gcs(cliente, uri, generacion):
    """Lee bytes de un objeto GCS inmutable por generación.

    El cliente debe ser suministrado por el llamador; admite mocks.
    """

    patron = r"^gs://([^/]+)/(.+)$"
    coincidencia = re.fullmatch(patron, uri or "")

    if not coincidencia:
        raise LecturaGCSQualityError("URI GCS inválida.")

    bucket_nombre, objeto_nombre = coincidencia.groups()

    if bucket_nombre not in {
        "bvl26-silver-7f3a9c",
        "bvl26-control-7f3a9c",
    }:
        raise LecturaGCSQualityError("Bucket no autorizado.")

    if (
        "/staging/" in "/" + objeto_nombre
        or ".." in objeto_nombre.split("/")
    ):
        raise LecturaGCSQualityError("Objeto GCS no permitido.")

    if (
        not isinstance(generacion, str)
        or not generacion.isdigit()
        or int(generacion) <= 0
    ):
        raise LecturaGCSQualityError("Generación inválida.")

    try:
        bucket = cliente.bucket(bucket_nombre)
        blob = bucket.blob(
            objeto_nombre,
            generation=int(generacion),
        )
        contenido = blob.download_as_bytes(
            if_generation_match=int(generacion)
        )

    except Exception as error:
        codigo = getattr(error, "code", None)
        if callable(codigo):
            codigo = codigo()

        if codigo == 403:
            raise LecturaGCSQualityError(
                "GCS_403: acceso denegado."
            ) from error
        if codigo == 404:
            raise LecturaGCSQualityError(
                "GCS_404: objeto o generación inexistente."
            ) from error
        if codigo == 412:
            raise LecturaGCSQualityError(
                "GCS_412: generación modificada."
            ) from error

        raise LecturaGCSQualityError(
            "Fallo de lectura GCS."
        ) from error

    return contenido
