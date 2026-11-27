"""Valida particiones Hive en rutas Silver autorizadas."""

import re


class ParticionesSilverError(ValueError):
    pass


def validar_particiones_uri(uri, prefijo, columnas):
    if not isinstance(uri, str) or not isinstance(prefijo, str):
        raise ParticionesSilverError("Ruta inválida")

    if not prefijo.startswith(
        "gs://bvl26-silver-7f3a9c/versions/"
    ) or not uri.startswith(prefijo):
        raise ParticionesSilverError("Archivo fuera de versión")

    if not isinstance(columnas, list) or len(columnas) != len(set(columnas)):
        raise ParticionesSilverError("Columnas inválidas")

    resto = uri[len(prefijo):]
    partes = resto.split("/")

    if len(partes) != len(columnas) + 1:
        raise ParticionesSilverError("Estructura de particiones inválida")

    if not partes[-1].endswith(".parquet"):
        raise ParticionesSilverError("Archivo no Parquet")

    valores = {}

    for columna, segmento in zip(columnas, partes[:-1]):
        patron = rf"{re.escape(columna)}=([0-9]+)"
        coincidencia = re.fullmatch(patron, segmento)

        if not coincidencia:
            raise ParticionesSilverError(
                f"Partición inválida: {columna}"
            )

        valores[columna] = int(coincidencia.group(1))

    return valores
