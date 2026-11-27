"""Validación preliminar de metadatos Silver oficiales.

La comparación física del esquema corresponde al validador Parquet.
"""

import pyarrow as pa


class EsquemaParticionesError(ValueError):
    pass


def validar_esquema_particiones(
    manifest, columnas_requeridas, particiones_requeridas
):
    if not isinstance(manifest, dict):
        raise EsquemaParticionesError("Manifest inválido")

    schema_texto = manifest.get("schema")
    particiones = manifest.get("partition_columns")

    if not isinstance(schema_texto, str) or not schema_texto.strip():
        raise EsquemaParticionesError("Schema Arrow ausente")

    if not isinstance(particiones, list):
        raise EsquemaParticionesError("Particiones inválidas")

    if (
        not isinstance(columnas_requeridas, (list, tuple))
        or not columnas_requeridas
        or not isinstance(particiones_requeridas, (list, tuple))
    ):
        raise EsquemaParticionesError("Requisitos inválidos")

    if not all(
        isinstance(x, str) and x.strip()
        for x in list(columnas_requeridas)
        + list(particiones_requeridas)
        + particiones
    ):
        raise EsquemaParticionesError("Nombres inválidos")

    if len(particiones) != len(set(particiones)):
        raise EsquemaParticionesError("Particiones duplicadas")

    # Se identifican nombres declarados; no se interpreta el texto
    # Arrow como prueba física del esquema.
    columnas = []
    for linea in schema_texto.splitlines():
        if ": " in linea and not linea.startswith((" ", "\t")):
            columnas.append(linea.split(": ", 1)[0])

    if not set(columnas_requeridas).issubset(columnas):
        raise EsquemaParticionesError("Faltan columnas requeridas")

    if not set(particiones_requeridas).issubset(particiones):
        raise EsquemaParticionesError("Faltan particiones requeridas")

    return {
        "metadatos_esquema_validos": True,
        "columnas": columnas,
        "particiones": particiones,
        "esquema_parquet_verificado": False,
        "consumo_productivo_autorizado": False,
    }
