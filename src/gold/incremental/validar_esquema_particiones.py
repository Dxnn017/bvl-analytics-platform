"""Validación de metadatos declarados por un manifest Silver autenticado.

No inspecciona el esquema interno de archivos Parquet.
"""


class EsquemaParticionesError(ValueError):
    pass


def validar_esquema_particiones(manifest, columnas_requeridas, particiones_requeridas):
    if not isinstance(manifest, dict):
        raise EsquemaParticionesError("Manifest inválido.")

    if not isinstance(columnas_requeridas, (list, tuple)) or not columnas_requeridas:
        raise EsquemaParticionesError("Columnas requeridas inválidas.")

    if not isinstance(particiones_requeridas, (list, tuple)):
        raise EsquemaParticionesError("Particiones requeridas inválidas.")

    columnas = manifest.get("columnas")
    particiones = manifest.get("particiones")

    if not isinstance(columnas, list) or not isinstance(particiones, list):
        raise EsquemaParticionesError("Metadatos de esquema ausentes.")

    if not all(isinstance(c, str) and c for c in columnas + particiones):
        raise EsquemaParticionesError("Nombres de columnas inválidos.")

    if len(columnas) != len(set(columnas)):
        raise EsquemaParticionesError("Columnas duplicadas.")

    if len(particiones) != len(set(particiones)):
        raise EsquemaParticionesError("Particiones duplicadas.")

    if not set(columnas_requeridas).issubset(set(columnas)):
        raise EsquemaParticionesError("Faltan columnas requeridas.")

    if not set(particiones_requeridas).issubset(set(particiones)):
        raise EsquemaParticionesError("Faltan particiones requeridas.")

    return {
        "metadatos_esquema_validos": True,
        "columnas": columnas,
        "particiones": particiones,
        "esquema_parquet_verificado": False,
        "consumo_productivo_autorizado": False,
    }
