"""Detecta claves nulas o duplicadas entre archivos Silver.

Solo lectura mediante cliente GCS inyectado.
"""

import hashlib
import pyarrow as pa
import pyarrow.parquet as pq

from adaptador_gcs_quality import leer_objeto_gcs


class ClavesGlobalesError(ValueError):
    pass


def validar_claves_globales(cliente, archivos, claves):
    if not isinstance(archivos, list) or not archivos:
        raise ClavesGlobalesError("Archivos ausentes")

    if (
        not isinstance(claves, (list, tuple))
        or not claves
        or len(claves) != len(set(claves))
    ):
        raise ClavesGlobalesError("Claves inválidas")

    identificadores = set()
    total = 0

    for ref in archivos:
        if not isinstance(ref, dict):
            raise ClavesGlobalesError("Referencia inválida")

        contenido = leer_objeto_gcs(
            cliente, ref["uri"], ref["generation"]
        )

        if (
            len(contenido) != ref["size_bytes"]
            or hashlib.sha256(contenido).hexdigest()
            != ref["sha256"]
        ):
            raise ClavesGlobalesError("Integridad incompatible")

        tabla = pq.read_table(pa.BufferReader(contenido))

        if not set(claves).issubset(tabla.column_names):
            raise ClavesGlobalesError("Faltan columnas clave")

        for fila in tabla.select(list(claves)).to_pylist():
            identidad = tuple(fila[clave] for clave in claves)

            if any(valor is None for valor in identidad):
                raise ClavesGlobalesError("Clave nula")

            if identidad in identificadores:
                raise ClavesGlobalesError(
                    "Clave duplicada entre archivos"
                )

            identificadores.add(identidad)
            total += 1

    return {
        "filas_validadas": total,
        "claves_globales_unicas": True,
        "consumo_productivo_autorizado": False,
    }
