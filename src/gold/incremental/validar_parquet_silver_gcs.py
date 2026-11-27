"""Validación física Silver con reconstrucción de particiones Hive."""

import hashlib
import pyarrow as pa
import pyarrow.parquet as pq

from adaptador_gcs_quality import leer_objeto_gcs
from contrato_silver_oficial import validar_referencia
from reconstruir_particiones_hive import reconstruir_particiones


class ParquetSilverGCSError(ValueError):
    pass


def validar_parquet_desde_gcs(
    cliente, referencia, tipos_esperados, claves, particiones,
    version_prefix=None, partition_columns=None,
):
    validar_referencia(referencia)

    uri = referencia["uri"]
    if not uri.startswith("gs://bvl26-silver-7f3a9c/versions/"):
        raise ParquetSilverGCSError("Ruta Silver inválida")

    if not uri.endswith(".parquet") or "/staging/" in uri:
        raise ParquetSilverGCSError("Archivo no autorizado")

    contenido = leer_objeto_gcs(
        cliente, uri, referencia["generation"]
    )

    if (
        len(contenido) != referencia["size_bytes"]
        or hashlib.sha256(contenido).hexdigest()
        != referencia["sha256"]
    ):
        raise ParquetSilverGCSError("Integridad incompatible")

    tabla = pq.read_table(pa.BufferReader(contenido), use_threads=False)

    if version_prefix is not None:
        if partition_columns is None:
            raise ParquetSilverGCSError("Particiones no definidas")

        tabla = reconstruir_particiones(
            tabla, uri, version_prefix,
            partition_columns, tipos_esperados,
        )

    for columna, tipo in tipos_esperados.items():
        if columna not in tabla.column_names:
            raise ParquetSilverGCSError(f"Columna ausente: {columna}")

        if str(tabla.schema.field(columna).type) != tipo:
            raise ParquetSilverGCSError(f"Tipo incompatible: {columna}")

    for columna, valor in particiones.items():
        if columna not in tabla.column_names:
            raise ParquetSilverGCSError("Partición ausente")

        if any(
            x != valor for x in tabla.column(columna).to_pylist()
        ):
            raise ParquetSilverGCSError("Partición incompatible")

    for clave in claves:
        if clave not in tabla.column_names:
            raise ParquetSilverGCSError("Clave ausente")

        if tabla.column(clave).null_count:
            raise ParquetSilverGCSError("Clave nula")

    identidades = [
        tuple(f[c] for c in claves)
        for f in tabla.select(list(claves)).to_pylist()
    ]

    if len(identidades) != len(set(identidades)):
        raise ParquetSilverGCSError("Claves duplicadas")

    return {
        "filas_validadas": tabla.num_rows,
        "esquema_fisico_verificado": True,
        "particiones_hive_verificadas": version_prefix is not None,
        "consumo_productivo_autorizado": False,
    }
