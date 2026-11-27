"""Validación física de Parquet local para pruebas Silver–Gold.

No descarga datos ni autoriza consumo productivo.
"""

from pathlib import Path


class ParquetFisicoError(ValueError):
    pass


def validar_parquet_fisico(
    archivo,
    tipos_esperados,
    claves,
    particiones,
):
    import pyarrow.parquet as pq

    ruta = Path(archivo)

    if not ruta.is_file() or ruta.suffix != ".parquet":
        raise ParquetFisicoError("Archivo Parquet inválido")

    if not isinstance(tipos_esperados, dict) or not tipos_esperados:
        raise ParquetFisicoError("Esquema esperado inválido")

    if not isinstance(claves, (list, tuple)) or not claves:
        raise ParquetFisicoError("Claves inválidas")

    if not isinstance(particiones, dict):
        raise ParquetFisicoError("Particiones inválidas")

    tabla = pq.read_table(ruta)
    esquema = tabla.schema

    for columna, tipo in tipos_esperados.items():
        if columna not in esquema.names:
            raise ParquetFisicoError(f"Falta columna: {columna}")

        if str(esquema.field(columna).type) != tipo:
            raise ParquetFisicoError(f"Tipo incompatible: {columna}")

    for clave in claves:
        if clave not in esquema.names:
            raise ParquetFisicoError(f"Clave ausente: {clave}")

        if tabla.column(clave).null_count:
            raise ParquetFisicoError(f"Clave nula: {clave}")

    filas = tabla.select(list(claves)).to_pylist()

    identidades = [tuple(f[c] for c in claves) for f in filas]

    if len(identidades) != len(set(identidades)):
        raise ParquetFisicoError("Claves duplicadas")

    for columna, valor in particiones.items():
        if columna not in esquema.names:
            raise ParquetFisicoError(f"Partición ausente: {columna}")

        if any(
            fila[columna] != valor
            for fila in tabla.select([columna]).to_pylist()
        ):
            raise ParquetFisicoError(
                f"Partición incompatible: {columna}"
            )

    return {
        "filas_validadas": tabla.num_rows,
        "esquema_fisico_verificado": True,
        "claves_validas": True,
        "particiones_validas": True,
        "consumo_productivo_autorizado": False,
    }
