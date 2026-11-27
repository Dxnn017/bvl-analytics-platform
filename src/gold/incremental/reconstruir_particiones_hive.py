"""Reconstruye particiones Hive después de validar la ruta Silver."""

import pyarrow as pa

from validar_particiones_silver import validar_particiones_uri


class ReconstruccionHiveError(ValueError):
    pass


def reconstruir_particiones(tabla, uri, prefijo, columnas, tipos):
    if not isinstance(tabla, pa.Table):
        raise ReconstruccionHiveError("Tabla Arrow inválida")

    valores = validar_particiones_uri(uri, prefijo, columnas)

    for columna, valor in valores.items():
        tipo = tipos.get(columna)

        if tipo not in ("int32", "int64"):
            raise ReconstruccionHiveError(
                f"Tipo de partición no autorizado: {columna}"
            )

        tipo_arrow = pa.int32() if tipo == "int32" else pa.int64()

        if columna in tabla.column_names:
            campo = tabla.schema.field(columna)

            if campo.type != tipo_arrow:
                raise ReconstruccionHiveError(
                    f"Tipo incompatible: {columna}"
                )

            observados = tabla.column(columna).to_pylist()

            if any(x != valor for x in observados):
                raise ReconstruccionHiveError(
                    f"Partición física incompatible: {columna}"
                )
        else:
            datos = pa.array(
                [valor] * tabla.num_rows,
                type=tipo_arrow,
            )
            tabla = tabla.append_column(columna, datos)

    return tabla
