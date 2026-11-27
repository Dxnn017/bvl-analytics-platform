import hashlib
import io
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from validar_parquet_silver_gcs import (
    validar_parquet_desde_gcs,
    ParquetSilverGCSError,
)
from validar_claves_globales_silver import (
    validar_claves_globales,
    ClavesGlobalesError,
)

PREFIJO = (
    "gs://bvl26-silver-7f3a9c/versions/"
    "macroeconomia/tipo_cambio/version=fixture-v1/"
)
TIPOS = {"id": "int64", "anio": "int32", "mes": "int32"}


def archivo(ids, nombre, anio_fisico=None):
    columnas = {"id": pa.array(ids, type=pa.int64())}
    if anio_fisico is not None:
        columnas["anio"] = pa.array(
            [anio_fisico] * len(ids), type=pa.int32()
        )

    salida = io.BytesIO()
    pq.write_table(pa.table(columnas), salida)
    datos = salida.getvalue()

    ref = {
        "uri": PREFIJO + "anio=2026/mes=11/" + nombre,
        "generation": "123",
        "sha256": hashlib.sha256(datos).hexdigest(),
        "size_bytes": len(datos),
    }
    return ref, datos


class TestHiveV03(unittest.TestCase):
    def validar(self, ref, datos):
        with patch(
            "validar_parquet_silver_gcs.leer_objeto_gcs",
            return_value=datos,
        ):
            return validar_parquet_desde_gcs(
                None, ref, TIPOS, ["id"],
                {"anio": 2026, "mes": 11},
                version_prefix=PREFIJO,
                partition_columns=["anio", "mes"],
            )

    def test_hive_sin_columnas_fisicas(self):
        ref, datos = archivo([1, 2], "a.parquet")
        self.assertEqual(
            self.validar(ref, datos)["filas_validadas"], 2
        )

    def test_particion_fisica_coincidente(self):
        ref, datos = archivo([1], "a.parquet", 2026)
        self.assertEqual(
            self.validar(ref, datos)["filas_validadas"], 1
        )

    def test_particion_fisica_incompatible(self):
        ref, datos = archivo([1], "a.parquet", 2025)
        with self.assertRaises(ValueError):
            self.validar(ref, datos)

    def test_claves_globales_hive(self):
        a, datos_a = archivo([1, 2], "a.parquet")
        b, datos_b = archivo([3], "b.parquet")
        objetos = {a["uri"]: datos_a, b["uri"]: datos_b}

        with patch(
            "validar_claves_globales_silver.leer_objeto_gcs",
            side_effect=lambda cliente, uri, gen: objetos[uri],
        ):
            resultado = validar_claves_globales(
                None, [a, b], ["anio", "mes", "id"],
                version_prefix=PREFIJO,
                partition_columns=["anio", "mes"],
                tipos_esperados=TIPOS,
            )
        self.assertEqual(resultado["filas_validadas"], 3)

    def test_duplicado_global_rechazado(self):
        a, datos_a = archivo([1], "a.parquet")
        b, datos_b = archivo([1], "b.parquet")
        objetos = {a["uri"]: datos_a, b["uri"]: datos_b}

        with patch(
            "validar_claves_globales_silver.leer_objeto_gcs",
            side_effect=lambda cliente, uri, gen: objetos[uri],
        ):
            with self.assertRaises(ClavesGlobalesError):
                validar_claves_globales(
                    None, [a, b], ["anio", "mes", "id"],
                    version_prefix=PREFIJO,
                    partition_columns=["anio", "mes"],
                    tipos_esperados=TIPOS,
                )


if __name__ == "__main__":
    unittest.main()
