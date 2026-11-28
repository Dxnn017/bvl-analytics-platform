import hashlib
import unittest

from datetime import date
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DateType, IntegerType,
)

from integracion_quality_v03 import (
    materializar_fuente_verificada,
)


class TestMaterializacionQuality(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("BVL-Materializacion-Quality")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        tabla = pa.table({
            "codigo_sab": ["S01", "S02"],
            "fecha_negociacion": [
                date(2026, 1, 15),
                date(2026, 1, 15),
            ],
        })

        buffer = pa.BufferOutputStream()

        pq.write_table(tabla, buffer)

        self.contenido = buffer.getvalue().to_pybytes()

        prefijo = (
            "gs://bvl26-silver-7f3a9c/versions/"
            "mercado_diario/montos_sab/"
            "version_id=SINTETICO-V1/"
        )

        self.ref = {
            "uri": prefijo + "anio=2026/mes=1/part-000.parquet",
            "generation": "123456",
            "sha256": hashlib.sha256(
                self.contenido
            ).hexdigest(),
            "size_bytes": len(self.contenido),
            "rows": 2,
        }

        self.fuente = {
            "manifest": {
                "version_prefix": prefijo,
                "partition_columns": ["anio", "mes"],
                "files": [self.ref],
                "records_count": 2,
                "bytes_total": len(self.contenido),
            },
            "evidencia": {
                "tipos_esperados": {
                    "codigo_sab": "string",
                    "fecha_negociacion": "date32[day]",
                    "anio": "int32",
                    "mes": "int32",
                },
                "schema_spark": StructType([
                    StructField(
                        "codigo_sab", StringType(), False
                    ),
                    StructField(
                        "fecha_negociacion", DateType(), False
                    ),
                    StructField(
                        "anio", IntegerType(), False
                    ),
                    StructField(
                        "mes", IntegerType(), False
                    ),
                ]),
            },
        }

        self.cliente = MagicMock()

    def test_01_materializacion_generacion_exacta(self):

        with patch(
            "flujo_silver_gold_v03.leer_objeto_gcs",
            return_value=self.contenido,
        ) as leer:

            df = materializar_fuente_verificada(
                self.spark,
                self.cliente,
                self.fuente,
            )

            filas = df.orderBy("codigo_sab").collect()

            self.assertEqual(len(filas), 2)

            self.assertEqual(
                [r.codigo_sab for r in filas],
                ["S01", "S02"]
            )

            self.assertTrue(
                all(r.anio == 2026 for r in filas)
            )

            self.assertTrue(
                all(r.mes == 1 for r in filas)
            )

            leer.assert_called_once_with(
                self.cliente,
                self.ref["uri"],
                "123456",
            )

            df.show(truncate=False)

            print("PARQUET_Y_PARTICIONES_VERIFICADOS_OK")

    def test_02_contenido_alterado_rechazado(self):

        alterado = bytearray(self.contenido)
        alterado[-1] ^= 1

        with patch(
            "flujo_silver_gold_v03.leer_objeto_gcs",
            return_value=bytes(alterado),
        ):

            with self.assertRaisesRegex(
                ValueError, "SHA-256 incompatible"
            ):
                materializar_fuente_verificada(
                    self.spark,
                    self.cliente,
                    self.fuente,
                )

    def test_03_conteo_incompatible_rechazado(self):

        self.ref["rows"] = 99

        with patch(
            "flujo_silver_gold_v03.leer_objeto_gcs",
            return_value=self.contenido,
        ):

            with self.assertRaisesRegex(
                ValueError, "Conteo fisico incompatible"
            ):
                materializar_fuente_verificada(
                    self.spark,
                    self.cliente,
                    self.fuente,
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
