"""Integracion focalizada del launcher correctivo.

Spark local, PyArrow y GCS simulado.
Sin credenciales, datos financieros reales ni operaciones cloud.
"""

import hashlib
import json
import tempfile
import unittest

from datetime import date
from pathlib import Path
from unittest.mock import patch

from cryptography.hazmat.primitives import (
    hashes, serialization,
)

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.exceptions import InvalidSignature

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    LongType, DateType, BooleanType,
    DoubleType, _parse_datatype_string,
)

from test_launcher_correctivo_seguridad import (
    BlobSimulado,
    GcsSimulado,
)

import autorizacion_correctiva_quality as aq
import preparar_candidatos_correctivos as pc
import escritura_correctiva_staging as es

from cobertura_sab import propagar_cobertura


def digest(datos):

    return hashlib.sha256(datos).hexdigest()


def huella_ids(ids):

    return digest(
        "".join(
            v + "\n"
            for v in sorted(ids)
        ).encode("utf-8")
    )


class IntegracionCorrectiva(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("BVL_Correctiva_Integration_Test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .config("spark.task.maxFailures", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):

        cls.spark.stop()

    def crear_sidecar(self):

        clave = ec.generate_private_key(
            ec.SECP256R1()
        )

        publica = clave.public_key()

        pem = publica.public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        der = publica.public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        sidecar = {
            "schema_version":
                "BVL-SILVER-SAB-COVERAGE-v0.1",
            "status": "QUALITY_RATIFIED",
            "product": "mercado_diario/montos_sab",
            "version_id": "silver-test",
            "quality_approved": True,
            "scope_quality_id": "scope-test",
            "rows": [
                {
                    "fecha_negociacion": "2025-01-02",
                    "registros_elegibles": 2,
                    "registros_cuarentena": 1,
                    "archivos_excluidos": 0,
                    "estado_cobertura": "PARCIAL",
                    "version_silver": "silver-test",
                    "scope_quality_id": "scope-test",
                    "trazabilidad_verificada": True,
                }
            ],
        }

        contenido = json.dumps(
            sidecar, sort_keys=True
        ).encode()

        firma = clave.sign(
            contenido,
            ec.ECDSA(hashes.SHA256()),
        )

        auth = {
            "sab_coverage": {
                "payload": {"uri": "payload-test"},
                "signature": {"uri": "signature-test"},
                "version_id": "silver-test",
            }
        }

        return (
            auth, contenido, firma,
            pem, digest(der)
        )

    def test_01_sidecar_firmado_y_propagado(self):

        auth, contenido, firma, pem, huella = (
            self.crear_sidecar()
        )

        with patch.object(
            aq, "HUELLA_OFICIAL", huella
        ), patch.object(
            pc, "leer_verificado",
            side_effect=[contenido, firma],
        ):

            cobertura = pc.cargar_cobertura_acreditada(
                None, auth, pem
            )

        gold = self.spark.createDataFrame(
            [
                (date(2025, 1, 2), "SAB1", 2),
                (date(2025, 1, 2), "SAB2", 2),
                (date(2025, 1, 3), "SAB1", 1),
            ],
            "fecha date, codigo_sab string, "
            "cantidad_sab_diaria long",
        )

        resultado = propagar_cobertura(
            gold, cobertura, "silver-test"
        )

        filas = resultado.collect()

        self.assertEqual(len(filas), 3)

        for fila in filas:

            if fila.fecha == date(2025, 1, 2):

                self.assertEqual(
                    fila.estado_cobertura,
                    "PARCIAL"
                )

                self.assertTrue(
                    fila.trazabilidad_verificada
                )

                self.assertEqual(
                    fila.registros_elegibles, 2
                )

            else:

                self.assertEqual(
                    fila.estado_cobertura,
                    "DESCONOCIDA"
                )

                self.assertIsNone(
                    fila.registros_elegibles
                )

            self.assertFalse(
                fila.cobertura_completa
            )

    def test_02_sidecar_manipulado(self):

        auth, contenido, firma, pem, huella = (
            self.crear_sidecar()
        )

        with patch.object(
            aq, "HUELLA_OFICIAL", huella
        ), patch.object(
            pc, "leer_verificado",
            side_effect=[
                contenido + b" ",
                firma,
            ],
        ):

            with self.assertRaises(InvalidSignature):

                pc.cargar_cobertura_acreditada(
                    None, auth, pem
                )

    def contrato_sintetico(self, producto):

        contenido = Path(
            "docs/bigquery"
        ).joinpath(
            producto + ".json"
        ).read_bytes()

        contrato = json.loads(contenido)

        campos = contrato["columns"]

        esquema = StructType([
            StructField(
                campo["name"],
                _parse_datatype_string(
                    campo["spark_type"]
                ),
                campo["nullable"],
            )
            for campo in campos
        ])

        from decimal import Decimal

        valores = []

        for campo in campos:

            nombre = campo["name"]
            tipo = campo["spark_type"]

            if tipo == "string":

                valor = "TEST"

            elif tipo == "date":

                valor = date(2025, 1, 2)

            elif tipo == "boolean":

                valor = True

            elif tipo in {"int", "bigint"}:

                valor = 1

            elif tipo == "double":

                valor = 1.0

            elif tipo.startswith("decimal("):

                valor = Decimal("1.00")

            else:

                raise AssertionError(tipo)

            if nombre == "anio":
                valor = 2025

            if nombre == "id_registro":
                valor = "a" * 64

            valores.append(valor)

        df = self.spark.createDataFrame(
            [tuple(valores)],
            esquema,
        )

        auth = {
            "output_contracts": {
                producto: digest(contenido)
            }
        }

        return df, contenido, auth

    def test_03_contratos_de_salida_3_productos(self):

        for producto in pc.ORDEN:

            with self.subTest(producto=producto):

                df, contrato, auth = (
                    self.contrato_sintetico(producto)
                )

                validado = pc.validar_contrato_salida(
                    producto,
                    df,
                    contrato,
                    auth,
                )

                self.assertEqual(
                    len(validado.columns),
                    pc.COLUMNAS_ESPERADAS[producto],
                )

    def test_04_tipo_y_columna_incompatibles(self):

        producto = "mercado_valor_diario"

        df, contrato, auth = (
            self.contrato_sintetico(producto)
        )

        incorrecto = df.withColumn(
            "precio_cierre",
            F.col("precio_cierre").cast("string")
        )

        with self.assertRaises(ValueError):

            pc.validar_contrato_salida(
                producto, incorrecto, contrato, auth
            )

        with self.assertRaises(ValueError):

            pc.validar_contrato_salida(
                producto,
                df.drop("nemonico_original"),
                contrato,
                auth,
            )

    def test_05_separacion_financiera_y_huellas(self):

        esquema = StructType([
            StructField("id_registro", StringType()),
            StructField("ruc", StringType()),
            StructField("rpj", StringType()),
            StructField("ruc_valido", BooleanType()),
            StructField(
                "colision_financiera", BooleanType()
            ),
            StructField(
                "apto_kpi_general", BooleanType()
            ),
            *[
                StructField(campo, DoubleType())
                for campo in (
                    "roa_aproximado_pct",
                    "roe_aproximado_pct",
                    "endeudamiento_pct",
                    "margen_neto_pct",
                    "autonomia_financiera_pct",
                )
            ],
        ])

        ids_ruc = [
            f"{n:064x}"
            for n in range(1, 86)
        ]

        ids_bnb = [
            f"{n:064x}"
            for n in range(86, 92)
        ]

        ids_aceptados = [
            f"{n:064x}"
            for n in range(92, 5250)
        ]

        filas = []

        for v in ids_ruc:

            filas.append((
                v, "0", "RUC-TEST",
                False, False, True,
                1.0, 1.0, 1.0, 1.0, 1.0,
            ))

        for v in ids_bnb:

            filas.append((
                v, "20513808519", "SG0005",
                True, True, False,
                None, None, None, None, None,
            ))

        for v in ids_aceptados:

            filas.append((
                v, "20123456789", "EMP-TEST",
                True, False, True,
                1.0, 1.0, 1.0, 1.0, 1.0,
            ))

        df = self.spark.createDataFrame(
            filas, esquema
        ).cache()

        self.addCleanup(df.unpersist)

        decision = {
            "expected_source_rows": 5249,
            "expected_accepted_rows": 5158,
            "excluded_ruc_ids_sha256":
                huella_ids(ids_ruc),
            "excluded_bnb_ids_sha256":
                huella_ids(ids_bnb),
            "accepted_ids_sha256":
                huella_ids(ids_aceptados),
        }

        fuente = df.select("id_registro")

        candidato, excepciones = (
            pc.separar_finanzas(
                df, fuente, decision
            )
        )

        self.assertEqual(
            candidato.count(), 5158
        )

        self.assertEqual(
            excepciones[
                "finanzas_ruc_no_conciliado"
            ].count(),
            85,
        )

        self.assertEqual(
            excepciones[
                "finanzas_ruc_valido_ambiguo"
            ].count(),
            6,
        )

        # Una alteracion nominal sin cambio
        # de conteo debe provocar rechazo.

        decision_incorrecta = {
            **decision,
            "accepted_ids_sha256": "0" * 64,
        }

        with self.assertRaises(ValueError):

            pc.separar_finanzas(
                df, fuente, decision_incorrecta
            )

    def test_06_parquet_fisico_y_reconstruccion(self):

        from decimal import Decimal
        import pyarrow.parquet as pq

        esquema = (
            "nemonico string, fecha date, "
            "anio int, mes int, "
            "monto decimal(24,2)"
        )

        df = self.spark.createDataFrame(
            [
                (
                    "TEST1",
                    date(2025, 1, 2),
                    2025, 1,
                    Decimal("123.45"),
                ),
                (
                    "TEST2",
                    date(2025, 2, 3),
                    2025, 2,
                    Decimal("987.65"),
                ),
            ],
            esquema,
        )

        with tempfile.TemporaryDirectory() as tmp:

            salida = es.materializar_parquet_local(
                self.spark,
                df,
                "mercado_valor_diario",
                Path(tmp) / "mercado",
                ["anio", "mes"],
                2,
            )

            self.assertEqual(
                salida["records_count"], 2
            )

            self.assertTrue(
                salida["content_roundtrip_verified"]
            )

            self.assertEqual(
                len(salida["files"]), 2
            )

            for archivo in salida["files"]:

                tabla = pq.ParquetFile(
                    archivo["ruta"]
                ).read()

                self.assertNotIn(
                    "anio", tabla.column_names
                )

                self.assertNotIn(
                    "mes", tabla.column_names
                )

                self.assertEqual(
                    str(tabla.schema.field("monto").type),
                    "decimal128(24, 2)"
                )

    def escritor_fixture(self):

        mercado = self.spark.createDataFrame(
            [("TEST", date(2025, 1, 2), 2025, 1)],
            "nemonico string, fecha date, "
            "anio int, mes int",
        )

        sab = self.spark.createDataFrame(
            [(date(2025, 1, 2), "TEST", 2025, 1)],
            "fecha date, codigo_sab string, "
            "anio int, mes int",
        )

        empresa = self.spark.createDataFrame(
            [("a" * 64, 2025, 1)],
            "id_registro string, "
            "anio int, trimestre int",
        )

        ruc = self.spark.createDataFrame(
            [("b" * 64, 2025, 1)],
            empresa.schema,
        )

        bnb = self.spark.createDataFrame(
            [("c" * 64, 2025, 1)],
            empresa.schema,
        )

        auth = {
            "authorization_id": "test-integracion",
            "batch_id": "batch-test",
            "run_id": "run-test",
            "attempt_id": "attempt-test",
            "processor_commit": "a" * 40,
            "package_sha256": "b" * 64,
            "launcher_sha256": "c" * 64,
            "quality_approved": True,
            "staging_write_allowed": True,
            "publication_allowed": False,
            "staging_uri": (
                f"gs://{aq.BUCKET}/"
                "staging/correccion_fase6/"
                "batch_id=batch-test/"
                "run_id=run-test/"
                "attempt_id=attempt-test/"
            ),
            "sources": {
                "mercado_diario/cotizaciones": {
                    "version_id": "version-test",
                },
            },
            "cotizaciones_adaptation": {
                "decision_id": "cotizaciones-test",
                "version_id": "version-test",
            },
            "output_contracts": {},
            "financial_decision": {},
            "sab_coverage": None,
        }

        preparado = {
            "autorizacion": auth,
            "cotizaciones_adaptation": {
                "status": "APPLIED",
                "authorization_id":
                    auth["authorization_id"],
                "version_id": "version-test",
                "decision":
                    auth["cotizaciones_adaptation"],
            },
            "candidatos": {
                "mercado_valor_diario": mercado,
                "actividad_sab_diaria": sab,
                "empresa_trimestre": empresa,
            },
            "excepciones": {
                "finanzas_ruc_no_conciliado": ruc,
                "finanzas_ruc_valido_ambiguo": bnb,
            },
            "cobertura_acreditada": False,
            "publication_allowed": False,
            "verificar_vigencia": lambda: None,
        }

        return preparado

    def ejecutar_escritura(self, fallar=False):

        cliente = GcsSimulado()

        preparado = self.escritor_fixture()

        def subir_archivo(blob, ruta, **kwargs):

            if fallar:

                blob.almacen.fallar = True

            blob.upload_from_string(
                Path(ruta).read_bytes(),
                **kwargs,
            )

        conteos = {
            producto: 1
            for producto in pc.ORDEN
        }

        excepciones = {
            nombre: 1
            for nombre in es.EXCEPCIONES
        }

        with patch.object(
            es, "CONTEOS", conteos
        ), patch.object(
            es, "EXCEPCIONES", excepciones
        ), patch.object(
            BlobSimulado,
            "upload_from_filename",
            subir_archivo,
            create=True,
        ):

            if fallar:

                with self.assertRaises(IOError):

                    es.escribir_correccion_supervisada(
                        self.spark,
                        cliente,
                        preparado,
                    )

                return cliente, None

            resultado = (
                es.escribir_correccion_supervisada(
                    self.spark,
                    cliente,
                    preparado,
                )
            )

        return cliente, resultado

    def test_07_escritura_integral_e_inventario(self):

        cliente, resultado = (
            self.ejecutar_escritura()
        )

        self.assertEqual(
            resultado["status"],
            "STAGING_COMPLETE",
        )

        self.assertFalse(
            resultado["publication_allowed"]
        )

        self.assertEqual(
            len(cliente.objetos), 9
        )

        prefijo = next(
            nombre.removesuffix(
                "_control/INVENTORY.json"
            )
            for nombre in cliente.objetos
            if nombre.endswith(
                "_control/INVENTORY.json"
            )
        )

        inventario = json.loads(
            cliente.objetos[
                prefijo + "_control/INVENTORY.json"
            ][1]
        )

        self.assertEqual(
            len(inventario["outputs"]), 5
        )

        for salida in inventario["outputs"].values():

            self.assertEqual(
                salida["records_count"], 1
            )

            self.assertFalse(
                salida["production_authorized"]
            )

            for ref in salida["files"]:

                nombre = ref["uri"].split(
                    "/", 3
                )[3]

                generation, datos = (
                    cliente.objetos[nombre]
                )

                self.assertEqual(
                    str(generation),
                    ref["generation"]
                )

                self.assertEqual(
                    digest(datos),
                    ref["sha256"]
                )

    def test_08_fallo_parcial_sin_finalizacion(self):

        cliente, resultado = (
            self.ejecutar_escritura(fallar=True)
        )

        self.assertIsNone(resultado)

        self.assertEqual(
            cliente.intentos, 2
        )

        self.assertEqual(
            len(cliente.objetos), 1
        )

        self.assertTrue(
            next(
                iter(cliente.objetos)
            ).endswith("_control/STARTED.json")
        )

        self.assertFalse(
            any(
                nombre.endswith(
                    "_control/COMPLETE.json"
                )
                for nombre in cliente.objetos
            )
        )


if __name__ == "__main__":

    unittest.main(verbosity=2)
