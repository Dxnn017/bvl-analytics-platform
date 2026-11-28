"""Pruebas focalizadas de adaptacion acreditada.

No repite las suites historicas.
No utiliza datos originales ni operaciones cloud.
"""

import copy
import json
import unittest

from datetime import date
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession

import test_launcher_correctivo_seguridad as seguridad
import test_launcher_correctivo_integracion as integracion

import preparar_candidatos_correctivos as pc
import escritura_correctiva_staging as es

from adaptador_cotizaciones_quality import (
    adaptar_cotizaciones,
)


FUENTE = "mercado_diario/cotizaciones"


class AdaptacionCotizaciones(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("BVL_Adaptacion_Cotizaciones_Test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):

        cls.spark.stop()

    def setUp(self):

        fixture = seguridad.SeguridadCorrectiva()

        fixture.setUp()

        self.addCleanup(fixture.doCleanups)

        self.fixture = fixture

        self.auth = fixture.validar()

        self.df = self.spark.createDataFrame(
            [
                ("TEST1", date(2025, 1, 2), 10.5),
                ("TEST2", date(2025, 1, 3), 20.0),
            ],
            "nemonico string, fecha date, "
            "precio_cierre double"
        )

        self.evidencia = {
            "auth": {
                "product": FUENTE,
                "version_id": "version-test",
                "manifest": {
                    "uri": "gs://test/manifest.json",
                    "generation": "123",
                    "sha256": "a" * 64,
                    "size_bytes": 100,
                },
            },
            "manifest": {
                "product": FUENTE,
                "version_id": "version-test",
                "records_count": 2,
                "bytes_total": 100,
            },
            "preflight": {
                "product": FUENTE,
                "version_id": "version-test",
                "esquema_fisico_verificado": True,
                "filas_validadas": 2,
            },
        }

    def test_01_null_tipado_y_conservacion(self):

        adaptado, auditoria = adaptar_cotizaciones(
            self.df, self.auth, self.evidencia
        )

        self.assertEqual(
            adaptado.schema["fuente"]
            .dataType.simpleString(),
            "string"
        )

        self.assertEqual(
            adaptado.schema["fecha_ingesta"]
            .dataType.simpleString(),
            "date"
        )

        self.assertTrue(
            adaptado.schema["fuente"].nullable
        )

        self.assertTrue(
            adaptado.schema["fecha_ingesta"].nullable
        )

        filas = adaptado.collect()

        self.assertEqual(len(filas), 2)

        self.assertTrue(all(
            fila.fuente is None
            and fila.fecha_ingesta is None
            for fila in filas
        ))

        self.assertEqual(
            adaptado.select(*self.df.columns)
            .orderBy("nemonico").collect(),
            self.df.orderBy("nemonico").collect(),
        )

        self.assertNotIn("fuente", self.df.columns)

        self.assertNotIn(
            "fecha_ingesta", self.df.columns
        )

        self.assertEqual(
            auditoria["status"], "APPLIED"
        )

        self.assertFalse(
            auditoria["metadata_invented"]
        )

    def test_02_decision_firmada_ausente(self):

        auth = copy.deepcopy(self.auth)

        auth.pop("cotizaciones_adaptation")

        with self.assertRaises(ValueError):

            self.fixture.validar(auth)

    def test_03_decision_no_aprobada(self):

        auth = copy.deepcopy(self.auth)

        auth["cotizaciones_adaptation"][
            "quality_approved"
        ] = False

        with self.assertRaises(ValueError):

            self.fixture.validar(auth)

    def test_04_valor_inventado_rechazado(self):

        auth = copy.deepcopy(self.auth)

        auth["cotizaciones_adaptation"][
            "columns"
        ]["fuente"]["value"] = "BVL"

        with self.assertRaises(ValueError):

            self.fixture.validar(auth)

    def test_05_version_incompatible(self):

        auth = copy.deepcopy(self.auth)

        auth["cotizaciones_adaptation"][
            "version_id"
        ] = "otra-version"

        with self.assertRaises(ValueError):

            self.fixture.validar(auth)

        evidencia = copy.deepcopy(self.evidencia)

        evidencia["manifest"][
            "version_id"
        ] = "otra-version"

        with self.assertRaises(ValueError):

            adaptar_cotizaciones(
                self.df, self.auth, evidencia
            )

    def test_06_no_sobrescribe_metadatos(self):

        from pyspark.sql import functions as F

        df = self.df.withColumn(
            "fuente", F.lit("ORIGINAL")
        )

        with self.assertRaises(ValueError):

            adaptar_cotizaciones(
                df, self.auth, self.evidencia
            )

    def test_07_fuente_no_acreditada(self):

        evidencia = copy.deepcopy(self.evidencia)

        evidencia["preflight"][
            "esquema_fisico_verificado"
        ] = False

        with self.assertRaises(ValueError):

            adaptar_cotizaciones(
                self.df, self.auth, evidencia
            )

    def test_08_integracion_preparacion_launcher(self):

        # Prueba focalizada del flujo de preparacion.
        # Se utiliza el adaptador real sobre Spark.
        # Se simulan las seis fuentes y los constructores
        # para no repetir las pruebas historicas.

        contenido, firma = self.fixture.firmar(
            self.auth
        )

        def verificar(
            cliente, auth, producto,
            fuente, evidencia, pem,
        ):

            resultado = copy.deepcopy(
                self.evidencia
            )

            resultado["auth"]["product"] = fuente

            resultado["manifest"][
                "product"
            ] = fuente

            resultado["preflight"][
                "product"
            ] = fuente

            return resultado

        def construir(producto, fuentes, **kwargs):

            if producto == "mercado_valor_diario":

                cotizaciones = fuentes[
                    "cotizaciones"
                ]

                self.assertEqual(
                    cotizaciones.schema["fuente"]
                    .dataType.simpleString(),
                    "string"
                )

                self.assertEqual(
                    cotizaciones.schema["fecha_ingesta"]
                    .dataType.simpleString(),
                    "date"
                )

                self.assertTrue(all(
                    fila.fuente is None
                    and fila.fecha_ingesta is None
                    for fila in cotizaciones.collect()
                ))

                return cotizaciones

            self.assertTrue(all(
                "fuente" not in df.columns
                and "fecha_ingesta" not in df.columns
                for df in fuentes.values()
            ))

            return self.df

        with patch.object(
            pc, "verificar_fuente",
            side_effect=verificar,
        ), patch.object(
            pc, "materializar_fuente_verificada",
            return_value=self.df,
        ), patch.object(
            pc, "verificar_vigencia",
        ), patch.object(
            pc, "construir_candidato",
            side_effect=construir,
        ) as constructor, patch.object(
            pc, "validar_contrato_salida",
            side_effect=lambda producto, df, *args: df,
        ), patch.object(
            pc, "separar_finanzas",
            return_value=(self.df, {}),
        ), patch.object(
            pc, "validar_contenido",
        ):

            preparado = pc.preparar_candidatos(
                self.spark,
                None,
                contenido,
                firma,
                self.fixture.pem,
                self.fixture.identidad,
                {
                    fuente: {}
                    for fuente in self.auth["sources"]
                },
                {
                    producto: b"test"
                    for producto in pc.ORDEN
                },
            )

        self.assertEqual(
            constructor.call_count, 3
        )

        evidencia = preparado[
            "cotizaciones_adaptation"
        ]

        self.assertEqual(
            evidencia["status"], "APPLIED"
        )

        self.assertEqual(
            evidencia["decision"],
            self.auth["cotizaciones_adaptation"]
        )

        self.assertFalse(
            preparado["publication_allowed"]
        )

    def test_09_auditoria_staging_con_adaptacion(self):

        # Se prueba la auditoria nueva del escritor.
        # Parquet se sustituye por archivos de prueba.
        # GCS permanece completamente simulado.

        fixture = (
            integracion.IntegracionCorrectiva()
        )

        fixture.spark = self.spark

        preparado = fixture.escritor_fixture()

        _, evidencia = adaptar_cotizaciones(
            self.df, self.auth, self.evidencia
        )

        auth = preparado["autorizacion"]

        evidencia["authorization_id"] = (
            auth["authorization_id"]
        )

        auth["cotizaciones_adaptation"] = (
            self.auth["cotizaciones_adaptation"]
        )

        preparado["cotizaciones_adaptation"] = (
            evidencia
        )

        cliente = seguridad.GcsSimulado()

        def materializar(
            spark, df, producto, destino,
            particiones, cantidad,
        ):

            destino.mkdir(
                parents=True,
                exist_ok=False
            )

            archivo = destino / "part-test.parquet"

            archivo.write_bytes(b"SYNTHETIC_TEST")

            return {
                "records_count": cantidad,
                "files": [{
                    "ruta": archivo,
                    "relativo": archivo.name,
                    "records_count": cantidad,
                    "size_bytes":
                        archivo.stat().st_size,
                }],
            }

        def subir_archivo(blob, ruta, **kwargs):

            blob.upload_from_string(
                Path(ruta).read_bytes(),
                **kwargs
            )

        with patch.object(
            es, "materializar_parquet_local",
            side_effect=materializar,
        ), patch.object(
            seguridad.BlobSimulado,
            "upload_from_filename",
            subir_archivo,
            create=True,
        ):

            resultado = (
                es.escribir_correccion_supervisada(
                    self.spark,
                    cliente,
                    preparado,
                )
            )

        self.assertEqual(
            resultado["status"], "STAGING_COMPLETE"
        )

        datos_auditoria = next(
            datos
            for nombre, (_, datos)
            in cliente.objetos.items()
            if nombre.endswith(
                "_control/AUDIT.json"
            )
        )

        auditoria = json.loads(
            datos_auditoria
        )

        registrada = auditoria[
            "cotizaciones_adaptation"
        ]

        self.assertEqual(
            registrada, evidencia
        )

        self.assertIsNone(
            registrada["metadata_values"]["fuente"]
        )

        self.assertIsNone(
            registrada["metadata_values"]["fecha_ingesta"]
        )

        self.assertFalse(
            auditoria["publication_allowed"]
        )

    def test_10_sin_evidencia_no_escribe(self):

        fixture = (
            integracion.IntegracionCorrectiva()
        )

        fixture.spark = self.spark

        preparado = fixture.escritor_fixture()

        preparado.pop(
            "cotizaciones_adaptation"
        )

        cliente = seguridad.GcsSimulado()

        with self.assertRaises(ValueError):

            es.escribir_correccion_supervisada(
                self.spark,
                cliente,
                preparado
            )

        self.assertEqual(cliente.intentos, 0)

        self.assertFalse(cliente.objetos)


if __name__ == "__main__":

    unittest.main(verbosity=2)
