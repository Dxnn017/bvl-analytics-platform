"""Pruebas focalizadas del nuevo launcher.

Claves efimeras y almacenamiento simulado.
No se realizan operaciones GCP.
"""

import copy
import hashlib
import json
import unittest

from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from cryptography.hazmat.primitives import (
    hashes, serialization,
)

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.exceptions import InvalidSignature

import autorizacion_correctiva_quality as aq

from preparar_candidatos_correctivos import (
    cargar_cobertura_acreditada,
)

from escritura_correctiva_staging import (
    EscritorStagingExclusivo,
)


def digest(datos):

    return hashlib.sha256(datos).hexdigest()


class BlobSimulado:

    def __init__(
        self, almacen, nombre, generation=None
    ):

        self.almacen = almacen
        self.name = nombre
        self.generation = generation

    def upload_from_string(self, datos, **kwargs):

        assert kwargs["if_generation_match"] == 0
        assert kwargs["retry"] is None

        self.almacen.intentos += 1

        if self.name in self.almacen.objetos:
            raise FileExistsError(self.name)

        if self.almacen.fallar:
            raise IOError("Fallo simulado")

        self.almacen.secuencia += 1

        self.generation = self.almacen.secuencia

        self.almacen.objetos[self.name] = (
            self.generation, bytes(datos)
        )

    def download_as_bytes(self, **kwargs):

        generation, datos = (
            self.almacen.objetos[self.name]
        )

        assert kwargs["retry"] is None

        assert kwargs[
            "if_generation_match"
        ] == generation

        assert self.generation == generation

        if self.almacen.corromper:
            return datos + b"CORRUPTO"

        return datos

    def reload(self, **kwargs):

        assert kwargs["retry"] is None

        self.generation = (
            self.almacen.objetos[self.name][0]
        )


class GcsSimulado:

    def __init__(self):

        self.objetos = {}
        self.intentos = 0
        self.secuencia = 0
        self.fallar = False
        self.corromper = False

    def bucket(self, nombre):

        assert nombre == aq.BUCKET

        return self

    def blob(self, nombre, generation=None):

        return BlobSimulado(
            self, nombre, generation
        )

    def list_blobs(
        self, bucket, prefix,
        max_results=None, retry=None,
    ):

        assert bucket == aq.BUCKET
        assert retry is None

        objetos = [

            self.blob(nombre, generation)

            for nombre, (generation, _)
            in self.objetos.items()

            if nombre.startswith(prefix)
        ]

        return iter(
            objetos[:max_results]
        )


class SeguridadCorrectiva(unittest.TestCase):

    def setUp(self):

        self.clave = ec.generate_private_key(
            ec.SECP256R1()
        )

        publica = self.clave.public_key()

        self.pem = publica.public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        der = publica.public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        parche = patch.object(
            aq,
            "HUELLA_OFICIAL",
            digest(der),
        )

        parche.start()

        self.addCleanup(parche.stop)

        self.ahora = datetime.now(timezone.utc)

        fuentes = {}

        for fuente in aq.FUENTES:

            fuentes[fuente] = {
                "version_id": "version-test",
                "authorization_sha256": "a" * 64,
                "authorization_signature_sha256": "b" * 64,
                "physical_contract_sha256": "c" * 64,
            }

        fuentes[
            "finanzas_empresariales/principales_cuentas"
        ]["version_id"] = aq.VERSION_FINANCIERA

        self.identidad = {
            "project_id": aq.PROYECTO,
            "region": aq.REGION,
            "consumer_service_account": aq.CUENTA,
            "batch_id": "batch-test",
            "run_id": "run-test",
            "attempt_id": "attempt-test",
            "processor_commit": "d" * 40,
            "package_sha256": "e" * 64,
            "launcher_sha256": "f" * 64,
        }

        self.auth = {
            **self.identidad,
            "schema_version": aq.CONTRATO,
            "authorization_id": "quality-test",
            "status": "CORRECTIVE_STAGING_AUTHORIZED",
            "key_version": aq.KEY_VERSION,
            "quality_approved": True,
            "staging_write_allowed": True,
            "publication_allowed": False,
            "not_before_utc": (
                self.ahora - timedelta(minutes=5)
            ).isoformat(),
            "expires_at_utc": (
                self.ahora + timedelta(minutes=5)
            ).isoformat(),
            "products": sorted(aq.PRODUCTOS),
            "sources": fuentes,
            "output_contracts": {
                producto: "a" * 64
                for producto in aq.PRODUCTOS
            },
            "staging_uri": (
                f"gs://{aq.BUCKET}/"
                "staging/correccion_fase6/"
                "batch_id=batch-test/"
                "run_id=run-test/"
                "attempt_id=attempt-test/"
            ),
            "sab_coverage": None,
            "cotizaciones_adaptation": {
                "decision_id": "cotizaciones-test",
                "decision": "ALLOW_TYPED_NULLS",
                "quality_approved": True,
                "product": "mercado_diario/cotizaciones",
                "version_id": "version-test",
                "reason":
                    "INSUFFICIENT_ACCREDITED_ROW_METADATA",
                "columns": {
                    "fuente": {
                        "spark_type": "string",
                        "value": None,
                    },
                    "fecha_ingesta": {
                        "spark_type": "date",
                        "value": None,
                    },
                },
            },
            "financial_decision": {
                "decision_id": "financial-test",
                "decision": (
                    "ALLOW_STAGING_WITH_91_EXCEPTIONS"
                ),
                "version_id": aq.VERSION_FINANCIERA,
                "financial_evidence_sha256":
                    aq.SHA_FINANCIERO,
                "excluded_ruc_ids_sha256": "a" * 64,
                "excluded_bnb_ids_sha256": "b" * 64,
                "accepted_ids_sha256": "c" * 64,
                "expected_source_rows": 5249,
                "expected_accepted_rows": 5158,
            },
        }

    def firmar(self, auth):

        datos = json.dumps(
            auth, sort_keys=True
        ).encode("utf-8")

        firma = self.clave.sign(
            datos,
            ec.ECDSA(hashes.SHA256())
        )

        return datos, firma

    def validar(self, auth=None, identidad=None):

        datos, firma = self.firmar(
            self.auth if auth is None else auth
        )

        return aq.validar_autorizacion_correctiva(
            datos,
            firma,
            self.pem,
            self.identidad if identidad is None
            else identidad,
            ahora=self.ahora,
        )

    def test_01_autorizacion_valida(self):

        resultado = self.validar()

        self.assertFalse(
            resultado["publication_allowed"]
        )

        self.assertEqual(
            len(resultado["products"]), 3
        )

    def test_02_firma_manipulada(self):

        datos, firma = self.firmar(self.auth)

        with self.assertRaises(InvalidSignature):

            aq.validar_autorizacion_correctiva(
                datos + b" ",
                firma,
                self.pem,
                self.identidad,
                ahora=self.ahora,
            )

    def test_03_clave_no_confiable(self):

        with patch.object(
            aq, "HUELLA_OFICIAL", "0" * 64
        ):

            with self.assertRaises(ValueError):
                self.validar()

    def test_04_autorizacion_vencida(self):

        auth = copy.deepcopy(self.auth)

        auth["expires_at_utc"] = (
            self.ahora.isoformat()
        )

        with self.assertRaises(ValueError):
            self.validar(auth)

    def test_05_identidad_incompatible(self):

        identidad = {
            **self.identidad,
            "batch_id": "otro-batch",
        }

        with self.assertRaises(ValueError):
            self.validar(identidad=identidad)

    def test_06_publicacion_prohibida(self):

        auth = copy.deepcopy(self.auth)

        auth["publication_allowed"] = True

        with self.assertRaises(ValueError):
            self.validar(auth)

    def test_07_destino_no_autorizado(self):

        auth = copy.deepcopy(self.auth)

        auth["staging_uri"] = (
            f"gs://{aq.BUCKET}/versions/"
        )

        with self.assertRaises(ValueError):
            self.validar(auth)

    def test_08_productos_bloqueados(self):

        for producto in aq.BLOQUEADOS:

            with self.subTest(producto=producto):

                auth = copy.deepcopy(self.auth)

                auth["products"].append(producto)

                with self.assertRaises(ValueError):
                    self.validar(auth)

    def test_09_empresa_sin_decision(self):

        auth = copy.deepcopy(self.auth)

        auth["financial_decision"][
            "decision"
        ] = "PENDING"

        with self.assertRaises(ValueError):
            self.validar(auth)

    def test_10_evidencia_financiera_distinta(self):

        auth = copy.deepcopy(self.auth)

        auth["financial_decision"][
            "financial_evidence_sha256"
        ] = "0" * 64

        with self.assertRaises(ValueError):
            self.validar(auth)

    def test_11_sidecar_ausente(self):

        self.assertIsNone(
            cargar_cobertura_acreditada(
                None,
                self.validar(),
                self.pem,
            )
        )

    def test_12_sidecar_version_incompatible(self):

        auth = copy.deepcopy(self.auth)

        ref = {
            "uri": (
                "gs://bvl26-control-7f3a9c/"
                "quality/cobertura-test.json"
            ),
            "generation": "123",
            "sha256": "a" * 64,
            "size_bytes": 10,
        }

        auth["sab_coverage"] = {
            "payload": ref,
            "signature": ref,
            "version_id": "otra-version",
        }

        with self.assertRaises(ValueError):
            self.validar(auth)

    def test_13_fuentes_incompletas(self):

        auth = copy.deepcopy(self.auth)

        auth["sources"].pop(
            "mercado_diario/montos_sab"
        )

        with self.assertRaises(ValueError):
            self.validar(auth)

    def test_14_contrato_fisico_manipulado(self):

        auth = self.validar()

        fuente = "mercado_diario/montos_sab"

        contenido, firma = self.firmar({
            "product": fuente,
            "version_id": "version-test",
        })

        auth["sources"][fuente][
            "authorization_sha256"
        ] = digest(contenido)

        auth["sources"][fuente][
            "authorization_signature_sha256"
        ] = digest(firma)

        with self.assertRaises(ValueError):

            aq.verificar_vinculo_fuente(
                auth,
                fuente,
                contenido,
                firma,
                b"contrato-manipulado",
            )

    def escritor(self):

        cliente = GcsSimulado()

        auth = self.validar()

        escritor = EscritorStagingExclusivo(
            cliente,
            auth,
            lambda: self.validar(),
        )

        return cliente, escritor

    def test_15_creacion_exclusiva(self):

        cliente, escritor = self.escritor()

        escritor.comprobar_prefijo_nuevo()

        ref = escritor.escribir_bytes(
            "_control/STARTED.json",
            b"{}",
        )

        self.assertEqual(
            ref["sha256"], digest(b"{}")
        )

        escritor.comprobar_inventario()

        with self.assertRaises(FileExistsError):

            escritor.escribir_bytes(
                "_control/STARTED.json",
                b"OTRO",
            )

        self.assertEqual(
            len(cliente.objetos), 1
        )

        original = next(
            iter(cliente.objetos.values())
        )[1]

        self.assertEqual(original, b"{}")

    def test_16_reutilizacion_rechazada(self):

        cliente, escritor = self.escritor()

        escritor.escribir_bytes(
            "_control/STARTED.json",
            b"{}",
        )

        with self.assertRaises(ValueError):

            escritor.comprobar_prefijo_nuevo()

    def test_17_fallo_sin_reintento(self):

        cliente, escritor = self.escritor()

        cliente.fallar = True

        with self.assertRaises(IOError):

            escritor.escribir_bytes(
                "_control/STARTED.json",
                b"{}",
            )

        self.assertEqual(cliente.intentos, 1)

        self.assertFalse(cliente.objetos)

    def test_18_corrupcion_remota(self):

        cliente, escritor = self.escritor()

        cliente.corromper = True

        with self.assertRaises(ValueError):

            escritor.escribir_bytes(
                "_control/STARTED.json",
                b"{}",
            )

        self.assertEqual(cliente.intentos, 1)

        self.assertEqual(
            len(escritor.inventario), 0
        )

    def test_19_objeto_ajeno_inventario(self):

        cliente, escritor = self.escritor()

        escritor.escribir_bytes(
            "_control/STARTED.json",
            b"{}",
        )

        cliente.objetos[
            escritor.prefijo + "objeto-ajeno"
        ] = (999, b"AJENO")

        with self.assertRaises(ValueError):

            escritor.comprobar_inventario()

    def test_20_vigencia_antes_de_escribir(self):

        cliente, escritor = self.escritor()

        def vencida():

            raise PermissionError(
                "Autorizacion vencida"
            )

        escritor.comprobar = vencida

        with self.assertRaises(PermissionError):

            escritor.escribir_bytes(
                "_control/STARTED.json",
                b"{}",
            )

        self.assertEqual(cliente.intentos, 0)


if __name__ == "__main__":

    unittest.main(verbosity=2)
