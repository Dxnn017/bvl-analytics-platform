import hashlib
import json
import sys
import unittest
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2] / "src/gold/incremental")
)

from flujo_silver_gold_seguro import preparar_consumo_silver, FlujoSilverGoldError
from validar_archivos_silver import validar_archivos_silver, ArchivoSilverError


class TestConsumidorIntegrado(unittest.TestCase):
    def setUp(self):
        self.privada = ec.generate_private_key(ec.SECP256R1())
        self.publica = self.privada.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        self.producto = "mercado_diario/cotizaciones"
        self.version = "fixture-v1"
        self.base = "gs://bvl26-control-7f3a9c/"
        self.uri_manifest = self.base + "manifests/silver/fixture.json"
        self.uri_current = self.base + "version_registry/silver/fixture/current.json"
        self.uri_datos = (
            "gs://bvl26-silver-7f3a9c/versions/fixture/data.parquet"
        )

        self.datos = b"fixture-parquet-simulado"
        self.manifest = json.dumps({
            "producto": self.producto,
            "version_id": self.version,
        }).encode()
        self.current = json.dumps({
            "producto": self.producto,
            "version_id": self.version,
            "manifest_uri": self.uri_manifest,
            "manifest_generacion": "200",
            "manifest_sha256": hashlib.sha256(self.manifest).hexdigest(),
        }).encode()

        self.objetos = {
            ("manifests/silver/fixture.json", 200): self.manifest,
            ("version_registry/silver/fixture/current.json", 100): self.current,
            ("versions/fixture/data.parquet", 300): self.datos,
        }

        objetos = self.objetos

        class Blob:
            def __init__(self, nombre, generacion):
                self.nombre, self.generacion = nombre, generacion

            def download_as_bytes(self, if_generation_match=None):
                if if_generation_match != self.generacion:
                    raise ValueError("Generación diferente")
                return objetos[(self.nombre, self.generacion)]

        class Bucket:
            def blob(self, nombre, generation=None):
                return Blob(nombre, generation)

        class Cliente:
            def bucket(self, nombre):
                return Bucket()

        self.cliente = Cliente()

    def referencia(self, uri, generacion, datos):
        return {
            "uri": uri,
            "generation": str(generacion),
            "sha256": hashlib.sha256(datos).hexdigest(),
            "size_bytes": len(datos),
        }

    def firma(self, datos):
        return self.privada.sign(datos, ec.ECDSA(hashes.SHA256()))

    def probar_flujo(self, version=None, autorizado=True):
        return preparar_consumo_silver(
            self.cliente,
            self.referencia(self.uri_current, 100, self.current),
            self.firma(self.current),
            self.referencia(self.uri_manifest, 200, self.manifest),
            self.firma(self.manifest),
            self.publica,
            {
                "autorizacion_gold": autorizado,
                "producto": self.producto,
                "version_id": version or self.version,
            },
        )

    def test_flujo_correcto(self):
        resultado = self.probar_flujo()
        self.assertTrue(resultado["verificado_localmente"])
        self.assertFalse(resultado["consumo_productivo_autorizado"])

    def test_version_incorrecta(self):
        with self.assertRaises(FlujoSilverGoldError):
            self.probar_flujo(version="otra-version")

    def test_sin_autorizacion(self):
        with self.assertRaises(FlujoSilverGoldError):
            self.probar_flujo(autorizado=False)

    def test_archivo_valido(self):
        resultado = validar_archivos_silver(
            self.cliente,
            [self.referencia(self.uri_datos, 300, self.datos)],
            "gs://bvl26-silver-7f3a9c/versions/fixture/",
        )
        self.assertEqual(resultado["archivos_validados"], 1)
        self.assertFalse(resultado["consumo_productivo_autorizado"])

    def test_archivo_alterado(self):
        referencia = self.referencia(self.uri_datos, 300, self.datos)
        referencia["sha256"] = "0" * 64
        with self.assertRaises(ArchivoSilverError):
            validar_archivos_silver(
                self.cliente, [referencia],
                "gs://bvl26-silver-7f3a9c/versions/fixture/"
            )

    def test_staging_rechazado(self):
        referencia = self.referencia(self.uri_datos, 300, self.datos)
        referencia["uri"] = (
            "gs://bvl26-silver-7f3a9c/staging/fixture/data.parquet"
        )
        with self.assertRaises(ArchivoSilverError):
            validar_archivos_silver(
                self.cliente, [referencia],
                "gs://bvl26-silver-7f3a9c/versions/fixture/"
            )


if __name__ == "__main__":
    unittest.main()
