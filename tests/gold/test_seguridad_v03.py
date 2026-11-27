import hashlib
import sys
import unittest
from pathlib import Path

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2] / "src/gold/incremental")
)

from contrato_silver_oficial import (
    validar_documento_silver, ContratoSilverError
)
from autorizacion_gold_quality_v03 import (
    validar_autorizacion, AutorizacionQualityError
)
from marcador_conjunto_quality_v03 import (
    validar_referencia, MarcadorQualityError
)
from control_concurrencia_gcs import (
    verificar_generaciones_vigentes, ConcurrenciaGCSError
)


class TestSeguridadV03(unittest.TestCase):

    def test_autorizacion_ausente(self):
        with self.assertRaises(AutorizacionQualityError):
            validar_autorizacion(
                b"", b"firma", b"clave", "mercado_valor_diario", {}
            )

    def test_autorizacion_sin_firma(self):
        with self.assertRaises(AutorizacionQualityError):
            validar_autorizacion(
                b'{}', b"", b"clave", "mercado_valor_diario", {}
            )

    def test_attempt_id_vacio(self):
        with self.assertRaises(ContratoSilverError):
            validar_documento_silver({
                "product": "macroeconomia/tipo_cambio",
                "attempt_id": "",
            }, "current")

    def test_referencia_alterada(self):
        with self.assertRaises(MarcadorQualityError):
            validar_referencia({
                "uri": "gs://bvl26-control-7f3a9c/manifests/silver/x.json",
                "generation": "0",
                "sha256": "a" * 64,
                "size_bytes": 100,
            })

    def test_staging(self):
        with self.assertRaises(MarcadorQualityError):
            validar_referencia({
                "uri": "gs://bvl26-control-7f3a9c/staging/x.json",
                "generation": "123",
                "sha256": "a" * 64,
                "size_bytes": 100,
            })

    def test_registry_concurrente(self):
        class Blob:
            generation = 999
            def reload(self):
                pass

        class Bucket:
            def blob(self, nombre):
                return Blob()

        class Cliente:
            def bucket(self, nombre):
                return Bucket()

        with self.assertRaises(ConcurrenciaGCSError):
            verificar_generaciones_vigentes(Cliente(), [{
                "uri": (
                    "gs://bvl26-control-7f3a9c/"
                    "version_registry/silver/macroeconomia/tipo_cambio/current.json"
                ),
                "generation": "123",
            }])


if __name__ == "__main__":
    unittest.main()
