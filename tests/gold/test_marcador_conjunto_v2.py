import json
import sys
import unittest
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/gold/incremental"))

from validar_marcador_conjunto_v2 import (
    validar_marcador_firmado,
    MarcadorConjuntoError,
)

BASE = Path(__file__).parent / "fixtures/marcador_conjunto"


class TestMarcadorConjunto(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.privada = ec.generate_private_key(ec.SECP256R1())
        cls.publica = cls.privada.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )

    def verificar_fixture(self, nombre):
        contenido = (BASE / nombre).read_bytes()
        firma = self.privada.sign(
            contenido, ec.ECDSA(hashes.SHA256())
        )
        return validar_marcador_firmado(contenido, firma, self.publica)

    def test_marcador_valido(self):
        resultado = self.verificar_fixture("marcador_valido.json")
        self.assertTrue(resultado["marcador_firmado_valido"])
        self.assertFalse(resultado["consumo_productivo_autorizado"])

    def test_marcador_incompleto(self):
        with self.assertRaises(MarcadorConjuntoError):
            self.verificar_fixture("marcador_incompleto.json")

    def test_intentos_distintos(self):
        with self.assertRaises(MarcadorConjuntoError):
            self.verificar_fixture("marcador_intentos_distintos.json")

    def test_firma_manipulada(self):
        contenido = (BASE / "marcador_valido.json").read_bytes()
        firma = self.privada.sign(
            contenido, ec.ECDSA(hashes.SHA256())
        )
        with self.assertRaises(Exception):
            validar_marcador_firmado(
                contenido + b"ALTERADO", firma, self.publica
            )

    def test_clave_no_confiable(self):
        contenido = (BASE / "marcador_valido.json").read_bytes()
        otra_privada = ec.generate_private_key(ec.SECP256R1())
        firma = otra_privada.sign(
            contenido, ec.ECDSA(hashes.SHA256())
        )
        with self.assertRaises(Exception):
            validar_marcador_firmado(contenido, firma, self.publica)


if __name__ == "__main__":
    unittest.main()
