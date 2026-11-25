"""Pruebas locales con claves efímeras sintéticas; nunca invoca KMS ni GCS."""
import importlib.util
from pathlib import Path
import unittest

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519

RUTA = Path(__file__).resolve().parents[1] / "scripts/ingestion/verificar_kms_p256_329c_offline.py"
SPEC = importlib.util.spec_from_file_location("verificar_kms_p256_329c_offline", RUTA)
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)


class VerificadorKMSTest(unittest.TestCase):
    def setUp(self):
        self.priv = ec.generate_private_key(ec.SECP256R1())
        self.pub = self.priv.public_key()
        self.pem = self.pub.public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        self.payload = b'{"run_id":"bvl-bronze-inicial-20261125-001","scope":"PILOT_12"}'
        self.sig = self.priv.sign(self.payload, ec.ECDSA(hashes.SHA256()))
        self.huella = mod.huella_spki_sha256(self.pub)

    def test_firma_valida_sin_autorizacion(self):
        r = mod.verificar_firma_kms_p256(self.payload, self.sig, self.pem, self.huella)
        self.assertEqual(r["verificacion_criptografica"], "PASS")
        self.assertFalse(r["transfer_allowed"])
        self.assertFalse(r["procedencia_iam_auditada"])
        self.assertEqual(r["operaciones_gcs"], 0)

    def test_payload_modificado_falla(self):
        with self.assertRaisesRegex(mod.FirmaKMSInvalida, "FIRMA_INVALIDA"):
            mod.verificar_firma_kms_p256(self.payload + b" ", self.sig, self.pem, self.huella)

    def test_firma_alterada_falla(self):
        with self.assertRaises(mod.FirmaKMSInvalida):
            mod.verificar_firma_kms_p256(self.payload, self.sig[:-1] + b"X", self.pem, self.huella)

    def test_huella_incorrecta_falla(self):
        with self.assertRaisesRegex(mod.FirmaKMSInvalida, "HUELLA_OFICIAL"):
            mod.verificar_firma_kms_p256(self.payload, self.sig, self.pem, "0"*64)

    def test_clave_de_otro_emisor_falla(self):
        other = ec.generate_private_key(ec.SECP256R1()).public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        with self.assertRaisesRegex(mod.FirmaKMSInvalida, "CLAVE_NO_COINCIDE"):
            mod.verificar_firma_kms_p256(self.payload, self.sig, other, self.huella)

    def test_curva_p384_falla(self):
        p384 = ec.generate_private_key(ec.SECP384R1()).public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        with self.assertRaisesRegex(mod.FirmaKMSInvalida, "ALGORITMO_O_CURVA"):
            mod.verificar_firma_kms_p256(self.payload, self.sig, p384, self.huella)

    def test_ed25519_falla(self):
        edpub = ed25519.Ed25519PrivateKey.generate().public_key()
        pem = edpub.public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        with self.assertRaisesRegex(mod.FirmaKMSInvalida, "ALGORITMO_O_CURVA"):
            mod.verificar_firma_kms_p256(self.payload, self.sig, pem, self.huella)

    def test_firma_con_clave_distinta_falla(self):
        other = ec.generate_private_key(ec.SECP256R1())
        sig_other = other.sign(self.payload, ec.ECDSA(hashes.SHA256()))
        with self.assertRaisesRegex(mod.FirmaKMSInvalida, "FIRMA_INVALIDA"):
            mod.verificar_firma_kms_p256(self.payload, sig_other, self.pem, self.huella)

    def test_huella_mal_formada_falla(self):
        with self.assertRaisesRegex(mod.FirmaKMSInvalida, "HUELLA_OFICIAL"):
            mod.verificar_firma_kms_p256(self.payload, self.sig, self.pem, "abc")

    def test_pem_invalido_falla(self):
        with self.assertRaisesRegex(mod.FirmaKMSInvalida, "CLAVE_PUBLICA_PEM"):
            mod.verificar_firma_kms_p256(self.payload, self.sig, b"not a pem", self.huella)

    def test_payload_vacio_falla(self):
        with self.assertRaisesRegex(mod.FirmaKMSInvalida, "BYTES_EXACTOS"):
            mod.verificar_firma_kms_p256(b"", self.sig, self.pem, self.huella)


if __name__ == "__main__":
    unittest.main()
