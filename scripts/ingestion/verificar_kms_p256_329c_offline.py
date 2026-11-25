#!/usr/bin/env python3
"""Verificación OFFLINE de firmas Cloud KMS EC_SIGN_P256_SHA256.

No accede a GCP, no confiere QUALITY_APPROVED y no habilita transferencias.
La clave pública y su huella de confianza deben provenir de un canal oficial
independiente: NUNCA se confía en una huella calculada del PEM no autenticado.
"""
import argparse
import hashlib
import hmac
import re
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec


class FirmaKMSInvalida(ValueError):
    """Firma, curva, identidad de clave o formato inválidos."""


def huella_spki_sha256(clave):
    """SHA-256 del DER SubjectPublicKeyInfo (no del texto PEM)."""
    return hashlib.sha256(clave.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )).hexdigest()


def verificar_firma_kms_p256(payload, firma_der, clave_pem, huella_oficial_spki):
    """Verifica bytes exactos firmados con ECDSA-P256/SHA256 y huella anclada.

    Return: evidencia criptográfica diagnóstica, SIEMPRE transfer_allowed=False.
    No verifica IAM, auditoría, generación GCS ni release independiente.
    """
    if any(type(v) is not bytes or not v for v in (payload, firma_der, clave_pem)):
        raise FirmaKMSInvalida("BYTES_EXACTOS_REQUERIDOS")
    if type(huella_oficial_spki) is not str or re.fullmatch(r"[0-9a-f]{64}", huella_oficial_spki) is None:
        raise FirmaKMSInvalida("HUELLA_OFICIAL_NO_VALIDA")
    try:
        clave = serialization.load_pem_public_key(clave_pem)
    except (TypeError, ValueError) as exc:
        raise FirmaKMSInvalida("CLAVE_PUBLICA_PEM_INVALIDA") from exc
    if not isinstance(clave, ec.EllipticCurvePublicKey) or not isinstance(clave.curve, ec.SECP256R1):
        raise FirmaKMSInvalida("ALGORITMO_O_CURVA_NO_ADMITIDOS")
    huella_observada = huella_spki_sha256(clave)
    if not hmac.compare_digest(huella_observada, huella_oficial_spki):
        raise FirmaKMSInvalida("CLAVE_NO_COINCIDE_CON_HUELLA_OFICIAL")
    try:
        # Google Cloud KMS EC_SIGN_P256_SHA256 devuelve firma ECDSA DER.
        clave.verify(firma_der, payload, ec.ECDSA(hashes.SHA256()))
    except (InvalidSignature, ValueError) as exc:
        raise FirmaKMSInvalida("FIRMA_INVALIDA_O_CONTENIDO_MODIFICADO") from exc
    return {
        "verificacion_criptografica": "PASS",
        "algoritmo": "EC_SIGN_P256_SHA256",
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "clave_spki_sha256": huella_observada,
        "procedencia_iam_auditada": False,
        "quality_approved_operativo": False,
        "transfer_allowed": False,
        "operaciones_gcs": 0,
        "estado": "FIRMA_VALIDADA_OFFLINE_NO_AUTORIZA_MIGRACION",
    }


def main():
    p = argparse.ArgumentParser(description="Diagnóstico OFFLINE de firma KMS P-256; nunca concede permiso")
    p.add_argument("--payload", type=Path, required=True, help="Archivo exacto cuyos bytes fueron firmados")
    p.add_argument("--signature-der", type=Path, required=True, help="Firma binaria DER original de KMS")
    p.add_argument("--public-key-pem", type=Path, required=True, help="PEM público oficial")
    p.add_argument("--trusted-spki-sha256", required=True, help="Huella SPKI fijada y verificada por Quality por canal independiente")
    a = p.parse_args()
    try:
        res = verificar_firma_kms_p256(
            a.payload.read_bytes(), a.signature_der.read_bytes(),
            a.public_key_pem.read_bytes(), a.trusted_spki_sha256,
        )
    except (OSError, FirmaKMSInvalida) as exc:
        p.exit(2, f"FIRMA_KMS_329C: FAIL | {exc}\nTRANSFER_ALLOWED: False\n")
    print("FIRMA_KMS_329C: PASS (solo criptografía)")
    print("PAYLOAD_SHA256:", res["payload_sha256"])
    print("CLAVE_SPKI_SHA256:", res["clave_spki_sha256"])
    print("TRANSFER_ALLOWED: False")
    print("RESULTADO: FIRMA_KMS_OFFLINE_NO_AUTORIZA_CLOUD")


if __name__ == "__main__":
    main()
