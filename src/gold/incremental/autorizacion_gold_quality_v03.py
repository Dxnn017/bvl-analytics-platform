"""Autorización Gold v0.3. Validación local, sin acceso a GCS."""

import hashlib
import json
import re
from datetime import datetime, timezone

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.exceptions import InvalidSignature


class AutorizacionQualityError(ValueError):
    pass


HUELLA_OFICIAL = (
    "5c70c40088a680cfd7ced1859c4d6b335652d96991cdcae4268819fb4f90f5ce"
)

KEY_VERSION = (
    "projects/bvl-analytics-2026/locations/southamerica-west1/"
    "keyRings/bvl-bronze-release-329c/cryptoKeys/"
    "quality-approved-signer/cryptoKeyVersions/1"
)

CAMPOS = {
    "schema_version", "status", "authorization_id", "project_id",
    "region", "product", "version_id", "run_id", "attempt_id",
    "batch_id", "processor_commit", "consumer_service_account",
    "current", "manifest", "manifest_signature", "joint_marker",
    "key_version", "quality_approved", "gold_consumption_allowed",
    "not_before_utc", "expires_at_utc", "restrictions",
}


def exigir(condicion, mensaje):
    if not condicion:
        raise AutorizacionQualityError(mensaje)


def validar_ref(ref):
    exigir(isinstance(ref, dict), "Referencia inválida")
    exigir(set(ref) == {"uri", "generation", "sha256", "size_bytes"},
           "Campos de referencia incorrectos")
    exigir(isinstance(ref["uri"], str) and
           ref["uri"].startswith("gs://bvl26-control-7f3a9c/") and
           "/staging/" not in ref["uri"], "URI no autorizada")
    exigir(isinstance(ref["generation"], str) and
           re.fullmatch(r"[1-9][0-9]*", ref["generation"]),
           "Generación inválida")
    exigir(isinstance(ref["sha256"], str) and
           re.fullmatch(r"[0-9a-f]{64}", ref["sha256"]),
           "SHA-256 inválido")
    exigir(type(ref["size_bytes"]) is int and ref["size_bytes"] > 0,
           "Tamaño inválido")


def fecha_utc(valor):
    exigir(isinstance(valor, str) and bool(valor.strip()),
           "Fecha ausente")
    try:
        fecha = datetime.fromisoformat(valor.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AutorizacionQualityError("Fecha inválida") from exc
    exigir(fecha.tzinfo is not None, "Fecha sin zona horaria")
    return fecha.astimezone(timezone.utc)


def validar_autorizacion(
    contenido, firma_der, pem_confiable, producto_gold,
    referencias_verificadas, ahora=None,
):
    """Solo valida evidencias locales. No habilita consumo productivo."""

    exigir(isinstance(contenido, bytes) and bool(contenido),
           "Autorización ausente")
    exigir(isinstance(firma_der, bytes) and bool(firma_der),
           "Autorización sin firma")
    exigir(isinstance(pem_confiable, bytes) and bool(pem_confiable),
           "Clave oficial ausente")

    try:
        clave = serialization.load_pem_public_key(pem_confiable)
        exigir(isinstance(clave, ec.EllipticCurvePublicKey),
               "Clave no ECDSA")
        spki = clave.public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        exigir(hashlib.sha256(spki).hexdigest() == HUELLA_OFICIAL,
               "Clave pública no confiable")
        clave.verify(firma_der, contenido, ec.ECDSA(hashes.SHA256()))
    except (ValueError, TypeError, InvalidSignature) as exc:
        raise AutorizacionQualityError(
            "Clave o firma inválida"
        ) from exc

    try:
        auth = json.loads(contenido)
    except (ValueError, UnicodeDecodeError) as exc:
        raise AutorizacionQualityError("JSON inválido") from exc

    exigir(isinstance(auth, dict) and set(auth) == CAMPOS,
           "Campos de autorización incompletos o desconocidos")
    exigir(auth["schema_version"] ==
           "BVL-GOLD-CONSUMPTION-AUTHORIZATION-CANDIDATE-v0.3",
           "Contrato incorrecto")
    exigir(auth["status"] == "GOLD_CONSUMPTION_AUTHORIZED" and
           auth["quality_approved"] is True and
           auth["gold_consumption_allowed"] is True,
           "Autorización Gold no concedida")
    exigir(auth["project_id"] == "bvl-analytics-2026" and
           auth["region"] == "southamerica-west1" and
           auth["key_version"] == KEY_VERSION,
           "Contexto Quality incorrecto")
    exigir(auth["consumer_service_account"] ==
           "sa-bvl-gold@bvl-analytics-2026.iam.gserviceaccount.com",
           "Cuenta consumidora incorrecta")

    for campo in (
        "authorization_id", "product", "version_id", "run_id",
        "attempt_id", "batch_id",
    ):
        exigir(isinstance(auth[campo], str) and
               bool(auth[campo].strip()), f"Campo vacío: {campo}")

    exigir(isinstance(auth["processor_commit"], str) and
           re.fullmatch(r"[0-9a-f]{40}", auth["processor_commit"]),
           "Commit inválido")

    inicio = fecha_utc(auth["not_before_utc"])
    vencimiento = fecha_utc(auth["expires_at_utc"])
    actual = ahora or datetime.now(timezone.utc)
    exigir(actual.tzinfo is not None and
           inicio <= actual.astimezone(timezone.utc) < vencimiento,
           "Autorización vencida o fuera de vigencia")

    restricciones = auth["restrictions"]
    exigir(isinstance(restricciones, dict) and
           isinstance(restricciones.get("allowed_gold_products"), list) and
           producto_gold in restricciones["allowed_gold_products"],
           "Producto Gold no autorizado")
    exigir(restricciones.get("publication_scope") ==
           "INTERNAL_SILVER_ONLY", "Alcance incorrecto")

    for campo in ("current", "manifest", "manifest_signature"):
        validar_ref(auth[campo])

    exigir(isinstance(referencias_verificadas, dict),
           "Evidencias externas ausentes")
    for campo in ("current", "manifest", "manifest_signature"):
        exigir(auth[campo] == referencias_verificadas.get(campo),
               f"Referencia no acreditada: {campo}")

    conjunto = {
        "referencias_smv/valores_inscritos",
        "referencias_smv/puente_ruc_nemonico",
    }
    if auth["product"] in conjunto:
        exigir(isinstance(auth["joint_marker"], dict) and
               set(auth["joint_marker"]) == {"payload", "signature"},
               "Marcador conjunto ausente")
        for ref in auth["joint_marker"].values():
            validar_ref(ref)
    else:
        exigir(auth["joint_marker"] is None,
               "Marcador inesperado")

    return {
        "firma_quality_verificada": True,
        "producto_silver": auth["product"],
        "version_id": auth["version_id"],
        "autorizacion_documental_valida": True,
        "consumo_productivo_autorizado": False,
    }
