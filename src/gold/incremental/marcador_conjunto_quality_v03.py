"""Marcador conjunto Silver v0.3. Contrato candidato Quality."""

import hashlib
import json
import re
from datetime import datetime
from cryptography.hazmat.primitives import serialization
from autorizacion_gold_quality_v03 import HUELLA_OFICIAL, KEY_VERSION

from verificar_firma_quality import verificar_firma_ecdsa


class MarcadorQualityError(ValueError):
    pass


PRODUCTOS = {
    "referencias_smv/valores_inscritos",
    "referencias_smv/puente_ruc_nemonico",
}


def exigir(condicion, mensaje):
    if not condicion:
        raise MarcadorQualityError(mensaje)


def validar_referencia(ref):
    exigir(isinstance(ref, dict), "Referencia inválida")

    for campo in ("uri", "generation", "sha256", "size_bytes"):
        exigir(campo in ref, f"Falta {campo}")

    exigir(
        isinstance(ref["uri"], str)
        and ref["uri"].startswith("gs://")
        and "/staging/" not in ref["uri"],
        "URI inválida",
    )

    exigir(
        isinstance(ref["generation"], str)
        and re.fullmatch(r"[1-9][0-9]*", ref["generation"]),
        "Generación inválida",
    )

    exigir(
        isinstance(ref["sha256"], str)
        and re.fullmatch(r"[0-9a-f]{64}", ref["sha256"]),
        "SHA-256 inválido",
    )

    exigir(
        type(ref["size_bytes"]) is int
        and ref["size_bytes"] > 0,
        "Tamaño inválido",
    )


def validar_marcador_v03(contenido, firma, clave_publica, referencias):
    exigir(bool(contenido) and bool(firma), "Marcador sin firma")
    exigir(bool(clave_publica), "Clave pública ausente")

    clave = serialization.load_pem_public_key(clave_publica)
    huella = hashlib.sha256(
        clave.public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    ).hexdigest()
    exigir(huella == HUELLA_OFICIAL, "Clave Quality no confiable")
    verificar_firma_ecdsa(contenido, firma, clave_publica)

    try:
        marcador = json.loads(contenido)
    except (ValueError, UnicodeDecodeError) as error:
        raise MarcadorQualityError("JSON inválido") from error

    exigir(isinstance(marcador, dict), "Marcador inválido")

    exigir(
        marcador.get("schema_version")
        == "BVL-SILVER-JOINT-PUBLICATION-CANDIDATE-v0.3",
        "Contrato incompatible",
    )

    exigir(
        marcador.get("status") == "SILVER_JOINT_COMMITTED",
        "Publicación conjunta no confirmada",
    )

    for campo in (
        "joint_publication_id", "project_id",
        "run_id", "attempt_id", "batch_id",
    ):
        exigir(
            isinstance(marcador.get(campo), str)
            and bool(marcador[campo].strip()),
            f"Campo inválido: {campo}",
        )

    exigir(marcador.get("project_id") == "bvl-analytics-2026",
           "Proyecto incorrecto")
    exigir(marcador.get("key_version") == KEY_VERSION,
           "Versión de clave incorrecta")
    exigir(marcador.get("quality_approved") is True,
           "Quality no aprobó el marcador")
    exigir(marcador.get("gold_consumption_allowed") is False,
           "El marcador no concede consumo Gold")

    fecha = marcador.get("confirmed_at_utc")
    exigir(isinstance(fecha, str) and bool(fecha.strip()),
           "Fecha de confirmación ausente")
    try:
        instante = datetime.fromisoformat(
            fecha.replace("Z", "+00:00")
        )
        exigir(instante.tzinfo is not None,
               "Confirmación sin zona horaria")
    except ValueError as error:
        raise MarcadorQualityError("Fecha inválida") from error

    commit = marcador.get("processor_commit")
    exigir(isinstance(commit, str) and
           bool(re.fullmatch(r"[0-9a-f]{40}", commit)),
           "Commit inválido")

    versiones = marcador.get("products")
    exigir(
        isinstance(versiones, list) and len(versiones) == 2,
        "Conjunto incompleto",
    )

    exigir(
        all(isinstance(v, dict) for v in versiones),
        "Versiones inválidas",
    )

    exigir(
        {v.get("product") for v in versiones} == PRODUCTOS,
        "Productos incompatibles",
    )

    exigir(set(marcador) == {
        "schema_version", "status", "joint_publication_id",
        "project_id", "run_id", "attempt_id", "batch_id",
        "processor_commit", "products", "confirmed_at_utc",
        "key_version", "quality_approved",
        "gold_consumption_allowed",
    }, "Campos obligatorios o inesperados")

    exigir(isinstance(referencias, dict), "Referencias ausentes")
    exigir(set(referencias) == PRODUCTOS, "Referencias incompletas")

    for version in versiones:
        producto = version["product"]
        exigir(set(version) == {
            "product", "version_id", "run_id", "attempt_id",
            "batch_id", "current", "manifest",
            "manifest_signature",
        }, "Campos de producto incompatibles")

        for campo in ("run_id", "attempt_id", "batch_id"):
            exigir(
                version.get(campo) == marcador[campo],
                f"Intento incompatible: {campo}",
            )

        exigir(
            isinstance(version.get("version_id"), str)
            and bool(version["version_id"].strip()),
            "Versión inválida",
        )

        version_id = version["version_id"]

        rutas_esperadas = {
            "current": (
                "gs://bvl26-control-7f3a9c/"
                f"version_registry/silver/{producto}/current.json"
            ),
            "manifest": (
                "gs://bvl26-control-7f3a9c/"
                f"manifests/silver/{producto}/"
                f"version_id={version_id}/manifest.json"
            ),
            "manifest_signature": (
                "gs://bvl26-control-7f3a9c/"
                f"manifests/silver/{producto}/"
                f"version_id={version_id}/manifest.sig"
            ),
        }

        exigir(
            "/" not in version_id
            and version_id not in (".", ".."),
            "Identificador de versión inseguro",
        )

        for tipo in ("current", "manifest", "manifest_signature"):
            esperado = version.get(tipo)
            observado = referencias[producto].get(tipo)

            validar_referencia(esperado)

            exigir(
                esperado["uri"] == rutas_esperadas[tipo],
                f"Ruta incompatible: {producto}/{tipo}",
            )

            exigir(
                observado == esperado,
                f"Referencia alterada: {producto}/{tipo}",
            )

    return {
        "marcador_conjunto_verificado": True,
        "consumo_productivo_autorizado": False,
    }
