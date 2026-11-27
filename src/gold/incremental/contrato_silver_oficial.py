"""Validación preliminar del contrato Silver oficial."""

import re


class ContratoSilverError(ValueError):
    pass


REGISTRY_CONTRACT = "BVL-SILVER-INTERNAL-REGISTRY-v1"
MANIFEST_CONTRACT = "BVL-SILVER-INTERNAL-MANIFEST-v1"

REGISTRY_STATE = "SILVER_CURRENT_INTERNAL"
MANIFEST_STATE = "SILVER_INTERNAL_PUBLISHED"


def requerido(datos, campo):
    if not isinstance(datos, dict) or campo not in datos:
        raise ContratoSilverError(f"Falta campo: {campo}")

    valor = datos[campo]

    if valor is None or isinstance(valor, str) and not valor.strip():
        raise ContratoSilverError(f"Campo vacío: {campo}")

    return valor


def validar_referencia(referencia):
    uri = requerido(referencia, "uri")
    generation = requerido(referencia, "generation")
    sha = requerido(referencia, "sha256")
    size = requerido(referencia, "size_bytes")

    if not isinstance(uri, str) or not uri.startswith("gs://"):
        raise ContratoSilverError("URI inválida")

    if not isinstance(generation, str) or not re.fullmatch(
        r"[1-9][0-9]*", generation
    ):
        raise ContratoSilverError("Generation inválida")

    if not isinstance(sha, str) or not re.fullmatch(
        r"[0-9a-f]{64}", sha
    ):
        raise ContratoSilverError("SHA-256 inválido")

    if type(size) is not int or size < 0:
        raise ContratoSilverError("Size_bytes inválido")

    return referencia


def validar_documento_silver(documento, tipo):
    if tipo not in ("current", "manifest"):
        raise ContratoSilverError("Tipo desconocido")

    product = requerido(documento, "product")
    attempt_id = requerido(documento, "attempt_id")

    if not isinstance(product, str) or not product.strip():
        raise ContratoSilverError("Product inválido")

    if not isinstance(attempt_id, str) or not attempt_id.strip():
        raise ContratoSilverError("Attempt_id inválido")

    contrato_esperado = (
        REGISTRY_CONTRACT if tipo == "current" else MANIFEST_CONTRACT
    )
    estado_esperado = (
        REGISTRY_STATE if tipo == "current" else MANIFEST_STATE
    )

    if requerido(documento, "schema_version") != contrato_esperado:
        raise ContratoSilverError("Contrato Silver incompatible")

    if requerido(documento, "status") != estado_esperado:
        raise ContratoSilverError("Estado Silver incompatible")

    if tipo == "current":
        generation = requerido(documento, "manifest_generation")

        if not isinstance(generation, str) or not re.fullmatch(
            r"[1-9][0-9]*", generation
        ):
            raise ContratoSilverError("Manifest_generation inválida")

    else:
        files = requerido(documento, "files")
        schema = requerido(documento, "schema")
        partitions = requerido(documento, "partition_columns")

        if not isinstance(files, list) or not files:
            raise ContratoSilverError("Files inválidos")

        if not isinstance(schema, str) or not schema.strip():
            raise ContratoSilverError("Schema inválido")

        if not isinstance(partitions, list):
            raise ContratoSilverError("Partition_columns inválidas")

        for archivo in files:
            validar_referencia(archivo)

    return documento


def validar_publicacion_interna(current, manifest):
    """Verifica consistencia documental sin autorizar consumo Gold."""

    validar_documento_silver(current, "current")
    validar_documento_silver(manifest, "manifest")

    campos_comunes = (
        "project_id",
        "product",
        "version_id",
        "run_id",
        "attempt_id",
    )

    for campo in campos_comunes:
        if requerido(current, campo) != requerido(manifest, campo):
            raise ContratoSilverError(f"Incompatibilidad: {campo}")

    if (
        current.get("publication_scope") != "INTERNAL_SILVER_ONLY"
        or manifest.get("publication_scope") != "INTERNAL_SILVER_ONLY"
    ):
        raise ContratoSilverError("Alcance de publicación inesperado")

    if (
        current.get("gold_consumption_allowed") is not False
        or manifest.get("gold_consumption_allowed") is not False
    ):
        raise ContratoSilverError("Estado de consumo histórico inesperado")

    if current.get("manifest_uri") is None:
        raise ContratoSilverError("Manifest URI ausente")

    if current.get("manifest_sha256") is None:
        raise ContratoSilverError("Manifest SHA-256 ausente")

    if current.get("version_prefix") != manifest.get("version_prefix"):
        raise ContratoSilverError("Prefijos de versión incompatibles")

    files = manifest["files"]

    if manifest.get("files_count") != len(files):
        raise ContratoSilverError("Cantidad de archivos incompatible")

    if manifest.get("bytes_total") != sum(
        archivo["size_bytes"] for archivo in files
    ):
        raise ContratoSilverError("Total de bytes incompatible")

    if manifest.get("records_count") != sum(
        archivo["rows"] for archivo in files
    ):
        raise ContratoSilverError("Total de registros incompatible")

    return {
        "contrato_silver_consistente": True,
        "archivos_declarados": len(files),
        "registros_declarados": manifest["records_count"],
        "consumo_gold_autorizado": False,
    }
