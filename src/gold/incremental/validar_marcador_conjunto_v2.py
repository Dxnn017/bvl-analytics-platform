"""Validación del marcador conjunto Silver v0.2 con firma externa."""

import json
import re

from verificar_firma_quality import verificar_firma_ecdsa


class MarcadorConjuntoError(ValueError):
    pass


PRODUCTOS = {
    "referencias_smv/valores_inscritos",
    "referencias_smv/puente_ruc_nemonico",
}


def validar_marcador_firmado(contenido, firma, clave_publica_confiable):
    if not contenido or not firma or not clave_publica_confiable:
        raise MarcadorConjuntoError("Firma o marcador ausente.")

    verificar_firma_ecdsa(contenido, firma, clave_publica_confiable)

    try:
        marcador = json.loads(contenido)
    except (ValueError, UnicodeDecodeError) as error:
        raise MarcadorConjuntoError("JSON inválido.") from error

    if not isinstance(marcador, dict):
        raise MarcadorConjuntoError("Marcador inválido.")

    if marcador.get("contrato") != "bvl-conjunta-candidato-v0.2":
        raise MarcadorConjuntoError("Contrato incompatible.")

    if marcador.get("proyecto") != "bvl-analytics-2026":
        raise MarcadorConjuntoError("Proyecto incompatible.")

    if marcador.get("estado") != "PUBLICACION_CONJUNTA_APROBADA":
        raise MarcadorConjuntoError("Publicación no confirmada.")

    if not marcador.get("publicacion_conjunta_id"):
        raise MarcadorConjuntoError("Identificador conjunto ausente.")

    if not marcador.get("fecha_confirmacion"):
        raise MarcadorConjuntoError("Confirmación sin fecha.")

    commit = marcador.get("commit_procesador")
    if not isinstance(commit, str) or not re.fullmatch(
        r"[0-9a-f]{40}", commit
    ):
        raise MarcadorConjuntoError("Commit inválido.")

    versiones = marcador.get("versiones")
    if not isinstance(versiones, list) or len(versiones) != 2:
        raise MarcadorConjuntoError("Conjunto incompleto.")

    if not all(isinstance(v, dict) for v in versiones):
        raise MarcadorConjuntoError("Versiones inválidas.")

    if {v.get("producto") for v in versiones} != PRODUCTOS:
        raise MarcadorConjuntoError("Productos incompatibles.")

    for version in versiones:
        for campo in ("run_id", "intento", "batch_id"):
            if version.get(campo) != marcador.get(campo):
                raise MarcadorConjuntoError(
                    f"Origen incompatible: {campo}"
                )

        if not version.get("version_id"):
            raise MarcadorConjuntoError("Versión ausente.")

        if not version.get("firma_manifest_uri"):
            raise MarcadorConjuntoError("Firma de manifest ausente.")

        for tipo in ("current", "manifest"):
            referencia = version.get(tipo)
            if not isinstance(referencia, dict):
                raise MarcadorConjuntoError("Referencia inválida.")

            if not isinstance(referencia.get("uri"), str):
                raise MarcadorConjuntoError("URI inválida.")

            generacion = referencia.get("generacion")
            if not isinstance(generacion, str) or not re.fullmatch(
                r"[1-9][0-9]*", generacion
            ):
                raise MarcadorConjuntoError("Generación inválida.")

            sha = referencia.get("sha256")
            if not isinstance(sha, str) or not re.fullmatch(
                r"[0-9a-f]{64}", sha
            ):
                raise MarcadorConjuntoError("SHA-256 inválido.")

    return {
        "marcador_firmado_valido": True,
        "publicacion_conjunta_id": marcador["publicacion_conjunta_id"],
        "versiones": versiones,
        "consumo_productivo_autorizado": False,
    }
