"""Interpretacion candidata del sidecar SAB original ratificado.

La ratificacion documental no autoriza ejecucion,
publicacion ni consumo productivo.
"""

from datetime import date

from autorizacion_correctiva_quality import (
    exigir,
    sha256,
    json_estricto,
    verificar_firma_oficial,
)


POLITICA = "SAB_RESTRICTED_RATIFICATION_V1"

FUENTE = "mercado_diario/montos_sab"

PRODUCTO = "actividad_sab_diaria"

VERSION_RATIFICACION = (
    "BVL-F6-RATIFICACION-QUALITY-v1"
)

CAMPOS_FILA = {
    "fecha_negociacion",
    "version_silver",
    "estado_cobertura",
    "registros_elegibles",
    "registros_cuarentena",
    "archivos_excluidos",
    "scope_quality_id",
    "trazabilidad_verificada",
}


def interpretar_sidecar_ratificado(
    contenido,
    firma,
    ratificacion_bytes,
    ratificacion_firma,
    pem,
    version_id,
    manifest_sha256,
    manifest_signature_sha256,
    manifest_generation,
    registros_esperados,
):

    verificar_firma_oficial(
        contenido, firma, pem
    )

    verificar_firma_oficial(
        ratificacion_bytes,
        ratificacion_firma,
        pem,
    )

    ratificacion = json_estricto(
        ratificacion_bytes
    )

    sidecar = json_estricto(contenido)

    exigir(
        ratificacion["schema_version"]
        == VERSION_RATIFICACION
        and ratificacion["status"]
        == "RATIFICADO_ALCANCE_RESTRINGIDO"
        and ratificacion["scope"]
        == "DECISIONES_QUALITY_NO_OPERATIVAS"
        and ratificacion["quality_approved"] is True
        and ratificacion["project_id"]
        == "bvl-analytics-2026"
        and ratificacion["region"]
        == "southamerica-west1"
        and ratificacion["sab_coverage"]
        == "DESCONOCIDA",
        "Ratificacion SAB incompatible"
    )

    for campo in (
        "execution_authorized",
        "full_coverage_attested",
        "gold_consumption_allowed",
        "publication_authorized",
        "iam_changes_authorized",
    ):

        exigir(
            ratificacion[campo] is False,
            "Alcance de ratificacion incompatible"
        )

    referencia = ratificacion[
        "documents"
    ]["sidecar_sab.json"]

    exigir(
        referencia["sha256"] == sha256(contenido)
        and referencia["size_bytes"] == len(contenido),
        "Sidecar no vinculado a ratificacion"
    )

    exigir(
        sidecar["producto_silver"] == FUENTE
        and sidecar["producto_gold_candidato"]
        == PRODUCTO
        and sidecar["quality_approved"] is False
        and sidecar[
            "cobertura_completa_acreditada"
        ] is False
        and sidecar[
            "gold_consumption_allowed"
        ] is False
        and sidecar[
            "publication_authorized"
        ] is False,
        "Sidecar original incompatible"
    )

    origen = sidecar["origen"]

    exigir(
        origen["manifest_silver_version_id"]
        == version_id
        and origen["manifest_silver_sha256"]
        == manifest_sha256
        and origen["manifest_silver_firma_sha256"]
        == manifest_signature_sha256
        and origen[
            "manifest_silver_generation_reportada_en_conteos"
        ] == str(manifest_generation),
        "Sidecar para otro manifest Silver"
    )

    registros = sidecar["registros"]

    exigir(
        isinstance(registros, list)
        and bool(registros),
        "Sidecar sin registros"
    )

    fechas = set()
    total = 0

    for fila in registros:

        exigir(
            isinstance(fila, dict)
            and set(fila) == CAMPOS_FILA,
            "Campos de cobertura incompatibles"
        )

        exigir(
            fila["version_silver"] == version_id
            and fila["estado_cobertura"]
            == "DESCONOCIDA"
            and fila["trazabilidad_verificada"]
            is False
            and fila["scope_quality_id"] is None
            and fila["registros_cuarentena"] is None
            and fila["archivos_excluidos"] is None,
            "Cobertura no acreditada"
        )

        fecha = date.fromisoformat(
            fila["fecha_negociacion"]
        )

        exigir(
            fecha.isoformat()
            == fila["fecha_negociacion"]
            and fecha not in fechas,
            "Fecha SAB invalida o duplicada"
        )

        fechas.add(fecha)

        cantidad = fila["registros_elegibles"]

        exigir(
            type(cantidad) is int
            and cantidad > 0,
            "Conteo elegible incompatible"
        )

        total += cantidad

    exigir(
        type(registros_esperados) is int
        and total == registros_esperados,
        "Conteo total del sidecar incompatible"
    )

    evidencia = {
        "policy": POLITICA,
        "status": "DOCUMENTARY_RATIFICATION_VERIFIED",
        "product": FUENTE,
        "version_id": version_id,
        "sidecar_sha256": sha256(contenido),
        "sidecar_signature_sha256": sha256(firma),
        "ratification_sha256":
            sha256(ratificacion_bytes),
        "ratification_signature_sha256":
            sha256(ratificacion_firma),
        "manifest_sha256": manifest_sha256,
        "manifest_signature_sha256":
            manifest_signature_sha256,
        "manifest_generation":
            str(manifest_generation),
        "dates": len(fechas),
        "eligible_records": total,
        "coverage": "DESCONOCIDA",
        "full_coverage_attested": False,
        "daily_counts_verified_against_dataframe":
            False,
        "production_authorized": False,
        "publication_allowed": False,
    }

    return registros, evidencia
