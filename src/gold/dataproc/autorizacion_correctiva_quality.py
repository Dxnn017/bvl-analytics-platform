"""Contrato candidato de ejecucion correctiva Gold.

Requiere ratificacion y firma de Quality.
No concede autorizaciones ni realiza operaciones GCP.
"""

import hashlib
import json
import re

from datetime import datetime, timezone

from cryptography.hazmat.primitives import (
    hashes, serialization,
)

from cryptography.hazmat.primitives.asymmetric import ec

from autorizacion_gold_quality_v03 import (
    HUELLA_OFICIAL,
    KEY_VERSION,
)


CONTRATO = "BVL-GOLD-CORRECTIVE-STAGING-CANDIDATE-v3"

PROYECTO = "bvl-analytics-2026"
REGION = "southamerica-west1"

CUENTA = (
    "sa-bvl-gold@bvl-analytics-2026.iam.gserviceaccount.com"
)

BUCKET = "bvl26-gold-7f3a9c"

PRODUCTOS = {
    "mercado_valor_diario",
    "actividad_sab_diaria",
    "empresa_trimestre",
}

FUENTES = {
    "mercado_diario/cotizaciones",
    "macroeconomia/tipo_cambio",
    "macroeconomia/embig",
    "referencias_smv/puente_ruc_nemonico",
    "mercado_diario/montos_sab",
    "finanzas_empresariales/principales_cuentas",
}

BLOQUEADOS = {
    "hechos_empresa",
    "impacto_hechos_valor",
}

VERSION_FINANCIERA = (
    "v01-bvl-bronze-inicial-20261125-001-"
    "principales-cuentas-4cd3944afbbf413e"
)

SHA_FINANCIERO = (
    "198a2d18c5acaf35f2918b1c7fde5dd90"
    "58f472e39e8f1a1d29bcb0e70ed8da4"
)

CAMPOS = {
    "schema_version",
    "authorization_id",
    "status",
    "project_id",
    "region",
    "consumer_service_account",
    "key_version",
    "quality_approved",
    "staging_write_allowed",
    "publication_allowed",
    "not_before_utc",
    "expires_at_utc",
    "batch_id",
    "run_id",
    "attempt_id",
    "processor_commit",
    "package_sha256",
    "launcher_sha256",
    "products",
    "sources",
    "output_contracts",
    "staging_uri",
    "sab_coverage",
    "financial_decision",
    "cotizaciones_adaptation",
    "numeric_adaptations",
}


def exigir(condicion, mensaje):
    if not condicion:
        raise ValueError(mensaje)


def sha256(datos):
    return hashlib.sha256(datos).hexdigest()


def json_estricto(datos):

    def sin_duplicados(pares):
        resultado = {}

        for clave, valor in pares:
            exigir(
                clave not in resultado,
                "Clave JSON duplicada"
            )

            resultado[clave] = valor

        return resultado

    def constante_invalida(valor):
        raise ValueError("Constante JSON invalida")

    return json.loads(
        datos,
        object_pairs_hook=sin_duplicados,
        parse_constant=constante_invalida,
    )


def validar_sha(valor):
    exigir(
        isinstance(valor, str)
        and re.fullmatch(r"[0-9a-f]{64}", valor),
        "SHA-256 invalido"
    )


def validar_id(valor):
    exigir(
        isinstance(valor, str)
        and re.fullmatch(
            r"[a-z0-9][a-z0-9_-]{0,127}",
            valor
        ),
        "Identificador inseguro"
    )


def fecha_utc(valor):
    fecha = datetime.fromisoformat(
        valor.replace("Z", "+00:00")
    )

    exigir(
        fecha.tzinfo is not None,
        "Fecha sin zona horaria"
    )

    return fecha.astimezone(timezone.utc)


def verificar_firma_oficial(contenido, firma, pem):
    clave = serialization.load_pem_public_key(pem)

    exigir(
        isinstance(clave, ec.EllipticCurvePublicKey),
        "Clave no ECDSA"
    )

    spki = clave.public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    exigir(
        sha256(spki) == HUELLA_OFICIAL,
        "Clave Quality no confiable"
    )

    clave.verify(
        firma,
        contenido,
        ec.ECDSA(hashes.SHA256()),
    )


def validar_referencia_control(ref):

    exigir(
        isinstance(ref, dict)
        and set(ref) == {
            "uri", "generation", "sha256", "size_bytes"
        },
        "Referencia de control invalida"
    )

    uri = ref["uri"]

    exigir(
        isinstance(uri, str)
        and uri.startswith("gs://bvl26-control-7f3a9c/")
        and ".." not in uri.split("/")
        and "/staging/" not in uri,
        "Ruta Quality no autorizada"
    )

    exigir(
        isinstance(ref["generation"], str)
        and re.fullmatch(
            r"[1-9][0-9]*",
            ref["generation"]
        ),
        "Generation invalida"
    )

    validar_sha(ref["sha256"])

    exigir(
        type(ref["size_bytes"]) is int
        and ref["size_bytes"] > 0,
        "Tamano invalido"
    )



def validar_decision_cotizaciones(auth):

    decision = auth.get("cotizaciones_adaptation")

    exigir(
        isinstance(decision, dict)
        and set(decision) == {
            "decision_id",
            "decision",
            "quality_approved",
            "product",
            "version_id",
            "reason",
            "columns",
        },
        "Decision de adaptacion ausente o incompleta"
    )

    validar_id(decision["decision_id"])

    exigir(
        decision["quality_approved"] is True
        and decision["decision"] == "ALLOW_TYPED_NULLS"
        and decision["product"]
        == "mercado_diario/cotizaciones",
        "Adaptacion de cotizaciones no autorizada"
    )

    exigir(
        decision["reason"]
        == "INSUFFICIENT_ACCREDITED_ROW_METADATA",
        "Justificacion Quality incompatible"
    )

    exigir(
        decision["version_id"]
        == auth["sources"][
            "mercado_diario/cotizaciones"
        ]["version_id"],
        "Decision para otra version de cotizaciones"
    )

    exigir(
        decision["columns"] == {
            "fuente": {
                "spark_type": "string",
                "value": None,
            },
            "fecha_ingesta": {
                "spark_type": "date",
                "value": None,
            },
        },
        "Tratamiento de metadatos no autorizado"
    )

    return decision



def validar_decision_numerica(auth):

    from adaptador_numerico_integral import (
        REGLAS,
        POLITICA,
    )

    decision = auth.get("numeric_adaptations")

    exigir(
        isinstance(decision, dict)
        and set(decision) == {
            "decision_id",
            "decision",
            "quality_approved",
            "policy",
            "sources",
        },
        "Decision numerica ausente o incompleta"
    )

    validar_id(decision["decision_id"])

    exigir(
        decision["quality_approved"] is True
        and decision["decision"]
        == "ALLOW_STRICT_NUMERIC_CONVERSIONS"
        and decision["policy"] == POLITICA,
        "Politica numerica no autorizada"
    )

    fuentes = decision["sources"]

    exigir(
        isinstance(fuentes, dict)
        and set(fuentes) == set(REGLAS),
        "Fuentes de adaptacion incompatibles"
    )

    for fuente, reglas in REGLAS.items():

        referencia = fuentes[fuente]

        exigir(
            isinstance(referencia, dict)
            and set(referencia) == {
                "version_id",
                "physical_contract_sha256",
                "columns",
            },
            "Referencia numerica incompleta"
        )

        oficial = auth["sources"][fuente]

        exigir(
            referencia["version_id"]
            == oficial["version_id"],
            "Adaptacion numerica para otra version"
        )

        validar_sha(
            referencia["physical_contract_sha256"]
        )

        exigir(
            referencia["physical_contract_sha256"]
            == oficial["physical_contract_sha256"],
            "Contrato fisico numerico incompatible"
        )

        columnas_esperadas = {
            nombre: {
                "input_type": "string",
                "output_type": tipo,
            }
            for nombre, tipo in reglas.items()
        }

        exigir(
            referencia["columns"]
            == columnas_esperadas,
            "Conversiones numericas no autorizadas"
        )

    return decision


def validar_autorizacion_correctiva(
    contenido,
    firma,
    pem,
    identidad,
    ahora=None,
):

    verificar_firma_oficial(
        contenido, firma, pem
    )

    auth = json_estricto(contenido)

    exigir(
        isinstance(auth, dict)
        and set(auth) == CAMPOS,
        "Contrato de autorizacion incompleto"
    )

    exigir(
        auth["schema_version"] == CONTRATO
        and auth["status"] == "CORRECTIVE_STAGING_AUTHORIZED",
        "Autorizacion correctiva ausente"
    )

    exigir(
        auth["quality_approved"] is True
        and auth["staging_write_allowed"] is True
        and auth["publication_allowed"] is False,
        "Alcance Quality incompatible"
    )

    exigir(
        auth["project_id"] == PROYECTO
        and auth["region"] == REGION
        and auth["consumer_service_account"] == CUENTA
        and auth["key_version"] == KEY_VERSION,
        "Contexto cloud incompatible"
    )

    inicio = fecha_utc(auth["not_before_utc"])
    fin = fecha_utc(auth["expires_at_utc"])

    actual = ahora or datetime.now(timezone.utc)

    exigir(
        actual.tzinfo is not None
        and inicio <= actual < fin,
        "Autorizacion vencida o fuera de vigencia"
    )

    for campo in (
        "authorization_id",
        "batch_id",
        "run_id",
        "attempt_id",
    ):
        validar_id(auth[campo])

    exigir(
        isinstance(auth["processor_commit"], str)
        and re.fullmatch(
            r"[0-9a-f]{40}",
            auth["processor_commit"]
        ),
        "Commit invalido"
    )

    validar_sha(auth["package_sha256"])
    validar_sha(auth["launcher_sha256"])

    campos_identidad = {
        "batch_id",
        "run_id",
        "attempt_id",
        "processor_commit",
        "package_sha256",
        "launcher_sha256",
        "project_id",
        "region",
        "consumer_service_account",
    }

    exigir(
        isinstance(identidad, dict)
        and set(identidad) == campos_identidad,
        "Identidad de ejecucion incompleta"
    )

    for campo in campos_identidad:
        exigir(
            identidad[campo] == auth[campo],
            "Identidad incompatible: " + campo
        )

    productos = auth["products"]

    exigir(
        isinstance(productos, list)
        and len(productos) == 3
        and set(productos) == PRODUCTOS,
        "Conjunto de productos incompatible"
    )

    destino = (
        f"gs://{BUCKET}/staging/correccion_fase6/"
        f"batch_id={auth['batch_id']}/"
        f"run_id={auth['run_id']}/"
        f"attempt_id={auth['attempt_id']}/"
    )

    exigir(
        auth["staging_uri"] == destino,
        "Destino staging no autorizado"
    )

    fuentes = auth["sources"]

    exigir(
        isinstance(fuentes, dict)
        and set(fuentes) == FUENTES,
        "Conjunto Silver incompleto"
    )

    for fuente, ref in fuentes.items():

        exigir(
            isinstance(ref, dict)
            and set(ref) == {
                "version_id",
                "authorization_sha256",
                "authorization_signature_sha256",
                "physical_contract_sha256",
            },
            "Referencia Silver incompleta"
        )

        exigir(
            isinstance(ref["version_id"], str)
            and re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9_.-]{0,255}",
                ref["version_id"]
            ),
            "Version Silver invalida"
        )

        for campo in (
            "authorization_sha256",
            "authorization_signature_sha256",
            "physical_contract_sha256",
        ):
            validar_sha(ref[campo])

    contratos = auth["output_contracts"]

    exigir(
        isinstance(contratos, dict)
        and set(contratos) == PRODUCTOS,
        "Contratos Gold incompletos"
    )

    for huella in contratos.values():
        validar_sha(huella)

    cobertura = auth["sab_coverage"]

    if cobertura is not None:

        exigir(
            isinstance(cobertura, dict)
            and set(cobertura) == {
                "payload",
                "signature",
                "ratification",
                "ratification_signature",
                "version_id",
                "policy",
            },
            "Sidecar SAB incompleto"
        )

        validar_referencia_control(
            cobertura["payload"]
        )

        validar_referencia_control(
            cobertura["signature"]
        )

        validar_referencia_control(
            cobertura["ratification"]
        )

        validar_referencia_control(
            cobertura["ratification_signature"]
        )

        exigir(
            cobertura["policy"]
            == "SAB_RESTRICTED_RATIFICATION_V1",
            "Politica de cobertura no autorizada"
        )

        exigir(
            cobertura["version_id"]
            == fuentes["mercado_diario/montos_sab"]["version_id"],
            "Sidecar de otra version Silver"
        )

    financiera = auth["financial_decision"]

    exigir(
        isinstance(financiera, dict)
        and set(financiera) == {
            "decision_id",
            "decision",
            "version_id",
            "financial_evidence_sha256",
            "excluded_ruc_ids_sha256",
            "excluded_bnb_ids_sha256",
            "accepted_ids_sha256",
            "expected_source_rows",
            "expected_accepted_rows",
        },
        "Decision financiera incompleta"
    )

    validar_id(financiera["decision_id"])

    exigir(
        financiera["decision"]
        == "ALLOW_STAGING_WITH_91_EXCEPTIONS",
        "Empresa no habilitada expresamente"
    )

    exigir(
        financiera["version_id"]
        == VERSION_FINANCIERA
        == fuentes[
            "finanzas_empresariales/principales_cuentas"
        ]["version_id"],
        "Version financiera no conciliada"
    )

    exigir(
        financiera["financial_evidence_sha256"]
        == SHA_FINANCIERO,
        "Evidencia financiera incompatible"
    )

    exigir(
        type(financiera["expected_source_rows"]) is int
        and financiera["expected_source_rows"] == 5249
        and type(financiera["expected_accepted_rows"]) is int
        and financiera["expected_accepted_rows"] == 5158,
        "Conteos financieros incompatibles"
    )

    for campo in (
        "excluded_ruc_ids_sha256",
        "excluded_bnb_ids_sha256",
        "accepted_ids_sha256",
    ):
        validar_sha(financiera[campo])

    validar_decision_cotizaciones(auth)
    validar_decision_numerica(auth)

    return auth


def verificar_vinculo_fuente(
    autorizacion_correctiva,
    fuente,
    contenido,
    firma,
    contrato_fisico_bytes,
):

    exigir(
        fuente in FUENTES,
        "Fuente no permitida"
    )

    referencia = autorizacion_correctiva["sources"][fuente]

    exigir(
        sha256(contenido)
        == referencia["authorization_sha256"],
        "Autorizacion Silver no vinculada"
    )

    exigir(
        sha256(firma)
        == referencia["authorization_signature_sha256"],
        "Firma Silver no vinculada"
    )

    exigir(
        sha256(contrato_fisico_bytes)
        == referencia["physical_contract_sha256"],
        "Contrato fisico no acreditado"
    )

    auth = json_estricto(contenido)

    exigir(
        auth["product"] == fuente
        and auth["version_id"] == referencia["version_id"],
        "Identidad de fuente incompatible"
    )

    # La firma, vigencia y referencias GCS de esta
    # autorizacion deben verificarse adicionalmente
    # con el flujo Quality v0.3 existente.

    return auth
