"""Trazabilidad local de carga historica.

Genera planes candidatos y valida su integridad.
No verifica fuentes oficiales ni concede permisos.
No contiene clientes cloud ni funciones de carga.
"""

import hashlib
import json
import re

from pathlib import Path

from preparador_carga_historica import (
    BASE_GOLD,
    PRODUCTOS,
    COLUMNAS_ESPERADAS,
    cargar_contratos,
    exigir,
)


VERSION_PLAN = "BVL-BQ-HISTORICAL-PLAN-v0.1"

ESTADO_PLAN = "LOCAL_CANDIDATE_NOT_AUTHORIZED"

BUCKET_CONTROL = "bvl26-control-7f3a9c"

PROYECTO = "bvl-analytics-2026"

REGION = "southamerica-west1"

HASHES_CONTRATOS = {
    "mercado_valor_diario":
        "3e984da7684e08e3d2c825933cb4d7e02"
        "e8f6a1dbadcf367c5749093c380d340",
    "actividad_sab_diaria":
        "f6c520defe557a979306dfcd8da05b97b"
        "e2ac30aeba8271be8adaa53edad0bbe",
    "empresa_trimestre":
        "6cfa9e3cf6ddb78d4ac8a7b97b4d35cb"
        "5b869185ab2ba7ccccb0ea6d5171e705",
}


def json_canonico(documento):

    return json.dumps(
        documento,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def huella_documento(documento):

    return hashlib.sha256(
        json_canonico(documento)
    ).hexdigest()


def huella_archivo(ruta):

    ruta = Path(ruta)

    digest = hashlib.sha256()

    cantidad = 0

    with ruta.open("rb") as archivo:

        while True:

            bloque = archivo.read(1024 * 1024)

            if not bloque:

                break

            digest.update(bloque)

            cantidad += len(bloque)

    return {
        "sha256": digest.hexdigest(),
        "size_bytes": cantidad,
    }


def validar_sha(valor):

    exigir(
        isinstance(valor, str)
        and re.fullmatch(r"[a-f0-9]{64}", valor)
        is not None,
        "SHA256 incompatible"
    )


def validar_identificador(valor):

    exigir(
        isinstance(valor, str)
        and re.fullmatch(
            r"[A-Za-z0-9_]{1,80}", valor
        ) is not None,
        "Identificador de lote incompatible"
    )


def validar_version(valor):

    exigir(
        isinstance(valor, str)
        and re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}",
            valor,
        ) is not None,
        "Version Gold incompatible"
    )


def validar_referencia_control(referencia):

    exigir(
        isinstance(referencia, dict)
        and set(referencia) == {
            "uri",
            "generation",
            "sha256",
            "size_bytes",
        },
        "Referencia documental incompleta"
    )

    uri = referencia["uri"]

    prefijo = f"gs://{BUCKET_CONTROL}/"

    exigir(
        isinstance(uri, str)
        and uri.startswith(prefijo),
        "Bucket de control incompatible"
    )

    objeto = uri[len(prefijo):]

    exigir(
        bool(objeto)
        and not any(
            caracter in objeto
            for caracter in "*?[]#\\\r\n"
        )
        and all(
            parte not in ("", ".", "..")
            for parte in objeto.split("/")
        ),
        "URI de control incompatible"
    )

    generacion = referencia["generation"]

    exigir(
        isinstance(generacion, str)
        and re.fullmatch(
            r"[1-9][0-9]*", generacion
        ) is not None,
        "Generacion incompatible"
    )

    validar_sha(referencia["sha256"])

    exigir(
        type(referencia["size_bytes"]) is int
        and referencia["size_bytes"] > 0,
        "Tamano documental incompatible"
    )


def validar_origen_candidato(origen):

    exigir(
        isinstance(origen, dict)
        and set(origen) == {
            "version_id",
            "manifest",
            "manifest_signature",
            "current_registry",
        },
        "Origen candidato incompleto"
    )

    validar_version(origen["version_id"])

    for campo in (
        "manifest",
        "manifest_signature",
        "current_registry",
    ):

        validar_referencia_control(origen[campo])

    referencias = [
        origen[campo]["uri"]
        for campo in (
            "manifest",
            "manifest_signature",
            "current_registry",
        )
    ]

    exigir(
        len(set(referencias)) == 3,
        "Referencias documentales repetidas"
    )


def inventariar_transferencia(raiz, producto):

    exigir(
        producto in PRODUCTOS,
        "Producto de transferencia no admitido"
    )

    raiz = Path(raiz).resolve()

    carpeta = raiz / producto

    exigir(
        carpeta.is_dir()
        and not carpeta.is_symlink(),
        "Carpeta de transferencia inexistente o invalida"
    )

    archivos = []

    for ruta in sorted(carpeta.rglob("*")):

        exigir(
            not ruta.is_symlink(),
            "Enlace simbolico no permitido"
        )

        if ruta.is_dir():

            continue

        exigir(
            ruta.is_file()
            and ruta.suffix == ".parquet",
            "Archivo de transferencia no admitido"
        )

        exigir(
            ruta.resolve() == ruta,
            "Ruta de transferencia no canonica"
        )

        referencia = {
            "relative_path":
                ruta.relative_to(raiz).as_posix(),
            **huella_archivo(ruta),
        }

        exigir(
            referencia["size_bytes"] > 0,
            "Parquet vacio"
        )

        archivos.append(referencia)

    exigir(
        bool(archivos),
        "Inventario de transferencia vacio"
    )

    return archivos


def validar_evidencia_local(
    producto, evidencia, contrato,
):

    exigir(
        evidencia["status"]
        == "LOCAL_VALIDATED_CANDIDATE"
        and evidencia["product"] == producto,
        "Evidencia local incompatible"
    )

    exigir(
        evidencia["gold_contract_sha256"]
        == contrato["sha256"]
        == HASHES_CONTRATOS[producto],
        "Contrato Gold incompatible"
    )

    exigir(
        evidencia["observed_rows"]
        == evidencia["expected_candidate_rows"]
        == PRODUCTOS[producto],
        "Conteo candidato incompatible"
    )

    exigir(
        evidencia["columns"]
        == COLUMNAS_ESPERADAS[producto]
        and evidencia["duplicate_keys"] == 0,
        "Estructura candidata incompatible"
    )

    exigir(
        evidencia["business_keys"]
        == contrato["gold"]["business_keys"],
        "Claves candidatas incompatibles"
    )

    for campo in (
        "gold_modified",
        "official_source_verified",
        "execution_authorized",
        "bigquery_write_authorized",
        "publication_authorized",
    ):

        exigir(
            evidencia[campo] is False,
            "Evidencia con alcance no permitido"
        )


def construir_plan_local(
    raiz_repo,
    raiz_transferencia,
    batch_id,
    run_id,
    origenes_candidatos,
    evidencias,
):

    validar_identificador(batch_id)

    validar_identificador(run_id)

    exigir(
        set(origenes_candidatos)
        == set(evidencias)
        == set(PRODUCTOS),
        "Lote historico incompleto"
    )

    historico, contratos = cargar_contratos(
        raiz_repo
    )

    exigir(
        historico["project_id"] == PROYECTO
        and historico["location_proposed"]
        == REGION,
        "Proyecto o region incompatible"
    )

    productos_plan = {}

    for producto in sorted(PRODUCTOS):

        contrato = contratos[producto]

        origen = origenes_candidatos[producto]

        evidencia = evidencias[producto]

        validar_origen_candidato(origen)

        validar_evidencia_local(
            producto,
            evidencia,
            contrato,
        )

        archivos = inventariar_transferencia(
            raiz_transferencia,
            producto,
        )

        productos_plan[producto] = {
            "source_references": origen,
            "source_references_verified": False,
            "gold_contract_sha256":
                contrato["sha256"],
            "target_table":
                f"{PROYECTO}.gold_analytics.{producto}",
            "staging_table": (
                f"{PROYECTO}.gold_analytics_staging."
                f"{producto}_hist_{batch_id}"
            ),
            "candidate_rows":
                evidencia["observed_rows"],
            "columns": evidencia["columns"],
            "evidence_sha256":
                huella_documento(evidencia),
            "transfer_inventory": archivos,
            "transfer_inventory_sha256":
                huella_documento(archivos),
            "official_transfer_attested": False,
        }

    plan = {
        "schema_version": VERSION_PLAN,
        "status": ESTADO_PLAN,
        "project_id": PROYECTO,
        "location_proposed": REGION,
        "gold_base_commit": BASE_GOLD,
        "historical_contract_sha256":
            huella_archivo(
                Path(raiz_repo)
                / "docs/bigquery/carga_historica"
                / "contrato_carga_historica.json"
            )["sha256"],
        "batch_id": batch_id,
        "run_id": run_id,
        "products": productos_plan,
        "total_candidate_rows":
            sum(PRODUCTOS.values()),
        "total_columns":
            sum(COLUMNAS_ESPERADAS.values()),
        "official_sources_verified": False,
        "runtime_identity_verified": False,
        "immutable_transfer_verified": False,
        "quality_authorization_verified": False,
        "execution_authorized": False,
        "staging_write_authorized": False,
        "production_write_authorized": False,
        "publication_authorized": False,
    }

    return {
        "plan": plan,
        "plan_sha256": huella_documento(plan),
    }


def verificar_integridad_plan(
    paquete,
    raiz_transferencia,
):

    exigir(
        isinstance(paquete, dict)
        and set(paquete) == {
            "plan",
            "plan_sha256",
        },
        "Paquete de plan incompatible"
    )

    plan = paquete["plan"]

    validar_sha(paquete["plan_sha256"])

    exigir(
        huella_documento(plan)
        == paquete["plan_sha256"],
        "Plan historico modificado"
    )

    exigir(
        plan["schema_version"] == VERSION_PLAN
        and plan["status"] == ESTADO_PLAN,
        "Plan local incompatible"
    )

    exigir(
        set(plan["products"]) == set(PRODUCTOS),
        "Productos de plan incompatibles"
    )

    for producto in PRODUCTOS:

        referencia = plan["products"][producto]

        exigir(
            referencia["gold_contract_sha256"]
            == HASHES_CONTRATOS[producto],
            "Contrato de plan incompatible"
        )

        actual = inventariar_transferencia(
            raiz_transferencia,
            producto,
        )

        exigir(
            actual == referencia["transfer_inventory"]
            and huella_documento(actual)
            == referencia["transfer_inventory_sha256"],
            "Inventario de transferencia modificado"
        )

    for campo in (
        "official_sources_verified",
        "runtime_identity_verified",
        "immutable_transfer_verified",
        "quality_authorization_verified",
        "execution_authorized",
        "staging_write_authorized",
        "production_write_authorized",
        "publication_authorized",
    ):

        exigir(
            plan[campo] is False,
            "Alcance local incompatible"
        )

    return {
        "status": "LOCAL_INTEGRITY_VERIFIED",
        "plan_sha256": paquete["plan_sha256"],
        "official_authorization_verified": False,
        "execution_authorized": False,
    }


def exigir_autorizacion_operativa(*args, **kwargs):

    # Bloqueo deliberado hasta disponer del
    # contrato operativo ratificado por Quality.

    raise PermissionError(
        "CARGA_HISTORICA_NO_AUTORIZADA: "
        "se requiere acreditacion operativa "
        "independiente de Gold v3"
    )
