"""Verificación conjunta Silver v0.3. Solo lectura; no autoriza producción."""

import json

from adaptador_gcs_quality import leer_objeto_gcs
from control_concurrencia_gcs import verificar_generaciones_vigentes
from contrato_silver_oficial import validar_publicacion_interna
from marcador_conjunto_quality_v03 import validar_marcador_v03, PRODUCTOS
from verificar_firma_quality import verificar_firma_ecdsa


class ConjuntoSilverError(ValueError):
    pass


def leer_verificado(cliente, ref):
    datos = leer_objeto_gcs(cliente, ref["uri"], ref["generation"])

    import hashlib
    if (
        len(datos) != ref["size_bytes"]
        or hashlib.sha256(datos).hexdigest() != ref["sha256"]
    ):
        raise ConjuntoSilverError("Integridad del objeto incompatible")

    return datos


def verificar_conjunto_silver(cliente, autorizacion, pem_quality):
    marcador_refs = autorizacion.get("joint_marker")
    if not isinstance(marcador_refs, dict) or set(marcador_refs) != {
        "payload", "signature"
    }:
        raise ConjuntoSilverError("Marcador conjunto ausente")

    for nombre, sufijo in (
        ("payload", ".json"),
        ("signature", ".sig"),
    ):
        ref = marcador_refs[nombre]
        if (
            not ref["uri"].startswith(
                "gs://bvl26-control-7f3a9c/"
            )
            or not ref["uri"].endswith(sufijo)
        ):
            raise ConjuntoSilverError("Ruta del marcador inválida")

    contenido = leer_verificado(cliente, marcador_refs["payload"])
    firma = leer_verificado(cliente, marcador_refs["signature"])
    marcador = json.loads(contenido)

    if not isinstance(marcador, dict):
        raise ConjuntoSilverError("Marcador inválido")

    versiones = marcador.get("products")
    if not isinstance(versiones, list) or len(versiones) != 2:
        raise ConjuntoSilverError("Conjunto incompleto")

    referencias = {}
    for version in versiones:
        if not isinstance(version, dict):
            raise ConjuntoSilverError("Producto conjunto inválido")
        producto = version.get("product")
        if producto not in PRODUCTOS or producto in referencias:
            raise ConjuntoSilverError("Productos conjuntos inválidos")
        referencias[producto] = {
            campo: version[campo]
            for campo in ("current", "manifest", "manifest_signature")
        }

    validar_marcador_v03(
        contenido, firma, pem_quality, referencias
    )

    producto_autorizado = autorizacion["product"]
    if producto_autorizado not in PRODUCTOS:
        raise ConjuntoSilverError("Producto no pertenece al conjunto")

    for campo in ("current", "manifest", "manifest_signature"):
        if autorizacion[campo] != referencias[producto_autorizado][campo]:
            raise ConjuntoSilverError(
                "Autorización incompatible con marcador firmado"
            )

    registros = [referencias[p]["current"] for p in sorted(PRODUCTOS)]
    verificar_generaciones_vigentes(cliente, registros)

    for version in versiones:
        producto = version["product"]
        refs = referencias[producto]

        current = json.loads(leer_verificado(cliente, refs["current"]))
        manifest_bytes = leer_verificado(cliente, refs["manifest"])
        firma_manifest = leer_verificado(
            cliente, refs["manifest_signature"]
        )

        verificar_firma_ecdsa(
            manifest_bytes, firma_manifest, pem_quality
        )
        manifest = json.loads(manifest_bytes)

        validar_publicacion_interna(current, manifest)

        if (
            current["product"] != producto
            or manifest["product"] != producto
            or current["version_id"] != version["version_id"]
            or manifest["version_id"] != version["version_id"]
            or current["manifest_uri"] != refs["manifest"]["uri"]
            or str(current["manifest_generation"])
            != refs["manifest"]["generation"]
            or current["manifest_sha256"] != refs["manifest"]["sha256"]
            or current["manifest_signature"]
            != refs["manifest_signature"]
        ):
            raise ConjuntoSilverError(
                "Registry o manifest incompatibles"
            )

        for campo in ("run_id", "attempt_id", "batch_id"):
            if (
                manifest.get(campo) != marcador[campo]
                or current.get(campo) != marcador[campo]
                or version[campo] != marcador[campo]
            ):
                raise ConjuntoSilverError(
                    f"Incompatibilidad conjunta: {campo}"
                )

        if manifest.get("processor_commit") != marcador["processor_commit"]:
            raise ConjuntoSilverError("Commit conjunto incompatible")

    for campo in ("run_id", "attempt_id", "batch_id", "processor_commit"):
        if autorizacion[campo] != marcador[campo]:
            raise ConjuntoSilverError(
                f"Autorización y conjunto incompatibles: {campo}"
            )

    verificar_generaciones_vigentes(cliente, registros)

    return {
        "marcador_firmado_verificado": True,
        "productos_conjuntos_verificados": 2,
        "referencias_verificadas": referencias[producto_autorizado],
        "registros_conjuntos": registros,
        "consumo_productivo_autorizado": False,
    }
