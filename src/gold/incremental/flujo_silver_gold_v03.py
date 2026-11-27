"""Integración candidata Silver–Gold v0.3, solo lectura y pruebas."""

import hashlib
import json

from adaptador_gcs_quality import leer_objeto_gcs
from autorizacion_gold_quality_v03 import validar_autorizacion
from contrato_silver_oficial import validar_publicacion_interna
from verificar_firma_quality import verificar_firma_ecdsa
from control_concurrencia_gcs import verificar_generaciones_vigentes
from vincular_archivos_manifest import validar_archivos_del_manifest
from validar_esquema_particiones import validar_esquema_particiones


class FlujoSilverV03Error(ValueError):
    pass


def leer_verificado(cliente, referencia):
    """Comprueba generación exacta, tamaño y SHA-256."""
    contenido = leer_objeto_gcs(
        cliente, referencia["uri"], referencia["generation"]
    )

    if len(contenido) != referencia["size_bytes"]:
        raise FlujoSilverV03Error("Tamaño incompatible")

    if hashlib.sha256(contenido).hexdigest() != referencia["sha256"]:
        raise FlujoSilverV03Error("SHA-256 incompatible")

    return contenido


def preparar_silver_v03(
    cliente,
    autorizacion_bytes,
    firma_autorizacion_der,
    pem_quality,
    producto_gold,
):
    """Prepara evidencia verificada; nunca habilita producción."""

    if not autorizacion_bytes or not firma_autorizacion_der:
        raise FlujoSilverV03Error("Autorización firmada ausente")

    try:
        autorizacion = json.loads(autorizacion_bytes)
    except (ValueError, UnicodeDecodeError) as exc:
        raise FlujoSilverV03Error("Autorización JSON inválida") from exc

    if not isinstance(autorizacion, dict):
        raise FlujoSilverV03Error("Autorización inválida")

    referencias = {
        campo: autorizacion.get(campo)
        for campo in ("current", "manifest", "manifest_signature")
    }

    validar_autorizacion(
        autorizacion_bytes,
        firma_autorizacion_der,
        pem_quality,
        producto_gold,
        referencias,
    )

    productos_conjuntos = {
        "referencias_smv/valores_inscritos",
        "referencias_smv/puente_ruc_nemonico",
    }

    if autorizacion["product"] in productos_conjuntos:
        from verificar_conjunto_silver_v03 import verificar_conjunto_silver

        evidencia_conjunta = verificar_conjunto_silver(
            cliente, autorizacion, pem_quality
        )

        referencias = evidencia_conjunta["referencias_verificadas"]
    else:
        evidencia_conjunta = None

    ref_current = referencias["current"]
    ref_manifest = referencias["manifest"]
    ref_firma = referencias["manifest_signature"]

    # Referencias obtenidas de objetos recuperados y verificados.
    if evidencia_conjunta is not None:
        for campo in ("current", "manifest", "manifest_signature"):
            if referencias[campo] != autorizacion[campo]:
                raise FlujoSilverV03Error(
                    "Referencia conjunta incompatible con autorización"
                )

    # Comprobación inicial de vigencia.
    verificar_generaciones_vigentes(cliente, [ref_current])

    current_bytes = leer_verificado(cliente, ref_current)
    manifest_bytes = leer_verificado(cliente, ref_manifest)
    firma_manifest = leer_verificado(cliente, ref_firma)

    # Evidencias calculadas sobre los bytes realmente recuperados.
    referencias_observadas = {}
    for nombre, ref, datos in (
        ("current", ref_current, current_bytes),
        ("manifest", ref_manifest, manifest_bytes),
        ("manifest_signature", ref_firma, firma_manifest),
    ):
        referencias_observadas[nombre] = {
            "uri": ref["uri"],
            "generation": ref["generation"],
            "sha256": hashlib.sha256(datos).hexdigest(),
            "size_bytes": len(datos),
        }

        if referencias_observadas[nombre] != autorizacion[nombre]:
            raise FlujoSilverV03Error(
                f"Referencia recuperada incompatible: {nombre}"
            )


    # La firma se verifica sobre los bytes originales.
    verificar_firma_ecdsa(
        manifest_bytes, firma_manifest, pem_quality
    )

    try:
        current = json.loads(current_bytes)
        manifest = json.loads(manifest_bytes)
    except (ValueError, UnicodeDecodeError) as exc:
        raise FlujoSilverV03Error("Documento Silver inválido") from exc

    validar_publicacion_interna(current, manifest)

    if (
        current.get("manifest_uri") != ref_manifest["uri"]
        or str(current.get("manifest_generation"))
        != ref_manifest["generation"]
        or current.get("manifest_sha256") != ref_manifest["sha256"]
        or current.get("manifest_signature") != ref_firma
    ):
        raise FlujoSilverV03Error("Manifest no vinculado al registry")

    if (
        manifest.get("product") != autorizacion["product"]
        or manifest.get("version_id") != autorizacion["version_id"]
        or manifest.get("run_id") != autorizacion["run_id"]
        or manifest.get("attempt_id") != autorizacion["attempt_id"]
        or manifest.get("batch_id") != autorizacion["batch_id"]
        or manifest.get("processor_commit")
        != autorizacion["processor_commit"]
    ):
        raise FlujoSilverV03Error("Origen Silver no autorizado")

    if manifest["product"] in {
        "hechos_importancia/documental_restringido",
        "hechos_importancia/identidad_empresarial",
    }:
        raise FlujoSilverV03Error("Fuente restringida para Gold oficial")

    esquema = validar_esquema_particiones(
        manifest,
        [campo.split(": ", 1)[0]
         for campo in manifest["schema"].splitlines()
         if ": " in campo and not campo.startswith((" ", "\t"))],
        manifest["partition_columns"],
    )

    archivos = validar_archivos_del_manifest(
        cliente,
        manifest,
        autorizacion["version_id"],
    )

    if archivos["archivos_validados"] != manifest["files_count"]:
        raise FlujoSilverV03Error("Cantidad de archivos incompatible")

    # Detecta cambios concurrentes durante la lectura.
    verificar_generaciones_vigentes(cliente, [ref_current])

    return {
        "product": manifest["product"],
        "version_id": manifest["version_id"],
        "manifest": manifest,
        "firma_manifest_verificada": True,
        "registry_verificado": True,
        "referencias_verificadas": referencias_observadas,
        "autorizacion_firmada_verificada": True,
        "archivos_integridad_verificada": archivos["archivos_validados"],
        "metadatos_esquema_validos": esquema["metadatos_esquema_validos"],
        "archivos_parquet_verificados": False,
        "consumo_productivo_autorizado": False,
    }


def preparar_silver_v03_con_parquet(
    cliente,
    autorizacion_bytes,
    firma_autorizacion_der,
    pem_quality,
    producto_gold,
    tipos_esperados,
    claves,
    particiones_por_uri,
):
    """Integra controles criptográficos, Parquet y concurrencia."""

    from validar_parquet_silver_gcs import validar_parquet_desde_gcs

    resultado = preparar_silver_v03(
        cliente,
        autorizacion_bytes,
        firma_autorizacion_der,
        pem_quality,
        producto_gold,
    )

    manifest = resultado["manifest"]
    archivos = manifest["files"]

    if (
        not isinstance(tipos_esperados, dict)
        or not tipos_esperados
        or not isinstance(claves, (list, tuple))
        or not claves
        or not isinstance(particiones_por_uri, dict)
    ):
        raise FlujoSilverV03Error("Contrato físico incompleto")

    if set(particiones_por_uri) != {
        ref["uri"] for ref in archivos
    }:
        raise FlujoSilverV03Error("Particiones sin correspondencia exacta")

    from validar_particiones_silver import validar_particiones_uri

    prefijo = manifest["version_prefix"]
    columnas_particion = manifest["partition_columns"]

    for ref in archivos:
        particiones_ruta = validar_particiones_uri(
            ref["uri"], prefijo, columnas_particion
        )

        if particiones_ruta != particiones_por_uri[ref["uri"]]:
            raise FlujoSilverV03Error(
                "Particiones declaradas incompatibles con la ruta"
            )

    filas = 0

    for ref in archivos:
        validacion = validar_parquet_desde_gcs(
            cliente,
            ref,
            tipos_esperados,
            claves,
            particiones_por_uri[ref["uri"]],
            version_prefix=prefijo,
            partition_columns=columnas_particion,
        )

        if validacion["filas_validadas"] != ref.get("rows"):
            raise FlujoSilverV03Error(
                "Cantidad de filas incompatible"
            )

        filas += validacion["filas_validadas"]

    if filas != manifest["records_count"]:
        raise FlujoSilverV03Error(
            "Total de registros incompatible"
        )

    from validar_claves_globales_silver import validar_claves_globales

    claves_globales = validar_claves_globales(
        cliente,
        archivos,
        claves,
    
        version_prefix=prefijo,
        partition_columns=columnas_particion,
        tipos_esperados=tipos_esperados,
    )

    if claves_globales["filas_validadas"] != filas:
        raise FlujoSilverV03Error(
            "Conteo de claves globales incompatible"
        )

    verificar_generaciones_vigentes(
        cliente,
        [{
            "uri": json.loads(autorizacion_bytes)["current"]["uri"],
            "generation": json.loads(
                autorizacion_bytes
            )["current"]["generation"],
        }],
    )

    # Evidencias procedentes de objetos recuperados y comprobados.
    referencias_finales = {
        campo: resultado["referencias_verificadas"][campo]
        for campo in ("current", "manifest", "manifest_signature")
    }

    # Cierre: autorización aún vigente y registros sin cambios.
    validar_autorizacion(
        autorizacion_bytes,
        firma_autorizacion_der,
        pem_quality,
        producto_gold,
        referencias_finales,
    )

    autorizacion_final = json.loads(autorizacion_bytes)

    if autorizacion_final["product"] in {
        "referencias_smv/valores_inscritos",
        "referencias_smv/puente_ruc_nemonico",
    }:
        from verificar_conjunto_silver_v03 import verificar_conjunto_silver

        verificar_conjunto_silver(
            cliente, autorizacion_final, pem_quality
        )

    # La vigencia del current se comprueba inmediatamente al finalizar.
    verificar_generaciones_vigentes(
        cliente, [autorizacion_final["current"]]
    )

    return {
        "product": resultado["product"],
        "version_id": resultado["version_id"],
        "archivos_parquet_verificados": len(archivos),
        "filas_validadas": filas,
        "esquema_fisico_verificado": True,
        "consumo_productivo_autorizado": False,
    }
