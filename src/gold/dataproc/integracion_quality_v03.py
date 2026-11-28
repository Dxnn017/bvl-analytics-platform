"""Integracion candidata Quality y ejecutor portable.

Sin escritura GCS, BigQuery ni publicacion.
La acreditacion documental no concede autorizacion productiva.
"""

import json
import re

from flujo_silver_gold_v03 import (
    preparar_silver_v03_con_parquet,
    leer_verificado,
)

from autorizacion_gold_quality_v03 import (
    validar_autorizacion,
)

from control_concurrencia_gcs import (
    verificar_generaciones_vigentes,
)

from verificar_conjunto_silver_v03 import (
    verificar_conjunto_silver,
)

from ejecutor_portable import construir_candidato


FUENTES = {
    "mercado_valor_diario": {
        "cotizaciones": "mercado_diario/cotizaciones",
        "tipo_cambio": "macroeconomia/tipo_cambio",
        "embig": "macroeconomia/embig",
        "puente_ruc_nemonico":
            "referencias_smv/puente_ruc_nemonico",
    },
    "actividad_sab_diaria": {
        "montos_sab": "mercado_diario/montos_sab",
    },
}

PENDIENTES = {"empresa_trimestre"}

BLOQUEADOS = {
    "hechos_empresa",
    "impacto_hechos_valor",
}

MAX_BYTES_CANDIDATO = 128 * 1024 * 1024


def exigir(condicion, mensaje):
    if not condicion:
        raise ValueError(mensaje)


def validar_producto_quality(producto):
    if producto in PENDIENTES:
        raise PermissionError(
            "Empresa pendiente de conciliacion financiera"
        )

    if producto in BLOQUEADOS:
        raise PermissionError(
            "Producto bloqueado por Quality"
        )

    exigir(producto in FUENTES, "Producto desconocido")


def verificar_fuentes_quality(
    cliente, producto, evidencias, pem_quality,
):
    """Reutiliza el preflight existente por cada dependencia."""

    validar_producto_quality(producto)

    exigir(
        isinstance(evidencias, dict)
        and set(evidencias) == set(FUENTES[producto]),
        "Dependencias Quality incompletas o inesperadas"
    )

    fuentes = {}

    for alias, producto_silver in FUENTES[producto].items():

        evidencia = evidencias[alias]

        contenido = evidencia["autorizacion_bytes"]
        firma = evidencia["firma_autorizacion_der"]

        auth = json.loads(contenido)

        exigir(
            auth.get("product") == producto_silver,
            "Autorizacion pertenece a otra fuente"
        )

        resultado = preparar_silver_v03_con_parquet(
            cliente,
            contenido,
            firma,
            pem_quality,
            producto,
            evidencia["tipos_esperados"],
            evidencia["claves"],
            evidencia["particiones_por_uri"],
        )

        exigir(
            resultado["product"] == producto_silver
            and resultado["version_id"] == auth["version_id"],
            "Identidad Silver incompatible"
        )

        exigir(
            resultado["esquema_fisico_verificado"] is True,
            "Esquema fisico no acreditado"
        )

        exigir(
            resultado["consumo_productivo_autorizado"] is False,
            "Estado de autorizacion inesperado"
        )

        # Recuperar exactamente el manifest firmado acreditado.
        manifest = json.loads(
            leer_verificado(cliente, auth["manifest"])
        )

        exigir(
            manifest["product"] == producto_silver
            and manifest["version_id"] == auth["version_id"],
            "Manifest incompatible"
        )

        exigir(
            resultado["archivos_parquet_verificados"]
            == len(manifest["files"])
            and resultado["filas_validadas"]
            == manifest["records_count"],
            "Inventario o conteo incompatible"
        )

        fuentes[alias] = {
            "auth": auth,
            "manifest": manifest,
            "preflight": resultado,
            "evidencia": evidencia,
        }

    total_bytes = sum(
        fuente["manifest"]["bytes_total"]
        for fuente in fuentes.values()
    )

    exigir(
        total_bytes <= MAX_BYTES_CANDIDATO,
        "Limite candidato de materializacion excedido"
    )

    verificar_vigencia(
        cliente, producto, fuentes, pem_quality
    )

    return fuentes


def verificar_vigencia(
    cliente, producto, fuentes, pem_quality,
):
    registros = []

    for fuente in fuentes.values():

        auth = fuente["auth"]
        evidencia = fuente["evidencia"]

        validar_autorizacion(
            evidencia["autorizacion_bytes"],
            evidencia["firma_autorizacion_der"],
            pem_quality,
            producto,
            {
                campo: auth[campo]
                for campo in (
                    "current",
                    "manifest",
                    "manifest_signature",
                )
            },
        )

        registros.append(auth["current"])

        if auth["joint_marker"] is not None:
            conjunto = verificar_conjunto_silver(
                cliente, auth, pem_quality
            )

            registros.extend(
                conjunto["registros_conjuntos"]
            )

    verificar_generaciones_vigentes(
        cliente, registros
    )


def materializar_fuente_verificada(
    spark, cliente, fuente,
):
    """Crea DataFrame desde bytes de generaciones exactas.

    No utiliza lectura Spark por prefijo GCS.
    """

    import pyarrow as pa
    import pyarrow.parquet as pq

    from reconstruir_particiones_hive import (
        reconstruir_particiones,
    )

    manifest = fuente["manifest"]
    evidencia = fuente["evidencia"]

    tipos = evidencia["tipos_esperados"]
    esquema_spark = evidencia["schema_spark"]

    columnas = list(esquema_spark.fieldNames())

    exigir(
        len(columnas) == len(set(columnas))
        and set(columnas) == set(tipos),
        "Esquema Spark incompatible con contrato fisico"
    )

    tablas = []

    for ref in manifest["files"]:

        # Verifica generation, size_bytes y SHA-256.
        contenido = leer_verificado(cliente, ref)

        tabla = pq.read_table(
            pa.BufferReader(contenido),
            use_threads=False,
        )

        tabla = reconstruir_particiones(
            tabla,
            ref["uri"],
            manifest["version_prefix"],
            manifest["partition_columns"],
            tipos,
        )

        exigir(
            tabla.num_rows == ref["rows"],
            "Conteo fisico incompatible"
        )

        exigir(
            set(tabla.column_names) == set(columnas),
            "Columnas fisicas incompatibles"
        )

        for columna, tipo in tipos.items():
            exigir(
                str(tabla.schema.field(columna).type)
                == tipo,
                "Tipo fisico incompatible"
            )

        tablas.append(tabla.select(columnas))

    consolidada = pa.concat_tables(tablas)

    exigir(
        consolidada.num_rows == manifest["records_count"],
        "Conteo total incompatible"
    )

    df = spark.createDataFrame(
        consolidada.to_pylist(),
        schema=esquema_spark,
        verifySchema=True,
    )

    exigir(
        df.count() == manifest["records_count"],
        "Conteo Spark incompatible"
    )

    return df


def preparar_candidato_quality(
    spark, cliente, producto, evidencias, pem_quality,
):
    """Entrada candidata en memoria. Sin escritura ni publicacion."""

    fuentes = verificar_fuentes_quality(
        cliente, producto, evidencias, pem_quality
    )

    dataframes = {
        alias: materializar_fuente_verificada(
            spark, cliente, fuente
        )
        for alias, fuente in fuentes.items()
    }

    verificar_vigencia(
        cliente, producto, fuentes, pem_quality
    )

    parametros = {}

    if producto == "actividad_sab_diaria":

        # No atribuir cobertura sin sidecar acreditado.
        parametros = {
            "cobertura": None,
            "version_silver":
                fuentes["montos_sab"]["auth"]["version_id"],
        }

    candidato = construir_candidato(
        producto, dataframes, **parametros
    ).cache()

    try:
        registros = candidato.count()

        verificar_vigencia(
            cliente, producto, fuentes, pem_quality
        )

        return candidato, {
            "estado": "CANDIDATO_MEMORIA_PREPARADO",
            "producto": producto,
            "registros": registros,
            "versiones_silver": {
                alias: fuente["auth"]["version_id"]
                for alias, fuente in fuentes.items()
            },
            "quality_preflight_verificado": True,
            "consumo_productivo_autorizado": False,
            "publicacion_autorizada": False,
        }

    except BaseException:
        candidato.unpersist()
        raise
