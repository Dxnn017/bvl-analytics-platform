"""Adaptacion de entrada exclusivamente para Cotizaciones.

No modifica Silver oficial ni inventa metadatos.
Requiere decision Quality vinculada a la autorizacion
correctiva firmada.
"""

from pyspark.sql import functions as F

from autorizacion_correctiva_quality import (
    exigir,
    validar_decision_cotizaciones,
)


FUENTE = "mercado_diario/cotizaciones"

COLUMNAS_ADAPTADAS = {
    "fuente": "string",
    "fecha_ingesta": "date",
}


def adaptar_cotizaciones(
    df,
    autorizacion,
    fuente_verificada,
):
    """Agrega NULL tipados sin alterar columnas originales."""

    decision = validar_decision_cotizaciones(
        autorizacion
    )

    auth_silver = fuente_verificada["auth"]
    preflight = fuente_verificada["preflight"]
    manifest = fuente_verificada["manifest"]

    version = decision["version_id"]

    exigir(
        auth_silver["product"] == FUENTE
        and manifest["product"] == FUENTE
        and preflight["product"] == FUENTE,
        "Adaptacion sobre fuente no autorizada"
    )

    exigir(
        auth_silver["version_id"]
        == manifest["version_id"]
        == preflight["version_id"]
        == version,
        "Version de cotizaciones incompatible"
    )

    exigir(
        preflight["esquema_fisico_verificado"] is True,
        "Cotizaciones sin preflight fisico"
    )

    exigir(
        len(df.columns) == len(set(df.columns)),
        "Columnas Silver duplicadas"
    )

    exigir(
        not set(COLUMNAS_ADAPTADAS) & set(df.columns),
        "La adaptacion no permite sobrescribir metadatos"
    )

    exigir(
        preflight["filas_validadas"]
        == manifest["records_count"],
        "Conteo Silver incompatible"
    )

    resultado = df

    for nombre, tipo in COLUMNAS_ADAPTADAS.items():

        resultado = resultado.withColumn(
            nombre,
            F.lit(None).cast(tipo)
        )

    evidencia = {
        "status": "APPLIED",
        "product": FUENTE,
        "version_id": version,
        "authorization_id":
            autorizacion["authorization_id"],
        "decision": decision,
        "manifest_reference":
            auth_silver["manifest"],
        "input_records":
            preflight["filas_validadas"],
        "input_columns": df.columns,
        "output_columns": resultado.columns,
        "adapted_columns": COLUMNAS_ADAPTADAS.copy(),
        "metadata_values": {
            "fuente": None,
            "fecha_ingesta": None,
        },
        "metadata_invented": False,
        "silver_modified": False,
        "constructor_modified": False,
        "publication_allowed": False,
    }

    return resultado, evidencia
