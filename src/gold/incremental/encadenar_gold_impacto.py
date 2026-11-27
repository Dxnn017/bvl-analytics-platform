"""Encadenamiento incremental real entre productos Gold.

Recibe versiones anteriores y vigentes de los predecesores.
No escribe archivos ni publica resultados.
"""

from pyspark.sql import functions as F

from actualizar_impacto_desde_cambios import (
    actualizar_impacto_desde_cambios,
)


def encadenar_actualizacion_impacto(
    hechos_base,
    hechos_candidato,
    mercado_base,
    mercado_candidato,
    impacto_base,
):
    """Reconstruye impactos desde candidatas Gold actualizadas."""

    resultado = actualizar_impacto_desde_cambios(
        hechos_anteriores=hechos_base,
        hechos_vigentes=hechos_candidato,
        mercado_anterior=mercado_base,
        mercado_vigente=mercado_candidato,
        impacto_base=impacto_base,
    )

    expedientes = resultado["expedientes_afectados"]

    candidato = resultado["impacto_candidato"]

    # Alinear columnas antes de comparar conjuntos.
    candidato = candidato.select(*impacto_base.columns)

    diferencias = (
        impacto_base.exceptAll(candidato).count()
        + candidato.exceptAll(impacto_base).count()
    )

    no_afectados_base = impacto_base.join(
        expedientes,
        "numero_expediente",
        "left_anti",
    )

    no_afectados_candidato = candidato.join(
        expedientes,
        "numero_expediente",
        "left_anti",
    )

    diferencias_no_afectados = (
        no_afectados_base.exceptAll(no_afectados_candidato).count()
        + no_afectados_candidato.exceptAll(no_afectados_base).count()
    )

    if diferencias_no_afectados != 0:
        raise ValueError(
            "El recálculo modificó impactos fuera del alcance."
        )

    duplicados = (
        candidato
        .groupBy("numero_expediente", "nemonico")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicados:
        raise ValueError(
            "La candidata de impacto contiene claves duplicadas."
        )

    return {
        "producto_gold": "impacto_hechos_valor",
        "expedientes_afectados": expedientes.count(),
        "registros_base": impacto_base.count(),
        "registros_candidato": candidato.count(),
        "diferencias": diferencias,
        "diferencias_no_afectados": diferencias_no_afectados,
        "impacto_candidato": candidato,
        "validacion_exitosa": True,
        "publicacion_autorizada": False,
    }
