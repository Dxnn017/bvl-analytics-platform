"""Propagación local de cambios Gold hacia impacto_hechos_valor.

No ejecuta publicaciones ni modifica datos históricos.
"""

from pyspark.sql import functions as F

from recalcular_impacto_hechos_valor import (
    resolver_expedientes_por_cotizaciones,
)


def resolver_alcance_impacto(
    hechos_anteriores,
    hechos_vigentes,
    hechos_modificados,
    cotizaciones_anteriores_modificadas,
    cotizaciones_nuevas_modificadas,
):
    """Obtiene los expedientes que requieren recalcular impactos.

    hechos_modificados:
        DataFrame con numero_expediente de los hechos alterados,
        tanto anteriores como nuevos.

    cotizaciones_*_modificadas:
        DataFrames con nemonico y fecha de las cotizaciones
        afectadas, antes y después del cambio.
    """

    expedientes_hechos = (
        hechos_modificados
        .select("numero_expediente")
        .distinct()
    )

    cotizaciones_cambiadas = (
        cotizaciones_anteriores_modificadas
        .select("nemonico", "fecha")
        .unionByName(
            cotizaciones_nuevas_modificadas.select(
                "nemonico", "fecha"
            )
        )
        .distinct()
    )

    afectados_por_mercado_anterior = (
        resolver_expedientes_por_cotizaciones(
            hechos_anteriores,
            cotizaciones_cambiadas,
        )
    )

    afectados_por_mercado_vigente = (
        resolver_expedientes_por_cotizaciones(
            hechos_vigentes,
            cotizaciones_cambiadas,
        )
    )

    expedientes = (
        expedientes_hechos
        .unionByName(afectados_por_mercado_anterior)
        .unionByName(afectados_por_mercado_vigente)
        .distinct()
    )

    invalidos = expedientes.filter(
        F.col("numero_expediente").isNull()
        | (
            F.trim(
                F.col("numero_expediente").cast("string")
            ) == ""
        )
    )

    if invalidos.limit(1).count():
        raise ValueError(
            "La propagación contiene expedientes inválidos."
        )

    return expedientes
