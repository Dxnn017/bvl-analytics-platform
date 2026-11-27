"""Actualización incremental de impactos desde cambios Gold.

Operación exclusivamente local y en memoria.
No publica versiones oficiales.
"""

from detectar_cambios_gold import detectar_cambios_gold
from propagar_cambios_impacto import resolver_alcance_impacto
from recalcular_impacto_hechos_valor import (
    recalcular_impacto_hechos_valor,
)


def actualizar_impacto_desde_cambios(
    hechos_anteriores,
    hechos_vigentes,
    mercado_anterior,
    mercado_vigente,
    impacto_base,
):
    """Detecta, propaga y recalcula cambios de impacto."""

    cambios_hechos = detectar_cambios_gold(
        hechos_anteriores,
        hechos_vigentes,
        "hechos_empresa",
    )

    cambios_mercado = detectar_cambios_gold(
        mercado_anterior,
        mercado_vigente,
        "mercado_valor_diario",
    )

    expedientes = resolver_alcance_impacto(
        hechos_anteriores=hechos_anteriores,
        hechos_vigentes=hechos_vigentes,
        hechos_modificados=cambios_hechos["claves_afectadas"],
        cotizaciones_anteriores_modificadas=(
            cambios_mercado["claves_anteriores"]
        ),
        cotizaciones_nuevas_modificadas=(
            cambios_mercado["claves_nuevas"]
        ),
    )

    impacto_candidato = recalcular_impacto_hechos_valor(
        hechos_vigentes,
        mercado_vigente,
        impacto_base,
        expedientes,
    )

    return {
        "expedientes_afectados": expedientes,
        "impacto_candidato": impacto_candidato,
        "cambios_hechos": cambios_hechos,
        "cambios_mercado": cambios_mercado,
    }
