"""Integración incremental Silver → Gold mercado."""

from detectar_alcance_mercado import detectar_claves_mercado_integral
from recalcular_mercado_valor import recalcular_mercado_valor


def actualizar_mercado_desde_silver(
    cot_antes, cot_nuevas,
    tc_antes, tc_nuevo,
    embig_antes, embig_nuevo,
    puente_antes, puente_nuevo,
    gold_base,
):
    """Detecta cambios Silver y construye candidata Gold local."""

    claves = detectar_claves_mercado_integral(
        cot_antes, cot_nuevas,
        tc_antes, tc_nuevo,
        embig_antes, embig_nuevo,
        puente_antes, puente_nuevo,
    )

    candidato = recalcular_mercado_valor(
        cot_nuevas,
        tc_nuevo,
        embig_nuevo,
        puente_nuevo,
        gold_base,
        claves,
    )

    return {
        "claves_afectadas": claves,
        "gold_candidato": candidato,
        "publicacion_autorizada": False,
    }
