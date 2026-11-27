"""Recálculo incremental Gold mercado_valor_diario."""

from pyspark.sql import functions as F


CLAVES = ["nemonico", "fecha"]


def recalcular_mercado_valor(
    cotizaciones,
    tipo_cambio,
    embig,
    puente,
    gold_base,
    claves_afectadas,
):
    """Reemplaza únicamente las claves afectadas; no publica datos."""

    from build_mercado_valor_diario import construir_gold, validar_clave

    claves = claves_afectadas.select(*CLAVES).distinct()

    # Incluye las claves antiguas para admitir eliminaciones.
    cotizaciones_afectadas = cotizaciones.join(
        claves,
        (cotizaciones["valor"] == claves["nemonico"])
        & (cotizaciones["fecha_cotizacion"] == claves["fecha"]),
        "left_semi",
    )

    recalculados = construir_gold(
        cotizaciones_afectadas,
        tipo_cambio,
        embig,
        puente,
    )

    columnas = gold_base.columns

    if set(recalculados.columns) != set(columnas):
        raise ValueError("Esquemas Gold incompatibles.")

    no_afectados = gold_base.join(
        claves,
        CLAVES,
        "left_anti",
    )

    candidato = no_afectados.unionByName(
        recalculados.select(*columnas)
    )

    validar_clave(candidato, CLAVES, "Gold mercado candidato")

    return candidato
