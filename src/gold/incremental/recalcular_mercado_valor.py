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

    from build_mercado_valor_diario import validar_nemonico_canonico

    validar_nemonico_canonico(
        cotizaciones, "valor", ["fecha_cotizacion"], "cotizaciones"
    )

    validar_nemonico_canonico(
        gold_base, "nemonico", ["fecha"], "gold_base"
    )

    claves = claves_afectadas.select(
        F.upper(F.trim("nemonico")).alias("nemonico"),
        "fecha",
    ).distinct()

    validar_clave(claves, CLAVES, "claves afectadas")

    if "nemonico_original" not in gold_base.columns:
        gold_base = gold_base.withColumn(
            "nemonico_original", F.col("nemonico")
        )

    gold_base = gold_base.withColumn(
        "nemonico", F.upper(F.trim("nemonico"))
    )

    # Incluye las claves antiguas para admitir eliminaciones.
    cotizaciones_afectadas = cotizaciones.join(
        claves,
        (F.upper(F.trim(cotizaciones["valor"])) == claves["nemonico"])
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
