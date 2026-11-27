"""Recálculo incremental local de impacto_hechos_valor.

Reutiliza el constructor histórico y no publica versiones oficiales.
"""

import sys
from pathlib import Path

from pyspark.sql import functions as F

DIRECTORIO_EVENTOS = Path(__file__).resolve().parent.parent / "eventos"
sys.path.insert(0, str(DIRECTORIO_EVENTOS))

from build_impacto_hechos_valor import (
    VENTANA_DIAS,
    preparar_candidatos,
    construir_gold,
    validar_clave,
)


CLAVE_IMPACTO = ["numero_expediente", "nemonico"]


def resolver_expedientes_por_cotizaciones(
    hechos,
    cotizaciones_afectadas,
):
    """Identifica expedientes con ventanas bursátiles afectadas."""

    candidatos = preparar_candidatos(hechos).select(
        "numero_expediente",
        "nemonico",
        "fecha_hecho",
    )

    cambios = cotizaciones_afectadas.select(
        "nemonico",
        F.col("fecha").alias("fecha_cambio"),
    ).distinct()

    condicion = (
        (candidatos["nemonico"] == cambios["nemonico"])
        & (F.abs(F.datediff(
            candidatos["fecha_hecho"],
            cambios["fecha_cambio"],
        )) <= VENTANA_DIAS)
        & (candidatos["fecha_hecho"] != cambios["fecha_cambio"])
    )

    return (
        candidatos.join(cambios, condicion, "left_semi")
        .select("numero_expediente")
        .distinct()
    )


def recalcular_impacto_hechos_valor(
    gold_hechos_vigente,
    gold_mercado_vigente,
    gold_impacto_base,
    expedientes_afectados,
):
    """Reconstruye todos los impactos de los expedientes afectados."""

    expedientes = expedientes_afectados.select(
        "numero_expediente"
    ).distinct()

    invalidos = expedientes.filter(
        F.col("numero_expediente").isNull()
        | (F.trim(F.col("numero_expediente").cast("string")) == "")
    )

    if invalidos.limit(1).count():
        raise ValueError("Expedientes de impacto inválidos.")

    hechos_afectados = gold_hechos_vigente.join(
        expedientes,
        "numero_expediente",
        "left_semi",
    )

    candidatos = preparar_candidatos(hechos_afectados)

    validar_clave(
        candidatos,
        CLAVE_IMPACTO,
        "candidatos impacto afectados",
    )

    impacto_recalculado = construir_gold(
        candidatos,
        gold_mercado_vigente,
    )

    impacto_conservado = gold_impacto_base.join(
        expedientes,
        "numero_expediente",
        "left_anti",
    )

    impacto_candidato = impacto_conservado.unionByName(
        impacto_recalculado
    )

    validar_clave(
        impacto_candidato,
        CLAVE_IMPACTO,
        "Gold impacto_hechos_valor candidato",
    )

    return impacto_candidato
