"""Procesamiento candidato Gold sobre DataFrames verificados.

No acredita fuentes ni autoriza consumo o publicación.
No realiza escrituras.
"""

from pyspark.sql import functions as F

from build_mercado_valor_diario import (
    construir_gold as construir_mercado,
)
from build_actividad_sab_diaria import (
    construir_gold as construir_sab,
)
from build_empresa_trimestre import (
    construir_gold as construir_empresa,
)

DEPENDENCIAS = {
    "mercado_valor_diario": (
        "cotizaciones",
        "tipo_cambio",
        "embig",
        "puente_ruc_nemonico",
    ),
    "actividad_sab_diaria": (
        "montos_sab",
    ),
    "empresa_trimestre": (
        "principales_cuentas",
    ),
}

CLAVES = {
    "mercado_valor_diario": ("nemonico", "fecha"),
    "actividad_sab_diaria": ("fecha", "codigo_sab"),
    "empresa_trimestre": ("id_registro",),
}

BLOQUEADOS = {
    "hechos_empresa",
    "impacto_hechos_valor",
}


def validar_producto(producto):
    if producto in BLOQUEADOS:
        raise PermissionError(
            "Producto bloqueado por identidad empresarial"
        )

    if producto not in DEPENDENCIAS:
        raise ValueError("Producto desconocido")


def validar_fuentes(producto, fuentes):
    validar_producto(producto)

    if not isinstance(fuentes, dict):
        raise ValueError("Fuentes inválidas")

    requeridas = set(DEPENDENCIAS[producto])

    if set(fuentes) != requeridas:
        raise ValueError(
            "Dependencias incompatibles: "
            f"faltantes={sorted(requeridas-set(fuentes))}, "
            f"adicionales={sorted(set(fuentes)-requeridas)}"
        )


def validar_claves(df, producto):
    claves = CLAVES[producto]

    invalida = F.lit(False)

    for clave in claves:
        invalida = (
            invalida
            | F.col(clave).isNull()
            | (F.trim(F.col(clave).cast("string")) == "")
        )

    if df.filter(invalida).limit(1).count():
        raise ValueError("Claves Gold inválidas")

    if (
        df.groupBy(*claves)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError("Claves Gold duplicadas")


def construir_candidato(
    producto, fuentes,
    *,
    cobertura=None,
    version_silver=None,
):
    validar_fuentes(producto, fuentes)

    if producto == "mercado_valor_diario":
        gold = construir_mercado(
            fuentes["cotizaciones"],
            fuentes["tipo_cambio"],
            fuentes["embig"],
            fuentes["puente_ruc_nemonico"],
        )

    elif producto == "actividad_sab_diaria":
        gold = construir_sab(
            fuentes["montos_sab"],
            cobertura,
            version_silver,
        )

    else:
        gold = construir_empresa(
            fuentes["principales_cuentas"]
        )

    validar_claves(gold, producto)

    return gold


def recalcular_candidato(
    producto, fuentes, gold_base, alcance,
    *,
    cobertura=None,
    version_silver=None,
):
    """Reutiliza recalculadores existentes sin publicar datos."""

    validar_fuentes(producto, fuentes)

    if gold_base is None or alcance is None:
        raise ValueError("Falta base Gold o alcance incremental")

    if producto == "mercado_valor_diario":

        from recalcular_mercado_valor import (
            recalcular_mercado_valor,
        )

        candidato = recalcular_mercado_valor(
            fuentes["cotizaciones"],
            fuentes["tipo_cambio"],
            fuentes["embig"],
            fuentes["puente_ruc_nemonico"],
            gold_base,
            alcance,
        )

    elif producto == "actividad_sab_diaria":

        from recalcular_actividad_sab import (
            recalcular_actividad_sab,
        )

        candidato = recalcular_actividad_sab(
            fuentes["montos_sab"],
            gold_base,
            alcance,
            cobertura,
            version_silver,
        )

    else:

        from recalcular_empresa_trimestre import (
            recalcular_empresa_trimestre,
        )

        candidato = recalcular_empresa_trimestre(
            fuentes["principales_cuentas"],
            gold_base,
            alcance,
        )

    validar_claves(candidato, producto)

    return candidato
