"""Detecta claves Gold afectadas por cambios en cotizaciones Silver."""

from pyspark.sql import functions as F


def detectar_claves_mercado(cotizaciones_anteriores, cotizaciones_nuevas):
    """Incluye claves anteriores y nuevas para correcciones y eliminaciones."""

    anteriores = cotizaciones_anteriores
    nuevas = cotizaciones_nuevas

    if dict(anteriores.dtypes) != dict(nuevas.dtypes):
        raise ValueError("Esquemas Silver incompatibles.")

    columnas = sorted(anteriores.columns)

    anteriores = anteriores.select(*columnas)
    nuevas = nuevas.select(*columnas)

    eliminadas_o_modificadas = anteriores.exceptAll(nuevas)
    insertadas_o_modificadas = nuevas.exceptAll(anteriores)

    cambios = eliminadas_o_modificadas.unionByName(
        insertadas_o_modificadas
    )

    claves = (
        cambios.select(
            F.col("valor").alias("nemonico"),
            F.col("fecha_cotizacion").alias("fecha"),
        )
        .distinct()
    )

    return claves


def detectar_claves_mercado_integral(
    cot_antes, cot_nuevas,
    tc_antes, tc_nuevo,
    embig_antes, embig_nuevo,
    puente_antes, puente_nuevo,
):
    """Resuelve claves afectadas por las cuatro fuentes Silver."""

    def cambios(antes, despues):
        if dict(antes.dtypes) != dict(despues.dtypes):
            raise ValueError("Esquemas Silver incompatibles.")
        columnas = sorted(antes.columns)
        a = antes.select(*columnas)
        b = despues.select(*columnas)
        return a.exceptAll(b).unionByName(b.exceptAll(a))

    claves_cot = detectar_claves_mercado(cot_antes, cot_nuevas)

    fechas = (
        cambios(tc_antes, tc_nuevo).select("fecha")
        .unionByName(
            cambios(embig_antes, embig_nuevo).select("fecha")
        )
        .distinct()
    )

    nemonicos = (
        cambios(puente_antes, puente_nuevo)
        .select(F.col("nemonico_valor").alias("nemonico"))
        .distinct()
    )

    cotizaciones = (
        cot_antes.select(
            F.col("valor").alias("nemonico"),
            F.col("fecha_cotizacion").alias("fecha"),
        )
        .unionByName(
            cot_nuevas.select(
                F.col("valor").alias("nemonico"),
                F.col("fecha_cotizacion").alias("fecha"),
            )
        )
        .distinct()
    )

    claves_macro = cotizaciones.join(fechas, "fecha", "left_semi")
    claves_puente = cotizaciones.join(
        nemonicos, "nemonico", "left_semi"
    )

    return (
        claves_cot.unionByName(claves_macro)
        .unionByName(claves_puente)
        .distinct()
    )
