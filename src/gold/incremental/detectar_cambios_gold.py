"""Detección de cambios efectivos entre dos versiones Gold.

Solo calcula diferencias en memoria. No escribe ni publica datos.
"""

from pyspark.sql import functions as F


CLAVES_PRODUCTO = {
    "mercado_valor_diario": ["nemonico", "fecha"],
    "actividad_sab_diaria": ["fecha", "codigo_sab"],
    "empresa_trimestre": ["id_registro"],
    "hechos_empresa": ["numero_expediente"],
    "impacto_hechos_valor": ["numero_expediente", "nemonico"],
}


def detectar_cambios_gold(anterior, vigente, producto):
    """Devuelve las claves anteriores y nuevas con cambios efectivos."""

    if producto not in CLAVES_PRODUCTO:
        raise ValueError(f"Producto Gold desconocido: {producto}")

    claves = CLAVES_PRODUCTO[producto]

    if set(anterior.columns) != set(vigente.columns):
        raise ValueError("Los esquemas Gold son incompatibles.")

    for nombre, datos in (
        ("anterior", anterior),
        ("vigente", vigente),
    ):
        invalidos = datos.filter(
            F.expr(
                " OR ".join(
                    f"`{clave}` IS NULL"
                    for clave in claves
                )
            )
        ).limit(1).count()

        if invalidos:
            raise ValueError(
                f"Existen claves nulas en Gold {nombre}."
            )

        duplicados = (
            datos.groupBy(*claves)
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )

        if duplicados:
            raise ValueError(
                f"Existen claves duplicadas en Gold {nombre}."
            )

    columnas = sorted(anterior.columns)

    antes = anterior.select(*columnas)
    despues = vigente.select(*columnas)

    filas_eliminadas_o_modificadas = antes.exceptAll(
        despues
    )

    filas_insertadas_o_modificadas = despues.exceptAll(
        antes
    )

    claves_anteriores = (
        filas_eliminadas_o_modificadas
        .select(*claves)
        .distinct()
    )

    claves_nuevas = (
        filas_insertadas_o_modificadas
        .select(*claves)
        .distinct()
    )

    return {
        "producto_gold": producto,
        "claves_anteriores": claves_anteriores,
        "claves_nuevas": claves_nuevas,
        "claves_afectadas": (
            claves_anteriores
            .unionByName(claves_nuevas)
            .distinct()
        ),
    }
