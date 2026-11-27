"""Ejecución integrada local de los cinco productos Gold.

Recibe DataFrames Silver/Gold y alcances ya resueltos.
No escribe archivos ni autoriza publicación.
"""

from recalcular_mercado_valor import recalcular_mercado_valor
from recalcular_actividad_sab import recalcular_actividad_sab
from recalcular_empresa_trimestre import recalcular_empresa_trimestre
from recalcular_hechos_empresa import recalcular_hechos_empresa
from encadenar_gold_impacto import encadenar_actualizacion_impacto


def ejecutar_cinco_gold_memoria(silver, gold_base, alcances):
    """Construye cinco resultados Gold con dependencias reales."""

    silver_requeridos = {
        "cotizaciones", "tipo_cambio", "embig", "puente",
        "montos_sab", "finanzas",
        "eventos", "identidad", "documentos", "textos",
    }
    gold_requeridos = {
        "mercado", "actividad_sab", "empresa_trimestre",
        "hechos_empresa", "impacto",
    }
    alcance_requeridos = {
        "claves_mercado", "fechas_sab",
        "grupos_empresa", "expedientes_hechos",
    }

    for nombre, datos, requeridos in (
        ("Silver", silver, silver_requeridos),
        ("Gold base", gold_base, gold_requeridos),
        ("Alcances", alcances, alcance_requeridos),
    ):
        if not isinstance(datos, dict):
            raise ValueError(f"{nombre} debe ser un diccionario")
        faltantes = requeridos - set(datos)
        if faltantes:
            raise ValueError(
                f"{nombre}: faltan {sorted(faltantes)}"
            )
        if any(datos[clave] is None for clave in requeridos):
            raise ValueError(f"{nombre}: existen entradas vacías")

    mercado = recalcular_mercado_valor(
        silver["cotizaciones"],
        silver["tipo_cambio"],
        silver["embig"],
        silver["puente"],
        gold_base["mercado"],
        alcances["claves_mercado"],
    )

    sab = recalcular_actividad_sab(
        silver["montos_sab"],
        gold_base["actividad_sab"],
        alcances["fechas_sab"],
    )

    empresa = recalcular_empresa_trimestre(
        silver["finanzas"],
        gold_base["empresa_trimestre"],
        alcances["grupos_empresa"],
    )

    hechos = recalcular_hechos_empresa(
        silver["eventos"],
        silver["identidad"],
        silver["documentos"],
        silver["textos"],
        gold_base["hechos_empresa"],
        alcances["expedientes_hechos"],
    )

    impacto = encadenar_actualizacion_impacto(
        hechos_base=gold_base["hechos_empresa"],
        hechos_candidato=hechos,
        mercado_base=gold_base["mercado"],
        mercado_candidato=mercado,
        impacto_base=gold_base["impacto"],
    )

    if impacto["validacion_exitosa"] is not True:
        raise ValueError("Falló la validación de impacto")

    return {
        "mercado_valor_diario": mercado,
        "actividad_sab_diaria": sab,
        "empresa_trimestre": empresa,
        "hechos_empresa": hechos,
        "impacto_hechos_valor": impacto,
        "productos_integrados": 5,
        "publicacion_autorizada": False,
    }
