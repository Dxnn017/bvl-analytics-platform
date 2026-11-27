"""Coordinación local de los cinco productos Gold."""

from orquestador_gold_local import ejecutar_orquestacion_local

PRODUCTOS = {
    "mercado_valor_diario",
    "actividad_sab_diaria",
    "empresa_trimestre",
    "hechos_empresa",
    "impacto_hechos_valor",
}


def ejecutar_gold_integral_local(plan, alcances):
    """Ejecuta el plan completo sin autorizar publicación."""

    if not isinstance(plan, dict):
        raise ValueError("Plan Gold inválido.")

    tareas = plan.get("tareas", [])

    productos = [t["producto_gold"] for t in tareas]

    if len(productos) != 5 or set(productos) != PRODUCTOS:
        raise ValueError("Se requieren exactamente los cinco productos.")

    if set(alcances) != PRODUCTOS:
        raise ValueError("Faltan alcances Gold.")

    resultado = ejecutar_orquestacion_local(plan, alcances)

    if set(resultado["productos_completados"]) != PRODUCTOS:
        raise ValueError("No finalizaron los cinco productos.")

    if resultado["publicacion_autorizada"] is not False:
        raise ValueError("Publicación no permitida.")

    return {
        **resultado,
        "integracion_cinco_productos": True,
        "publicacion_autorizada": False,
    }
