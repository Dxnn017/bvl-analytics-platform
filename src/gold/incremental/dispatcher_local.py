#!/usr/bin/env python3
"""Despachador preliminar Gold.

Valida las tareas y prepara decisiones de despacho.
No ejecuta Spark, no escribe archivos ni publica resultados.
"""

import json
from pathlib import Path


EJECUTORES = {
    "mercado_valor_diario": "incremental_executor.py",
    "actividad_sab_diaria": "ejecutar_actividad_sab_local.py",
    "empresa_trimestre": "ejecutar_empresa_trimestre_local.py",
    "hechos_empresa": "ejecutar_hechos_empresa_local.py",
    "impacto_hechos_valor": "ejecutar_impacto_hechos_valor_local.py",
}


class DispatcherError(ValueError):
    """Plan o tarea incompatible con el despacho local."""


def preparar_despacho(plan):
    """Genera un diagnóstico sin ejecutar tareas."""

    if not isinstance(plan, dict):
        raise DispatcherError("El plan debe ser un objeto JSON.")

    tareas = plan.get("tareas")

    if not isinstance(tareas, list):
        raise DispatcherError("El plan no contiene una lista de tareas.")

    productos = [
        tarea.get("producto_gold")
        for tarea in tareas
    ]

    if len(productos) != len(set(productos)):
        raise DispatcherError("Hay productos Gold duplicados.")

    if any(producto not in EJECUTORES for producto in productos):
        raise DispatcherError("Existe un producto Gold desconocido.")

    ordenes = [tarea.get("orden") for tarea in tareas]

    if any(
        type(orden) is not int or orden < 0
        for orden in ordenes
    ):
        raise DispatcherError("Orden de tarea inválido.")

    if len(ordenes) != len(set(ordenes)):
        raise DispatcherError("Existen posiciones de ejecución duplicadas.")

    tareas_ordenadas = sorted(
        tareas, key=lambda tarea: tarea["orden"]
    )

    completados = set()
    decisiones = []

    for tarea in tareas_ordenadas:
        producto = tarea["producto_gold"]

        pendientes = [
            predecesor
            for predecesor in tarea.get(
                "predecesores_planificados", []
            )
            if predecesor not in completados
        ]

        motivos = []

        if pendientes:
            motivos.append("PREDECESORES_SIN_EJECUTAR")

        if tarea.get("estado") != "LISTA_PARA_EJECUCION":
            motivos.append("VALIDACION_OPERATIVA_PENDIENTE")

        if not tarea.get("alcance_efectivo_resuelto", False):
            motivos.append("ALCANCE_NO_RESUELTO")

        if tarea.get("requiere_resolver_detalle_externo", False):
            motivos.append("DETALLE_EXTERNO_PENDIENTE")

        if tarea.get(
            "requiere_cambios_efectivos_predecesores", False
        ):
            motivos.append("PROPAGACION_INDIRECTA_PENDIENTE")

        if not tarea.get("ejecucion_autorizada", False):
            motivos.append("EJECUCION_NO_AUTORIZADA")

        decisiones.append({
            "producto_gold": producto,
            "orden": tarea["orden"],
            "ejecutor_local": EJECUTORES[producto],
            "despachable": len(motivos) == 0,
            "motivos_bloqueo": motivos,
            "publicacion_autorizada": False,
        })

        # No marcamos como completado un producto solo por estar
        # planificado o por tener un diagnóstico favorable.

    return {
        "entorno": "PRUEBA_LOCAL",
        "tipo": "DIAGNOSTICO_DESPACHO_GOLD",
        "total_tareas": len(decisiones),
        "tareas_despachables": sum(
            decision["despachable"]
            for decision in decisiones
        ),
        "decisiones": decisiones,
        "ejecucion_realizada": False,
        "publicacion_autorizada": False,
    }



def preparar_despacho_desde_manifest(ruta_manifest):
    """Planifica un manifest Silver y diagnostica sus tareas Gold."""

    try:
        from .incremental_planner import planificar_archivo_manifest
    except ImportError:
        from incremental_planner import planificar_archivo_manifest

    plan = planificar_archivo_manifest(str(ruta_manifest))
    return preparar_despacho(plan)




def preparar_despacho_con_progreso(plan, productos_completados=None):
    """Diagnostica tareas considerando ejecuciones Gold completadas."""

    try:
        from .validar_secuencia_gold import validar_tarea_gold
    except ImportError:
        from validar_secuencia_gold import validar_tarea_gold

    if not isinstance(plan, dict):
        raise DispatcherError("Plan Gold invalido.")

    tareas = plan.get("tareas")

    if not isinstance(tareas, list):
        raise DispatcherError("El plan no contiene tareas.")

    completados = set(productos_completados or [])

    productos = [t.get("producto_gold") for t in tareas]

    if len(productos) != len(set(productos)):
        raise DispatcherError("Productos duplicados en el plan.")

    if any(p not in EJECUTORES for p in productos):
        raise DispatcherError("Producto Gold desconocido.")

    if not completados.issubset(set(productos)):
        raise DispatcherError(
            "Hay productos completados ajenos al plan."
        )

    ordenes = [t.get("orden") for t in tareas]

    if (
        any(type(o) is not int or o < 0 for o in ordenes)
        or len(ordenes) != len(set(ordenes))
    ):
        raise DispatcherError("Orden de tareas invalido.")

    decisiones = []

    for tarea in sorted(tareas, key=lambda t: t["orden"]):
        producto = tarea["producto_gold"]

        diagnostico = validar_tarea_gold(
            tarea, completados
        )

        motivos = list(diagnostico["motivos_bloqueo"])

        if producto in completados:
            motivos.append("PRODUCTO_YA_COMPLETADO")

        if tarea.get("estado") != "LISTA_PARA_EJECUCION":
            motivos.append("VALIDACION_OPERATIVA_PENDIENTE")

        if tarea.get("requiere_cambios_efectivos_predecesores", False):
            if tarea.get(
                "cambios_efectivos_predecesores_resueltos",
                False
            ):
                motivos = [
                    m for m in motivos
                    if m != "CAMBIOS_PREDECESORES_NO_RESUELTOS"
                ]

        decisiones.append({
            "producto_gold": producto,
            "orden": tarea["orden"],
            "ejecutor_local": EJECUTORES[producto],
            "completado": producto in completados,
            "despachable": len(motivos) == 0,
            "motivos_bloqueo": motivos,
            "publicacion_autorizada": False,
        })

    return {
        "entorno": "PRUEBA_LOCAL",
        "tipo": "DESPACHO_CON_PROGRESO",
        "productos_completados": sorted(completados),
        "tareas_despachables": sum(
            d["despachable"] for d in decisiones
        ),
        "decisiones": decisiones,
        "ejecucion_realizada": False,
        "publicacion_autorizada": False,
    }



def main():
    import argparse

    parser = argparse.ArgumentParser()
    origen = parser.add_mutually_exclusive_group(required=True)
    origen.add_argument("--plan")
    origen.add_argument("--manifest")
    args = parser.parse_args()

    if args.manifest:
        diagnostico = preparar_despacho_desde_manifest(
            args.manifest
        )
    else:
        plan = json.loads(
            Path(args.plan).read_text(encoding="utf-8")
        )
        diagnostico = preparar_despacho(plan)

    print(json.dumps(
        diagnostico,
        indent=2,
        ensure_ascii=False,
    ))


if __name__ == "__main__":
    main()
