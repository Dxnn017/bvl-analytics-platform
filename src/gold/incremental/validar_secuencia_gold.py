"""Control de dependencias para ejecución incremental Gold.

No ejecuta Spark ni autoriza publicaciones oficiales.
"""


class SecuenciaGoldError(ValueError):
    """Una tarea no cumple las condiciones de ejecución."""


def validar_tarea_gold(tarea, productos_completados):
    """Comprueba si una tarea puede iniciar su ejecución local."""

    producto = tarea.get("producto_gold")

    predecesores = tarea.get(
        "predecesores_planificados", []
    )

    pendientes = [
        nombre
        for nombre in predecesores
        if nombre not in productos_completados
    ]

    motivos = []

    if pendientes:
        motivos.append(
            "PREDECESORES_PENDIENTES: " + ", ".join(pendientes)
        )

    if not tarea.get("alcance_efectivo_resuelto", False):
        motivos.append("ALCANCE_NO_RESUELTO")

    if tarea.get("requiere_resolver_detalle_externo", False):
        motivos.append("DETALLE_EXTERNO_PENDIENTE")

    if (
        tarea.get("requiere_cambios_efectivos_predecesores", False)
        and not tarea.get(
            "cambios_efectivos_predecesores_resueltos", False
        )
    ):
        motivos.append("CAMBIOS_PREDECESORES_NO_RESUELTOS")

    if not tarea.get("ejecucion_autorizada", False):
        motivos.append("EJECUCION_NO_AUTORIZADA")

    return {
        "producto_gold": producto,
        "puede_ejecutarse": len(motivos) == 0,
        "predecesores_pendientes": pendientes,
        "motivos_bloqueo": motivos,
        "publicacion_autorizada": False,
    }


def registrar_producto_completado(
    tarea,
    productos_completados,
    ejecucion_exitosa,
    validacion_exitosa,
):
    """Registra un producto después de una ejecución validada."""

    if not ejecucion_exitosa or not validacion_exitosa:
        raise SecuenciaGoldError(
            "No se puede completar una tarea sin ejecución "
            "y validación exitosas."
        )

    producto = tarea.get("producto_gold")

    if not producto:
        raise SecuenciaGoldError("Producto Gold no identificado.")

    pendientes = [
        nombre
        for nombre in tarea.get("predecesores_planificados", [])
        if nombre not in productos_completados
    ]

    if pendientes:
        raise SecuenciaGoldError(
            "Existen predecesores sin completar: "
            + ", ".join(pendientes)
        )

    return set(productos_completados) | {producto}
