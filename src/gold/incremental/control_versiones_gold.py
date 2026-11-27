"""Control local de continuidad, idempotencia y conservación Gold.

Contrato candidato. No modifica registros ni publica versiones.
"""


class ControlVersionesError(ValueError):
    pass


def preparar_actualizacion_gold(
    versiones_vigentes,
    cambios,
    ejecuciones_previas,
    run_id,
):
    """Prepara una decisión segura sin ejecutar ni publicar."""

    if not isinstance(versiones_vigentes, dict):
        raise ControlVersionesError("Registro Gold inválido.")

    if not isinstance(cambios, dict):
        raise ControlVersionesError("Cambios Gold inválidos.")

    if not isinstance(ejecuciones_previas, dict):
        raise ControlVersionesError("Historial inválido.")

    if not isinstance(run_id, str) or not run_id.strip():
        raise ControlVersionesError("run_id inválido.")

    if set(cambios) - set(versiones_vigentes):
        raise ControlVersionesError("Producto Gold desconocido.")

    for producto, detalle in cambios.items():
        if not isinstance(detalle, dict):
            raise ControlVersionesError("Cambio inválido.")

        if not detalle.get("version_anterior"):
            raise ControlVersionesError("Falta versión anterior.")

        if detalle["version_anterior"] != versiones_vigentes[producto]:
            raise ControlVersionesError(
                f"Continuidad incorrecta: {producto}"
            )

    if run_id in ejecuciones_previas:
        if ejecuciones_previas[run_id] != cambios:
            raise ControlVersionesError(
                "run_id reutilizado con cambios distintos."
            )

        return {
            "estado": "EJECUCION_REPETIDA",
            "versiones_resultantes": dict(versiones_vigentes),
            "publicacion_autorizada": False,
        }

    nuevas = dict(versiones_vigentes)

    for producto, detalle in cambios.items():
        version_nueva = detalle.get("version_nueva")

        if not version_nueva:
            raise ControlVersionesError("Falta versión nueva.")

        nuevas[producto] = version_nueva

    return {
        "estado": "ACTUALIZACION_PREPARADA",
        "versiones_resultantes": nuevas,
        "productos_conservados": sorted(
            set(versiones_vigentes) - set(cambios)
        ),
        "publicacion_autorizada": False,
    }
