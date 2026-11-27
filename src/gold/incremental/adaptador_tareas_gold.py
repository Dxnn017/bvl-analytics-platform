"""Adaptador de tareas planificadas a ejecutores Gold locales.

Prepara argumentos, sin ejecutar procesos ni publicar datos.
"""

import sys
from pathlib import Path

from registro_ejecucion_local import obtener_ejecutor_local
from validar_secuencia_gold import validar_tarea_gold


class AdaptadorGoldError(ValueError):
    """Tarea Gold incompatible con el ejecutor local."""


def preparar_comando_gold(
    tarea,
    productos_completados=None,
    ruta_tarea=None,
    expedientes=None,
    cotizaciones=None,
    rutas=None,
    claves_afectadas=None,
):
    """Prepara un comando local para una tarea resuelta y autorizada."""

    if not isinstance(tarea, dict):
        raise AdaptadorGoldError("La tarea debe ser un diccionario.")

    producto = tarea.get("producto_gold")

    diagnostico = validar_tarea_gold(
        tarea,
        set(productos_completados or []),
    )

    motivos = list(diagnostico["motivos_bloqueo"])

    if tarea.get("estado") != "LISTA_PARA_EJECUCION":
        motivos.append("VALIDACION_OPERATIVA_PENDIENTE")

    if motivos:
        raise AdaptadorGoldError(
            "Tarea bloqueada: " + ", ".join(motivos)
        )

    configuracion = obtener_ejecutor_local(producto)

    if producto == "mercado_valor_diario":
        if not claves_afectadas or not Path(claves_afectadas).is_file():
            raise AdaptadorGoldError(
                "Se requiere un archivo de claves afectadas."
            )

    ruta_ejecutor = Path(configuracion["ruta"])

    if not ruta_ejecutor.is_file():
        raise AdaptadorGoldError(
            f"Ejecutor no encontrado: {ruta_ejecutor}"
        )

    comando = [sys.executable, str(ruta_ejecutor)]

    if producto == "mercado_valor_diario":
        comando.extend([
            "--claves-afectadas", str(claves_afectadas)
        ])

    elif producto in {
        "actividad_sab_diaria",
        "empresa_trimestre",
    }:
        if not ruta_tarea:
            raise AdaptadorGoldError(
                "Se requiere la ruta de la tarea resuelta."
            )

        if not Path(ruta_tarea).is_file():
            raise AdaptadorGoldError(
                "No existe el archivo de tarea resuelta."
            )

        comando.extend([
            "--tarea-planificada",
            str(ruta_tarea),
        ])

    elif producto in {
        "hechos_empresa",
        "impacto_hechos_valor",
    }:
        numeros = sorted({
            str(valor).strip()
            for valor in (expedientes or [])
        })

        if any(not numero for numero in numeros):
            raise AdaptadorGoldError(
                "Existen expedientes vacíos."
            )

        for numero in numeros:
            comando.extend(["--expediente", numero])

        if producto == "impacto_hechos_valor":
            for nemonico, fecha in (cotizaciones or []):
                if not str(nemonico).strip() or not str(fecha).strip():
                    raise AdaptadorGoldError(
                        "Cotización afectada incompleta."
                    )

                comando.extend([
                    "--cotizacion-afectada",
                    str(nemonico),
                    str(fecha),
                ])

        if not numeros and not (
            producto == "impacto_hechos_valor"
            and cotizaciones
        ):
            raise AdaptadorGoldError(
                "No se proporcionó el alcance efectivo."
            )

    else:
        raise AdaptadorGoldError(
            f"Producto sin adaptador: {producto}"
        )

    opciones_rutas = {
        "mercado_valor_diario": {
            "silver_root": "--silver-root",
            "gold_base_path": "--gold-base-path",
        },
        "actividad_sab_diaria": {
            "silver_path": "--silver-path",
            "gold_base_path": "--gold-base-path",
        },
        "empresa_trimestre": {
            "silver_path": "--silver-path",
            "gold_base_path": "--gold-base-path",
        },
        "hechos_empresa": {
            "silver_root": "--silver-root",
            "gold_base_path": "--gold-base-path",
        },
        "impacto_hechos_valor": {
            "hechos_path": "--hechos-path",
            "mercado_path": "--mercado-path",
            "impacto_base_path": "--impacto-base-path",
        },
    }

    rutas = {} if rutas is None else rutas

    if not isinstance(rutas, dict):
        raise AdaptadorGoldError("Las rutas deben ser un diccionario.")

    permitidas = opciones_rutas.get(producto, {})

    if producto == "mercado_valor_diario":
        if not {"silver_root", "gold_base_path"} <= set(rutas):
            raise AdaptadorGoldError(
                "Mercado requiere rutas Silver y Gold explícitas."
            )
    desconocidas = set(rutas) - set(permitidas)

    if desconocidas:
        raise AdaptadorGoldError(
            f"Rutas no admitidas: {sorted(desconocidas)}"
        )

    for nombre, valor in rutas.items():
        if not isinstance(valor, str) or not valor.strip():
            raise AdaptadorGoldError(
                f"Ruta inválida: {nombre}"
            )
        comando.extend([permitidas[nombre], valor.strip()])

    return {
        "producto_gold": producto,
        "comando": comando,
        "ejecucion_realizada": False,
        "publicacion_autorizada": False,
    }
