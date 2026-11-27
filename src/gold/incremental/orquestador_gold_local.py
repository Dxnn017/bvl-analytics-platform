#!/usr/bin/env python3
"""Orquestador local Gold en modo simulación.

No ejecuta Spark, no escribe candidatas y no publica datos.
"""

import argparse
import json
from pathlib import Path

from adaptador_tareas_gold import (
    preparar_comando_gold,
    AdaptadorGoldError,
)
from dispatcher_local import preparar_despacho_con_progreso


def simular_orquestacion(plan, alcances):
    """Prepara comandos sin ejecutar ni completar tareas."""

    if not isinstance(alcances, dict):
        raise ValueError("Los alcances deben ser un objeto JSON.")

    diagnostico = preparar_despacho_con_progreso(plan)
    decisiones = []

    for tarea in sorted(
        plan["tareas"],
        key=lambda elemento: elemento["orden"],
    ):
        producto = tarea["producto_gold"]
        alcance = alcances.get(producto, {})

        if not isinstance(alcance, dict):
            raise ValueError(
                f"Alcance inválido para {producto}."
            )

        try:
            preparado = preparar_comando_gold(
                tarea,
                productos_completados=set(),
                ruta_tarea=alcance.get("ruta_tarea"),
                expedientes=alcance.get("expedientes"),
                cotizaciones=alcance.get("cotizaciones"),
                rutas=alcance.get("rutas"),
                claves_afectadas=alcance.get("claves_afectadas"),
            )

            decision = {
                "producto_gold": producto,
                "comando_preparado": preparado["comando"],
                "despachable": True,
                "motivos_bloqueo": [],
            }

        except (AdaptadorGoldError, ValueError) as error:
            decision = {
                "producto_gold": producto,
                "comando_preparado": None,
                "despachable": False,
                "motivos_bloqueo": [str(error)],
            }

        decisiones.append(decision)

    return {
        "entorno": "PRUEBA_LOCAL",
        "modo": "SIMULACION",
        "diagnostico": diagnostico,
        "decisiones": decisiones,
        "productos_completados": [],
        "ejecucion_realizada": False,
        "publicacion_autorizada": False,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", required=True)
    parser.add_argument("--alcances", required=True)
    args = parser.parse_args()

    plan = json.loads(
        Path(args.plan).read_text(encoding="utf-8")
    )

    alcances = json.loads(
        Path(args.alcances).read_text(encoding="utf-8")
    )

    resultado = simular_orquestacion(plan, alcances)

    print(json.dumps(
        resultado,
        indent=2,
        ensure_ascii=False,
    ))


if __name__ == "__main__":
    main()


def ejecutar_orquestacion_local(plan, alcances):
    """Ejecuta tareas locales autorizadas respetando dependencias.

    Requiere un plan explícitamente habilitado para pruebas locales.
    No concede autorizaciones ni publica resultados.
    """
    from ejecucion_controlada_local import ejecutar_comando_validado
    from validar_secuencia_gold import registrar_producto_completado

    if not isinstance(plan, dict) or not isinstance(alcances, dict):
        raise ValueError("Plan o alcances inválidos.")

    if plan.get("entorno") != "PRUEBA_LOCAL":
        raise ValueError("Solo se admiten planes de prueba local.")

    if plan.get("publicacion_autorizada") is not False:
        raise ValueError("La publicación debe permanecer bloqueada.")

    tareas = plan.get("tareas")
    if not isinstance(tareas, list) or not tareas:
        raise ValueError("El plan no contiene tareas.")

    # Verifica estructura, productos y orden antes de ejecutar.
    preparar_despacho_con_progreso(plan)

    completados = set()
    resultados = []

    for tarea in sorted(tareas, key=lambda t: t["orden"]):
        producto = tarea["producto_gold"]
        alcance = alcances.get(producto)

        if not isinstance(alcance, dict):
            raise ValueError(
                f"Falta el alcance resuelto de {producto}."
            )

        preparado = preparar_comando_gold(
            tarea,
            productos_completados=completados,
            ruta_tarea=alcance.get("ruta_tarea"),
            expedientes=alcance.get("expedientes"),
            cotizaciones=alcance.get("cotizaciones"),
            rutas=alcance.get("rutas"),
            claves_afectadas=alcance.get("claves_afectadas"),
        )

        ejecucion = ejecutar_comando_validado(preparado)
        reporte = ejecucion["reporte"]

        duplicados = reporte.get(
            "claves_duplicadas",
            reporte.get("expedientes_duplicados"),
        )

        validacion_exitosa = (
            reporte.get("producto") == producto
            and reporte.get("entorno") == "PRUEBA_LOCAL"
            and reporte.get("publicacion_autorizada") is False
            and type(reporte.get("registros_candidato")) is int
            and reporte["registros_candidato"] >= 0
            and duplicados == 0
        )

        if not validacion_exitosa:
            raise ValueError(
                f"Validación local fallida: {producto}"
            )

        completados = registrar_producto_completado(
            tarea,
            completados,
            ejecucion_exitosa=ejecucion["ejecucion_exitosa"],
            validacion_exitosa=validacion_exitosa,
        )

        resultados.append({
            "producto_gold": producto,
            "ejecucion_exitosa": True,
            "validacion_exitosa": True,
            "registros_candidato": reporte["registros_candidato"],
            "publicacion_autorizada": False,
        })

    return {
        "entorno": "PRUEBA_LOCAL",
        "modo": "EJECUCION_CONTROLADA",
        "productos_completados": sorted(completados),
        "resultados": resultados,
        "publicacion_autorizada": False,
    }


def ejecutar_dependencia_impacto_local(
    versiones_gold,
    productos_completados,
):
    """Encadena candidatas Gold verificadas con impacto_hechos_valor.

    No escribe Parquet ni realiza publicaciones oficiales.
    """

    from encadenar_gold_impacto import (
        encadenar_actualizacion_impacto,
    )

    if not isinstance(versiones_gold, dict):
        raise ValueError("Registro de versiones Gold inválido.")

    requeridos = {
        "hechos_base",
        "hechos_candidato",
        "mercado_base",
        "mercado_candidato",
        "impacto_base",
    }

    faltantes = requeridos - set(versiones_gold)

    if faltantes:
        raise ValueError(
            "Faltan versiones Gold: " + ", ".join(sorted(faltantes))
        )

    predecesores = {
        "hechos_empresa",
        "mercado_valor_diario",
    }

    completados = set(productos_completados)

    pendientes = predecesores - completados

    if pendientes:
        raise ValueError(
            "Predecesores Gold pendientes: "
            + ", ".join(sorted(pendientes))
        )

    if any(
        versiones_gold[nombre] is None
        for nombre in requeridos
    ):
        raise ValueError("Existen versiones Gold sin datos.")

    resultado = encadenar_actualizacion_impacto(
        hechos_base=versiones_gold["hechos_base"],
        hechos_candidato=versiones_gold["hechos_candidato"],
        mercado_base=versiones_gold["mercado_base"],
        mercado_candidato=versiones_gold["mercado_candidato"],
        impacto_base=versiones_gold["impacto_base"],
    )

    if resultado["validacion_exitosa"] is not True:
        raise ValueError("El encadenamiento Gold no fue validado.")

    if resultado["publicacion_autorizada"] is not False:
        raise ValueError("Publicación Gold no permitida.")

    return {
        **resultado,
        "predecesores_verificados": sorted(predecesores),
        "entorno": "PRUEBA_LOCAL",
        "ejecucion_incremental_local": True,
        "publicacion_autorizada": False,
    }
