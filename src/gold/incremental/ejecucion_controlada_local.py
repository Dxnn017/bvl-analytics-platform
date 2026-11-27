"""Ejecución controlada de candidatos Gold locales.

Solo admite los ejecutores locales conocidos.
No publica datos ni concede autorizaciones.
"""

import json
import subprocess
import sys
from pathlib import Path

from registro_ejecucion_local import obtener_ejecutor_local


class EjecucionGoldError(RuntimeError):
    """Error durante una ejecución Gold local."""


def ejecutar_comando_validado(preparado, timeout=600):
    """Ejecuta un comando previamente preparado y valida su reporte."""

    if not isinstance(preparado, dict):
        raise EjecucionGoldError("Comando preparado inválido.")

    producto = preparado.get("producto_gold")
    comando = preparado.get("comando")

    if preparado.get("ejecucion_realizada") is not False:
        raise EjecucionGoldError("Estado previo de ejecución inválido.")

    if preparado.get("publicacion_autorizada") is not False:
        raise EjecucionGoldError("Publicación no permitida.")

    configuracion = obtener_ejecutor_local(producto)

    ejecutor = Path(configuracion["ruta"]).resolve()

    if not isinstance(comando, list) or len(comando) < 3:
        raise EjecucionGoldError("Comando incompleto.")

    if comando[0] != sys.executable:
        raise EjecucionGoldError("Intérprete Python no permitido.")

    if Path(comando[1]).resolve() != ejecutor:
        raise EjecucionGoldError("Ejecutor no autorizado.")

    if not ejecutor.is_file():
        raise EjecucionGoldError("Archivo ejecutor inexistente.")

    argumentos = comando[2:]

    if "--guardar-candidata" in argumentos:
        raise EjecucionGoldError(
            "La persistencia debe habilitarse en otra etapa."
        )

    permitidos = set(configuracion["parametros"])

    if not permitidos:
        raise EjecucionGoldError(
            "El ejecutor no tiene interfaz autorizada."
        )

    posicion = 0
    parametros_encontrados = set()

    while posicion < len(argumentos):
        parametro = argumentos[posicion]

        if parametro not in permitidos:
            raise EjecucionGoldError(
                f"Parámetro no permitido: {parametro}"
            )

        parametros_encontrados.add(parametro)

        cantidad_valores = (
            2 if parametro == "--cotizacion-afectada" else 1
        )

        valores = argumentos[
            posicion + 1:posicion + 1 + cantidad_valores
        ]

        if len(valores) != cantidad_valores:
            raise EjecucionGoldError(
                f"Faltan valores para {parametro}."
            )

        if any(
            not isinstance(valor, str)
            or not valor.strip()
            or valor.startswith("--")
            for valor in valores
        ):
            raise EjecucionGoldError(
                f"Valor inválido para {parametro}."
            )

        if parametro == "--claves-afectadas":
            if not Path(valores[0]).is_file():
                raise EjecucionGoldError(
                    "El archivo de claves afectadas no existe."
                )

        if parametro == "--tarea-planificada":
            if not Path(valores[0]).is_file():
                raise EjecucionGoldError(
                    "El archivo de tarea no existe."
                )

        if parametro == "--cotizacion-afectada":
            from datetime import date

            try:
                fecha = date.fromisoformat(valores[1])
            except ValueError as error:
                raise EjecucionGoldError(
                    "Fecha de cotización inválida."
                ) from error

        posicion += 1 + cantidad_valores

    if not parametros_encontrados:
        raise EjecucionGoldError(
            "Faltan parámetros de alcance reconocidos."
        )

    if (
        producto in {"actividad_sab_diaria", "empresa_trimestre"}
        and argumentos.count("--tarea-planificada") != 1
    ):
        raise EjecucionGoldError(
            "Se requiere exactamente una tarea planificada."
        )

    proceso = subprocess.run(
        comando,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )

    if proceso.returncode != 0:
        raise EjecucionGoldError(
            "Falló el proceso Gold: "
            + proceso.stderr[-1500:]
        )

    inicio = proceso.stdout.find('{\n  "entorno"')

    if inicio < 0:
        raise EjecucionGoldError(
            "El ejecutor no devolvió un reporte reconocible."
        )

    try:
        reporte = json.loads(proceso.stdout[inicio:])
    except json.JSONDecodeError as error:
        raise EjecucionGoldError(
            "El reporte Gold no es JSON válido."
        ) from error

    if reporte.get("entorno") != "PRUEBA_LOCAL":
        raise EjecucionGoldError("Entorno de ejecución inválido.")

    if reporte.get("producto") != producto:
        raise EjecucionGoldError("Producto del reporte incompatible.")

    if reporte.get("publicacion_autorizada") is not False:
        raise EjecucionGoldError("El reporte permite publicación.")

    duplicados = reporte.get(
        "claves_duplicadas",
        reporte.get("expedientes_duplicados"),
    )

    if duplicados is not None and duplicados != 0:
        raise EjecucionGoldError(
            "El candidato contiene claves duplicadas."
        )

    if reporte.get("registros_candidato") is None:
        raise EjecucionGoldError(
            "Falta el conteo de registros candidatos."
        )

    return {
        "producto_gold": producto,
        "ejecucion_exitosa": True,
        "reporte": reporte,
        "publicacion_autorizada": False,
    }
