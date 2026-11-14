#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics Platform.

Planificador incremental preliminar de la capa Gold.

Contrato Silver -> Gold: v0.1.

Responsabilidades:
- Validar el manifest recibido.
- Identificar dependencias potenciales.
- Preparar tareas de procesamiento.
- Conservar las claves anteriores y nuevas.
- Definir estrategias de recalculo.
- Establecer el orden de ejecucion.
- Identificar validaciones operativas pendientes.

Este componente NO ejecuta Spark.
Este componente NO modifica Silver ni Gold.
Este componente NO publica resultados.
"""

import argparse
import copy
import json
import sys

from pathlib import Path


# ============================================================
# 1. IMPORTACIONES
# ============================================================

if __package__:

    from .manifest_validator import (
        ManifestError,
        cargar_manifest,
    )

    from .dependency_registry import (
        DependencyError,
        PRODUCTOS_GOLD,
        DEPENDENCIAS_SILVER_GOLD,
        DEPENDENCIAS_GOLD_GOLD,
        REGLAS_PROPAGACION,
        ORDEN_EJECUCION_GOLD,
        asegurar_registro_valido,
        analizar_dependencias,
        obtener_producto,
        obtener_dependencia_silver,
        cantidad_cambios,
    )

else:

    from manifest_validator import (
        ManifestError,
        cargar_manifest,
    )

    from dependency_registry import (
        DependencyError,
        PRODUCTOS_GOLD,
        DEPENDENCIAS_SILVER_GOLD,
        DEPENDENCIAS_GOLD_GOLD,
        REGLAS_PROPAGACION,
        ORDEN_EJECUCION_GOLD,
        asegurar_registro_valido,
        analizar_dependencias,
        obtener_producto,
        obtener_dependencia_silver,
        cantidad_cambios,
    )


# ============================================================
# 2. CONFIGURACION GENERAL
# ============================================================

VERSION_PLANIFICADOR = "0.1"

TIPO_PLAN = "PRELIMINAR"

ESTADO_PLANIFICADO = "GOLD_PLANIFICADO"

ESTADO_SIN_PRODUCTOS = "SIN_PRODUCTOS_POTENCIALES"

ESTADO_RECHAZADO = "GOLD_RECHAZADO"

ESTADO_TAREA_PENDIENTE = (
    "PENDIENTE_VALIDACION_OPERATIVA"
)


class PlannerError(Exception):
    """Error controlado del planificador incremental."""

    pass


# ============================================================
# 3. ESTRATEGIAS POR PRODUCTO GOLD
# ============================================================

ESTRATEGIAS_GOLD = {

    "mercado_valor_diario": {

        "estrategia": (
            "RECALCULO_MERCADO_POR_CLAVES"
        ),

        "unidad_recalculo": [
            "nemonico",
            "fecha",
        ],

        "fuentes_silver_requeridas": [
            "cotizaciones",
            "tipo_cambio",
            "embig",
            "puente_ruc_nemonico",
        ],

        "fuentes_gold_requeridas": [],

        "acciones": [

            "Resolver las claves anteriores y nuevas "
            "de cotizaciones.",

            "Identificar fechas macroeconomicas "
            "afectadas.",

            "Localizar cotizaciones relacionadas "
            "con las fechas macroeconomicas.",

            "Identificar nemonicos modificados "
            "en el puente empresarial.",

            "Resolver el historico de los "
            "nemonicos afectados.",

            "Reutilizar el constructor historico "
            "de mercado sobre el alcance requerido.",

            "Comparar las filas anteriores y nuevas.",

            "Identificar cambios efectivos y "
            "particiones Gold afectadas.",

        ],

        "controles_especificos": [

            "Unicidad por nemonico y fecha.",

            "Integracion macroeconomica sin "
            "multiplicacion de registros.",

            "Conservacion de cotizaciones "
            "no afectadas.",

            "Consistencia de retornos, precios "
            "y montos negociados.",

        ],

    },

    "actividad_sab_diaria": {

        "estrategia": (
            "RECALCULO_SAB_POR_FECHAS_COMPLETAS"
        ),

        "unidad_recalculo": [
            "fecha",
        ],

        "fuentes_silver_requeridas": [
            "montos_sab",
        ],

        "fuentes_gold_requeridas": [],

        "acciones": [

            "Identificar fechas anteriores "
            "y nuevas afectadas.",

            "Recuperar todas las sociedades "
            "agente de bolsa de cada fecha.",

            "Recalcular montos, participaciones, "
            "rankings y concentracion diaria.",

            "Comparar el conjunto completo "
            "de cada fecha afectada.",

            "Conservar las fechas no afectadas.",

        ],

        "controles_especificos": [

            "Unicidad por codigo_sab y fecha.",

            "Consistencia de totales diarios.",

            "Consistencia de participaciones "
            "y rankings.",

            "Recalculo completo del HHI diario.",

        ],

    },

    "empresa_trimestre": {

        "estrategia": (
            "RECALCULO_FINANCIERO_POR_GRUPOS"
        ),

        "unidad_recalculo": [
            "rpj",
            "ejercicio",
            "trimestre_num",
        ],

        "fuentes_silver_requeridas": [
            "principales_cuentas",
        ],

        "fuentes_gold_requeridas": [],

        "acciones": [

            "Resolver los id_registro anteriores "
            "y nuevos afectados.",

            "Consultar las versiones anteriores "
            "y publicadas de principales cuentas.",

            "Identificar grupos financieros "
            "anteriores y nuevos.",

            "Recuperar todos los registros "
            "de cada grupo afectado.",

            "Recalcular indicadores financieros "
            "y controles de calidad.",

            "Preservar las colisiones financieras "
            "legitimas.",

            "Comparar resultados y determinar "
            "particiones Gold afectadas.",

        ],

        "controles_especificos": [

            "Unicidad por id_registro.",

            "Conservacion de registros financieros "
            "legitimos.",

            "Separacion de monedas.",

            "Consistencia de identidad contable.",

            "Consistencia de indicadores "
            "y banderas de calidad.",

        ],

    },

    "hechos_empresa": {

        "estrategia": (
            "RECALCULO_HECHOS_POR_EXPEDIENTES"
        ),

        "unidad_recalculo": [
            "numero_expediente",
        ],

        "fuentes_silver_requeridas": [
            "eventos",
            "documentos",
            "contenido_textual",
            "identidad_empresarial",
        ],

        "fuentes_gold_requeridas": [],

        "acciones": [

            "Identificar expedientes modificados "
            "directamente.",

            "Resolver expedientes anteriores y nuevos "
            "de documentos afectados.",

            "Resolver expedientes anteriores y nuevos "
            "de textos afectados.",

            "Recuperar eventos, identidad, documentos "
            "y textos de cada expediente.",

            "Recalcular agregaciones documentales "
            "y calidad textual.",

            "Recalcular identidad empresarial "
            "y nemonicos asociados.",

            "Identificar cambios efectivos "
            "en los hechos empresariales.",

        ],

        "controles_especificos": [

            "Unicidad por numero_expediente.",

            "Integridad referencial entre eventos, "
            "documentos y textos.",

            "Ausencia de multiplicacion de filas.",

            "Consistencia temporal y documental.",

            "Conservacion del historico no afectado.",

        ],

    },

    "impacto_hechos_valor": {

        "estrategia": (
            "RECALCULO_IMPACTO_POR_PARES"
        ),

        "unidad_recalculo": [
            "numero_expediente",
            "nemonico",
        ],

        "fuentes_silver_requeridas": [],

        "fuentes_gold_requeridas": [
            "mercado_valor_diario",
            "hechos_empresa",
        ],

        "ventana_dias": 7,

        "excluir_dia_evento": True,

        "acciones": [

            "Recibir los cambios efectivos "
            "de los productos Gold predecesores.",

            "Resolver pares expediente-nemonico "
            "anteriores y nuevos.",

            "Identificar cotizaciones modificadas "
            "dentro de las ventanas de impacto.",

            "Consultar cotizaciones anteriores "
            "y nuevas de cada ventana afectada.",

            "Recalcular el impacto observado "
            "para cada par.",

            "Identificar incorporaciones y retiros "
            "justificados de pares derivados.",

            "Conservar los pares no afectados.",

        ],

        "controles_especificos": [

            "Unicidad por expediente y nemonico.",

            "Ventana de siete dias calendario.",

            "Exclusion del dia del evento.",

            "Consistencia de precios anteriores "
            "y posteriores.",

            "Conservacion de pares con "
            "ventanas incompletas.",

        ],

    },

}


# ============================================================
# 4. REQUISITOS OPERATIVOS GENERALES
# ============================================================

REQUISITOS_OPERATIVOS = [

    {
        "codigo": "OP01",
        "descripcion": (
            "Confirmar el contrato y catalogo "
            "definitivos Silver-Gold."
        ),
    },

    {
        "codigo": "OP02",
        "descripcion": (
            "Verificar que el manifest corresponda "
            "a una publicacion Silver real y aprobada."
        ),
    },

    {
        "codigo": "OP03",
        "descripcion": (
            "Comprobar rutas autorizadas y "
            "existencia de versiones Silver."
        ),
    },

    {
        "codigo": "OP04",
        "descripcion": (
            "Verificar versiones de esquema, "
            "columnas y tipos de datos."
        ),
    },

    {
        "codigo": "OP05",
        "descripcion": (
            "Validar la version Gold base y "
            "las versiones Silver de referencia."
        ),
    },

    {
        "codigo": "OP06",
        "descripcion": (
            "Comprobar idempotencia mediante "
            "run_id y huella SHA-256."
        ),
    },

    {
        "codigo": "OP07",
        "descripcion": (
            "Resolver y validar los detalles "
            "externos de cambios cuando existan."
        ),
    },

    {
        "codigo": "OP08",
        "descripcion": (
            "Contrastar claves, conteos y cambios "
            "declarados con las versiones Silver."
        ),
    },

    {
        "codigo": "OP09",
        "descripcion": (
            "Determinar las claves y particiones "
            "Gold efectivamente afectadas."
        ),
    },

    {
        "codigo": "OP10",
        "descripcion": (
            "Comprobar que no exista otra "
            "publicacion incompatible en curso."
        ),
    },

    {
        "codigo": "OP11",
        "descripcion": (
            "Confirmar las versiones coherentes "
            "de todas las fuentes requeridas."
        ),
    },

    {
        "codigo": "OP12",
        "descripcion": (
            "Preparar un destino de staging "
            "independiente del Gold publicado."
        ),
    },

]


# ============================================================
# 5. VALIDACION DE CONFIGURACION
# ============================================================

def validar_configuracion_planificador():

    errores = []

    asegurar_registro_valido()

    if set(ESTRATEGIAS_GOLD) != set(PRODUCTOS_GOLD):

        errores.append(
            "Las estrategias Gold no coinciden "
            "con los productos registrados."
        )

    for producto, estrategia in (
        ESTRATEGIAS_GOLD.items()
    ):

        if producto not in PRODUCTOS_GOLD:

            continue

        campos_requeridos = {

            "estrategia",

            "unidad_recalculo",

            "fuentes_silver_requeridas",

            "fuentes_gold_requeridas",

            "acciones",

            "controles_especificos",

        }

        faltantes = campos_requeridos - set(
            estrategia
        )

        if faltantes:

            errores.append(
                f"{producto}: campos faltantes "
                f"{sorted(faltantes)}."
            )

            continue

        fuentes_silver = estrategia[
            "fuentes_silver_requeridas"
        ]

        fuentes_gold = estrategia[
            "fuentes_gold_requeridas"
        ]

        fuentes_silver_esperadas = {

            dataset

            for dataset, dependencia
            in DEPENDENCIAS_SILVER_GOLD.items()

            if producto in dependencia[
                "productos"
            ]

        }

        fuentes_gold_esperadas = {

            origen

            for origen, destinos
            in DEPENDENCIAS_GOLD_GOLD.items()

            if producto in destinos

        }

        if set(fuentes_silver) != (
            fuentes_silver_esperadas
        ):

            errores.append(
                f"{producto}: fuentes Silver "
                "inconsistentes."
            )

        if set(fuentes_gold) != (
            fuentes_gold_esperadas
        ):

            errores.append(
                f"{producto}: fuentes Gold "
                "inconsistentes."
            )

        if len(fuentes_silver) != len(
            set(fuentes_silver)
        ):

            errores.append(
                f"{producto}: fuentes Silver "
                "duplicadas."
            )

        if len(fuentes_gold) != len(
            set(fuentes_gold)
        ):

            errores.append(
                f"{producto}: fuentes Gold "
                "duplicadas."
            )

        if not estrategia["acciones"]:

            errores.append(
                f"{producto}: acciones vacias."
            )

        if not estrategia["unidad_recalculo"]:

            errores.append(
                f"{producto}: unidad de "
                "recalculo vacia."
            )

        if not estrategia["controles_especificos"]:

            errores.append(
                f"{producto}: controles vacios."
            )

    regla_impacto = REGLAS_PROPAGACION.get(
        (
            "mercado_valor_diario",
            "impacto_hechos_valor",
        ),
        {},
    )

    estrategia_impacto = ESTRATEGIAS_GOLD.get(
        "impacto_hechos_valor",
        {},
    )

    if estrategia_impacto.get(
        "ventana_dias"
    ) != regla_impacto.get(
        "ventana_dias"
    ):

        errores.append(
            "La ventana de impacto no coincide "
            "con el registro de dependencias."
        )

    if estrategia_impacto.get(
        "excluir_dia_evento"
    ) != regla_impacto.get(
        "excluir_dia_evento"
    ):

        errores.append(
            "La exclusion del dia del evento "
            "no coincide con el registro."
        )

    codigos = [

        requisito["codigo"]

        for requisito in REQUISITOS_OPERATIVOS

    ]

    if len(codigos) != len(set(codigos)):

        errores.append(
            "Existen requisitos operativos "
            "duplicados."
        )

    return errores


def asegurar_configuracion_valida():

    errores = validar_configuracion_planificador()

    if errores:

        raise PlannerError(
            "Configuracion invalida: "
            + "; ".join(errores)
        )


# ============================================================
# 6. PROTECCION DE ENTRADAS
# ============================================================

def verificar_json_finito(manifest):

    """
    Impide que valores NaN o infinitos lleguen
    al planificador.

    No reemplaza la validacion operativa.
    """

    try:

        json.dumps(
            manifest,
            allow_nan=False,
        )

    except (
        ValueError,
        TypeError,
        OverflowError,
    ) as error:

        raise PlannerError(
            "El manifest contiene valores "
            f"JSON invalidos: {error}"
        ) from error


# ============================================================
# 7. RESUMEN DE CAMBIOS DECLARADOS
# ============================================================

def preparar_cambios_dataset(dataset):

    nombre = dataset["dataset"]

    dependencia = obtener_dependencia_silver(
        nombre
    )

    detalle_externo = bool(
        dataset["ruta_detalle_cambios"]
    )

    operaciones = copy.deepcopy(
        dataset["claves_afectadas"]
    )

    claves_anteriores = None

    claves_nuevas = None

    if operaciones is not None:

        claves_anteriores = [

            copy.deepcopy(
                operacion["clave_anterior"]
            )

            for operacion in operaciones

            if operacion["clave_anterior"]
            is not None

        ]

        claves_nuevas = [

            copy.deepcopy(
                operacion["clave_nueva"]
            )

            for operacion in operaciones

            if operacion["clave_nueva"]
            is not None

        ]

    return {

        "dataset": nombre,

        "ruta_silver": dataset[
            "ruta_silver"
        ],

        "version_esquema": dataset[
            "version_esquema"
        ],

        "version_anterior": dataset[
            "version_anterior"
        ],

        "version_publicada": dataset[
            "version_publicada"
        ],

        "clave_silver": copy.deepcopy(
            dataset["clave"]
        ),

        "alcance": dependencia[
            "alcance"
        ],

        "registros_nuevos": dataset[
            "registros_nuevos"
        ],

        "registros_corregidos": dataset[
            "registros_corregidos"
        ],

        "registros_eliminados": dataset[
            "registros_eliminados"
        ],

        "registros_antes": dataset[
            "registros_antes"
        ],

        "registros_despues": dataset[
            "registros_despues"
        ],

        "total_cambios_declarados": (
            cantidad_cambios(dataset)
        ),

        "particiones_silver_declaradas": (
            copy.deepcopy(
                dataset[
                    "particiones_afectadas"
                ]
            )
        ),

        "operaciones_declaradas": operaciones,

        "claves_anteriores_declaradas": (
            claves_anteriores
        ),

        "claves_nuevas_declaradas": (
            claves_nuevas
        ),

        "detalle_externo": detalle_externo,

        "ruta_detalle_cambios": dataset[
            "ruta_detalle_cambios"
        ],

        "detalle_externo_resuelto": (
            False if detalle_externo else None
        ),

        "autorizacion_eliminaciones_declarada": (
            dataset["autorizacion_eliminaciones"]
        ),

        "cambios_verificados": False,

    }


# ============================================================
# 8. RESOLUCION DE PREDECESORES
# ============================================================

def obtener_predecesores_gold(producto):

    if producto not in PRODUCTOS_GOLD:

        raise PlannerError(
            f"Producto Gold desconocido: "
            f"{producto}"
        )

    predecesores = {

        origen

        for origen, destinos
        in DEPENDENCIAS_GOLD_GOLD.items()

        if producto in destinos

    }

    return [

        nombre

        for nombre in ORDEN_EJECUCION_GOLD

        if nombre in predecesores

    ]


def obtener_reglas_predecesores(
    producto,
    productos_planificados,
):

    predecesores = obtener_predecesores_gold(
        producto
    )

    reglas = []

    for origen in predecesores:

        if origen not in productos_planificados:

            continue

        regla = copy.deepcopy(
            REGLAS_PROPAGACION[
                (origen, producto)
            ]
        )

        reglas.append(
            {
                "producto_origen": origen,

                "producto_destino": producto,

                **regla,

                "requiere_cambios_efectivos": True,

            }
        )

    return reglas


# ============================================================
# 9. PREPARACION DE FUENTES REQUERIDAS
# ============================================================

def preparar_fuentes_silver(
    producto,
    datasets_manifest,
):

    estrategia = ESTRATEGIAS_GOLD[
        producto
    ]

    fuentes = []

    for nombre in estrategia[
        "fuentes_silver_requeridas"
    ]:

        dataset = datasets_manifest.get(
            nombre
        )

        if dataset is None:

            fuentes.append(
                {
                    "dataset": nombre,

                    "presente_en_manifest": False,

                    "ruta_silver": None,

                    "version_anterior": None,

                    "version_publicada": None,

                    "resolucion": (
                        "DESDE_REFERENCIAS_"
                        "DE_VERSIONES_APROBADAS"
                    ),

                    "version_verificada": False,

                }
            )

        else:

            fuentes.append(
                {
                    "dataset": nombre,

                    "presente_en_manifest": True,

                    "ruta_silver": dataset[
                        "ruta_silver"
                    ],

                    "version_anterior": dataset[
                        "version_anterior"
                    ],

                    "version_publicada": dataset[
                        "version_publicada"
                    ],

                    "resolucion": (
                        "DESDE_MANIFEST_PENDIENTE_"
                        "DE_VERIFICACION"
                    ),

                    "version_verificada": False,

                }
            )

    return fuentes


def preparar_fuentes_gold(
    producto,
    productos_planificados,
):

    fuentes = []

    for nombre in obtener_predecesores_gold(
        producto
    ):

        planificado = (
            nombre in productos_planificados
        )

        fuentes.append(
            {
                "producto": nombre,

                "tiene_tarea_planificada": (
                    planificado
                ),

                "resolucion": (

                    "VERSION_CANDIDATA_SI_EXISTE_"
                    "CAMBIO_EFECTIVO"

                    if planificado

                    else

                    "VERSION_GOLD_BASE_APROBADA"

                ),

                "alternativa_si_no_hay_cambios": (
                    "VERSION_GOLD_BASE_APROBADA"
                ),

                "version_verificada": False,

            }
        )

    return fuentes


# ============================================================
# 10. CONSTRUCCION DE TAREAS
# ============================================================

def construir_tarea(
    run_id,
    producto,
    posicion,
    datasets_manifest,
    cambios_declarados,
    productos_planificados,
):

    configuracion = obtener_producto(
        producto
    )

    estrategia = copy.deepcopy(
        ESTRATEGIAS_GOLD[producto]
    )

    cambios_directos = []

    for cambio in cambios_declarados:

        nombre_dataset = cambio[
            "dataset"
        ]

        dependencia = (
            DEPENDENCIAS_SILVER_GOLD[
                nombre_dataset
            ]
        )

        if producto not in dependencia[
            "productos"
        ]:

            continue

        if cambio[
            "total_cambios_declarados"
        ] == 0:

            continue

        cambios_directos.append(
            copy.deepcopy(cambio)
        )

    predecesores = obtener_predecesores_gold(
        producto
    )

    predecesores_planificados = [

        nombre

        for nombre in predecesores

        if nombre in productos_planificados

    ]

    reglas_indirectas = (
        obtener_reglas_predecesores(
            producto,
            productos_planificados,
        )
    )

    tipos_activacion = []

    if cambios_directos:

        tipos_activacion.append(
            "DIRECTA"
        )

    if reglas_indirectas:

        tipos_activacion.append(
            "INDIRECTA"
        )

    if not tipos_activacion:

        raise PlannerError(
            f"{producto}: tarea sin "
            "causa de activacion."
        )

    tarea = {

        "id_tarea": (
            f"{run_id}:{producto}"
        ),

        "orden": posicion,

        "producto_gold": producto,

        "estado": (
            ESTADO_TAREA_PENDIENTE
        ),

        "ruta_gold_relativa": configuracion[
            "ruta"
        ],

        "clave_gold": configuracion[
            "clave"
        ],

        "columnas_particion_gold": (
            configuracion["particiones"]
        ),

        "granularidad": configuracion[
            "granularidad"
        ],

        "constructor_historico": (
            configuracion["constructor"]
        ),

        "validador_historico": (
            configuracion["validador"]
        ),

        "estrategia_recalculo": (
            estrategia["estrategia"]
        ),

        "unidad_recalculo": (
            estrategia["unidad_recalculo"]
        ),

        "tipos_activacion": (
            tipos_activacion
        ),

        "datasets_disparadores": [

            cambio["dataset"]

            for cambio in cambios_directos

        ],

        "cambios_directos_declarados": (
            cambios_directos
        ),

        "predecesores_gold": (
            predecesores
        ),

        "predecesores_planificados": (
            predecesores_planificados
        ),

        "reglas_propagacion_indirecta": (
            reglas_indirectas
        ),

        "fuentes_silver_requeridas": (
            preparar_fuentes_silver(
                producto,
                datasets_manifest,
            )
        ),

        "fuentes_gold_requeridas": (
            preparar_fuentes_gold(
                producto,
                productos_planificados,
            )
        ),

        "acciones_previstas": (
            estrategia["acciones"]
        ),

        "controles_especificos": (
            estrategia[
                "controles_especificos"
            ]
        ),

        "requiere_resolver_detalle_externo": any(

            cambio["detalle_externo"]

            for cambio in cambios_directos

        ),

        "requiere_cambios_efectivos_predecesores": (
            bool(reglas_indirectas)
        ),

        "alcance_efectivo_resuelto": False,

        "claves_gold_afectadas": None,

        "particiones_gold_afectadas": None,

        "cantidad_registros_gold_afectados": None,

        "version_gold_base": None,

        "ruta_gold_staging": None,

        "ejecucion_autorizada": False,

    }

    if producto == "impacto_hechos_valor":

        tarea["ventana_dias"] = estrategia[
            "ventana_dias"
        ]

        tarea["excluir_dia_evento"] = estrategia[
            "excluir_dia_evento"
        ]

    return tarea


# ============================================================
# 11. VALIDACION DEL ORDEN DE TAREAS
# ============================================================

def validar_orden_tareas(tareas):

    errores = []

    productos = [

        tarea["producto_gold"]

        for tarea in tareas

    ]

    if len(productos) != len(set(productos)):

        errores.append(
            "El plan contiene productos duplicados."
        )

    posiciones = {

        producto: indice

        for indice, producto
        in enumerate(productos)

    }

    orden_esperado = [

        producto

        for producto in ORDEN_EJECUCION_GOLD

        if producto in posiciones

    ]

    if productos != orden_esperado:

        errores.append(
            "El orden de tareas no coincide "
            "con el orden Gold registrado."
        )

    for tarea in tareas:

        producto = tarea["producto_gold"]

        for predecesor in tarea[
            "predecesores_planificados"
        ]:

            if predecesor not in posiciones:

                errores.append(
                    f"{producto}: predecesor "
                    f"ausente: {predecesor}."
                )

                continue

            if posiciones[predecesor] >= (
                posiciones[producto]
            ):

                errores.append(
                    f"{producto}: orden incompatible "
                    f"con {predecesor}."
                )

    return errores


# ============================================================
# 12. REQUISITOS PENDIENTES
# ============================================================

def preparar_requisitos_operativos():

    return [

        {
            **copy.deepcopy(requisito),

            "estado": "PENDIENTE",

            "validado": False,

        }

        for requisito in REQUISITOS_OPERATIVOS

    ]


# ============================================================
# 13. CONSTRUCCION DEL PLAN PRELIMINAR
# ============================================================

def construir_plan(
    manifest,
    huella_sha256=None,
    entorno_esperado="PRUEBA",
):

    verificar_json_finito(manifest)

    asegurar_configuracion_valida()

    dependencias = analizar_dependencias(
        manifest,

        entorno_esperado=entorno_esperado,
    )

    run_id = manifest[
        "run_id"
    ]

    datasets_manifest = {

        dataset["dataset"]: dataset

        for dataset in manifest[
            "datasets"
        ]

    }

    cambios_declarados = [

        preparar_cambios_dataset(
            dataset
        )

        for dataset in manifest[
            "datasets"
        ]

    ]

    productos_planificados = list(
        dependencias[
            "orden_ejecucion_propuesto"
        ]
    )

    tareas = []

    for posicion, producto in enumerate(
        productos_planificados,
        start=1,
    ):

        tarea = construir_tarea(

            run_id=run_id,

            producto=producto,

            posicion=posicion,

            datasets_manifest=(
                datasets_manifest
            ),

            cambios_declarados=(
                cambios_declarados
            ),

            productos_planificados=(
                productos_planificados
            ),

        )

        tareas.append(tarea)

    errores_orden = validar_orden_tareas(
        tareas
    )

    if errores_orden:

        raise PlannerError(
            "Orden de tareas invalido: "
            + "; ".join(errores_orden)
        )

    total_nuevos = sum(

        cambio["registros_nuevos"]

        for cambio in cambios_declarados

    )

    total_corregidos = sum(

        cambio["registros_corregidos"]

        for cambio in cambios_declarados

    )

    total_eliminados = sum(

        cambio["registros_eliminados"]

        for cambio in cambios_declarados

    )

    datasets_detalle_externo = [

        cambio["dataset"]

        for cambio in cambios_declarados

        if cambio["detalle_externo"]

    ]

    advertencias = list(
        dependencias.get(
            "advertencias",
            [],
        )
    )

    advertencias.extend(
        [
            "El plan es preliminar y no autoriza "
            "la ejecucion de transformaciones.",

            "Las claves Silver declaradas no "
            "constituyen automaticamente claves "
            "Gold afectadas.",

            "Las versiones y los cambios "
            "declarados aun no han sido "
            "contrastados con los datos reales.",

            "Los productos indirectos requieren "
            "cambios efectivos de sus predecesores.",

            "La publicacion Gold permanece "
            "bloqueada hasta completar las "
            "validaciones operativas.",

        ]
    )

    if huella_sha256 is None:

        advertencias.append(
            "No se proporciono una huella "
            "SHA-256 del archivo original."
        )

    if not tareas:

        advertencias.append(
            "No existen productos potenciales "
            "segun los cambios declarados. "
            "Esto no equivale a una publicacion "
            "GOLD_SIN_CAMBIOS validada."
        )

    if datasets_detalle_externo:

        advertencias.append(
            "Existen detalles externos de cambios "
            "pendientes de lectura y validacion."
        )

    estado = (

        ESTADO_PLANIFICADO

        if tareas

        else

        ESTADO_SIN_PRODUCTOS

    )

    return {

        "version_planificador": (
            VERSION_PLANIFICADOR
        ),

        "tipo_plan": TIPO_PLAN,

        "version_manifest": manifest[
            "version_manifest"
        ],

        "run_id": run_id,

        "entorno": manifest[
            "entorno"
        ],

        "huella_sha256": huella_sha256,

        "estado": estado,

        "estado_silver_declarado": (
            manifest["estado"]
        ),

        "fecha_publicacion_silver_declarada": (
            manifest["fecha_publicacion"]
        ),

        "validacion_estructural_aprobada": True,

        "validacion_operativa_aprobada": False,

        "alcance_efectivo_resuelto": False,

        "autorizacion_ejecucion_gold": False,

        "autorizacion_publicacion_gold": False,

        "version_gold_base": None,

        "version_gold_candidata": None,

        "resumen": {

            "cantidad_datasets_manifest": (
                len(datasets_manifest)
            ),

            "cantidad_datasets_con_cambios": sum(

                1

                for cambio in cambios_declarados

                if cambio[
                    "total_cambios_declarados"
                ] > 0

            ),

            "registros_silver_nuevos_declarados": (
                total_nuevos
            ),

            "registros_silver_corregidos_declarados": (
                total_corregidos
            ),

            "registros_silver_eliminados_declarados": (
                total_eliminados
            ),

            "total_cambios_silver_declarados": (

                total_nuevos

                + total_corregidos

                + total_eliminados

            ),

            "cantidad_productos_directos": len(

                dependencias[
                    "productos_directos"
                ]

            ),

            "cantidad_productos_indirectos": len(

                dependencias[
                    "productos_indirectos"
                ]

            ),

            "cantidad_productos_potenciales": (
                len(productos_planificados)
            ),

            "cantidad_tareas": len(tareas),

            "cantidad_tareas_autorizadas": 0,

            "cantidad_registros_gold_afectados": None,

            "cantidad_particiones_gold_afectadas": None,

            "requiere_detalle_externo": bool(
                datasets_detalle_externo
            ),

        },

        "datasets_analizados": (
            cambios_declarados
        ),

        "datasets_sin_cambios_declarados": (
            dependencias[
                "datasets_sin_cambios_declarados"
            ]
        ),

        "datasets_no_consumidos": (
            dependencias[
                "datasets_no_consumidos"
            ]
        ),

        "datasets_detalle_externo_pendiente": (
            datasets_detalle_externo
        ),

        "productos_directos": (
            dependencias[
                "productos_directos"
            ]
        ),

        "productos_indirectos": (
            dependencias[
                "productos_indirectos"
            ]
        ),

        "productos_potenciales": (
            productos_planificados
        ),

        "orden_ejecucion_propuesto": (
            productos_planificados.copy()
        ),

        "tareas": tareas,

        "requisitos_operativos": (
            preparar_requisitos_operativos()
        ),

        "advertencias": list(
            dict.fromkeys(advertencias)
        ),

        "errores": [],

    }


# ============================================================
# 14. RESPUESTA DE RECHAZO
# ============================================================

def construir_plan_rechazado(
    ruta,
    error,
    huella_sha256=None,
):

    return {

        "archivo": str(ruta),

        "version_planificador": (
            VERSION_PLANIFICADOR
        ),

        "tipo_plan": TIPO_PLAN,

        "huella_sha256": huella_sha256,

        "estado": ESTADO_RECHAZADO,

        "validacion_estructural_aprobada": False,

        "validacion_operativa_aprobada": False,

        "alcance_efectivo_resuelto": False,

        "autorizacion_ejecucion_gold": False,

        "autorizacion_publicacion_gold": False,

        "productos_directos": [],

        "productos_indirectos": [],

        "productos_potenciales": [],

        "orden_ejecucion_propuesto": [],

        "tareas": [],

        "advertencias": [],

        "errores": [
            str(error)
        ],

    }


# ============================================================
# 15. PLANIFICACION DESDE ARCHIVO
# ============================================================

def planificar_archivo_manifest(
    ruta,
    entorno_esperado="PRUEBA",
):

    huella = None

    try:

        manifest, huella = cargar_manifest(
            ruta
        )

        plan = construir_plan(

            manifest=manifest,

            huella_sha256=huella,

            entorno_esperado=entorno_esperado,

        )

        plan["archivo"] = str(
            ruta
        )

        return plan

    except (

        ManifestError,

        DependencyError,

        PlannerError,

        ValueError,

        TypeError,

        OverflowError,

        RecursionError,

    ) as error:

        return construir_plan_rechazado(

            ruta=ruta,

            error=error,

            huella_sha256=huella,

        )


# ============================================================
# 16. CONFIGURACION DEL PLANIFICADOR
# ============================================================

def obtener_configuracion():

    asegurar_configuracion_valida()

    return {

        "estado": (
            "CONFIGURACION_PLANIFICADOR_VALIDA"
        ),

        "version_planificador": (
            VERSION_PLANIFICADOR
        ),

        "tipo_plan": TIPO_PLAN,

        "cantidad_estrategias": len(
            ESTRATEGIAS_GOLD
        ),

        "cantidad_requisitos_operativos": len(
            REQUISITOS_OPERATIVOS
        ),

        "orden_ejecucion_gold": list(
            ORDEN_EJECUCION_GOLD
        ),

        "estrategias_gold": copy.deepcopy(
            ESTRATEGIAS_GOLD
        ),

        "requisitos_operativos": (
            preparar_requisitos_operativos()
        ),

        "autorizacion_ejecucion_gold": False,

        "autorizacion_publicacion_gold": False,

    }


# ============================================================
# 17. RESUMEN COMPACTO DEL PLAN
# ============================================================

def obtener_resumen_plan(plan):

    if plan["estado"] == ESTADO_RECHAZADO:

        return copy.deepcopy(plan)

    return {

        "run_id": plan[
            "run_id"
        ],

        "estado": plan[
            "estado"
        ],

        "tipo_plan": plan[
            "tipo_plan"
        ],

        "huella_sha256": plan[
            "huella_sha256"
        ],

        "validacion_estructural_aprobada": (
            plan[
                "validacion_estructural_aprobada"
            ]
        ),

        "validacion_operativa_aprobada": (
            plan[
                "validacion_operativa_aprobada"
            ]
        ),

        "autorizacion_ejecucion_gold": (
            plan[
                "autorizacion_ejecucion_gold"
            ]
        ),

        "autorizacion_publicacion_gold": (
            plan[
                "autorizacion_publicacion_gold"
            ]
        ),

        "resumen": copy.deepcopy(
            plan["resumen"]
        ),

        "orden_ejecucion_propuesto": list(
            plan["orden_ejecucion_propuesto"]
        ),

        "tareas": [

            {
                "id_tarea": tarea[
                    "id_tarea"
                ],

                "producto_gold": tarea[
                    "producto_gold"
                ],

                "estado": tarea[
                    "estado"
                ],

                "estrategia_recalculo": tarea[
                    "estrategia_recalculo"
                ],

                "unidad_recalculo": tarea[
                    "unidad_recalculo"
                ],

                "datasets_disparadores": tarea[
                    "datasets_disparadores"
                ],

                "predecesores_planificados": tarea[
                    "predecesores_planificados"
                ],

                "claves_gold_afectadas": tarea[
                    "claves_gold_afectadas"
                ],

                "particiones_gold_afectadas": tarea[
                    "particiones_gold_afectadas"
                ],

                "ejecucion_autorizada": tarea[
                    "ejecucion_autorizada"
                ],

            }

            for tarea in plan["tareas"]

        ],

        "advertencias": list(
            plan["advertencias"]
        ),

        "errores": list(
            plan["errores"]
        ),

    }


# ============================================================
# 18. INTERFAZ DE LINEA DE COMANDOS
# ============================================================

def main():

    parser = argparse.ArgumentParser(

        description=(
            "Planificador incremental "
            "preliminar Silver-Gold."
        ),

    )

    grupo = (
        parser.add_mutually_exclusive_group(
            required=True
        )
    )

    grupo.add_argument(

        "--manifest",

        type=Path,

        help=(
            "Ruta del manifest Silver."
        ),

    )

    grupo.add_argument(

        "--mostrar-configuracion",

        action="store_true",

        help=(
            "Mostrar estrategias y requisitos "
            "del planificador."
        ),

    )

    parser.add_argument(

        "--entorno",

        choices=[
            "PRUEBA",
            "PRODUCCION",
        ],

        default="PRUEBA",

    )

    parser.add_argument(

        "--resumen",

        action="store_true",

        help=(
            "Mostrar una version compacta "
            "del plan preliminar."
        ),

    )

    args = parser.parse_args()

    if args.mostrar_configuracion:

        try:

            resultado = obtener_configuracion()

        except (
            PlannerError,
            DependencyError,
        ) as error:

            resultado = {
                "estado": ESTADO_RECHAZADO,

                "autorizacion_ejecucion_gold": (
                    False
                ),

                "autorizacion_publicacion_gold": (
                    False
                ),

                "errores": [
                    str(error)
                ],
            }

    else:

        resultado = planificar_archivo_manifest(

            ruta=args.manifest,

            entorno_esperado=args.entorno,

        )

        if args.resumen:

            resultado = obtener_resumen_plan(
                resultado
            )

    print(
        json.dumps(

            resultado,

            indent=4,

            ensure_ascii=False,

            allow_nan=False,

        )
    )

    if resultado["estado"] == ESTADO_RECHAZADO:

        return 1

    return 0


if __name__ == "__main__":

    sys.exit(main())
