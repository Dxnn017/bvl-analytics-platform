#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics
Registro de dependencias Silver -> Gold.

Contrato provisional: v0.1.

Este componente identifica:

- Productos Gold registrados.
- Dependencias directas de Silver.
- Dependencias entre productos Gold.
- Productos potencialmente afectados.
- Orden de ejecucion.
- Reglas de alcance incremental.

No ejecuta Spark.
No modifica Silver ni Gold.
No autoriza publicaciones.
"""

import argparse
import json
import sys

from copy import deepcopy

if __package__:
    from .manifest_validator import (
        CATALOGO_DATASETS,
        ManifestError,
        cargar_manifest,
        validar_manifest,
    )
else:
    from manifest_validator import (
        CATALOGO_DATASETS,
        ManifestError,
        cargar_manifest,
        validar_manifest,
    )


# ============================================================
# 1. EXCEPCIONES
# ============================================================

class DependencyError(Exception):
    """Error en el registro de dependencias."""


# ============================================================
# 2. CATALOGO DE PRODUCTOS GOLD
# ============================================================

PRODUCTOS_GOLD = {

    "mercado_valor_diario": {
        "ruta": "mercado/mercado_valor_diario",
        "clave": [
            "nemonico",
            "fecha",
        ],
        "particiones": [
            "anio",
            "mes",
        ],
        "granularidad": (
            "Una fila por nemonico y fecha"
        ),
        "constructor": (
            "src/gold/mercado/"
            "build_mercado_valor_diario.py"
        ),
        "validador": (
            "src/gold/mercado/"
            "validate_mercado_valor_diario.py"
        ),
    },

    "actividad_sab_diaria": {
        "ruta": "mercado/actividad_sab_diaria",
        "clave": [
            "codigo_sab",
            "fecha",
        ],
        "particiones": [
            "anio",
            "mes",
        ],
        "granularidad": (
            "Una fila por sociedad agente "
            "de bolsa y fecha"
        ),
        "constructor": (
            "src/gold/mercado/"
            "build_actividad_sab_diaria.py"
        ),
        "validador": (
            "src/gold/mercado/"
            "validate_actividad_sab_diaria.py"
        ),
    },

    "empresa_trimestre": {
        "ruta": "empresas/empresa_trimestre",
        "clave": [
            "id_registro",
        ],
        "particiones": [
            "anio",
            "trimestre",
        ],
        "granularidad": (
            "Una fila por registro financiero "
            "identificado mediante id_registro"
        ),
        "constructor": (
            "src/gold/empresas/"
            "build_empresa_trimestre.py"
        ),
        "validador": (
            "src/gold/empresas/"
            "validate_empresa_trimestre.py"
        ),
    },

    "hechos_empresa": {
        "ruta": "eventos/hechos_empresa",
        "clave": [
            "numero_expediente",
        ],
        "particiones": [
            "anio",
        ],
        "granularidad": (
            "Una fila por expediente"
        ),
        "constructor": (
            "src/gold/eventos/"
            "build_hechos_empresa.py"
        ),
        "validador": (
            "src/gold/eventos/"
            "validate_hechos_empresa.py"
        ),
    },

    "impacto_hechos_valor": {
        "ruta": "eventos/impacto_hechos_valor",
        "clave": [
            "numero_expediente",
            "nemonico",
        ],
        "particiones": [
            "anio",
        ],
        "granularidad": (
            "Una fila por expediente y nemonico"
        ),
        "constructor": (
            "src/gold/eventos/"
            "build_impacto_hechos_valor.py"
        ),
        "validador": (
            "src/gold/eventos/"
            "validate_impacto_hechos_valor.py"
        ),
    },
}


# ============================================================
# 3. DEPENDENCIAS DIRECTAS SILVER -> GOLD
# ============================================================

DEPENDENCIAS_SILVER_GOLD = {

    "cotizaciones": {
        "productos": [
            "mercado_valor_diario",
        ],
        "alcance": (
            "CLAVES_COTIZACION"
        ),
        "descripcion": (
            "Recalcular nemonicos y fechas "
            "modificados."
        ),
    },

    "montos_sab": {
        "productos": [
            "actividad_sab_diaria",
        ],
        "alcance": (
            "FECHAS_SAB_COMPLETAS"
        ),
        "descripcion": (
            "Recalcular todas las sociedades "
            "de las fechas modificadas."
        ),
    },

    "tipo_cambio": {
        "productos": [
            "mercado_valor_diario",
        ],
        "alcance": (
            "FECHAS_MACROECONOMICAS"
        ),
        "descripcion": (
            "Recalcular cotizaciones relacionadas "
            "con las fechas de tipo de cambio."
        ),
    },

    "embig": {
        "productos": [
            "mercado_valor_diario",
        ],
        "alcance": (
            "FECHAS_MACROECONOMICAS"
        ),
        "descripcion": (
            "Recalcular cotizaciones relacionadas "
            "con las fechas de EMBIG."
        ),
    },

    "principales_cuentas": {
        "productos": [
            "empresa_trimestre",
        ],
        "alcance": (
            "GRUPOS_FINANCIEROS_COMPLETOS"
        ),
        "descripcion": (
            "Recalcular grupos rpj, ejercicio "
            "y trimestre_num anteriores y nuevos."
        ),
    },

    "eventos": {
        "productos": [
            "hechos_empresa",
        ],
        "alcance": (
            "EXPEDIENTES_AFECTADOS"
        ),
        "descripcion": (
            "Recalcular expedientes modificados."
        ),
    },

    "documentos": {
        "productos": [
            "hechos_empresa",
        ],
        "alcance": (
            "EXPEDIENTES_DESDE_DOCUMENTOS"
        ),
        "descripcion": (
            "Identificar los expedientes "
            "anteriores y nuevos de los documentos."
        ),
    },

    "contenido_textual": {
        "productos": [
            "hechos_empresa",
        ],
        "alcance": (
            "EXPEDIENTES_DESDE_TEXTOS"
        ),
        "descripcion": (
            "Identificar expedientes relacionados "
            "con los documentos y textos afectados."
        ),
    },

    "identidad_empresarial": {
        "productos": [
            "hechos_empresa",
        ],
        "alcance": (
            "EXPEDIENTES_AFECTADOS"
        ),
        "descripcion": (
            "Recalcular identidad y nemonicos "
            "asociados a cada expediente."
        ),
    },

    "puente_ruc_nemonico": {
        "productos": [
            "mercado_valor_diario",
        ],
        "alcance": (
            "HISTORICO_NEMONICOS_AFECTADOS"
        ),
        "descripcion": (
            "Revisar el historico de los "
            "nemonicos modificados."
        ),
    },

    "indices": {
        "productos": [],
        "alcance": (
            "SIN_DEPENDENCIA_GOLD"
        ),
        "descripcion": (
            "Dataset reconocido que actualmente "
            "no participa en los productos Gold."
        ),
    },
}


# ============================================================
# 4. DEPENDENCIAS GOLD -> GOLD
# ============================================================

DEPENDENCIAS_GOLD_GOLD = {

    "mercado_valor_diario": [
        "impacto_hechos_valor",
    ],

    "actividad_sab_diaria": [],

    "empresa_trimestre": [],

    "hechos_empresa": [
        "impacto_hechos_valor",
    ],

    "impacto_hechos_valor": [],
}


# ============================================================
# 5. REGLAS DE PROPAGACION
# ============================================================

REGLAS_PROPAGACION = {

    (
        "mercado_valor_diario",
        "impacto_hechos_valor",
    ): {
        "alcance": (
            "PARES_NEMONICO_EVENTO_AFECTADOS"
        ),
        "ventana_dias": 7,
        "excluir_dia_evento": True,
        "descripcion": (
            "Revisar los pares expediente-nemonico "
            "cuyas ventanas temporales o datos "
            "asociados puedan verse afectados por "
            "los cambios efectivos de mercado."
        ),
    },

    (
        "hechos_empresa",
        "impacto_hechos_valor",
    ): {
        "alcance": (
            "PARES_EXPEDIENTE_NEMONICO_AFECTADOS"
        ),
        "descripcion": (
            "Revisar los pares anteriores y nuevos "
            "de los expedientes modificados, "
            "incluyendo incorporaciones y retiros "
            "justificados de pares derivados."
        ),
    },
}


# ============================================================
# 6. ORDEN DE EJECUCION GOLD
# ============================================================

ORDEN_EJECUCION_GOLD = [

    "mercado_valor_diario",

    "actividad_sab_diaria",

    "empresa_trimestre",

    "hechos_empresa",

    "impacto_hechos_valor",
]


# ============================================================
# 7. VALIDACION DEL REGISTRO
# ============================================================

def validar_registro():

    errores = []

    productos = set(PRODUCTOS_GOLD)

    datasets = set(CATALOGO_DATASETS)

    dependencias_silver = set(
        DEPENDENCIAS_SILVER_GOLD
    )

    faltantes = datasets - dependencias_silver

    adicionales = dependencias_silver - datasets

    if faltantes:

        errores.append(
            "Datasets sin dependencia definida: "
            f"{sorted(faltantes)}"
        )

    if adicionales:

        errores.append(
            "Dependencias de datasets desconocidos: "
            f"{sorted(adicionales)}"
        )

    for dataset, configuracion in (
        DEPENDENCIAS_SILVER_GOLD.items()
    ):

        destinos = configuracion["productos"]

        for producto in destinos:

            if producto not in productos:

                errores.append(
                    f"{dataset}: producto Gold "
                    f"desconocido: {producto}"
                )

        if len(destinos) != len(set(destinos)):

            errores.append(
                f"{dataset}: dependencias "
                "directas duplicadas."
            )

        if dataset in CATALOGO_DATASETS:

            consumido = CATALOGO_DATASETS[
                dataset
            ]["consumido_gold"]

            if consumido != bool(destinos):

                errores.append(
                    f"{dataset}: inconsistencia "
                    "en consumido_gold."
                )

    if set(DEPENDENCIAS_GOLD_GOLD) != productos:

        errores.append(
            "El registro Gold -> Gold no coincide "
            "con el catalogo de productos."
        )

    if set(ORDEN_EJECUCION_GOLD) != productos:

        errores.append(
            "El orden de ejecucion no coincide "
            "con el catalogo Gold."
        )

    if len(ORDEN_EJECUCION_GOLD) != len(
        set(ORDEN_EJECUCION_GOLD)
    ):

        errores.append(
            "El orden de ejecucion contiene "
            "productos repetidos."
        )

    posiciones = {

        producto: indice

        for indice, producto in enumerate(
            ORDEN_EJECUCION_GOLD
        )
    }

    aristas = set()

    for origen, destinos in (
        DEPENDENCIAS_GOLD_GOLD.items()
    ):

        if len(destinos) != len(set(destinos)):

            errores.append(
                f"{origen}: dependencias Gold "
                "duplicadas."
            )

        for destino in destinos:

            aristas.add((origen, destino))

            if destino not in productos:

                errores.append(
                    f"{origen}: dependencia "
                    f"desconocida: {destino}"
                )

                continue

            if origen == destino:

                errores.append(
                    f"{origen}: dependencia "
                    "consigo mismo."
                )

            if (
                origen in posiciones
                and destino in posiciones
                and posiciones[origen]
                >= posiciones[destino]
            ):

                errores.append(
                    "Orden de ejecucion incompatible: "
                    f"{origen} -> {destino}"
                )

    if aristas != set(REGLAS_PROPAGACION):

        errores.append(
            "Las reglas de propagacion no coinciden "
            "con las dependencias Gold -> Gold."
        )

    return errores


def asegurar_registro_valido():

    errores = validar_registro()

    if errores:

        raise DependencyError(
            "Registro de dependencias invalido: "
            + "; ".join(errores)
        )


# ============================================================
# 8. CONSULTAR PRODUCTOS
# ============================================================

def obtener_producto(nombre):

    if (
        not isinstance(nombre, str)
        or nombre not in PRODUCTOS_GOLD
    ):

        raise DependencyError(
            f"Producto Gold desconocido: {nombre}"
        )

    return deepcopy(
        PRODUCTOS_GOLD[nombre]
    )


def obtener_dependencia_silver(dataset):

    if (
        not isinstance(dataset, str)
        or dataset
        not in DEPENDENCIAS_SILVER_GOLD
    ):

        raise DependencyError(
            f"Dataset Silver desconocido: {dataset}"
        )

    return deepcopy(
        DEPENDENCIAS_SILVER_GOLD[dataset]
    )


# ============================================================
# 9. DEPENDENCIAS DIRECTAS
# ============================================================

def obtener_dependencias_directas(dataset):

    configuracion = obtener_dependencia_silver(
        dataset
    )

    return configuracion["productos"]


# ============================================================
# 10. DEPENDENCIAS TRANSITIVAS
# ============================================================

def obtener_dependencias_transitivas(
    productos_iniciales,
):

    asegurar_registro_valido()

    visitados = set()

    pendientes = list(productos_iniciales)

    while pendientes:

        producto = pendientes.pop()

        if producto not in PRODUCTOS_GOLD:

            raise DependencyError(
                f"Producto Gold desconocido: "
                f"{producto}"
            )

        if producto in visitados:

            continue

        visitados.add(producto)

        pendientes.extend(
            DEPENDENCIAS_GOLD_GOLD[producto]
        )

    return [

        producto

        for producto in ORDEN_EJECUCION_GOLD

        if producto in visitados
    ]


# ============================================================
# 11. PRODUCTOS POTENCIALMENTE AFECTADOS
# ============================================================

def obtener_productos_afectados(datasets):

    asegurar_registro_valido()

    directos = set()

    for dataset in datasets:

        directos.update(
            obtener_dependencias_directas(
                dataset
            )
        )

    return obtener_dependencias_transitivas(
        directos
    )


# ============================================================
# 12. CONTEOS DEL MANIFEST
# ============================================================

def cantidad_cambios(dataset):

    campos = (

        "registros_nuevos",

        "registros_corregidos",

        "registros_eliminados",
    )

    total = 0

    for campo in campos:

        valor = dataset.get(campo)

        if (
            type(valor) is not int
            or valor < 0
        ):

            raise DependencyError(
                f"Conteo invalido: {campo}"
            )

        total += valor

    return total


# ============================================================
# 13. ANALIZAR MANIFEST
# ============================================================

def analizar_dependencias(
    manifest,
    entorno_esperado="PRUEBA",
):

    asegurar_registro_valido()

    errores, advertencias = validar_manifest(
        manifest,
        entorno_esperado,
    )

    if errores:

        raise DependencyError(
            "Manifest rechazado: "
            + "; ".join(errores)
        )

    datasets_analizados = []

    productos_directos = set()

    productos_potenciales = set()

    reglas_directas = []

    datasets_sin_cambios = []

    datasets_no_consumidos = []

    for dataset in manifest["datasets"]:

        nombre = dataset["dataset"]

        configuracion = (
            obtener_dependencia_silver(nombre)
        )

        cambios = cantidad_cambios(dataset)

        directos = configuracion["productos"]

        potenciales = []

        if not directos:

            datasets_no_consumidos.append(
                nombre
            )

        if cambios == 0:

            datasets_sin_cambios.append(
                nombre
            )

        elif directos:

            productos_directos.update(
                directos
            )

            potenciales = (
                obtener_dependencias_transitivas(
                    directos
                )
            )

            productos_potenciales.update(
                potenciales
            )

            for producto in directos:

                reglas_directas.append({
                    "dataset_silver": nombre,
                    "producto_gold": producto,
                    "alcance": (
                        configuracion["alcance"]
                    ),
                    "descripcion": (
                        configuracion["descripcion"]
                    ),
                })

        datasets_analizados.append({

            "dataset": nombre,

            "registros_nuevos": (
                dataset["registros_nuevos"]
            ),

            "registros_corregidos": (
                dataset["registros_corregidos"]
            ),

            "registros_eliminados": (
                dataset["registros_eliminados"]
            ),

            "total_cambios_declarados": cambios,

            "productos_directos": directos,

            "productos_potenciales": potenciales,

            "alcance": configuracion["alcance"],

            "particiones_silver_declaradas": (
                deepcopy(
                    dataset["particiones_afectadas"]
                )
            ),

            "detalle_externo": (
                dataset["ruta_detalle_cambios"]
                is not None
            ),
        })

    productos_indirectos = (
        productos_potenciales
        - productos_directos
    )

    orden_ejecucion = [

        producto

        for producto in ORDEN_EJECUCION_GOLD

        if producto in productos_potenciales
    ]

    reglas_indirectas = []

    for origen in orden_ejecucion:

        for destino in (
            DEPENDENCIAS_GOLD_GOLD[origen]
        ):

            if destino in productos_potenciales:

                regla = deepcopy(
                    REGLAS_PROPAGACION[
                        (origen, destino)
                    ]
                )

                reglas_indirectas.append({

                    "producto_origen": origen,

                    "producto_destino": destino,

                    **regla,
                })

    if orden_ejecucion:

        estado = (
            "DEPENDENCIAS_IDENTIFICADAS"
        )

    else:

        estado = (
            "SIN_PRODUCTOS_POTENCIALES"
        )

    advertencias = list(advertencias)

    advertencias.append(
        "Las dependencias son potenciales. "
        "Se requiere validacion operativa "
        "para confirmar cambios efectivos."
    )

    advertencias.append(
        "Las particiones Silver declaradas "
        "no constituyen automaticamente "
        "particiones Gold afectadas."
    )

    return {

        "run_id": manifest["run_id"],

        "entorno": manifest["entorno"],

        "version_manifest": (
            manifest["version_manifest"]
        ),

        "estado": estado,

        "validacion_estructural_aprobada": True,

        "validacion_operativa_aprobada": False,

        "autorizacion_ejecucion_gold": False,

        "datasets_analizados": (
            datasets_analizados
        ),

        "datasets_sin_cambios_declarados": (
            sorted(datasets_sin_cambios)
        ),

        "datasets_no_consumidos": (
            sorted(datasets_no_consumidos)
        ),

        "productos_directos": [

            producto

            for producto in ORDEN_EJECUCION_GOLD

            if producto in productos_directos
        ],

        "productos_indirectos": [

            producto

            for producto in ORDEN_EJECUCION_GOLD

            if producto in productos_indirectos
        ],

        "productos_potenciales": (
            orden_ejecucion
        ),

        "orden_ejecucion_propuesto": (
            orden_ejecucion
        ),

        "reglas_directas": reglas_directas,

        "reglas_indirectas": reglas_indirectas,

        "advertencias": advertencias,

        "errores": [],
    }


# ============================================================
# 14. ANALIZAR ARCHIVO MANIFEST
# ============================================================

def analizar_archivo_manifest(
    ruta,
    entorno_esperado="PRUEBA",
):

    try:

        manifest, huella = cargar_manifest(
            ruta
        )

        resultado = analizar_dependencias(
            manifest,
            entorno_esperado,
        )

        resultado["archivo"] = str(ruta)

        resultado["huella_sha256"] = huella

        return resultado

    except (
        ManifestError,
        DependencyError,
    ) as exc:

        return {

            "archivo": str(ruta),

            "estado": "GOLD_RECHAZADO",

            "validacion_operativa_aprobada": (
                False
            ),

            "autorizacion_ejecucion_gold": (
                False
            ),

            "productos_directos": [],

            "productos_indirectos": [],

            "productos_potenciales": [],

            "orden_ejecucion_propuesto": [],

            "errores": [
                str(exc)
            ],
        }


# ============================================================
# 15. MOSTRAR REGISTRO
# ============================================================

def obtener_registro_completo():

    asegurar_registro_valido()

    propagaciones = []

    for (
        origen,
        destino,
    ), regla in REGLAS_PROPAGACION.items():

        propagaciones.append({

            "producto_origen": origen,

            "producto_destino": destino,

            **deepcopy(regla),
        })

    return {

        "estado": "REGISTRO_VALIDO",

        "version_contrato": "0.1",

        "cantidad_datasets": len(
            DEPENDENCIAS_SILVER_GOLD
        ),

        "cantidad_productos_gold": len(
            PRODUCTOS_GOLD
        ),

        "productos_gold": deepcopy(
            PRODUCTOS_GOLD
        ),

        "dependencias_silver_gold": deepcopy(
            DEPENDENCIAS_SILVER_GOLD
        ),

        "dependencias_gold_gold": deepcopy(
            DEPENDENCIAS_GOLD_GOLD
        ),

        "reglas_propagacion": (
            propagaciones
        ),

        "orden_ejecucion_gold": list(
            ORDEN_EJECUCION_GOLD
        ),

        "autorizacion_ejecucion_gold": False,
    }


# ============================================================
# 16. INTERFAZ CLI
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Registro de dependencias "
            "Silver -> Gold para BVL Analytics."
        )
    )

    grupo = (
        parser.add_mutually_exclusive_group(
            required=True
        )
    )

    grupo.add_argument(

        "--manifest",

        help=(
            "Ruta del manifest JSON "
            "que se desea analizar."
        ),
    )

    grupo.add_argument(

        "--mostrar-registro",

        action="store_true",

        help=(
            "Mostrar el registro completo "
            "de dependencias."
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

    argumentos = parser.parse_args()

    try:

        if argumentos.mostrar_registro:

            resultado = (
                obtener_registro_completo()
            )

        else:

            resultado = (
                analizar_archivo_manifest(
                    argumentos.manifest,
                    argumentos.entorno,
                )
            )

    except DependencyError as exc:

        resultado = {

            "estado": "GOLD_RECHAZADO",

            "autorizacion_ejecucion_gold": (
                False
            ),

            "errores": [
                str(exc)
            ],
        }

    print(
        json.dumps(

            resultado,

            indent=4,

            ensure_ascii=False,

            allow_nan=False,
        )
    )

    if resultado["estado"] == "GOLD_RECHAZADO":

        return 1

    return 0


if __name__ == "__main__":

    sys.exit(main())
