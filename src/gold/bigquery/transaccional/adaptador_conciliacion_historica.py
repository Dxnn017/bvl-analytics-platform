"""Adaptacion local de resultados historicos BigQuery.

No ejecuta consultas ni verifica fuentes oficiales.
No concede autorizaciones operativas.
"""

import math

from datetime import date
from decimal import Decimal
from collections.abc import Mapping

from preparador_carga_historica import (
    PRODUCTOS,
    exigir,
)

from conciliar_validacion_historica import (
    conciliar_lote,
    metricas_calidad,
    decimal_exacto,
    entero,
    fecha_canonica,
    POLITICA,
)

from trazabilidad_carga_historica import (
    huella_documento,
)


def normalizar_decimal_exacto(valor):

    if isinstance(valor, Decimal):

        exigir(
            valor.is_finite(),
            "Decimal no finito"
        )

        texto = format(valor, "f")

    else:

        exigir(
            type(valor) is str,
            "Decimal requiere texto o Decimal"
        )

        texto = valor

    decimal_exacto(texto)

    return texto


def normalizar_fecha(valor):

    if type(valor) is date:

        valor = valor.isoformat()

    return fecha_canonica(
        valor
    ).isoformat()


def normalizar_escalar(valor, tipo):

    if valor is None:

        return None

    if tipo == "DATE":

        return normalizar_fecha(valor)

    if tipo in ("NUMERIC", "BIGNUMERIC"):

        return normalizar_decimal_exacto(
            valor
        )

    if tipo == "INT64":

        exigir(
            type(valor) is int
            and -(2**63) <= valor < 2**63,
            "INT64 incompatible"
        )

        return valor

    if tipo == "FLOAT64":

        exigir(
            type(valor) in (int, float),
            "FLOAT64 incompatible"
        )

        convertido = float(valor)

        exigir(
            math.isfinite(convertido),
            "FLOAT64 no finito"
        )

        return convertido

    if tipo == "BOOL":

        exigir(
            type(valor) is bool,
            "BOOL incompatible"
        )

        return valor

    if tipo == "STRING":

        exigir(
            type(valor) is str,
            "STRING incompatible"
        )

        return valor

    raise ValueError(
        "Tipo no admitido: " + tipo
    )


def normalizar_fila(fila, campos):

    exigir(
        isinstance(fila, Mapping),
        "Resultado debe ser un mapping"
    )

    exigir(
        set(fila) == set(campos),
        "Aliases de resultado incompatibles"
    )

    return {
        nombre: normalizar_escalar(
            fila[nombre], tipo
        )
        for nombre, tipo in campos.items()
    }


def esquema_calidad(contrato):

    return {
        "producto": "STRING",
        "registros_esperados": "INT64",
        "registros_observados": "INT64",
        "controles_aprobados": "BOOL",
        **{
            metrica: "INT64"
            for metrica in sorted(
                metricas_calidad(contrato)
            )
        },
    }


def esquema_particiones(contrato):

    return {
        **{
            nombre: "INT64"
            for nombre in contrato["gold"][
                "source_partition_columns"
            ]
        },
        "registros": "INT64",
        "fecha_minima": "DATE",
        "fecha_maxima": "DATE",
    }


def esquema_columnas():

    return {
        "columna": "STRING",
        "posicion_esperada": "INT64",
        "posicion_observada": "INT64",
        "tipo_esperado": "STRING",
        "tipo_observado": "STRING",
        "nullable_esperado": "STRING",
        "nullable_observado": "STRING",
        "estado": "STRING",
    }


def esquema_indicadores(contrato):

    campos = {
        "registros": "INT64"
    }

    for campo in contrato["gold"]["columns"]:

        nombre = campo["name"]

        tipo = campo["bigquery_type"]

        if tipo not in (
            "INT64", "NUMERIC", "FLOAT64"
        ):

            continue

        campos["presentes__" + nombre] = "INT64"
        campos["nulos__" + nombre] = "INT64"

        campos["min__" + nombre] = tipo
        campos["max__" + nombre] = tipo

        campos["avg__" + nombre] = (
            "FLOAT64"
            if tipo == "FLOAT64"
            else "BIGNUMERIC"
        )

    return campos


def validar_intervalos_perfil(
    perfil,
    contrato,
):

    total = entero(perfil["registros"])

    for campo in contrato["gold"]["columns"]:

        nombre = campo["name"]
        tipo = campo["bigquery_type"]

        if tipo not in (
            "INT64", "NUMERIC", "FLOAT64"
        ):

            continue

        presentes = entero(
            perfil["presentes__" + nombre]
        )

        nulos = entero(
            perfil["nulos__" + nombre]
        )

        exigir(
            presentes + nulos == total,
            "Conteos numericos incompatibles"
        )

        minimo, maximo, promedio = (
            perfil[prefijo + nombre]
            for prefijo in (
                "min__", "max__", "avg__"
            )
        )

        if presentes == 0:

            exigir(
                minimo is None
                and maximo is None
                and promedio is None,
                "Perfil nulo incompatible"
            )

            continue

        exigir(
            all(
                v is not None
                for v in (
                    minimo, maximo, promedio
                )
            ),
            "Estadistico obligatorio ausente"
        )

        if tipo == "FLOAT64":

            exigir(
                minimo <= maximo,
                "Intervalo FLOAT64 incompatible"
            )

            def cercano(a, b):

                return math.isclose(
                    a,
                    b,
                    rel_tol=POLITICA[
                        "float_relative_tolerance"
                    ],
                    abs_tol=POLITICA[
                        "float_absolute_tolerance"
                    ],
                )

            exigir(
                (
                    minimo <= promedio
                    or cercano(minimo, promedio)
                )
                and (
                    promedio <= maximo
                    or cercano(promedio, maximo)
                ),
                "Promedio FLOAT64 fuera de intervalo"
            )

        else:

            minimo = Decimal(minimo)
            maximo = Decimal(maximo)
            promedio = Decimal(promedio)

            exigir(
                minimo <= promedio <= maximo,
                "Promedio exacto fuera de intervalo"
            )


def normalizar_resultados_originales(
    resultados,
    contrato,
):

    exigir(
        set(resultados) == {
            "calidad",
            "esquema",
            "particiones",
            "indicadores",
        },
        "Resultados originales incompletos"
    )

    exigir(
        isinstance(resultados["esquema"], list)
        and isinstance(
            resultados["particiones"], list
        ),
        "Resultados tabulares incompatibles"
    )

    salida = {
        "calidad": normalizar_fila(
            resultados["calidad"],
            esquema_calidad(contrato),
        ),
        "esquema": [
            normalizar_fila(
                fila,
                esquema_columnas(),
            )
            for fila in resultados["esquema"]
        ],
        "particiones": [
            normalizar_fila(
                fila,
                esquema_particiones(contrato),
            )
            for fila in resultados["particiones"]
        ],
        "indicadores": normalizar_fila(
            resultados["indicadores"],
            esquema_indicadores(contrato),
        ),
    }

    validar_intervalos_perfil(
        salida["indicadores"],
        contrato,
    )

    return salida


def esquema_igualdad():

    return {
        "producto": "STRING",
        "filas_esperadas": "INT64",
        "columnas_comparadas": "INT64",
        "filas_origen": "INT64",
        "filas_destino": "INT64",
        "claves_invalidas_origen": "INT64",
        "claves_invalidas_destino": "INT64",
        "claves_duplicadas_origen": "INT64",
        "claves_duplicadas_destino": "INT64",
        "filas_adicionales": "INT64",
        "filas_faltantes": "INT64",
        "filas_modificadas": "INT64",
        "filas_iguales": "INT64",
        "igualdad_aprobada": "BOOL",
    }


def validar_igualdad(
    producto,
    resultado,
    contrato,
    total,
):

    fila = normalizar_fila(
        resultado,
        esquema_igualdad(),
    )

    exigir(
        fila["producto"] == producto,
        "Producto de igualdad incompatible"
    )

    exigir(
        fila["columnas_comparadas"]
        == len(contrato["gold"]["columns"]),
        "Columnas de igualdad incompatibles"
    )

    exigir(
        entero(total) > 0,
        "Conteo esperado incompatible"
    )

    for campo in (
        "filas_esperadas",
        "filas_origen",
        "filas_destino",
        "filas_iguales",
    ):

        exigir(
            entero(fila[campo]) == total,
            "Conteo de igualdad incompatible"
        )

    for campo in (
        "claves_invalidas_origen",
        "claves_invalidas_destino",
        "claves_duplicadas_origen",
        "claves_duplicadas_destino",
        "filas_adicionales",
        "filas_faltantes",
        "filas_modificadas",
    ):

        exigir(
            entero(fila[campo]) == 0,
            "Diferencia historica: " + campo
        )

    exigir(
        fila["igualdad_aprobada"] is True,
        "Igualdad historica rechazada"
    )

    return fila


def esquema_layout():

    return {
        "producto": "STRING",
        "columna": "STRING",
        "particion_esperada": "STRING",
        "particion_observada": "STRING",
        "clustering_esperado": "INT64",
        "clustering_observado": "INT64",
        "estado": "STRING",
    }


def validar_layout(
    producto,
    resultados,
    contrato,
):

    exigir(
        isinstance(resultados, list),
        "Layout debe ser una lista"
    )

    columnas = {
        c["name"]
        for c in contrato["gold"]["columns"]
    }

    config = contrato["configuracion"]

    particion = config["bigquery_partition"]

    clustering = config["clustering"]

    exigir(
        len(resultados) == len(columnas),
        "Cantidad de columnas layout incompatible"
    )

    vistas = set()

    normalizadas = []

    for resultado in resultados:

        fila = normalizar_fila(
            resultado,
            esquema_layout(),
        )

        nombre = fila["columna"]

        exigir(
            fila["producto"] == producto,
            "Producto layout incompatible"
        )

        exigir(
            nombre in columnas
            and nombre not in vistas,
            "Columna layout adicional o repetida"
        )

        vistas.add(nombre)

        particion_esperada = (
            "YES" if nombre == particion
            else "NO"
        )

        clustering_esperado = (
            clustering.index(nombre) + 1
            if nombre in clustering
            else None
        )

        exigir(
            fila["estado"] == "OK"
            and fila["particion_esperada"]
                == fila["particion_observada"]
                == particion_esperada
            and fila["clustering_esperado"]
                == fila["clustering_observado"]
                == clustering_esperado,
            "Layout incompatible: " + nombre
        )

        normalizadas.append(fila)

    exigir(
        vistas == columnas,
        "Layout incompleto"
    )

    return sorted(
        normalizadas,
        key=lambda fila: fila["columna"],
    )


def conciliar_historico_completo_local(
    origenes,
    resultados,
    complementarios,
    contratos,
):

    exigir(
        set(origenes)
        == set(resultados)
        == set(complementarios)
        == set(contratos)
        == set(PRODUCTOS),
        "Lote historico completo incompatible"
    )

    normalizados = {}

    origenes_normalizados = {}

    controles = {}

    for producto in sorted(PRODUCTOS):

        contrato = contratos[producto]

        origen = dict(
            origenes[producto]
        )

        origen["particiones"] = [
            normalizar_fila(
                fila,
                esquema_particiones(contrato),
            )
            for fila in origen["particiones"]
        ]

        origen["indicadores"] = normalizar_fila(
            origen["indicadores"],
            esquema_indicadores(contrato),
        )

        validar_intervalos_perfil(
            origen["indicadores"],
            contrato,
        )

        origenes_normalizados[
            producto
        ] = origen

        normalizados[producto] = (
            normalizar_resultados_originales(
                resultados[producto],
                contrato,
            )
        )

        adicional = complementarios[producto]

        exigir(
            set(adicional) == {
                "igualdad", "layout"
            },
            "Controles complementarios incompletos"
        )

        igualdad = validar_igualdad(
            producto,
            adicional["igualdad"],
            contrato,
            origen["records_count"],
        )

        layout = validar_layout(
            producto,
            adicional["layout"],
            contrato,
        )

        controles[producto] = {
            "equality_result_sha256":
                huella_documento(igualdad),
            "layout_result_sha256":
                huella_documento(layout),
            "candidate_equality_metrics_validated": True,
            "candidate_layout_metrics_validated": True,
        }

    original = conciliar_lote(
        origenes_normalizados,
        normalizados,
        contratos,
    )

    informe = {
        "status":
            "LOCAL_COMPLETE_RECONCILIATION_CANDIDATE",
        "original_report_sha256":
            original["report_sha256"],
        "products": controles,
        "products_reconciled":
            len(controles),
        "total_records":
            original["report"]["total_records"],
        "sql_result_types_normalized": True,
        "official_sources_verified": False,
        "google_sql_engine_validated": False,
        "row_level_equality_verified": False,
        "physical_table_layout_verified": False,
        "quality_authorization_verified": False,
        "production_authorized": False,
        "publication_authorized": False,
    }

    return {
        "original_reconciliation": original,
        "report": informe,
        "report_sha256":
            huella_documento(informe),
    }
