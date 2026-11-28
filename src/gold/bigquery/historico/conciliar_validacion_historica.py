"""Conciliacion local de resultados historicos.

No ejecuta SQL, no accede a GCP y no autoriza
cargas ni consumo productivo.
"""

import math
import re

from datetime import date
from decimal import Decimal

from preparador_carga_historica import (
    PRODUCTOS,
    exigir,
)

from trazabilidad_carga_historica import (
    HASHES_CONTRATOS,
    huella_documento,
    validar_sha,
    validar_version,
)


POLITICA = {
    "status": "CANDIDATE_PENDING_QUALITY",
    "float_relative_tolerance": 1e-9,
    "float_absolute_tolerance": 1e-8,
    "decimal_exact_comparison": True,
}


def entero(valor):

    exigir(
        type(valor) is int and valor >= 0,
        "Conteo invalido"
    )

    return valor


def decimal_exacto(valor):

    exigir(
        isinstance(valor, str)
        and re.fullmatch(
            r"-?[0-9]+(?:\.[0-9]+)?",
            valor,
        ) is not None,
        "Decimal debe conservarse como texto"
    )

    resultado = Decimal(valor)

    exigir(
        resultado.is_finite(),
        "Decimal no finito"
    )

    return resultado


def flotante(valor):

    exigir(
        type(valor) in (float, int),
        "FLOAT64 incompatible"
    )

    resultado = float(valor)

    exigir(
        math.isfinite(resultado),
        "FLOAT64 no finito"
    )

    return resultado


def comparar_numero(
    esperado,
    observado,
    tipo,
):

    if esperado is None or observado is None:

        return (
            esperado is None
            and observado is None
        )

    if tipo == "FLOAT64":

        return math.isclose(
            flotante(esperado),
            flotante(observado),
            rel_tol=POLITICA[
                "float_relative_tolerance"
            ],
            abs_tol=POLITICA[
                "float_absolute_tolerance"
            ],
        )

    if tipo == "INT64":

        exigir(
            type(esperado) is int
            and type(observado) is int,
            "INT64 incompatible"
        )

        return esperado == observado

    exigir(
        tipo in ("NUMERIC", "BIGNUMERIC"),
        "Tipo numerico no admitido"
    )

    return (
        decimal_exacto(esperado)
        == decimal_exacto(observado)
    )


def metricas_calidad(contrato):

    gold = contrato["gold"]

    metricas = {
        "claves_duplicadas",
        "particiones_invalidas",
    }

    for clave in gold["business_keys"]:

        metricas.add(
            "clave_invalida__" + clave
        )

    for campo in gold["columns"]:

        nombre = campo["name"]

        if campo["nullable"] is False:

            metricas.add(
                "nulo_obligatorio__" + nombre
            )

        if campo["bigquery_type"] == "FLOAT64":

            metricas.add(
                "no_finito__" + nombre
            )

        if campo["bigquery_type"] == "NUMERIC":

            metricas.add(
                "escala_invalida__" + nombre
            )

    return metricas


def validar_calidad(
    producto,
    resultado,
    esperado,
    contrato,
):

    metricas = metricas_calidad(contrato)

    campos = metricas | {
        "producto",
        "registros_esperados",
        "registros_observados",
        "controles_aprobados",
    }

    exigir(
        set(resultado) == campos,
        "Resultado de calidad incompleto"
    )

    exigir(
        resultado["producto"] == producto,
        "Producto de calidad incompatible"
    )

    exigir(
        entero(resultado["registros_esperados"])
        == entero(resultado["registros_observados"])
        == esperado,
        "Conteo historico no conciliado"
    )

    for metrica in sorted(metricas):

        exigir(
            entero(resultado[metrica]) == 0,
            "Control de calidad rechazado: "
            + metrica
        )

    exigir(
        resultado["controles_aprobados"] is True,
        "Calidad historica no aprobada"
    )


def validar_esquema_observado(
    resultado,
    contrato,
):

    columnas = contrato["gold"]["columns"]

    exigir(
        len(resultado) == len(columnas),
        "Cantidad de columnas incompatible"
    )

    esperadas = {
        c["name"]: (i, c)
        for i, c in enumerate(
            columnas, start=1
        )
    }

    vistas = set()

    for fila in resultado:

        nombre = fila["columna"]

        exigir(
            nombre in esperadas
            and nombre not in vistas,
            "Columna adicional o repetida"
        )

        vistas.add(nombre)

        posicion, campo = esperadas[nombre]

        nulabilidad = (
            "YES" if campo["nullable"] else "NO"
        )

        exigir(
            fila["estado"] == "OK"
            and fila["posicion_esperada"]
            == fila["posicion_observada"]
            == posicion
            and fila["tipo_esperado"]
            == fila["tipo_observado"]
            == campo["bigquery_type"]
            and fila["nullable_esperado"]
            == fila["nullable_observado"]
            == nulabilidad,
            "Diferencia de esquema: " + nombre
        )

    exigir(
        vistas == set(esperadas),
        "Esquema incompleto"
    )


def fecha_canonica(valor):

    exigir(
        isinstance(valor, str),
        "Fecha debe conservarse como texto ISO"
    )

    resultado = date.fromisoformat(valor)

    exigir(
        resultado.isoformat() == valor,
        "Fecha no canonica"
    )

    return resultado


def normalizar_particiones(
    producto,
    filas,
    contrato,
    total,
):

    columnas = contrato["gold"][
        "source_partition_columns"
    ]

    resultado = {}

    for fila in filas:

        exigir(
            set(fila) == set(columnas) | {
                "registros",
                "fecha_minima",
                "fecha_maxima",
            },
            "Campos de particion incompatibles"
        )

        clave = tuple(
            entero(fila[c])
            for c in columnas
        )

        exigir(
            clave not in resultado,
            "Particion repetida"
        )

        cantidad = entero(fila["registros"])

        exigir(
            cantidad > 0,
            "Particion vacia"
        )

        minimo = fecha_canonica(
            fila["fecha_minima"]
        )

        maximo = fecha_canonica(
            fila["fecha_maxima"]
        )

        exigir(
            minimo <= maximo,
            "Intervalo temporal incompatible"
        )

        for fecha in (minimo, maximo):

            periodo = fecha.month

            if producto == "empresa_trimestre":

                periodo = (
                    (fecha.month - 1) // 3 + 1
                )

            exigir(
                clave == (fecha.year, periodo),
                "Fecha fuera de particion"
            )

        resultado[clave] = (
            cantidad,
            minimo.isoformat(),
            maximo.isoformat(),
        )

    exigir(
        sum(
            valor[0]
            for valor in resultado.values()
        ) == total,
        "Total por particiones incompatible"
    )

    return resultado


def comparar_particiones(
    producto,
    esperadas,
    observadas,
    contrato,
    total,
):

    origen = normalizar_particiones(
        producto, esperadas, contrato, total
    )

    destino = normalizar_particiones(
        producto, observadas, contrato, total
    )

    exigir(
        origen == destino,
        "Particiones no conciliadas"
    )


def validar_perfiles(
    esperado,
    observado,
    contrato,
    total,
):

    campos = {"registros"}

    numericas = [
        c for c in contrato["gold"]["columns"]
        if c["bigquery_type"] in (
            "FLOAT64",
            "NUMERIC",
            "INT64",
        )
    ]

    for columna in numericas:

        nombre = columna["name"]

        campos.update(
            prefijo + nombre
            for prefijo in (
                "presentes__",
                "nulos__",
                "min__",
                "max__",
                "avg__",
            )
        )

    exigir(
        set(esperado) == set(observado) == campos,
        "Perfil numerico incompleto"
    )

    exigir(
        entero(esperado["registros"])
        == entero(observado["registros"])
        == total,
        "Conteo de perfil incompatible"
    )

    for columna in numericas:

        nombre = columna["name"]

        tipo = columna["bigquery_type"]

        presentes = "presentes__" + nombre
        nulos = "nulos__" + nombre

        for perfil in (esperado, observado):

            cantidad = entero(perfil[presentes])

            exigir(
                cantidad + entero(perfil[nulos])
                == total,
                "Conteos de perfil inconsistentes"
            )

            valores = [
                perfil[prefijo + nombre]
                for prefijo in (
                    "min__", "max__", "avg__"
                )
            ]

            exigir(
                all(
                    (valor is None)
                    == (cantidad == 0)
                    for valor in valores
                ),
                "Nulabilidad de perfil inconsistente"
            )

        exigir(
            esperado[presentes]
            == observado[presentes]
            and esperado[nulos]
            == observado[nulos],
            "Conteos numericos no conciliados: "
            + nombre
        )

        for prefijo in (
            "min__", "max__", "avg__"
        ):

            campo = prefijo + nombre

            tipo_comparacion = tipo

            if (
                prefijo == "avg__"
                and tipo != "FLOAT64"
            ):

                tipo_comparacion = "BIGNUMERIC"

            exigir(
                comparar_numero(
                    esperado[campo],
                    observado[campo],
                    tipo_comparacion,
                ),
                "Perfil numerico no conciliado: "
                + campo
            )


def conciliar_producto(
    producto,
    origen,
    resultados,
    contrato,
):

    exigir(
        producto in PRODUCTOS,
        "Producto incompatible"
    )

    exigir(
        origen["product"] == producto,
        "Identidad de origen incompatible"
    )

    validar_version(
        origen["version_id"]
    )

    validar_sha(
        origen["manifest_sha256"]
    )

    exigir(
        origen["gold_contract_sha256"]
        == contrato["sha256"]
        == HASHES_CONTRATOS[producto],
        "Contrato Gold incompatible"
    )

    total = entero(origen["records_count"])

    exigir(
        total > 0,
        "Origen sin registros"
    )

    exigir(
        set(resultados) == {
            "calidad",
            "esquema",
            "particiones",
            "indicadores",
        },
        "Resultados historicos incompletos"
    )

    validar_esquema_observado(
        resultados["esquema"],
        contrato,
    )

    validar_calidad(
        producto,
        resultados["calidad"],
        total,
        contrato,
    )

    comparar_particiones(
        producto,
        origen["particiones"],
        resultados["particiones"],
        contrato,
        total,
    )

    validar_perfiles(
        origen["indicadores"],
        resultados["indicadores"],
        contrato,
        total,
    )

    return {
        "product": producto,
        "version_id": origen["version_id"],
        "records_count": total,
        "manifest_sha256":
            origen["manifest_sha256"],
        "gold_contract_sha256":
            contrato["sha256"],
        "source_evidence_sha256":
            huella_documento(origen),
        "observed_results_sha256":
            huella_documento(resultados),
        "quality_reconciled": True,
        "schema_reconciled": True,
        "partitions_reconciled": True,
        "numeric_profiles_reconciled": True,
        "row_level_equality_verified": False,
        "physical_table_layout_verified": False,
        "official_source_verified": False,
        "production_authorized": False,
    }


def conciliar_lote(
    origenes,
    resultados,
    contratos,
):

    exigir(
        set(origenes)
        == set(resultados)
        == set(contratos)
        == set(PRODUCTOS),
        "Lote historico incompleto"
    )

    productos = {
        producto: conciliar_producto(
            producto,
            origenes[producto],
            resultados[producto],
            contratos[producto],
        )
        for producto in sorted(PRODUCTOS)
    }

    informe = {
        "status":
            "LOCAL_PROFILE_RECONCILIATION_ONLY",
        "products": productos,
        "products_reconciled": len(productos),
        "total_records": sum(
            p["records_count"]
            for p in productos.values()
        ),
        "comparison_policy": dict(POLITICA),
        "official_sources_verified": False,
        "row_level_equality_verified": False,
        "physical_table_layout_verified": False,
        "quality_authorization_verified": False,
        "production_authorized": False,
        "publication_authorized": False,
    }

    return {
        "report": informe,
        "report_sha256":
            huella_documento(informe),
    }
