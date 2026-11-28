"""Preparador local de carga historica Gold.

No contiene clientes GCS ni BigQuery.
No concede autorizaciones operativas.
No modifica Gold oficial.
"""

import hashlib
import json

from pathlib import Path
from functools import reduce

from pyspark.sql import functions as F


BASE_GOLD = (
    "62e0f0f76743822bc19859459e4307b2fb9b6ef2"
)

PRODUCTOS = {
    "mercado_valor_diario": 83130,
    "actividad_sab_diaria": 21911,
    "empresa_trimestre": 5158,
}

COLUMNAS_ESPERADAS = {
    "mercado_valor_diario": 41,
    "actividad_sab_diaria": 41,
    "empresa_trimestre": 58,
}

DECIMALES_SAB = {
    "total_compras_diario",
    "total_ventas_diario",
    "total_actividad_diaria",
}

TIPOS_DIRECTOS = {
    "string": "STRING",
    "date": "DATE",
    "double": "FLOAT64",
    "boolean": "BOOL",
    "integer": "INT64",
    "int": "INT64",
    "long": "INT64",
    "bigint": "INT64",
    "decimal(24,2)": "NUMERIC",
    "decimal(25,2)": "NUMERIC",
    "decimal(26,2)": "NUMERIC",
}


def exigir(condicion, mensaje):

    if not condicion:

        raise ValueError(mensaje)


def sha256(contenido):

    return hashlib.sha256(
        contenido
    ).hexdigest()


def pares_sin_duplicados(pares):

    resultado = {}

    for clave, valor in pares:

        exigir(
            clave not in resultado,
            f"Clave JSON duplicada: {clave}"
        )

        resultado[clave] = valor

    return resultado


def leer_json(contenido):

    return json.loads(
        contenido,
        object_pairs_hook=pares_sin_duplicados,
    )


def cargar_contratos(raiz):

    raiz = Path(raiz).resolve()

    carpeta = raiz / "docs/bigquery"

    ruta = (
        carpeta
        / "carga_historica"
        / "contrato_carga_historica.json"
    )

    contenido = ruta.read_bytes()

    historico = leer_json(contenido)

    exigir(
        historico["schema_version"]
        == "BVL-BQ-HISTORICAL-LOAD-CANDIDATE-v0.1",
        "Contrato historico incompatible"
    )

    exigir(
        historico["gold_base_commit"] == BASE_GOLD,
        "Base Gold incompatible"
    )

    exigir(
        historico["status"]
        == "PROPUESTA_LOCAL_NO_AUTORIZADA",
        "Estado de contrato incompatible"
    )

    for campo in (
        "execution_authorized",
        "staging_write_authorized",
        "production_write_authorized",
        "publication_authorized",
    ):

        exigir(
            historico[campo] is False,
            "Autorizacion no admitida en modo local"
        )

    exigir(
        set(historico["products"])
        == set(PRODUCTOS),
        "Productos historicos incompatibles"
    )

    exigir(
        historico["expected_total_rows"]
        == sum(PRODUCTOS.values()),
        "Conteo total incompatible"
    )

    exigir(
        historico["expected_total_columns"]
        == sum(COLUMNAS_ESPERADAS.values()),
        "Total de columnas incompatible"
    )

    politica = historico["decimal_policy"]

    exigir(
        politica["source_type"] == "decimal(34,2)"
        and politica["transfer_type"]
        == "decimal(31,2)"
        and politica["target_type"] == "NUMERIC"
        and set(politica["columns"]) == DECIMALES_SAB
        and politica["rounding_allowed"] is False
        and politica["overflow_allowed"] is False
        and politica["invalid_to_null_allowed"] is False,
        "Politica decimal incompatible"
    )

    contratos = {}

    for producto, configuracion in (
        historico["products"].items()
    ):

        ruta_gold = (
            carpeta / f"{producto}.json"
        )

        contenido_gold = ruta_gold.read_bytes()

        referencia = configuracion["gold_contract"]

        exigir(
            referencia["path"]
            == f"docs/bigquery/{producto}.json",
            "Ruta de contrato incompatible"
        )

        exigir(
            sha256(contenido_gold)
            == referencia["sha256"],
            f"Hash de contrato incorrecto: {producto}"
        )

        gold = leer_json(contenido_gold)

        exigir(
            gold["product"] == producto,
            "Identidad Gold incompatible"
        )

        exigir(
            gold["table_proposed"]
            == configuracion["target_table"],
            "Destino BigQuery incompatible"
        )

        exigir(
            gold["business_keys"]
            == configuracion["keys"],
            "Claves de negocio incompatibles"
        )

        exigir(
            gold["source_partition_columns"]
            == configuracion["gcs_partitions"],
            "Particiones incompatibles"
        )

        exigir(
            configuracion["expected_rows"]
            == PRODUCTOS[producto],
            "Conteo candidato incompatible"
        )

        columnas = gold["columns"]

        nombres = [
            c["name"] for c in columnas
        ]

        exigir(
            len(columnas)
            == len(set(nombres))
            == configuracion["expected_columns"]
            == COLUMNAS_ESPERADAS[producto],
            "Cantidad de columnas incompatible"
        )

        for columna in columnas:

            nombre = columna["name"]

            tipo = columna["spark_type"]

            destino = columna["bigquery_type"]

            exigir(
                type(columna["nullable"]) is bool,
                "Nulabilidad de contrato invalida"
            )

            if tipo == "decimal(34,2)":

                exigir(
                    producto == "actividad_sab_diaria"
                    and nombre in DECIMALES_SAB
                    and destino == "NUMERIC",
                    "Decimal especial no contemplado"
                )

            else:

                exigir(
                    TIPOS_DIRECTOS.get(tipo)
                    == destino,
                    f"Tipo incompatible: {nombre}"
                )

        if producto == "actividad_sab_diaria":

            especiales = {
                c["name"]
                for c in columnas
                if c["spark_type"] == "decimal(34,2)"
            }

            exigir(
                especiales == DECIMALES_SAB,
                "Columnas decimales SAB incompatibles"
            )

        contratos[producto] = {
            "configuracion": configuracion,
            "gold": gold,
            "sha256": referencia["sha256"],
        }

    return historico, contratos


def esquema_dataframe(df):

    exigir(
        len(df.columns) == len(set(df.columns)),
        "Columnas duplicadas en DataFrame"
    )

    return {
        campo.name: campo.dataType.simpleString()
        for campo in df.schema.fields
    }


def validar_esquema(df, contrato):

    esperado = {
        c["name"]: c["spark_type"]
        for c in contrato["columns"]
    }

    observado = esquema_dataframe(df)

    exigir(
        set(esperado) == set(observado),
        "Columnas faltantes o adicionales"
    )

    diferencias = {
        nombre: {
            "esperado": tipo,
            "observado": observado[nombre],
        }
        for nombre, tipo in esperado.items()
        if observado[nombre] != tipo
    }

    exigir(
        not diferencias,
        f"Tipos Gold incompatibles: {diferencias}"
    )

    return df.select(*esperado.keys())


def condicion_particiones_invalidas(
    producto, configuracion,
):

    fecha = F.col(
        configuracion["bigquery_partition"]
    )

    invalida = (
        fecha.isNull()
        | ~F.col("anio").eqNullSafe(
            F.year(fecha)
        )
    )

    if producto == "empresa_trimestre":

        invalida = (
            invalida
            | ~F.col("trimestre").eqNullSafe(
                F.quarter(fecha)
            )
        )

    else:

        invalida = (
            invalida
            | ~F.col("mes").eqNullSafe(
                F.month(fecha)
            )
        )

    return invalida


def contar_condicion(condicion, nombre):

    return F.count(
        F.when(condicion, 1)
    ).alias(nombre)


def expresion_decimal_exacta(nombre):

    original = F.col(nombre)

    convertido = F.expr(
        f"try_cast(`{nombre}` as decimal(31,2))"
    )

    reconstruido = convertido.cast(
        "decimal(34,2)"
    )

    invalido = (
        original.isNotNull()
        & (
            convertido.isNull()
            | ~original.eqNullSafe(
                reconstruido
            )
        )
    )

    return convertido, invalido


def preparar_producto(
    producto,
    df,
    contrato,
):

    exigir(
        producto in PRODUCTOS,
        "Producto historico no admitido"
    )

    configuracion = contrato["configuracion"]

    gold = contrato["gold"]

    exigir(
        gold["product"] == producto,
        "Contrato asociado a otro producto"
    )

    esperado = PRODUCTOS[producto]

    df = validar_esquema(df, gold)

    tipos = esquema_dataframe(df)

    claves = configuracion["keys"]

    controles = [
        F.count("*").alias("__filas")
    ]

    for nombre in claves:

        invalida = F.col(nombre).isNull()

        if tipos[nombre] == "string":

            invalida = (
                invalida
                | (F.length(
                    F.trim(F.col(nombre))
                ) == 0)
            )

        controles.append(
            contar_condicion(
                invalida,
                f"clave__{nombre}"
            )
        )

    for columna in gold["columns"]:

        nombre = columna["name"]

        if not columna["nullable"]:

            controles.append(
                contar_condicion(
                    F.col(nombre).isNull(),
                    f"nulo__{nombre}"
                )
            )

        if columna["spark_type"] == "double":

            valor = F.col(nombre)

            invalido = (
                valor.isNotNull()
                & (
                    F.isnan(valor)
                    | (F.abs(valor) == float("inf"))
                )
            )

            controles.append(
                contar_condicion(
                    invalido,
                    f"no_finito__{nombre}"
                )
            )

    controles.append(
        contar_condicion(
            condicion_particiones_invalidas(
                producto,
                configuracion,
            ),
            "particiones_invalidas",
        )
    )

    conversiones = {}

    if producto == "actividad_sab_diaria":

        for nombre in sorted(DECIMALES_SAB):

            convertido, invalido = (
                expresion_decimal_exacta(nombre)
            )

            conversiones[nombre] = convertido

            controles.append(
                contar_condicion(
                    invalido,
                    f"decimal_invalido__{nombre}"
                )
            )

            controles.append(
                contar_condicion(
                    F.col(nombre).isNull(),
                    f"decimal_nulo__{nombre}"
                )
            )

    metricas = df.agg(
        *controles
    ).first().asDict()

    exigir(
        metricas["__filas"] == esperado,
        f"Conteo incompatible: "
        f"{metricas['__filas']} != {esperado}"
    )

    errores = {
        nombre: cantidad
        for nombre, cantidad
        in metricas.items()
        if nombre != "__filas"
        and not nombre.startswith("decimal_nulo__")
        and cantidad > 0
    }

    exigir(
        not errores,
        f"Controles de calidad rechazados: {errores}"
    )

    duplicadas = (
        df.groupBy(*claves)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    exigir(
        duplicadas == 0,
        "Claves de negocio duplicadas"
    )

    transferencia = df.select(*[
        conversiones[nombre].alias(nombre)
        if nombre in conversiones
        else F.col(nombre)
        for nombre in df.columns
    ])

    tipos_transferencia = {
        **tipos,
        **{
            nombre: "decimal(31,2)"
            for nombre in conversiones
        },
    }

    exigir(
        esquema_dataframe(transferencia)
        == tipos_transferencia,
        "Esquema de transferencia incompatible"
    )

    evidencia = {
        "status": "LOCAL_VALIDATED_CANDIDATE",
        "product": producto,
        "gold_contract_sha256": contrato["sha256"],
        "expected_candidate_rows": esperado,
        "observed_rows": metricas["__filas"],
        "columns": len(df.columns),
        "business_keys": claves,
        "duplicate_keys": 0,
        "partitions": configuracion["gcs_partitions"],
        "quality_metrics": metricas,
        "decimal_adaptations": {
            nombre: {
                "source_type": "decimal(34,2)",
                "transfer_type": "decimal(31,2)",
                "target_type": "NUMERIC",
                "original_nulls":
                    metricas[f"decimal_nulo__{nombre}"],
                "invalid_records": 0,
                "exact_conversion": True,
                "rounding_allowed": False,
            }
            for nombre in sorted(conversiones)
        },
        "gold_modified": False,
        "official_source_verified": False,
        "execution_authorized": False,
        "bigquery_write_authorized": False,
        "publication_authorized": False,
    }

    return transferencia, evidencia


def preparar_lote_historico(
    dataframes,
    contratos,
):

    exigir(
        set(dataframes) == set(PRODUCTOS),
        "Lote historico incompleto"
    )

    exigir(
        set(contratos) == set(PRODUCTOS),
        "Contratos historicos incompletos"
    )

    transferencias = {}
    evidencias = {}

    for producto in PRODUCTOS:

        (
            transferencias[producto],
            evidencias[producto],
        ) = preparar_producto(
            producto,
            dataframes[producto],
            contratos[producto],
        )

    total = sum(
        evidencia["observed_rows"]
        for evidencia in evidencias.values()
    )

    columnas = sum(
        evidencia["columns"]
        for evidencia in evidencias.values()
    )

    exigir(
        total == 110199
        and columnas == 140,
        "Totales historicos incompatibles"
    )

    return transferencias, {
        "status": "LOCAL_CANDIDATE_ONLY",
        "products": evidencias,
        "total_rows": total,
        "total_columns": columnas,
        "official_sources_verified": False,
        "execution_authorized": False,
        "bigquery_write_authorized": False,
        "publication_authorized": False,
    }
