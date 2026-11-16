#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics
Generador de versiones Silver simuladas para pruebas
del procesamiento incremental Gold.

Escenarios:
1. Insercion de tipo de cambio para 2026-09-18.
2. Correccion de tipo de cambio para 2025-12-30.

Ambos escenarios parten del mismo snapshot Silver.

Este script:
- Valida el dataset original.
- Genera versiones independientes.
- Conserva el esquema y las particiones.
- Comprueba los cambios efectivos.
- Genera un reporte de resultados.

No modifica Silver original.
No modifica Gold historico.
No publica datos productivos.
No utiliza HDFS.
"""

import argparse
import json
import math

from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURACION DE PRUEBAS
# ============================================================

HOME = Path.home()

DIRECTORIO_ENTREGAS = HOME / "bvl_entregas"

SILVER_ORIGINAL = (
    DIRECTORIO_ENTREGAS
    / "silver_20261109"
    / "silver"
)

DIRECTORIO_PRUEBAS = (
    DIRECTORIO_ENTREGAS
    / "incremental_pruebas_20261114"
)

RUTA_DATASET = Path(
    "macroeconomia/tipo_cambio"
)

VERSION_BASE = "snapshot_20261109"

VERSION_INSERCION = "bcrp_insercion_v1"

VERSION_CORRECCION = "bcrp_correccion_v1"

REGISTROS_BASE = 7751

FECHA_MINIMA = date(1997, 1, 2)

FECHA_MAXIMA = date(2026, 9, 17)

FECHA_INSERCION = date(2026, 9, 18)

FECHA_CORRECCION = date(2025, 12, 30)

VALOR_INSERCION = 3.370

VALOR_CORRECCION = 3.400

VALOR_ORIGINAL_CORRECCION = 3.36435714285714

COLUMNAS_ESPERADAS = [
    "fecha",
    "tipo_cambio_venta",
    "anio",
    "mes",
]

TIPOS_ESPERADOS = {
    "fecha": "date",
    "tipo_cambio_venta": "double",
    "anio": "int",
    "mes": "int",
}


# ============================================================
# 2. EXCEPCIONES Y FUNCIONES AUXILIARES
# ============================================================

class PreparacionError(Exception):
    """Error durante la preparacion de pruebas."""


def exigir(condicion, mensaje):

    if not condicion:

        raise PreparacionError(
            mensaje
        )


def fecha_actual_utc():

    return datetime.now(
        timezone.utc
    ).isoformat()


def ruta_uri(ruta):

    return ruta.resolve().as_uri()


def imprimir_seccion(titulo):

    print(
        "\n" + "=" * 70
    )

    print(titulo)

    print(
        "=" * 70
    )


def comprobar_destino_nuevo(ruta):

    exigir(
        not ruta.exists()
        and not ruta.is_symlink(),
        f"El destino ya existe: {ruta}",
    )


def comprobar_entorno(
    silver_root,
    pruebas_root,
):

    exigir(
        silver_root.is_dir(),
        f"No existe Silver original: {silver_root}",
    )

    exigir(
        pruebas_root.is_dir(),
        f"No existe el entorno de pruebas: {pruebas_root}",
    )

    silver_real = silver_root.resolve()

    pruebas_real = pruebas_root.resolve()

    exigir(
        silver_real != pruebas_real,
        "Silver original y pruebas coinciden.",
    )

    exigir(
        not pruebas_real.is_relative_to(
            silver_real
        ),
        "El entorno de pruebas no puede estar dentro de Silver.",
    )

    exigir(
        not silver_real.is_relative_to(
            pruebas_real
        ),
        "Silver original no puede estar dentro del entorno de pruebas.",
    )

    exigir(
        pruebas_real == DIRECTORIO_PRUEBAS.resolve(),
        "El destino no coincide con el entorno aislado autorizado.",
    )

    for nombre in [
        "silver_versiones",
        "reportes",
    ]:

        directorio = pruebas_real / nombre

        exigir(
            directorio.is_dir(),
            f"Falta el directorio: {directorio}",
        )

        exigir(
            not directorio.is_symlink(),
            f"No se permiten enlaces simbolicos: {directorio}",
        )

        exigir(
            directorio.resolve().is_relative_to(
                pruebas_real
            ),
            f"Directorio fuera del entorno autorizado: {directorio}",
        )


# ============================================================
# 3. VALIDACIONES DEL DATASET
# ============================================================

def validar_esquema(df):

    exigir(
        df.columns == COLUMNAS_ESPERADAS,
        f"Columnas inesperadas: {df.columns}",
    )

    tipos = dict(
        df.dtypes
    )

    exigir(
        tipos == TIPOS_ESPERADOS,
        f"Tipos de datos inesperados: {tipos}",
    )

    return tipos


def obtener_metricas(df):

    resultado = df.agg(

        F.count("*").alias(
            "total_registros"
        ),

        F.countDistinct("fecha").alias(
            "fechas_unicas"
        ),

        F.count(
            F.when(
                F.col("fecha").isNull(),
                1,
            )
        ).alias(
            "fechas_nulas"
        ),

        F.min("fecha").alias(
            "fecha_minima"
        ),

        F.max("fecha").alias(
            "fecha_maxima"
        ),

        F.count(
            F.when(
                F.col("anio").isNull()
                | F.col("mes").isNull()
                | (
                    F.year("fecha")
                    != F.col("anio")
                )
                | (
                    F.month("fecha")
                    != F.col("mes")
                ),
                1,
            )
        ).alias(
            "particiones_inconsistentes"
        ),

        F.count(
            F.when(
                F.col("tipo_cambio_venta").isNull(),
                1,
            )
        ).alias(
            "valores_nulos"
        ),

        F.count(
            F.when(
                F.isnan("tipo_cambio_venta")
                | (
                    F.abs("tipo_cambio_venta")
                    == float("inf")
                ),
                1,
            )
        ).alias(
            "valores_no_finitos"
        ),

    ).first().asDict()

    resultado["fecha_minima"] = (
        resultado["fecha_minima"].isoformat()
        if resultado["fecha_minima"]
        else None
    )

    resultado["fecha_maxima"] = (
        resultado["fecha_maxima"].isoformat()
        if resultado["fecha_maxima"]
        else None
    )

    return resultado


def validar_metricas(
    metricas,
    total_esperado,
    fecha_maxima_esperada,
):

    exigir(
        metricas["total_registros"] == total_esperado,
        f"Conteo inesperado: {metricas}",
    )

    exigir(
        metricas["fechas_unicas"] == total_esperado,
        "Existen fechas duplicadas.",
    )

    exigir(
        metricas["fechas_nulas"] == 0,
        "Existen fechas nulas.",
    )

    exigir(
        metricas["particiones_inconsistentes"] == 0,
        "Existen particiones inconsistentes.",
    )

    exigir(
        metricas["fecha_minima"] == FECHA_MINIMA.isoformat(),
        "Fecha minima inesperada.",
    )

    exigir(
        metricas["fecha_maxima"]
        == fecha_maxima_esperada.isoformat(),
        "Fecha maxima inesperada.",
    )

    exigir(
        metricas["valores_no_finitos"] == 0,
        "Existen valores no finitos de tipo de cambio.",
    )


def obtener_registro_fecha(
    df,
    fecha,
):

    registros = df.filter(
        F.col("fecha") == F.lit(fecha)
    ).collect()

    exigir(
        len(registros) <= 1,
        f"Fecha duplicada: {fecha}",
    )

    if not registros:

        return None

    return registros[0].asDict()


def validar_registro(
    registro,
    fecha,
    valor,
):

    exigir(
        registro is not None,
        f"No existe el registro esperado: {fecha}",
    )

    exigir(
        registro["fecha"] == fecha,
        "Fecha incorrecta.",
    )

    exigir(
        registro["anio"] == fecha.year,
        "Anio incorrecto.",
    )

    exigir(
        registro["mes"] == fecha.month,
        "Mes incorrecto.",
    )

    exigir(
        registro["tipo_cambio_venta"] is not None,
        "Tipo de cambio nulo.",
    )

    exigir(
        math.isclose(
            registro["tipo_cambio_venta"],
            valor,
            rel_tol=0.0,
            abs_tol=1e-12,
        ),
        f"Tipo de cambio inesperado: {registro}",
    )


def serializar_registro(registro):

    if registro is None:

        return None

    resultado = dict(
        registro
    )

    resultado["fecha"] = (
        resultado["fecha"].isoformat()
    )

    return resultado


# ============================================================
# 4. VALIDACION DEL SNAPSHOT ORIGINAL
# ============================================================

def validar_original(df):

    imprimir_seccion(
        "VALIDACION DEL SNAPSHOT SILVER ORIGINAL"
    )

    esquema = validar_esquema(
        df
    )

    metricas = obtener_metricas(
        df
    )

    validar_metricas(
        metricas,
        REGISTROS_BASE,
        FECHA_MAXIMA,
    )

    registro_correccion = obtener_registro_fecha(
        df,
        FECHA_CORRECCION,
    )

    validar_registro(
        registro_correccion,
        FECHA_CORRECCION,
        VALOR_ORIGINAL_CORRECCION,
    )

    registro_insercion = obtener_registro_fecha(
        df,
        FECHA_INSERCION,
    )

    exigir(
        registro_insercion is None,
        "La fecha de insercion ya existe.",
    )

    print(
        json.dumps(
            metricas,
            indent=4,
            ensure_ascii=False,
        )
    )

    print(
        "OK: snapshot Silver original validado."
    )

    return {
        "version": VERSION_BASE,
        "esquema": esquema,
        "metricas": metricas,
        "registro_correccion": serializar_registro(
            registro_correccion
        ),
        "registro_insercion": None,
    }


# ============================================================
# 5. GENERACION DE ESCENARIOS
# ============================================================

def construir_version_insercion(
    spark,
    original,
):

    registro = [

        (
            FECHA_INSERCION,
            VALOR_INSERCION,
            FECHA_INSERCION.year,
            FECHA_INSERCION.month,
        )

    ]

    nuevo = spark.createDataFrame(
        registro,
        schema=original.schema,
    )

    version = original.unionByName(
        nuevo
    )

    return version.select(
        *COLUMNAS_ESPERADAS
    )


def construir_version_correccion(
    original,
):

    version = original.withColumn(

        "tipo_cambio_venta",

        F.when(

            F.col("fecha")
            == F.lit(FECHA_CORRECCION),

            F.lit(
                VALOR_CORRECCION
            ).cast("double"),

        ).otherwise(

            F.col(
                "tipo_cambio_venta"
            )

        ),

    )

    return version.select(
        *COLUMNAS_ESPERADAS
    )


# ============================================================
# 6. ESCRITURA DE VERSIONES INDEPENDIENTES
# ============================================================

def escribir_version(
    df,
    destino,
):

    comprobar_destino_nuevo(
        destino
    )

    df.repartition(

        8,

        "anio",

        "mes",

    ).write.mode(

        "errorifexists"

    ).option(

        "compression",
        "snappy",

    ).partitionBy(

        "anio",
        "mes",

    ).parquet(

        ruta_uri(destino)

    )

    exigir(
        destino.is_dir(),
        f"No se genero el destino: {destino}",
    )

    exigir(
        (destino / "_SUCCESS").is_file(),
        f"No existe _SUCCESS: {destino}",
    )

    print(
        f"Version escrita: {destino}"
    )


# ============================================================
# 7. COMPARACION DE CAMBIOS EFECTIVOS
# ============================================================

def obtener_cambios(
    original,
    version,
):

    anteriores = original.alias(
        "anterior"
    )

    posteriores = version.alias(
        "posterior"
    )

    comparacion = anteriores.join(

        posteriores,

        F.col("anterior.fecha")
        == F.col("posterior.fecha"),

        "full_outer",

    ).select(

        F.coalesce(
            F.col("anterior.fecha"),
            F.col("posterior.fecha"),
        ).alias("fecha"),

        F.col(
            "anterior.fecha"
        ).alias("fecha_anterior"),

        F.col(
            "posterior.fecha"
        ).alias("fecha_nueva"),

        F.col(
            "anterior.tipo_cambio_venta"
        ).alias("valor_anterior"),

        F.col(
            "posterior.tipo_cambio_venta"
        ).alias("valor_nuevo"),

        F.col(
            "anterior.anio"
        ).alias("anio_anterior"),

        F.col(
            "posterior.anio"
        ).alias("anio_nuevo"),

        F.col(
            "anterior.mes"
        ).alias("mes_anterior"),

        F.col(
            "posterior.mes"
        ).alias("mes_nuevo"),

    )

    comparacion = comparacion.withColumn(

        "operacion",

        F.when(

            F.col("fecha_anterior").isNull(),

            F.lit("INSERT"),

        ).when(

            F.col("fecha_nueva").isNull(),

            F.lit("DELETE"),

        ).when(

            ~F.col("valor_anterior").eqNullSafe(
                F.col("valor_nuevo")
            )

            | ~F.col("anio_anterior").eqNullSafe(
                F.col("anio_nuevo")
            )

            | ~F.col("mes_anterior").eqNullSafe(
                F.col("mes_nuevo")
            ),

            F.lit("UPDATE"),

        ).otherwise(

            F.lit("SIN_CAMBIOS")

        ),

    )

    return comparacion.filter(

        F.col("operacion")
        != "SIN_CAMBIOS"

    )


def obtener_conteos_cambios(cambios):

    conteos = {

        "INSERT": 0,

        "UPDATE": 0,

        "DELETE": 0,

    }

    for registro in cambios.groupBy(
        "operacion"
    ).count().collect():

        conteos[
            registro["operacion"]
        ] = registro["count"]

    return conteos


def serializar_cambios(cambios):

    resultados = []

    for registro in cambios.orderBy(
        "fecha"
    ).collect():

        item = registro.asDict()

        for campo in [

            "fecha",

            "fecha_anterior",

            "fecha_nueva",

        ]:

            if item[campo] is not None:

                item[campo] = (
                    item[campo].isoformat()
                )

        resultados.append(
            item
        )

    return resultados


# ============================================================
# 8. VALIDACION DE VERSIONES GENERADAS
# ============================================================

def validar_version_insercion(
    spark,
    original,
    destino,
    metricas_originales,
):

    imprimir_seccion(
        "VALIDACION DE VERSION INSERT"
    )

    version = spark.read.parquet(
        ruta_uri(destino)
    ).persist()

    try:

        validar_esquema(
            version
        )

        metricas = obtener_metricas(
            version
        )

        validar_metricas(

            metricas,

            REGISTROS_BASE + 1,

            FECHA_INSERCION,

        )

        exigir(
            metricas["valores_nulos"]
            == metricas_originales["valores_nulos"],
            "Cambio inesperado en valores nulos.",
        )

        nuevo = obtener_registro_fecha(
            version,
            FECHA_INSERCION,
        )

        validar_registro(
            nuevo,
            FECHA_INSERCION,
            VALOR_INSERCION,
        )

        cambios = obtener_cambios(
            original,
            version,
        ).persist()

        try:

            conteos = obtener_conteos_cambios(
                cambios
            )

            exigir(

                conteos == {
                    "INSERT": 1,
                    "UPDATE": 0,
                    "DELETE": 0,
                },

                f"Cambios inesperados: {conteos}",

            )

            detalle = serializar_cambios(
                cambios
            )

            exigir(
                detalle[0]["fecha"]
                == FECHA_INSERCION.isoformat(),
                "La insercion afecta una fecha inesperada.",
            )

            resultado = {

                "estado": "VERSION_PRUEBA_VALIDADA",

                "version": VERSION_INSERCION,

                "version_anterior": VERSION_BASE,

                "ruta_silver": ruta_uri(destino),

                "metricas": metricas,

                "conteos_cambios": conteos,

                "detalle_cambios": detalle,

                "registro_nuevo": serializar_registro(
                    nuevo
                ),

            }

        finally:

            cambios.unpersist()

    finally:

        version.unpersist()

    print(
        json.dumps(
            resultado,
            indent=4,
            ensure_ascii=False,
        )
    )

    return resultado


def validar_version_correccion(
    spark,
    original,
    destino,
    metricas_originales,
):

    imprimir_seccion(
        "VALIDACION DE VERSION UPDATE"
    )

    version = spark.read.parquet(
        ruta_uri(destino)
    ).persist()

    try:

        validar_esquema(
            version
        )

        metricas = obtener_metricas(
            version
        )

        validar_metricas(

            metricas,

            REGISTROS_BASE,

            FECHA_MAXIMA,

        )

        exigir(
            metricas["valores_nulos"]
            == metricas_originales["valores_nulos"],
            "Cambio inesperado en valores nulos.",
        )

        corregido = obtener_registro_fecha(
            version,
            FECHA_CORRECCION,
        )

        validar_registro(
            corregido,
            FECHA_CORRECCION,
            VALOR_CORRECCION,
        )

        cambios = obtener_cambios(
            original,
            version,
        ).persist()

        try:

            conteos = obtener_conteos_cambios(
                cambios
            )

            exigir(

                conteos == {
                    "INSERT": 0,
                    "UPDATE": 1,
                    "DELETE": 0,
                },

                f"Cambios inesperados: {conteos}",

            )

            detalle = serializar_cambios(
                cambios
            )

            exigir(
                detalle[0]["fecha"]
                == FECHA_CORRECCION.isoformat(),
                "La correccion afecta una fecha inesperada.",
            )

            resultado = {

                "estado": "VERSION_PRUEBA_VALIDADA",

                "version": VERSION_CORRECCION,

                "version_anterior": VERSION_BASE,

                "ruta_silver": ruta_uri(destino),

                "metricas": metricas,

                "conteos_cambios": conteos,

                "detalle_cambios": detalle,

                "registro_corregido": serializar_registro(
                    corregido
                ),

            }

        finally:

            cambios.unpersist()

    finally:

        version.unpersist()

    print(
        json.dumps(
            resultado,
            indent=4,
            ensure_ascii=False,
        )
    )

    return resultado


# ============================================================
# 9. GENERACION DEL REPORTE
# ============================================================

def guardar_reporte(
    reporte,
    destino,
):

    comprobar_destino_nuevo(
        destino
    )

    with destino.open(

        "x",

        encoding="utf-8",

    ) as archivo:

        json.dump(

            reporte,

            archivo,

            indent=4,

            ensure_ascii=False,

            allow_nan=False,

        )

        archivo.write(
            "\n"
        )

    print(
        f"Reporte generado: {destino}"
    )


# ============================================================
# 10. EJECUCION PRINCIPAL
# ============================================================

def preparar_versiones(
    spark,
    silver_root,
    pruebas_root,
):

    comprobar_entorno(
        silver_root,
        pruebas_root,
    )

    origen = (
        silver_root.resolve()
        / RUTA_DATASET
    )

    exigir(
        origen.is_dir(),
        f"No existe el dataset: {origen}",
    )

    exigir(
        (origen / "_SUCCESS").is_file(),
        "No existe _SUCCESS en el snapshot original.",
    )

    versiones_root = (
        pruebas_root.resolve()
        / "silver_versiones"
    )

    raiz_insercion = (
        versiones_root
        / VERSION_INSERCION
    )

    raiz_correccion = (
        versiones_root
        / VERSION_CORRECCION
    )

    destino_insercion = (
        raiz_insercion
        / RUTA_DATASET
    )

    destino_correccion = (
        raiz_correccion
        / RUTA_DATASET
    )

    ruta_reporte = (
        pruebas_root.resolve()
        / "reportes"
        / "preparacion_versiones_bcrp.json"
    )

    comprobar_destino_nuevo(
        raiz_insercion
    )

    comprobar_destino_nuevo(
        raiz_correccion
    )

    comprobar_destino_nuevo(
        ruta_reporte
    )

    imprimir_seccion(
        "PREPARACION DE VERSIONES SILVER BCRP"
    )

    print(
        f"Origen: {origen}"
    )

    print(
        f"Destino insercion: {destino_insercion}"
    )

    print(
        f"Destino correccion: {destino_correccion}"
    )

    original = spark.read.parquet(

        ruta_uri(origen)

    ).persist()

    try:

        resultado_original = validar_original(
            original
        )

        resultado_original["ruta_silver"] = (
            ruta_uri(origen)
        )

        metricas_originales = (
            resultado_original["metricas"]
        )

        imprimir_seccion(
            "GENERACION DE VERSION INSERT"
        )

        version_insercion = (
            construir_version_insercion(
                spark,
                original,
            )
        )

        escribir_version(

            version_insercion,

            destino_insercion,

        )

        resultado_insercion = (
            validar_version_insercion(

                spark,

                original,

                destino_insercion,

                metricas_originales,

            )
        )

        imprimir_seccion(
            "GENERACION DE VERSION UPDATE"
        )

        version_correccion = (
            construir_version_correccion(
                original
            )
        )

        escribir_version(

            version_correccion,

            destino_correccion,

        )

        resultado_correccion = (
            validar_version_correccion(

                spark,

                original,

                destino_correccion,

                metricas_originales,

            )
        )

        imprimir_seccion(
            "VERIFICACION FINAL DEL SNAPSHOT ORIGINAL"
        )

        original_final = spark.read.parquet(
            ruta_uri(origen)
        )

        validar_esquema(
            original_final
        )

        metricas_finales = obtener_metricas(
            original_final
        )

        exigir(
            metricas_finales
            == metricas_originales,
            "Las metricas del snapshot original cambiaron.",
        )

        exigir(
            obtener_registro_fecha(
                original_final,
                FECHA_INSERCION,
            ) is None,
            "La insercion aparece en el snapshot original.",
        )

        validar_registro(

            obtener_registro_fecha(
                original_final,
                FECHA_CORRECCION,
            ),

            FECHA_CORRECCION,

            VALOR_ORIGINAL_CORRECCION,

        )

        cambios_original = obtener_cambios(

            original,

            original_final,

        )

        exigir(
            cambios_original.count() == 0,
            "Se detectaron diferencias en el snapshot original.",
        )

        reporte = {

            "estado": "VERSIONES_PRUEBA_PREPARADAS",

            "entorno": "PRUEBA",

            "fecha_generacion": fecha_actual_utc(),

            "dataset": "tipo_cambio",

            "clave_negocio": ["fecha"],

            "columnas_particion": [
                "anio",
                "mes",
            ],

            "datos_simulados": True,

            "publicacion_productiva": False,

            "autorizacion_ejecucion_gold": False,

            "snapshot_original": resultado_original,

            "version_insercion": resultado_insercion,

            "version_correccion": resultado_correccion,

            "verificacion_final_original": {

                "estado": "SIN_CAMBIOS_DETECTADOS",

                "metricas_conservadas": True,

                "registros_prueba_conservados": True,

            },

        }

        guardar_reporte(

            reporte,

            ruta_reporte,

        )

        imprimir_seccion(
            "RESULTADO FINAL"
        )

        print(
            "OK: snapshot original validado."
        )

        print(
            "OK: version INSERT validada."
        )

        print(
            "OK: version UPDATE validada."
        )

        print(
            "OK: diferencias efectivas verificadas."
        )

        print(
            "OK: snapshot original conservado."
        )

        print(
            "OK: reporte de pruebas generado."
        )

        print(
            "\nESTADO: VERSIONES_PRUEBA_PREPARADAS"
        )

        print(
            "No se autoriza una ejecucion productiva de Gold."
        )

    finally:

        original.unpersist()


# ============================================================
# 11. INTERFAZ DE LINEA DE COMANDOS
# ============================================================

def main():

    parser = argparse.ArgumentParser(

        description=(

            "Preparar versiones simuladas de Silver "
            "para pruebas incrementales BCRP."

        )

    )

    parser.add_argument(

        "--silver-root",

        default=str(
            SILVER_ORIGINAL
        ),

        help="Directorio raiz del snapshot Silver original.",

    )

    parser.add_argument(

        "--pruebas-root",

        default=str(
            DIRECTORIO_PRUEBAS
        ),

        help="Directorio aislado de pruebas operativas.",

    )

    argumentos = parser.parse_args()

    silver_root = Path(
        argumentos.silver_root
    )

    pruebas_root = Path(
        argumentos.pruebas_root
    )

    spark = SparkSession.builder.appName(

        "BVL-Preparar-Versiones-BCRP"

    ).getOrCreate()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:

        preparar_versiones(

            spark,

            silver_root,

            pruebas_root,

        )

    finally:

        spark.stop()


if __name__ == "__main__":

    main()
