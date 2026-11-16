#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics
Validador operativo Silver -> Gold.

Contrato Silver-Gold v0.1.

Primera implementacion operativa:
- Dataset tipo_cambio (BCRP).
- Versiones locales registradas.
- Inserciones, correcciones y eliminaciones.
- Integracion experimental con Daniela.

No modifica Silver ni Gold.
No ejecuta transformaciones Gold.
No autoriza publicaciones productivas.
"""

import argparse
import copy
import json
import sys

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __package__:

    from .manifest_validator import (
        ManifestError,
        cargar_manifest,
    )

    from .version_registry import (
        VersionRegistryError,
        DIRECTORIO_PRUEBAS,
        RUTA_REGISTRO,
        cargar_registro,
        obtener_version,
        resolver_manifest,
        verificar_integridad_version,
        validar_ruta_dataset,
    )

else:

    from manifest_validator import (
        ManifestError,
        cargar_manifest,
    )

    from version_registry import (
        VersionRegistryError,
        DIRECTORIO_PRUEBAS,
        RUTA_REGISTRO,
        cargar_registro,
        obtener_version,
        resolver_manifest,
        verificar_integridad_version,
        validar_ruta_dataset,
    )


# ============================================================
# 1. CONFIGURACION
# ============================================================

VERSION_VALIDADOR = "0.1"

DATASETS_SOPORTADOS = {
    "tipo_cambio",
}

ESQUEMA_TIPO_CAMBIO = {
    "fecha": "date",
    "tipo_cambio_venta": "double",
    "anio": "int",
    "mes": "int",
}

CLAVE_TIPO_CAMBIO = ["fecha"]

PARTICIONES_TIPO_CAMBIO = [
    "anio",
    "mes",
]

MAX_CAMBIOS_DETALLE = 10000

ESTADO_SILVER_APROBADO = (
    "SILVER_PUBLICADO_APROBADO"
)

ESTADO_SILVER_LABORATORIO = (
    "SILVER_VERSION_LABORATORIO_VERIFICADA"
)


# Referencias de la entrega experimental de Daniela.

DANIELA_RUN_ID = (
    "20261116T003053Z_78127719"
)

DANIELA_VERSION_BASE = (
    "v_20261116T003053Z_78127719_base"
)

DANIELA_VERSION_FULL = (
    "v_20261116T003053Z_78127719_full"
)

DANIELA_VERSION_ESQUEMA = (
    "sha256_1c5916a4d7a5ae6d"
)

DANIELA_HUELLA_MANIFEST = (
    "b1403c6ff41942650f8492fd6d7fa899"
    "cb2f4acb416ff0ccc3e77a341478af2b"
)

DANIELA_HUELLA_BASE = (
    "776eb4476b4820d98908ed0101b47089"
    "bf3fe424ce782998f9df0ae70d62dae1"
)

DANIELA_HUELLA_FULL = (
    "2267476b8dafc97003d787a81dd2b498"
    "2bd0b82831614bfb004d3a2015bc46e5"
)


# ============================================================
# 2. EXCEPCIONES Y FUNCIONES AUXILIARES
# ============================================================

class OperationalValidationError(Exception):
    """Error durante la validacion operativa Silver."""


def exigir(condicion, mensaje):

    if not condicion:

        raise OperationalValidationError(
            mensaje
        )


def fecha_actual_utc():

    return datetime.now(
        timezone.utc
    ).isoformat()


def imprimir_seccion(titulo):

    print("\n" + "=" * 70)
    print(titulo)
    print("=" * 70)


def json_canonico(contenido):

    return json.dumps(
        contenido,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def serializar_fecha(valor):

    if valor is None:

        return None

    return valor.isoformat()


def guardar_reporte_nuevo(
    reporte,
    ruta,
):

    ruta = Path(ruta).absolute()

    raiz_reportes = (
        DIRECTORIO_PRUEBAS / "reportes"
    )

    exigir(
        raiz_reportes.is_dir(),
        "No existe el directorio de reportes.",
    )

    for componente in [
        ruta,
        *ruta.parents,
    ]:

        exigir(
            not componente.is_symlink(),
            f"Enlace simbolico no permitido: {componente}",
        )

    exigir(
        ruta.parent.resolve()
        == raiz_reportes.resolve(),
        "El reporte debe guardarse dentro de reportes.",
    )

    exigir(
        ruta.suffix == ".json",
        "El reporte debe ser un archivo JSON.",
    )

    exigir(
        not ruta.exists(),
        f"El reporte ya existe: {ruta}",
    )

    contenido = json.dumps(
        reporte,
        indent=4,
        ensure_ascii=False,
        allow_nan=False,
    ) + "\n"

    with ruta.open(
        "x",
        encoding="utf-8",
    ) as archivo:

        archivo.write(contenido)

    return ruta


# ============================================================
# 3. VALIDACION DEL MANIFEST DE LABORATORIO
# ============================================================

def preparar_manifest_laboratorio(
    manifest,
    huella_manifest,
    registro,
):

    exigir(
        huella_manifest == DANIELA_HUELLA_MANIFEST,
        "La huella del manifest de Daniela no coincide.",
    )

    exigir(
        manifest["run_id"] == DANIELA_RUN_ID,
        "run_id de laboratorio desconocido.",
    )

    exigir(
        manifest["entorno"] == "PRUEBA",
        "El laboratorio debe pertenecer a PRUEBA.",
    )

    exigir(
        manifest["estado"] == ESTADO_SILVER_LABORATORIO,
        "Estado de laboratorio incompatible.",
    )

    exigir(
        manifest["validacion_aprobada"] is False,
        "El manifest de laboratorio no debe declarar "
        "una publicacion oficial aprobada.",
    )

    exigir(
        len(manifest["datasets"]) == 1,
        "Se esperaba un solo dataset de laboratorio.",
    )

    dataset = manifest["datasets"][0]

    exigir(
        dataset["dataset"] == "tipo_cambio",
        "Dataset de laboratorio incompatible.",
    )

    exigir(
        dataset["version_anterior"]
        == DANIELA_VERSION_BASE,
        "Version base de Daniela incompatible.",
    )

    exigir(
        dataset["version_publicada"]
        == DANIELA_VERSION_FULL,
        "Version nueva de Daniela incompatible.",
    )

    exigir(
        dataset["version_esquema"]
        == DANIELA_VERSION_ESQUEMA,
        "Version de esquema incompatible.",
    )

    anterior = obtener_version(
        registro,
        "tipo_cambio",
        DANIELA_VERSION_BASE,
    )

    publicada = obtener_version(
        registro,
        "tipo_cambio",
        DANIELA_VERSION_FULL,
    )

    exigir(
        anterior["integridad"]["huella_dataset_sha256"]
        == DANIELA_HUELLA_BASE,
        "La version base registrada no coincide "
        "con la entrega de Daniela.",
    )

    exigir(
        publicada["integridad"]["huella_dataset_sha256"]
        == DANIELA_HUELLA_FULL,
        "La version nueva registrada no coincide "
        "con la entrega de Daniela.",
    )

    original = obtener_version(
        registro,
        "tipo_cambio",
        "snapshot_20261109",
    )

    exigir(
        anterior["integridad"]
        == original["integridad"],
        "La version base no coincide con "
        "el snapshot Silver historico.",
    )

    verificar_integridad_version(original)

    # Proyeccion temporal exclusivamente en memoria.
    #
    # El validador estructural actual exige el estado
    # SILVER_PUBLICADO_APROBADO.
    #
    # Adaptamos una copia para reutilizar sus controles.
    # Esta copia NO se guarda como manifest publicado,
    # NO sustituye al original y NO autoriza Gold.

    proyeccion = copy.deepcopy(manifest)

    proyeccion["estado"] = (
        ESTADO_SILVER_APROBADO
    )

    proyeccion["validacion_aprobada"] = True

    proyeccion["datasets"][0]["ruta_silver"] = (
        publicada["ruta_silver"]
    )

    return proyeccion


# ============================================================
# 4. PREPARAR MANIFEST PARA VALIDACION
# ============================================================

def preparar_validacion(
    manifest,
    huella_manifest,
    registro,
    modo_laboratorio=False,
):

    exigir(
        isinstance(manifest, dict),
        "El manifest debe ser un objeto JSON.",
    )

    exigir(
        manifest.get("entorno") == "PRUEBA",
        "Solo se permite el entorno PRUEBA.",
    )

    if modo_laboratorio:

        proyeccion = preparar_manifest_laboratorio(
            manifest,
            huella_manifest,
            registro,
        )

    else:

        exigir(
            manifest.get("estado")
            == ESTADO_SILVER_APROBADO,
            "Silver no tiene un estado aprobado "
            "para pruebas.",
        )

        exigir(
            manifest.get("validacion_aprobada") is True,
            "La validacion Silver no fue aprobada.",
        )

        proyeccion = copy.deepcopy(manifest)

    resolucion = resolver_manifest(
        registro,
        proyeccion,
        verificar_integridad=True,
    )

    exigir(
        resolucion["versiones_resueltas"] is True,
        "No se resolvieron las versiones Silver.",
    )

    exigir(
        resolucion["integridad_verificada"] is True,
        "No se verifico la integridad Silver.",
    )

    exigir(
        resolucion["autorizacion_ejecucion_gold"] is False,
        "La resolucion no debe autorizar Gold.",
    )

    for dataset in proyeccion["datasets"]:

        exigir(
            dataset["dataset"] in DATASETS_SOPORTADOS,
            "Dataset todavia no soportado operativamente: "
            + dataset["dataset"],
        )

        exigir(
            dataset["ruta_detalle_cambios"] is None,
            "El detalle externo todavia no esta soportado.",
        )

    return proyeccion, resolucion


# ============================================================
# 5. INICIALIZACION DE SPARK
# ============================================================

def crear_spark():

    spark = (
        SparkSession.builder
        .appName(
            "BVL-Validacion-Operativa-Silver"
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.sql.sources.partitionColumnTypeInference.enabled",
            "true",
        )
        .config(
            "spark.sql.files.ignoreCorruptFiles",
            "false",
        )
        .config(
            "spark.sql.files.ignoreMissingFiles",
            "false",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# 6. LECTURA DE VERSIONES REGISTRADAS
# ============================================================

def leer_version(
    spark,
    version,
):

    ruta = validar_ruta_dataset(
        version["dataset"],
        version["ruta_silver"],
    )

    archivos = version["integridad"]["archivos"]

    exigir(
        bool(archivos),
        "La version no contiene archivos Parquet.",
    )

    # Leemos exclusivamente los archivos incluidos
    # en el inventario registrado.
    #
    # basePath permite recuperar las columnas
    # de particion anio y mes.

    rutas_archivos = [

        str(
            ruta / archivo["ruta_relativa"]
        )

        for archivo in archivos

    ]

    dataframe = (
        spark.read
        .option("basePath", str(ruta))
        .option("mergeSchema", "true")
        .parquet(*rutas_archivos)
    )

    return dataframe


# ============================================================
# 7. VALIDACION DE ESQUEMA FISICO
# ============================================================

def validar_esquema_tipo_cambio(
    dataframe,
    version,
):

    esquema_fisico = dict(
        dataframe.dtypes
    )

    exigir(
        len(dataframe.columns)
        == len(set(dataframe.columns)),
        "Existen nombres de columnas duplicados.",
    )

    exigir(
        esquema_fisico == ESQUEMA_TIPO_CAMBIO,
        f"Esquema fisico incompatible: {esquema_fisico}",
    )

    exigir(
        esquema_fisico == version["esquema"],
        "El esquema fisico no coincide "
        "con el esquema registrado.",
    )

    exigir(
        version["clave_negocio"]
        == CLAVE_TIPO_CAMBIO,
        "Clave de negocio registrada incompatible.",
    )

    exigir(
        version["columnas_particion"]
        == PARTICIONES_TIPO_CAMBIO,
        "Particiones registradas incompatibles.",
    )

    return esquema_fisico


# ============================================================
# 8. METRICAS FISICAS DE TIPO DE CAMBIO
# ============================================================

def obtener_metricas_tipo_cambio(
    dataframe,
):

    valor = F.col("tipo_cambio_venta")

    particion_inconsistente = (

        ~F.col("anio").eqNullSafe(
            F.year("fecha")
        )

        |

        ~F.col("mes").eqNullSafe(
            F.month("fecha")
        )

    )

    valor_no_finito = (

        F.isnan(valor)

        |

        (F.abs(valor) == float("inf"))

    )

    fila = dataframe.agg(

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
                particion_inconsistente,
                1,
            )
        ).alias(
            "particiones_inconsistentes"
        ),

        F.count(
            F.when(
                valor.isNull(),
                1,
            )
        ).alias(
            "valores_nulos"
        ),

        F.count(
            F.when(
                valor_no_finito,
                1,
            )
        ).alias(
            "valores_no_finitos"
        ),

    ).first()

    metricas = fila.asDict()

    metricas["fecha_minima"] = serializar_fecha(
        metricas["fecha_minima"]
    )

    metricas["fecha_maxima"] = serializar_fecha(
        metricas["fecha_maxima"]
    )

    return metricas


def validar_metricas_tipo_cambio(
    metricas,
    version,
):

    esperado = version["cantidad_registros"]

    exigir(
        metricas["total_registros"] == esperado,
        "Conteo fisico incompatible con el registrado: "
        f"esperado={esperado}, "
        f"real={metricas['total_registros']}",
    )

    exigir(
        metricas["fechas_nulas"] == 0,
        "Se encontraron claves de negocio nulas.",
    )

    exigir(
        metricas["fechas_unicas"]
        == metricas["total_registros"],
        "Se encontraron claves de negocio duplicadas.",
    )

    exigir(
        metricas["particiones_inconsistentes"] == 0,
        "Se encontraron particiones inconsistentes.",
    )

    exigir(
        metricas["valores_no_finitos"] == 0,
        "Se encontraron valores NaN o infinitos.",
    )


# ============================================================
# 9. COMPARACION DE VERSIONES SILVER
# ============================================================

def detectar_cambios_tipo_cambio(
    anterior,
    publicada,
):

    a = anterior.alias("a")

    b = publicada.alias("b")

    comparacion = a.join(
        b,
        F.col("a.fecha") == F.col("b.fecha"),
        "full_outer",
    )

    valores_diferentes = (

        ~F.col("a.tipo_cambio_venta").eqNullSafe(
            F.col("b.tipo_cambio_venta")
        )

        |

        ~F.col("a.anio").eqNullSafe(
            F.col("b.anio")
        )

        |

        ~F.col("a.mes").eqNullSafe(
            F.col("b.mes")
        )

    )

    operacion = (

        F.when(
            F.col("a.fecha").isNull(),
            F.lit("INSERT"),
        )

        .when(
            F.col("b.fecha").isNull(),
            F.lit("DELETE"),
        )

        .when(
            valores_diferentes,
            F.lit("UPDATE"),
        )

        .otherwise(
            F.lit("SIN_CAMBIOS")
        )

    )

    cambios = comparacion.select(

        F.coalesce(
            F.col("a.fecha"),
            F.col("b.fecha"),
        ).alias("fecha"),

        F.col("a.fecha").alias(
            "fecha_anterior"
        ),

        F.col("b.fecha").alias(
            "fecha_nueva"
        ),

        F.col("a.tipo_cambio_venta").alias(
            "valor_anterior"
        ),

        F.col("b.tipo_cambio_venta").alias(
            "valor_nuevo"
        ),

        F.col("a.anio").alias(
            "anio_anterior"
        ),

        F.col("b.anio").alias(
            "anio_nuevo"
        ),

        F.col("a.mes").alias(
            "mes_anterior"
        ),

        F.col("b.mes").alias(
            "mes_nuevo"
        ),

        operacion.alias(
            "operacion"
        ),

    )

    return cambios.filter(

        F.col("operacion") != "SIN_CAMBIOS"

    )


# ============================================================
# 10. CONTEOS DE CAMBIOS EFECTIVOS
# ============================================================

def obtener_conteos_cambios(
    cambios,
):

    conteos = {
        "INSERT": 0,
        "UPDATE": 0,
        "DELETE": 0,
    }

    filas = (
        cambios.groupBy("operacion")
        .count()
        .collect()
    )

    for fila in filas:

        operacion = fila["operacion"]

        exigir(
            operacion in conteos,
            f"Operacion desconocida: {operacion}",
        )

        conteos[operacion] = fila["count"]

    return conteos


def validar_conteos_cambios(
    conteos,
    dataset,
):

    esperados = {

        "INSERT": dataset["registros_nuevos"],

        "UPDATE": dataset["registros_corregidos"],

        "DELETE": dataset["registros_eliminados"],

    }

    exigir(
        conteos == esperados,
        "Los cambios efectivos no coinciden "
        f"con el manifest. "
        f"Esperados={esperados}, reales={conteos}",
    )

    total = sum(
        conteos.values()
    )

    exigir(
        total <= MAX_CAMBIOS_DETALLE,
        "Se supera el limite de detalle "
        "soportado por esta implementacion.",
    )

    return total


# ============================================================
# 11. SERIALIZACION DE CAMBIOS
# ============================================================

def obtener_detalle_cambios(
    cambios,
):

    detalle = []

    filas = (
        cambios.orderBy(
            "fecha",
            "operacion",
        )
        .collect()
    )

    for fila in filas:

        item = fila.asDict()

        for campo in [
            "fecha",
            "fecha_anterior",
            "fecha_nueva",
        ]:

            item[campo] = serializar_fecha(
                item[campo]
            )

        detalle.append(item)

    return detalle


def obtener_claves_efectivas(
    detalle,
):

    resultado = []

    for cambio in detalle:

        anterior = (

            {
                "fecha": cambio["fecha_anterior"]
            }

            if cambio["fecha_anterior"] is not None

            else None

        )

        nueva = (

            {
                "fecha": cambio["fecha_nueva"]
            }

            if cambio["fecha_nueva"] is not None

            else None

        )

        resultado.append({

            "operacion": cambio["operacion"],

            "clave_anterior": anterior,

            "clave_nueva": nueva,

        })

    return resultado


def obtener_particiones_efectivas(
    detalle,
):

    particiones = set()

    for cambio in detalle:

        for sufijo in [
            "anterior",
            "nuevo",
        ]:

            anio = cambio[
                f"anio_{sufijo}"
            ]

            mes = cambio[
                f"mes_{sufijo}"
            ]

            if anio is None and mes is None:

                continue

            exigir(
                anio is not None
                and mes is not None,
                "Particion efectiva incompleta.",
            )

            particiones.add(
                (anio, mes)
            )

    return [

        {
            "anio": anio,
            "mes": mes,
        }

        for anio, mes in sorted(particiones)

    ]


# ============================================================
# 12. CONTRASTAR MANIFEST Y CAMBIOS REALES
# ============================================================

def validar_claves_efectivas(
    claves,
    dataset,
):

    declaradas = (
        dataset["claves_afectadas"] or []
    )

    esperadas = Counter(

        json_canonico(item)

        for item in declaradas

    )

    reales = Counter(

        json_canonico(item)

        for item in claves

    )

    exigir(
        esperadas == reales,
        "Las claves efectivas no coinciden "
        "con las declaradas en el manifest.",
    )


def validar_particiones_efectivas(
    particiones,
    dataset,
):

    declaradas = dataset[
        "particiones_afectadas"
    ]

    esperadas = Counter(

        json_canonico(item)

        for item in declaradas

    )

    reales = Counter(

        json_canonico(item)

        for item in particiones

    )

    exigir(
        esperadas == reales,
        "Las particiones efectivas no coinciden "
        "con las declaradas.",
    )


def validar_balance_registros(
    metricas_anterior,
    metricas_publicada,
    conteos,
    dataset,
):

    antes = metricas_anterior[
        "total_registros"
    ]

    despues = metricas_publicada[
        "total_registros"
    ]

    exigir(
        antes == dataset["registros_antes"],
        "Conteo anterior incompatible con el manifest.",
    )

    exigir(
        despues == dataset["registros_despues"],
        "Conteo posterior incompatible con el manifest.",
    )

    esperado = (

        antes

        + conteos["INSERT"]

        - conteos["DELETE"]

    )

    exigir(
        despues == esperado,
        "Balance de registros incompatible.",
    )

    if conteos["DELETE"] > 0:

        exigir(
            dataset["autorizacion_eliminaciones"]
            is True,
            "Se detectaron eliminaciones no autorizadas.",
        )

    return {

        "registros_antes": antes,

        "registros_despues": despues,

        "registros_despues_esperados": esperado,

        "balance_correcto": True,

    }


# ============================================================
# 13. VALIDACION OPERATIVA DE TIPO CAMBIO
# ============================================================

def validar_tipo_cambio(
    spark,
    dataset,
    resolucion,
):

    version_anterior = resolucion[
        "version_anterior"
    ]

    version_publicada = resolucion[
        "version_publicada"
    ]

    anterior = None

    publicada = None

    cambios = None

    try:

        imprimir_seccion(
            "LECTURA DE VERSIONES TIPO_CAMBIO"
        )

        anterior = leer_version(
            spark,
            version_anterior,
        ).persist()

        publicada = leer_version(
            spark,
            version_publicada,
        ).persist()

        imprimir_seccion(
            "VALIDACION DE ESQUEMAS FISICOS"
        )

        esquema_anterior = validar_esquema_tipo_cambio(
            anterior,
            version_anterior,
        )

        esquema_publicada = validar_esquema_tipo_cambio(
            publicada,
            version_publicada,
        )

        exigir(
            esquema_anterior == esquema_publicada,
            "Los esquemas fisicos no coinciden.",
        )

        print(
            "OK: esquemas fisicos compatibles."
        )

        imprimir_seccion(
            "VALIDACION DE METRICAS FISICAS"
        )

        metricas_anterior = obtener_metricas_tipo_cambio(
            anterior
        )

        metricas_publicada = obtener_metricas_tipo_cambio(
            publicada
        )

        validar_metricas_tipo_cambio(
            metricas_anterior,
            version_anterior,
        )

        validar_metricas_tipo_cambio(
            metricas_publicada,
            version_publicada,
        )

        print("Version anterior:")

        print(
            json.dumps(
                metricas_anterior,
                indent=4,
                ensure_ascii=False,
            )
        )

        print("Version publicada:")

        print(
            json.dumps(
                metricas_publicada,
                indent=4,
                ensure_ascii=False,
            )
        )

        print(
            "OK: conteos y claves verificados."
        )

        imprimir_seccion(
            "DETECCION DE CAMBIOS EFECTIVOS"
        )

        cambios = detectar_cambios_tipo_cambio(
            anterior,
            publicada,
        ).persist()

        conteos = obtener_conteos_cambios(
            cambios
        )

        total_cambios = validar_conteos_cambios(
            conteos,
            dataset,
        )

        detalle = obtener_detalle_cambios(
            cambios
        )

        exigir(
            len(detalle) == total_cambios,
            "El detalle de cambios esta incompleto.",
        )

        claves = obtener_claves_efectivas(
            detalle
        )

        particiones = obtener_particiones_efectivas(
            detalle
        )

        validar_claves_efectivas(
            claves,
            dataset,
        )

        validar_particiones_efectivas(
            particiones,
            dataset,
        )

        balance = validar_balance_registros(
            metricas_anterior,
            metricas_publicada,
            conteos,
            dataset,
        )

        print("Conteos efectivos:")

        print(
            json.dumps(
                conteos,
                indent=4,
                ensure_ascii=False,
            )
        )

        print("Detalle de cambios:")

        print(
            json.dumps(
                detalle,
                indent=4,
                ensure_ascii=False,
                allow_nan=False,
            )
        )

        print(
            "OK: claves efectivas coinciden."
        )

        print(
            "OK: particiones efectivas coinciden."
        )

        print(
            "OK: balance de registros correcto."
        )

        imprimir_seccion(
            "VERIFICACION FINAL DE INTEGRIDAD"
        )

        integridad_anterior = (
            verificar_integridad_version(
                version_anterior
            )
        )

        integridad_publicada = (
            verificar_integridad_version(
                version_publicada
            )
        )

        print(
            "OK: integridad anterior conservada."
        )

        print(
            "OK: integridad publicada conservada."
        )

        return {

            "dataset": "tipo_cambio",

            "estado": (
                "DATASET_VALIDACION_OPERATIVA_APROBADA"
            ),

            "version_anterior": (
                version_anterior["version"]
            ),

            "version_publicada": (
                version_publicada["version"]
            ),

            "version_esquema": (
                version_publicada["version_esquema"]
            ),

            "ruta_anterior": (
                version_anterior["ruta_silver"]
            ),

            "ruta_publicada": (
                version_publicada["ruta_silver"]
            ),

            "clave_negocio": CLAVE_TIPO_CAMBIO,

            "columnas_particion": (
                PARTICIONES_TIPO_CAMBIO
            ),

            "esquema_fisico": esquema_publicada,

            "metricas_anterior": (
                metricas_anterior
            ),

            "metricas_publicada": (
                metricas_publicada
            ),

            "conteos_cambios": conteos,

            "total_cambios_efectivos": (
                total_cambios
            ),

            "detalle_cambios": detalle,

            "claves_efectivas": claves,

            "particiones_efectivas": particiones,

            "balance_registros": balance,

            "integridad_anterior": (
                integridad_anterior
            ),

            "integridad_publicada": (
                integridad_publicada
            ),

            "esquema_fisico_verificado": True,

            "conteos_fisicos_verificados": True,

            "claves_negocio_verificadas": True,

            "particiones_verificadas": True,

            "cambios_efectivos_verificados": True,

            "integridad_verificada": True,

            "validacion_operativa_aprobada": True,

            "alcance_gold_resuelto": False,

            "autorizacion_ejecucion_gold": False,

        }

    finally:

        if cambios is not None:

            cambios.unpersist()

        if anterior is not None:

            anterior.unpersist()

        if publicada is not None:

            publicada.unpersist()


# ============================================================
# 14. VALIDACION OPERATIVA GENERAL
# ============================================================

def validar_operativamente(

    ruta_manifest,

    ruta_registro,

    modo_laboratorio=False,

):

    imprimir_seccion(
        "CARGA DEL MANIFEST Y REGISTRO"
    )

    manifest, huella_manifest = cargar_manifest(
        ruta_manifest
    )

    registro, huella_registro = cargar_registro(
        ruta_registro
    )

    estado_original = manifest.get(
        "estado"
    )

    aprobacion_original = manifest.get(
        "validacion_aprobada"
    )

    print(
        f"Run ID: {manifest.get('run_id')}"
    )

    print(
        f"Estado Silver original: {estado_original}"
    )

    print(
        f"Modo laboratorio: {modo_laboratorio}"
    )

    print(
        f"SHA-256 manifest: {huella_manifest}"
    )

    print(
        f"SHA-256 registro: {huella_registro}"
    )

    imprimir_seccion(
        "RESOLUCION DE VERSIONES REGISTRADAS"
    )

    proyeccion, resolucion = preparar_validacion(
        manifest,
        huella_manifest,
        registro,
        modo_laboratorio,
    )

    print(
        "OK: versiones Silver resueltas."
    )

    print(
        "OK: integridad previa verificada."
    )

    if modo_laboratorio:

        print(
            "OK: entrega de laboratorio identificada."
        )

        print(
            "El manifest original no sera modificado."
        )

    spark = crear_spark()

    resultados = []

    try:

        for dataset, item in zip(

            proyeccion["datasets"],

            resolucion["datasets"],

        ):

            nombre = dataset["dataset"]

            if nombre == "tipo_cambio":

                resultado = validar_tipo_cambio(
                    spark,
                    dataset,
                    item,
                )

            else:

                raise OperationalValidationError(
                    f"Dataset no soportado: {nombre}"
                )

            resultados.append(
                resultado
            )

        exigir(
            len(resultados)
            == len(proyeccion["datasets"]),
            "No se validaron todos los datasets.",
        )

    finally:

        spark.stop()

    # Comprobamos que los archivos de control
    # tampoco hayan cambiado durante la ejecucion.

    _, huella_manifest_final = cargar_manifest(
        ruta_manifest
    )

    _, huella_registro_final = cargar_registro(
        ruta_registro
    )

    exigir(
        huella_manifest_final == huella_manifest,
        "El manifest cambio durante la validacion.",
    )

    exigir(
        huella_registro_final == huella_registro,
        "El registro cambio durante la validacion.",
    )

    total_cambios = sum(

        item["total_cambios_efectivos"]

        for item in resultados

    )

    reporte = {

        "version_validador": VERSION_VALIDADOR,

        "estado": (
            "VALIDACION_OPERATIVA_PRUEBA_APROBADA"
        ),

        "entorno": "PRUEBA",

        "fecha_validacion": fecha_actual_utc(),

        "run_id": manifest["run_id"],

        "version_manifest": (
            manifest["version_manifest"]
        ),

        "modo_laboratorio": modo_laboratorio,

        "estado_silver_original": estado_original,

        "validacion_silver_original": (
            aprobacion_original
        ),

        "manifest_original_conservado": True,

        "registro_original_conservado": True,

        "ruta_manifest": str(
            Path(ruta_manifest).absolute()
        ),

        "huella_manifest_sha256": huella_manifest,

        "ruta_registro": str(
            Path(ruta_registro).absolute()
        ),

        "huella_registro_sha256": huella_registro,

        "versiones_resueltas": True,

        "integridad_verificada": True,

        "validacion_estructura_normalizada_aprobada": True,

        "validacion_estructural_original_aprobada": (
            not modo_laboratorio
        ),

        "esquemas_fisicos_verificados": True,

        "conteos_fisicos_verificados": True,

        "claves_negocio_verificadas": True,

        "particiones_verificadas": True,

        "cambios_efectivos_verificados": True,

        "validacion_operativa_aprobada": True,

        "cantidad_datasets_validados": len(
            resultados
        ),

        "total_cambios_efectivos": total_cambios,

        "datasets": resultados,

        "publicacion_productiva": False,

        "alcance_gold_resuelto": False,

        "autorizacion_ejecucion_gold": False,

        "autorizacion_publicacion_gold": False,

    }

    return reporte


# ============================================================
# 15. RESUMEN DE RESULTADOS
# ============================================================

def imprimir_resumen(reporte):

    imprimir_seccion(
        "RESULTADO FINAL"
    )

    print(
        "OK: versiones registradas resueltas."
    )

    print(
        "OK: integridad Parquet verificada."
    )

    print(
        "OK: esquemas fisicos verificados."
    )

    print(
        "OK: conteos fisicos verificados."
    )

    print(
        "OK: claves de negocio verificadas."
    )

    print(
        "OK: cambios efectivos verificados."
    )

    print(
        "OK: particiones afectadas verificadas."
    )

    print(
        "OK: balance de registros verificado."
    )

    print(
        "OK: manifest original conservado."
    )

    print(
        f"Total cambios efectivos: "
        f"{reporte['total_cambios_efectivos']}"
    )

    print(
        "\nESTADO: "
        + reporte["estado"]
    )

    print(
        "Alcance efectivo Gold pendiente."
    )

    print(
        "No se autoriza ejecutar ni publicar Gold."
    )


# ============================================================
# 16. INTERFAZ DE LINEA DE COMANDOS
# ============================================================

def main():

    parser = argparse.ArgumentParser(

        description=(

            "Validar operativamente versiones Silver "
            "para procesamiento incremental Gold."

        )

    )

    parser.add_argument(

        "--manifest",

        required=True,

        help="Ruta del manifest Silver.",

    )

    parser.add_argument(

        "--registro",

        default=str(RUTA_REGISTRO),

        help="Ruta del registro de versiones.",

    )

    parser.add_argument(

        "--laboratorio-daniela",

        action="store_true",

        help=(

            "Validar exclusivamente la entrega "
            "experimental identificada de Daniela. "
            "No autoriza Gold."

        ),

    )

    parser.add_argument(

        "--salida",

        help="Ruta del reporte JSON de validacion.",

    )

    argumentos = parser.parse_args()

    try:

        if argumentos.salida:

            ruta_salida = Path(
                argumentos.salida
            )

            exigir(
                not ruta_salida.exists()
                and not ruta_salida.is_symlink(),
                f"El reporte ya existe: {ruta_salida}",
            )

        reporte = validar_operativamente(

            argumentos.manifest,

            argumentos.registro,

            modo_laboratorio=(
                argumentos.laboratorio_daniela
            ),

        )

        if argumentos.salida:

            ruta = guardar_reporte_nuevo(
                reporte,
                argumentos.salida,
            )

            print(
                f"\nReporte generado: {ruta}"
            )

        else:

            print(
                json.dumps(
                    reporte,
                    indent=4,
                    ensure_ascii=False,
                    allow_nan=False,
                )
            )

        imprimir_resumen(
            reporte
        )

        return 0

    except Exception as exc:

        error = {

            "estado": (
                "VALIDACION_OPERATIVA_PRUEBA_RECHAZADA"
            ),

            "entorno": "PRUEBA",

            "fecha_validacion": fecha_actual_utc(),

            "validacion_operativa_aprobada": False,

            "autorizacion_ejecucion_gold": False,

            "autorizacion_publicacion_gold": False,

            "error": str(exc),

        }

        print(

            json.dumps(
                error,
                indent=4,
                ensure_ascii=True,
                allow_nan=False,
            ),

            file=sys.stderr,

        )

        return 1


if __name__ == "__main__":

    sys.exit(
        main()
    )
