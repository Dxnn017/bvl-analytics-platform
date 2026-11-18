#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics
Resolvedor de alcance efectivo Gold.

Primera implementacion incremental BCRP.

Determina:
- Fechas Silver afectadas.
- Claves Gold de mercado afectadas.
- Particiones Gold afectadas.
- Dependencias indirectas sobre impacto de hechos.
- Productos que no requieren recalculo.

No modifica Silver.
No modifica Gold historico.
No ejecuta ni publica Gold.
"""

import argparse
import hashlib
import json
import sys

from datetime import date, datetime, timezone
from pathlib import Path

if __package__:

    from .storage_config import (
        DIRECTORIO_ENTREGAS,
        DIRECTORIO_PRUEBAS,
        RAIZ_REPORTES,
        SILVER_ORIGINAL,
        GOLD_HISTORICO,
        RUTA_REGISTRO_INTEGRACION,
    )

else:

    from storage_config import (
        DIRECTORIO_ENTREGAS,
        DIRECTORIO_PRUEBAS,
        RAIZ_REPORTES,
        SILVER_ORIGINAL,
        GOLD_HISTORICO,
        RUTA_REGISTRO_INTEGRACION,
    )

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
)

if __package__:

    from .version_registry import (
        cargar_registro,
        obtener_version,
        verificar_integridad_version,
        construir_integridad,
    )

else:

    from version_registry import (
        cargar_registro,
        obtener_version,
        verificar_integridad_version,
        construir_integridad,
    )


# ============================================================
# 1. CONFIGURACION
# ============================================================

VERSION_RESOLVEDOR = "0.1"

RAIZ_ENTREGAS = DIRECTORIO_ENTREGAS

RAIZ_PRUEBAS = DIRECTORIO_PRUEBAS

RUTA_REGISTRO = RUTA_REGISTRO_INTEGRACION

PRODUCTOS_GOLD = {

    "mercado_valor_diario": (
        "mercado/mercado_valor_diario"
    ),

    "actividad_sab_diaria": (
        "mercado/actividad_sab_diaria"
    ),

    "empresa_trimestre": (
        "empresas/empresa_trimestre"
    ),

    "hechos_empresa": (
        "eventos/hechos_empresa"
    ),

    "impacto_hechos_valor": (
        "eventos/impacto_hechos_valor"
    ),

}

FUENTES_SILVER = {

    "cotizaciones": (
        "mercado_diario/cotizaciones"
    ),

    "embig": (
        "macroeconomia/embig"
    ),

    "puente_ruc_nemonico": (
        "referencias_smv/puente_ruc_nemonico"
    ),

}

CONTEOS_GOLD_ESPERADOS = {

    "mercado_valor_diario": 83130,

    "hechos_empresa": 346,

    "impacto_hechos_valor": 175,

}

VENTANA_DIAS = 7

MAX_CLAVES_MERCADO = 100000

MAX_CLAVES_IMPACTO = 100000

ESCENARIOS = {

    "insercion": {

        "reporte": (
            "validacion_operativa_bcrp_insercion.json"
        ),

        "run_id": (
            "prueba_bcrp_insercion_001"
        ),

        "modo_laboratorio": False,

    },

    "correccion": {

        "reporte": (
            "validacion_operativa_bcrp_correccion.json"
        ),

        "run_id": (
            "prueba_bcrp_correccion_001"
        ),

        "modo_laboratorio": False,

    },

    "daniela": {

        "reporte": (
            "validacion_operativa_bcrp_daniela.json"
        ),

        "run_id": (
            "20261116T003053Z_78127719"
        ),

        "modo_laboratorio": True,

    },

}


# ============================================================
# 2. EXCEPCIONES Y UTILIDADES
# ============================================================

class ScopeResolutionError(Exception):
    """Error de resolucion de alcance Gold."""


def exigir(condicion, mensaje):

    if not condicion:

        raise ScopeResolutionError(mensaje)


def fecha_actual_utc():

    return datetime.now(
        timezone.utc
    ).isoformat()


def imprimir_seccion(titulo):

    print("\n" + "=" * 70)
    print(titulo)
    print("=" * 70)


def cargar_json(ruta):

    ruta = Path(ruta)

    exigir(
        ruta.is_file()
        and not ruta.is_symlink(),
        f"Archivo de control invalido: {ruta}",
    )

    contenido = ruta.read_bytes()

    huella = hashlib.sha256(
        contenido
    ).hexdigest()

    datos = json.loads(
        contenido.decode("utf-8")
    )

    exigir(
        isinstance(datos, dict),
        f"El JSON debe ser un objeto: {ruta}",
    )

    return datos, huella


def guardar_reporte(reporte, ruta):

    ruta = Path(ruta).absolute()

    exigir(
        ruta.parent.resolve()
        == RAIZ_REPORTES.resolve(),
        "La salida debe guardarse en reportes.",
    )

    exigir(
        ruta.suffix == ".json",
        "La salida debe ser un archivo JSON.",
    )

    exigir(
        not ruta.exists()
        and not ruta.is_symlink(),
        f"El reporte ya existe: {ruta}",
    )

    for componente in ruta.parents:

        exigir(
            not componente.is_symlink(),
            f"Ruta simbolica no permitida: {componente}",
        )

    with ruta.open(
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

        archivo.write("\n")

    return ruta


def serializar_fila(fila):

    datos = fila.asDict()

    for campo, valor in datos.items():

        if isinstance(
            valor,
            (date, datetime),
        ):

            datos[campo] = valor.isoformat()

    return datos


def serializar_dataframe(
    dataframe,
    columnas,
    limite,
):

    seleccion = dataframe.select(
        *columnas
    ).orderBy(
        *columnas
    )

    filas = seleccion.limit(
        limite + 1
    ).collect()

    exigir(
        len(filas) <= limite,
        "Se supero el limite de claves "
        "soportado por el resolvedor.",
    )

    return [
        serializar_fila(fila)
        for fila in filas
    ]


# ============================================================
# 3. VALIDAR EVIDENCIA OPERATIVA
# ============================================================

def cargar_evidencia(
    escenario,
    registro,
    huella_registro,
):

    configuracion = ESCENARIOS[escenario]

    ruta = (
        RAIZ_REPORTES
        / configuracion["reporte"]
    )

    reporte, huella = cargar_json(ruta)

    exigir(
        reporte.get("estado")
        == "VALIDACION_OPERATIVA_PRUEBA_APROBADA",
        "La validacion operativa no fue aprobada.",
    )

    exigir(
        reporte.get("entorno") == "PRUEBA",
        "Solo se permite el entorno PRUEBA.",
    )

    exigir(
        reporte.get("run_id")
        == configuracion["run_id"],
        "run_id incompatible con el escenario.",
    )

    exigir(
        reporte.get("modo_laboratorio")
        is configuracion["modo_laboratorio"],
        "Modo laboratorio incompatible.",
    )

    exigir(
        reporte.get("huella_registro_sha256")
        == huella_registro,
        "El registro no coincide con "
        "la evidencia operativa.",
    )

    exigir(
        reporte.get("validacion_operativa_aprobada")
        is True,
        "Validacion operativa pendiente.",
    )

    exigir(
        reporte.get("cambios_efectivos_verificados")
        is True,
        "Cambios efectivos no verificados.",
    )

    exigir(
        reporte.get("integridad_verificada")
        is True,
        "Integridad no verificada.",
    )

    exigir(
        reporte.get("autorizacion_ejecucion_gold")
        is False,
        "La evidencia no debe autorizar "
        "ejecucion Gold automaticamente.",
    )

    exigir(
        reporte.get("publicacion_productiva")
        is False,
        "No se admite una publicacion productiva.",
    )

    ruta_manifest = Path(
        reporte["ruta_manifest"]
    )

    manifest, huella_manifest = cargar_json(
        ruta_manifest
    )

    exigir(
        huella_manifest
        == reporte["huella_manifest_sha256"],
        "El manifest cambio despues "
        "de la validacion operativa.",
    )

    exigir(
        manifest["run_id"] == reporte["run_id"],
        "run_id incompatible con el manifest.",
    )

    exigir(
        len(reporte["datasets"]) == 1,
        "Se esperaba un unico dataset.",
    )

    dataset = reporte["datasets"][0]

    exigir(
        dataset["dataset"] == "tipo_cambio",
        "Dataset todavia no soportado.",
    )

    exigir(
        dataset["validacion_operativa_aprobada"]
        is True,
        "El dataset no fue aprobado.",
    )

    exigir(
        len(manifest["datasets"]) == 1,
        "Manifest incompatible.",
    )

    declarado = manifest["datasets"][0]

    exigir(
        declarado["dataset"] == "tipo_cambio",
        "Dataset del manifest incompatible.",
    )

    exigir(
        declarado["version_anterior"]
        == dataset["version_anterior"],
        "Version anterior incompatible.",
    )

    exigir(
        declarado["version_publicada"]
        == dataset["version_publicada"],
        "Version publicada incompatible.",
    )

    anterior = obtener_version(
        registro,
        "tipo_cambio",
        dataset["version_anterior"],
    )

    publicada = obtener_version(
        registro,
        "tipo_cambio",
        dataset["version_publicada"],
    )

    original = obtener_version(
        registro,
        "tipo_cambio",
        "snapshot_20261109",
    )

    exigir(
        anterior["integridad"]
        == original["integridad"],
        "La version anterior no coincide "
        "con la base Silver historica.",
    )

    exigir(
        dataset["ruta_anterior"]
        == anterior["ruta_silver"],
        "Ruta anterior incompatible.",
    )

    exigir(
        dataset["ruta_publicada"]
        == publicada["ruta_silver"],
        "Ruta publicada incompatible.",
    )

    verificar_integridad_version(anterior)

    verificar_integridad_version(publicada)

    verificar_integridad_version(original)

    return {

        "ruta_reporte": str(ruta),

        "huella_reporte_sha256": huella,

        "reporte": reporte,

        "dataset": dataset,

        "version_anterior": anterior,

        "version_publicada": publicada,

    }


# ============================================================
# 4. RESOLVER FECHAS SILVER AFECTADAS
# ============================================================

def obtener_fechas_afectadas(dataset):

    fechas = set()

    cambios = dataset["detalle_cambios"]

    exigir(
        len(cambios)
        == dataset["total_cambios_efectivos"],
        "Detalle de cambios incompleto.",
    )

    for cambio in cambios:

        exigir(
            cambio["operacion"]
            in {"INSERT", "UPDATE", "DELETE"},
            "Operacion no soportada.",
        )

        for campo in [

            "fecha_anterior",

            "fecha_nueva",

        ]:

            valor = cambio.get(campo)

            if valor is not None:

                fecha = date.fromisoformat(valor)

                fechas.add(fecha)

    return sorted(fechas)


def construir_dataframe_fechas(
    spark,
    fechas,
):

    esquema = StructType([

        StructField(
            "fecha",
            DateType(),
            False,
        ),

    ])

    return spark.createDataFrame(

        [(fecha,) for fecha in fechas],

        schema=esquema,

    )


# ============================================================
# 5. CONFIGURACION SPARK
# ============================================================

def crear_spark():

    spark = (
        SparkSession.builder
        .appName(
            "BVL-Resolucion-Alcance-Gold"
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
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
# 6. INVENTARIO DE FUENTES
# ============================================================

def inventariar_dataset(
    nombre,
    ruta,
):

    ruta = Path(ruta)

    exigir(
        ruta.is_dir()
        and not ruta.is_symlink(),
        f"Dataset no disponible: {ruta}",
    )

    exigir(
        (ruta / "_SUCCESS").is_file(),
        f"Dataset sin marcador _SUCCESS: {ruta}",
    )

    integridad = construir_integridad(ruta)

    return {

        "dataset": nombre,

        "ruta": ruta.as_uri(),

        "cantidad_archivos_parquet": (
            integridad["cantidad_archivos_parquet"]
        ),

        "total_bytes_parquet": (
            integridad["total_bytes_parquet"]
        ),

        "huella_dataset_sha256": (
            integridad["huella_dataset_sha256"]
        ),

    }


def inventariar_gold():

    resultado = {}

    for producto, relativa in PRODUCTOS_GOLD.items():

        resultado[producto] = inventariar_dataset(

            producto,

            GOLD_HISTORICO / relativa,

        )

    return resultado


def inventariar_fuentes_silver():

    resultado = {}

    for dataset, relativa in FUENTES_SILVER.items():

        resultado[dataset] = inventariar_dataset(

            dataset,

            SILVER_ORIGINAL / relativa,

        )

    return resultado


# ============================================================
# 7. VALIDACIONES GOLD BASE
# ============================================================

def validar_clave(
    dataframe,
    claves,
    nombre,
):

    condicion = F.lit(False)

    for clave in claves:

        condicion = condicion | (

            F.col(clave).isNull()

            |

            (
                F.trim(
                    F.col(clave).cast("string")
                ) == ""
            )

        )

    invalidos = dataframe.filter(
        condicion
    ).count()

    duplicados = (

        dataframe.groupBy(*claves)

        .count()

        .filter(F.col("count") > 1)

        .count()

    )

    exigir(
        invalidos == 0,
        f"Claves invalidas en {nombre}: {invalidos}",
    )

    exigir(
        duplicados == 0,
        f"Claves duplicadas en {nombre}: {duplicados}",
    )


def validar_conteo_gold(
    dataframe,
    producto,
):

    real = dataframe.count()

    esperado = CONTEOS_GOLD_ESPERADOS[
        producto
    ]

    exigir(
        real == esperado,
        f"Conteo incompatible en {producto}: "
        f"esperado={esperado}, real={real}",
    )

    return real


# ============================================================
# 8. ALCANCE DIRECTO MERCADO
# ============================================================

def resolver_mercado(
    cotizaciones,
    mercado,
    fechas,
):

    claves_silver = (

        cotizaciones.select(

            F.col("valor").alias("nemonico"),

            F.col("fecha_cotizacion").alias(
                "fecha"
            ),

        )

        .join(
            fechas,
            "fecha",
            "inner",
        )

    )

    validar_clave(

        claves_silver,

        ["nemonico", "fecha"],

        "cotizaciones afectadas",

    )

    claves_gold = (

        mercado.select(
            "nemonico",
            "fecha",
        )

        .join(
            fechas,
            "fecha",
            "inner",
        )

    )

    validar_clave(

        claves_gold,

        ["nemonico", "fecha"],

        "mercado afectado",

    )

    faltantes = claves_silver.join(

        claves_gold,

        ["nemonico", "fecha"],

        "left_anti",

    ).count()

    adicionales = claves_gold.join(

        claves_silver,

        ["nemonico", "fecha"],

        "left_anti",

    ).count()

    exigir(
        faltantes == 0
        and adicionales == 0,
        "Las claves Gold base no coinciden "
        "con las cotizaciones Silver afectadas.",
    )

    return claves_silver


def obtener_particiones_mercado(
    claves,
):

    return (

        claves.select(

            F.year("fecha").alias("anio"),

            F.month("fecha").alias("mes"),

        )

        .distinct()

    )


# ============================================================
# 9. ALCANCE INDIRECTO IMPACTO
# ============================================================

def resolver_impacto(
    hechos,
    mercado,
    claves_mercado,
):

    candidatos = (

        hechos.filter(

            F.col("candidato_impacto_bursatil")

        )

        .select(

            "numero_expediente",

            "fecha_hecho",

            F.explode(
                "nemonicos_cotizables"
            ).alias("nemonico"),

        )

    )

    validar_clave(

        candidatos,

        ["numero_expediente", "nemonico"],

        "candidatos impacto",

    )

    precio = F.col("precio_cierre")

    cotizaciones_afectadas = (

        mercado.join(

            claves_mercado,

            ["nemonico", "fecha"],

            "inner",

        )

        .filter(

            F.col("fecha").isNotNull()

            & precio.isNotNull()

            & ~F.isnan(
                precio.cast("double")
            )

            & (
                F.abs(precio.cast("double"))
                != float("inf")
            )

            & (precio > 0)

        )

        .select(

            "nemonico",

            F.col("fecha").alias(
                "fecha_cotizacion"
            ),

        )

    )

    candidatos = candidatos.alias("e")

    afectadas = cotizaciones_afectadas.alias("m")

    diferencia_dias = F.abs(

        F.datediff(

            F.col("m.fecha_cotizacion"),

            F.col("e.fecha_hecho"),

        )

    )

    coincidencias = candidatos.join(

        afectadas,

        (

            F.col("e.nemonico")
            == F.col("m.nemonico")

        )

        & (diferencia_dias >= 1)

        & (diferencia_dias <= VENTANA_DIAS),

        "inner",

    )

    return (

        coincidencias.select(

            F.col("e.numero_expediente").alias(
                "numero_expediente"
            ),

            F.col("e.nemonico").alias(
                "nemonico"
            ),

            F.col("e.fecha_hecho").alias(
                "fecha_hecho"
            ),

        )

        .distinct()

    )


def obtener_particiones_impacto(
    claves,
):

    return (

        claves.select(

            F.year("fecha_hecho").alias("anio")

        )

        .distinct()

    )


# ============================================================
# 10. CONFIRMAR CLAVES DE IMPACTO
# ============================================================

def validar_impacto_base(
    claves,
    impacto,
):

    faltantes = claves.select(

        "numero_expediente",

        "nemonico",

    ).join(

        impacto.select(

            "numero_expediente",

            "nemonico",

        ),

        ["numero_expediente", "nemonico"],

        "left_anti",

    ).count()

    exigir(
        faltantes == 0,
        "Existen candidatos afectados ausentes "
        "en impacto Gold historico.",
    )


# ============================================================
# 11. CONSTRUIR TAREA DE RECALCULO
# ============================================================

def construir_tarea(
    producto,
    claves,
    particiones,
    granularidad,
    tipo_dependencia,
):

    cantidad = len(claves)

    if cantidad > 0:

        estado = (
            "ALCANCE_CANDIDATO_RESUELTO"
        )

    else:

        estado = (
            "SIN_REGISTROS_AFECTADOS"
        )

    return {

        "producto_gold": producto,

        "ruta_gold_relativa": (
            PRODUCTOS_GOLD[producto]
        ),

        "tipo_dependencia": tipo_dependencia,

        "granularidad": granularidad,

        "estado": estado,

        "requiere_recalculo": cantidad > 0,

        "cantidad_claves_candidatas": cantidad,

        "cantidad_registros_gold_afectados": (
            cantidad
        ),

        "claves_gold_afectadas": claves,

        "particiones_gold_afectadas": particiones,

        "requiere_verificar_cambios_efectivos": (
            cantidad > 0
        ),

        "autorizacion_ejecucion_gold": False,

    }


# ============================================================
# 12. RESOLUCION COMPLETA DEL ESCENARIO
# ============================================================

def resolver_escenario(
    spark,
    escenario,
    registro,
    huella_registro,
    inventario_gold,
    inventario_silver,
    cotizaciones,
    mercado,
    hechos,
    impacto,
):

    imprimir_seccion(

        "ESCENARIO: "
        + escenario.upper()

    )

    evidencia = cargar_evidencia(

        escenario,

        registro,

        huella_registro,

    )

    dataset = evidencia["dataset"]

    fechas = obtener_fechas_afectadas(
        dataset
    )

    print(

        "Fechas Silver afectadas:",

        [
            fecha.isoformat()
            for fecha in fechas
        ],

    )

    fechas_df = construir_dataframe_fechas(

        spark,

        fechas,

    )

    claves_mercado = resolver_mercado(

        cotizaciones,

        mercado,

        fechas_df,

    ).persist()

    claves_impacto = None

    try:

        detalle_mercado = serializar_dataframe(

            claves_mercado,

            ["nemonico", "fecha"],

            MAX_CLAVES_MERCADO,

        )

        particiones_mercado = (

            serializar_dataframe(

                obtener_particiones_mercado(
                    claves_mercado
                ),

                ["anio", "mes"],

                MAX_CLAVES_MERCADO,

            )

        )

        imprimir_seccion(
            "DEPENDENCIA DIRECTA: MERCADO"
        )

        print(

            "Claves candidatas:",

            len(detalle_mercado),

        )

        print(

            "Particiones candidatas:",

            particiones_mercado,

        )

        claves_impacto = resolver_impacto(

            hechos,

            mercado,

            claves_mercado,

        ).persist()

        validar_impacto_base(

            claves_impacto,

            impacto,

        )

        detalle_impacto = serializar_dataframe(

            claves_impacto,

            [
                "numero_expediente",
                "nemonico",
                "fecha_hecho",
            ],

            MAX_CLAVES_IMPACTO,

        )

        particiones_impacto = (

            serializar_dataframe(

                obtener_particiones_impacto(
                    claves_impacto
                ),

                ["anio"],

                MAX_CLAVES_IMPACTO,

            )

        )

        imprimir_seccion(

            "DEPENDENCIA INDIRECTA: IMPACTO"

        )

        print(

            "Pares candidatos:",

            len(detalle_impacto),

        )

        print(

            "Particiones candidatas:",

            particiones_impacto,

        )

        tarea_mercado = construir_tarea(

            "mercado_valor_diario",

            detalle_mercado,

            particiones_mercado,

            ["nemonico", "fecha"],

            "DIRECTA",

        )

        tarea_impacto = construir_tarea(

            "impacto_hechos_valor",

            detalle_impacto,

            particiones_impacto,

            [
                "numero_expediente",
                "nemonico",
            ],

            "INDIRECTA",

        )

        tareas = [

            tarea_mercado,

            tarea_impacto,

        ]

        productos_con_recalculo = [

            tarea["producto_gold"]

            for tarea in tareas

            if tarea["requiere_recalculo"]

        ]

        productos_sin_recalculo = [

            producto

            for producto in PRODUCTOS_GOLD

            if producto not in productos_con_recalculo

        ]

        if productos_con_recalculo:

            estado = (

                "GOLD_ALCANCE_CANDIDATO_RESUELTO"

            )

        else:

            estado = (

                "GOLD_SIN_REGISTROS_AFECTADOS"

            )

        verificar_integridad_version(

            evidencia["version_anterior"]

        )

        verificar_integridad_version(

            evidencia["version_publicada"]

        )

        _, huella_evidencia_final = cargar_json(

            evidencia["ruta_reporte"]

        )

        exigir(

            huella_evidencia_final

            == evidencia["huella_reporte_sha256"],

            "La evidencia operativa cambio.",

        )

        reporte = {

            "version_resolvedor": (
                VERSION_RESOLVEDOR
            ),

            "estado": estado,

            "entorno": "PRUEBA",

            "fecha_resolucion": (
                fecha_actual_utc()
            ),

            "escenario": escenario,

            "run_id": (
                evidencia["reporte"]["run_id"]
            ),

            "modo_laboratorio": (
                evidencia["reporte"]["modo_laboratorio"]
            ),

            "estado_silver_original": (

                evidencia["reporte"][
                    "estado_silver_original"
                ]

            ),

            "validacion_operativa_aprobada": True,

            "ruta_evidencia_operativa": (

                evidencia["ruta_reporte"]

            ),

            "huella_evidencia_operativa_sha256": (

                evidencia["huella_reporte_sha256"]

            ),

            "huella_manifest_sha256": (

                evidencia["reporte"][
                    "huella_manifest_sha256"
                ]

            ),

            "ruta_registro": str(
                RUTA_REGISTRO
            ),

            "huella_registro_sha256": (
                huella_registro
            ),

            "dataset_silver": "tipo_cambio",

            "version_silver_anterior": (

                dataset["version_anterior"]

            ),

            "version_silver_publicada": (

                dataset["version_publicada"]

            ),

            "ruta_silver_anterior": (

                dataset["ruta_anterior"]

            ),

            "ruta_silver_publicada": (

                dataset["ruta_publicada"]

            ),

            "fechas_silver_afectadas": [

                fecha.isoformat()

                for fecha in fechas

            ],

            "total_cambios_silver": (

                dataset["total_cambios_efectivos"]

            ),

            "detalle_cambios_silver": (

                dataset["detalle_cambios"]

            ),

            "version_gold_base": (
                "gold_historico_20261114"
            ),

            "ruta_gold_base": (
                GOLD_HISTORICO.as_uri()
            ),

            "inventario_gold_base": (
                inventario_gold
            ),

            "fuentes_silver_fijadas": (
                inventario_silver
            ),

            "ventana_impacto_dias": (
                VENTANA_DIAS
            ),

            "excluir_dia_hecho": True,

            "productos_con_recalculo": (

                productos_con_recalculo

            ),

            "productos_sin_recalculo": (

                productos_sin_recalculo

            ),

            "cantidad_claves_mercado": (

                len(detalle_mercado)

            ),

            "cantidad_claves_impacto": (

                len(detalle_impacto)

            ),

            "orden_ejecucion_propuesto": (

                productos_con_recalculo

            ),

            "tareas": tareas,

            "alcance_candidato_resuelto": True,

            "alcance_efectivo_resuelto": (

                not productos_con_recalculo

            ),

            "requiere_verificar_resultados_gold": (

                bool(productos_con_recalculo)

            ),

            "autorizacion_ejecucion_gold": False,

            "autorizacion_publicacion_gold": False,

            "publicacion_productiva": False,

        }

        print(

            "\nProductos con recalculo:",

            productos_con_recalculo,

        )

        print(

            "Productos sin recalculo:",

            productos_sin_recalculo,

        )

        print(

            "ESTADO:", estado

        )

        return reporte

    finally:

        claves_mercado.unpersist()

        if claves_impacto is not None:

            claves_impacto.unpersist()


# ============================================================
# 13. EJECUCION GENERAL
# ============================================================

def resolver_alcance(
    escenarios,
):

    imprimir_seccion(

        "CARGA DEL REGISTRO DE VERSIONES"

    )

    registro, huella_registro = cargar_registro(

        RUTA_REGISTRO

    )

    print(

        "SHA-256 registro:",

        huella_registro,

    )

    imprimir_seccion(

        "INVENTARIO GOLD HISTORICO"

    )

    inventario_gold = inventariar_gold()

    for producto, item in inventario_gold.items():

        print(

            producto,

            "->",

            item["cantidad_archivos_parquet"],

            "Parquet",

        )

    imprimir_seccion(

        "INVENTARIO DE FUENTES SILVER"

    )

    inventario_silver = (

        inventariar_fuentes_silver()

    )

    spark = crear_spark()

    dataframes = []

    resultados = []

    try:

        cotizaciones = spark.read.parquet(

            inventario_silver[
                "cotizaciones"
            ]["ruta"]

        ).persist()

        dataframes.append(cotizaciones)

        mercado = spark.read.parquet(

            inventario_gold[
                "mercado_valor_diario"
            ]["ruta"]

        ).persist()

        dataframes.append(mercado)

        hechos = spark.read.parquet(

            inventario_gold[
                "hechos_empresa"
            ]["ruta"]

        ).persist()

        dataframes.append(hechos)

        impacto = spark.read.parquet(

            inventario_gold[
                "impacto_hechos_valor"
            ]["ruta"]

        ).persist()

        dataframes.append(impacto)

        imprimir_seccion(

            "VALIDACION DE GOLD BASE"

        )

        validar_conteo_gold(

            mercado,

            "mercado_valor_diario",

        )

        validar_conteo_gold(

            hechos,

            "hechos_empresa",

        )

        validar_conteo_gold(

            impacto,

            "impacto_hechos_valor",

        )

        validar_clave(

            mercado,

            ["nemonico", "fecha"],

            "mercado_valor_diario",

        )

        validar_clave(

            hechos,

            ["numero_expediente"],

            "hechos_empresa",

        )

        validar_clave(

            impacto,

            ["numero_expediente", "nemonico"],

            "impacto_hechos_valor",

        )

        print(

            "OK: conteos y claves Gold base."

        )

        for escenario in escenarios:

            resultado = resolver_escenario(

                spark,

                escenario,

                registro,

                huella_registro,

                inventario_gold,

                inventario_silver,

                cotizaciones,

                mercado,

                hechos,

                impacto,

            )

            resultados.append(resultado)

    finally:

        for dataframe in dataframes:

            dataframe.unpersist()

        spark.stop()

    imprimir_seccion(

        "VERIFICACION FINAL DE INTEGRIDAD"

    )

    exigir(

        inventariar_gold() == inventario_gold,

        "Gold historico cambio durante la resolucion.",

    )

    exigir(

        inventariar_fuentes_silver()

        == inventario_silver,

        "Las fuentes Silver cambiaron.",

    )

    _, huella_registro_final = cargar_registro(

        RUTA_REGISTRO

    )

    exigir(

        huella_registro_final == huella_registro,

        "El registro de versiones fue modificado.",

    )

    print(

        "OK: Gold historico conservado."

    )

    print(

        "OK: fuentes Silver conservadas."

    )

    print(

        "OK: registro de versiones conservado."

    )

    for resultado in resultados:

        resultado["gold_historico_conservado"] = True

        resultado["fuentes_silver_conservadas"] = True

        resultado["registro_conservado"] = True

    return resultados


# ============================================================
# 14. INTERFAZ DE LINEA DE COMANDOS
# ============================================================

def main():

    parser = argparse.ArgumentParser(

        description=(

            "Resolver alcance incremental Gold "

            "para escenarios BCRP."

        )

    )

    parser.add_argument(

        "--escenario",

        choices=[

            "insercion",

            "correccion",

            "daniela",

            "todos",

        ],

        default="todos",

    )

    argumentos = parser.parse_args()

    if argumentos.escenario == "todos":

        escenarios = list(ESCENARIOS)

    else:

        escenarios = [

            argumentos.escenario

        ]

    try:

        rutas_salida = {}

        for escenario in escenarios:

            ruta = (

                RAIZ_REPORTES

                / f"alcance_gold_bcrp_{escenario}.json"

            )

            exigir(

                not ruta.exists()

                and not ruta.is_symlink(),

                f"El reporte ya existe: {ruta}",

            )

            rutas_salida[escenario] = ruta

        resultados = resolver_alcance(

            escenarios

        )

        imprimir_seccion(

            "RESUMEN GENERAL"

        )

        for resultado in resultados:

            escenario = resultado["escenario"]

            ruta = guardar_reporte(

                resultado,

                rutas_salida[escenario],

            )

            print(

                "\nEscenario:", escenario

            )

            print(

                "Claves mercado:",

                resultado["cantidad_claves_mercado"],

            )

            print(

                "Claves impacto:",

                resultado["cantidad_claves_impacto"],

            )

            print(

                "Estado:", resultado["estado"]

            )

            print(

                "Reporte:", ruta

            )

        print(

            "\nESTADO: RESOLUCION_ALCANCE_COMPLETADA"

        )

        print(

            "No se modifico Gold historico."

        )

        print(

            "No se autoriza publicar Gold."

        )

        return 0

    except Exception as exc:

        error = {

            "estado": (

                "RESOLUCION_ALCANCE_RECHAZADA"

            ),

            "entorno": "PRUEBA",

            "fecha": fecha_actual_utc(),

            "autorizacion_ejecucion_gold": False,

            "autorizacion_publicacion_gold": False,

            "error": str(exc),

        }

        print(

            json.dumps(

                error,

                indent=4,

                ensure_ascii=True,

            ),

            file=sys.stderr,

        )

        return 1


if __name__ == "__main__":

    sys.exit(main())
