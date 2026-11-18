#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics
Ejecutor incremental Gold - BCRP v0.1.

Procesamiento experimental local:
- Consume evidencias operativas y reportes de alcance.
- Reutiliza constructores Gold historicos.
- Recalcula exclusivamente claves candidatas.
- Conserva las particiones no afectadas.
- Valida equivalencia con reconstruccion completa.
- Gestiona escenarios sin cambios Gold.

No publica Gold productivo.
No modifica Silver original ni Gold historico.
"""

import argparse
import json
import shutil
import sys

from pathlib import Path

if __package__:

    from .storage_config import (
        RAIZ_CANDIDATAS,
    )

else:

    from storage_config import (
        RAIZ_CANDIDATAS,
    )

from datetime import date, datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
)

# ============================================================
# 1. IMPORTACIONES DE COMPONENTES EXISTENTES
# ============================================================

DIRECTORIO_MODULO = Path(__file__).resolve().parent

DIRECTORIO_GOLD = DIRECTORIO_MODULO.parent

sys.path.insert(
    0,
    str(DIRECTORIO_MODULO),
)

sys.path.insert(
    0,
    str(DIRECTORIO_GOLD / "mercado"),
)

sys.path.insert(
    0,
    str(DIRECTORIO_GOLD / "eventos"),
)

from scope_resolver import (
    RAIZ_PRUEBAS,
    RAIZ_REPORTES,
    GOLD_HISTORICO,
    RUTA_REGISTRO,
    PRODUCTOS_GOLD,
    ESCENARIOS,
    cargar_json,
    cargar_evidencia,
    inventariar_gold,
    inventariar_fuentes_silver,
    inventariar_dataset,
    guardar_reporte,
    validar_clave,
    crear_spark,
)

from version_registry import (
    cargar_registro,
    verificar_integridad_version,
)

from operational_validator import leer_version

from build_mercado_valor_diario import (
    construir_gold as construir_mercado,
)

from build_impacto_hechos_valor import (
    construir_gold as construir_impacto,
    preparar_candidatos,
)


# ============================================================
# 2. CONFIGURACION
# ============================================================

VERSION_EJECUTOR = "0.1"

CLAVE_MERCADO = [
    "nemonico",
    "fecha",
]

CLAVE_IMPACTO = [
    "numero_expediente",
    "nemonico",
]

COLUMNAS_MACRO_MODIFICABLES = {

    "tipo_cambio_venta",

    "tipo_cambio_disponible",

    "macro_disponible",

}


# ============================================================
# 3. UTILIDADES
# ============================================================

class IncrementalExecutionError(Exception):
    pass


def exigir(condicion, mensaje):

    if not condicion:
        raise IncrementalExecutionError(mensaje)


def imprimir_seccion(titulo):

    print("\n" + "=" * 70)
    print(titulo)
    print("=" * 70)


def fecha_actual_utc():

    return datetime.now(
        timezone.utc
    ).isoformat()


def alinear_esquema(dataframe, referencia):

    exigir(
        dict(dataframe.dtypes)
        == dict(referencia.dtypes),
        "Esquemas incompatibles para comparacion.",
    )

    return dataframe.select(
        *referencia.columns
    )


def comprobar_equivalencia(
    izquierda,
    derecha,
    nombre,
):

    izquierda = alinear_esquema(
        izquierda,
        derecha,
    )

    solo_izquierda = (
        izquierda.exceptAll(derecha).count()
    )

    solo_derecha = (
        derecha.exceptAll(izquierda).count()
    )

    print(f"\nComparacion: {nombre}")

    print(
        "Solo izquierda:",
        solo_izquierda,
    )

    print(
        "Solo derecha:",
        solo_derecha,
    )

    exigir(
        solo_izquierda == 0
        and solo_derecha == 0,
        f"Equivalencia rechazada: {nombre}",
    )

    return {

        "comparacion": nombre,

        "solo_izquierda": solo_izquierda,

        "solo_derecha": solo_derecha,

        "equivalencia_aprobada": True,

    }


# ============================================================
# 4. CARGAR REPORTE DE ALCANCE
# ============================================================

def cargar_alcance(
    escenario,
    registro,
    huella_registro,
    inventario_gold,
    inventario_silver,
):

    ruta = (

        RAIZ_REPORTES

        / f"alcance_gold_bcrp_{escenario}.json"

    )

    alcance, huella = cargar_json(ruta)

    evidencia = cargar_evidencia(

        escenario,

        registro,

        huella_registro,

    )

    exigir(
        alcance["entorno"] == "PRUEBA",
        "Entorno incompatible.",
    )

    exigir(
        alcance["escenario"] == escenario,
        "Escenario incompatible.",
    )

    exigir(
        alcance["run_id"]
        == evidencia["reporte"]["run_id"],
        "run_id incompatible.",
    )

    exigir(
        alcance["estado"] in {
            "GOLD_SIN_REGISTROS_AFECTADOS",
            "GOLD_ALCANCE_CANDIDATO_RESUELTO",
        },
        "Estado de alcance incompatible.",
    )

    exigir(
        alcance["alcance_candidato_resuelto"]
        is True,
        "Alcance candidato pendiente.",
    )

    exigir(
        alcance["validacion_operativa_aprobada"]
        is True,
        "Validacion operativa pendiente.",
    )

    exigir(
        alcance["huella_registro_sha256"]
        == huella_registro,
        "El registro fue modificado.",
    )

    exigir(
        alcance["huella_evidencia_operativa_sha256"]
        == evidencia["huella_reporte_sha256"],
        "La evidencia operativa fue modificada.",
    )

    exigir(
        alcance["huella_manifest_sha256"]
        == evidencia["reporte"]["huella_manifest_sha256"],
        "Manifest incompatible.",
    )

    exigir(
        alcance["inventario_gold_base"]
        == inventario_gold,
        "Gold base incompatible con el alcance.",
    )

    exigir(
        alcance["fuentes_silver_fijadas"]
        == inventario_silver,
        "Fuentes Silver incompatibles.",
    )

    exigir(
        alcance["version_silver_anterior"]
        == evidencia["version_anterior"]["version"],
        "Version Silver anterior incompatible.",
    )

    exigir(
        alcance["version_silver_publicada"]
        == evidencia["version_publicada"]["version"],
        "Version Silver publicada incompatible.",
    )

    exigir(
        alcance["ruta_silver_publicada"]
        == evidencia["version_publicada"]["ruta_silver"],
        "Ruta Silver incompatible.",
    )

    exigir(
        alcance["detalle_cambios_silver"]
        == evidencia["dataset"]["detalle_cambios"],
        "Detalle de cambios incompatible.",
    )

    exigir(
        alcance["publicacion_productiva"] is False,
        "Publicacion productiva bloqueada.",
    )

    exigir(
        alcance["autorizacion_publicacion_gold"]
        is False,
        "No se permite publicar Gold.",
    )

    exigir(
        alcance["autorizacion_ejecucion_gold"]
        is False,
        "El reporte no debe autorizar ejecuciones.",
    )

    # Esta primera version del ejecutor soporta
    # mercado afectado e impacto sin candidatos.
    # No ignora dependencias indirectas.

    exigir(
        alcance["cantidad_claves_impacto"] == 0,
        "Existen candidatos de impacto. "
        "Este escenario requiere ampliar el ejecutor.",
    )

    exigir(
        set(alcance["productos_con_recalculo"])
        <= {"mercado_valor_diario"},
        "Existe un producto no soportado.",
    )

    return alcance, huella, evidencia


# ============================================================
# 5. CONSTRUIR CLAVES CANDIDATAS
# ============================================================

def construir_claves_mercado(
    spark,
    alcance,
):

    tareas = [

        tarea

        for tarea in alcance["tareas"]

        if tarea["producto_gold"]
        == "mercado_valor_diario"

    ]

    exigir(
        len(tareas) == 1,
        "Se esperaba una tarea de mercado.",
    )

    tarea = tareas[0]

    claves = tarea["claves_gold_afectadas"]

    exigir(
        len(claves)
        == alcance["cantidad_claves_mercado"],
        "Cantidad de claves incompatible.",
    )

    filas = [

        (

            item["nemonico"],

            date.fromisoformat(item["fecha"]),

        )

        for item in claves

    ]

    esquema = StructType([

        StructField(
            "nemonico",
            StringType(),
            False,
        ),

        StructField(
            "fecha",
            DateType(),
            False,
        ),

    ])

    dataframe = spark.createDataFrame(

        filas,

        schema=esquema,

    )

    validar_clave(

        dataframe,

        CLAVE_MERCADO,

        "claves candidatas mercado",

    )

    return dataframe, tarea


# ============================================================
# 6. VALIDAR CLAVES CONTRA FUENTES REALES
# ============================================================

def validar_alcance_mercado(
    claves,
    cotizaciones,
    mercado_base,
    alcance,
):

    fechas = [

        date.fromisoformat(valor)

        for valor in alcance[
            "fechas_silver_afectadas"
        ]

    ]

    claves_silver = (

        cotizaciones.filter(

            F.col("fecha_cotizacion").isin(fechas)

        )

        .select(

            F.col("valor").alias("nemonico"),

            F.col("fecha_cotizacion").alias("fecha"),

        )

    )

    claves_gold = (

        mercado_base.filter(

            F.col("fecha").isin(fechas)

        )

        .select(*CLAVE_MERCADO)

    )

    comprobar_equivalencia(

        claves,

        claves_silver,

        "Claves alcance vs cotizaciones Silver",

    )

    comprobar_equivalencia(

        claves,

        claves_gold,

        "Claves alcance vs mercado historico",

    )


# ============================================================
# 7. RECALCULO INCREMENTAL DE MERCADO
# ============================================================

def recalcular_mercado(
    cotizaciones,
    tc_nuevo,
    embig,
    puente,
    mercado_base,
    claves,
):

    cotizaciones_afectadas = (

        cotizaciones.alias("c")

        .join(

            claves.alias("k"),

            (

                F.col("c.valor")
                == F.col("k.nemonico")

            )

            & (

                F.col("c.fecha_cotizacion")
                == F.col("k.fecha")

            ),

            "left_semi",

        )

    )

    # Reutilizacion del constructor historico.
    # Solo recibe las cotizaciones afectadas.

    recalculados = construir_mercado(

        cotizaciones_afectadas,

        tc_nuevo,

        embig,

        puente,

    )

    recalculados = alinear_esquema(

        recalculados,

        mercado_base,

    ).persist()

    validar_clave(

        recalculados,

        CLAVE_MERCADO,

        "mercado recalculado",

    )

    comprobar_equivalencia(

        recalculados.select(*CLAVE_MERCADO),

        claves,

        "Claves recalculadas vs alcance",

    )

    anteriores = mercado_base.join(

        claves,

        CLAVE_MERCADO,

        "left_semi",

    )

    columnas_no_modificables = [

        columna

        for columna in mercado_base.columns

        if columna not in COLUMNAS_MACRO_MODIFICABLES

    ]

    comprobar_equivalencia(

        recalculados.select(
            *columnas_no_modificables
        ),

        anteriores.select(
            *columnas_no_modificables
        ),

        "Columnas ajenas al tipo de cambio",

    )

    # Registros Gold que no necesitan recalculo.

    conservados = mercado_base.join(

        claves,

        CLAVE_MERCADO,

        "left_anti",

    )

    candidato = conservados.unionByName(

        recalculados

    ).persist()

    diferencias = recalculados.exceptAll(

        anteriores.select(*recalculados.columns)

    ).count()

    return (

        candidato,

        recalculados,

        diferencias,

    )


# ============================================================
# 8. ESCRITURA DE VERSION GOLD CANDIDATA
# ============================================================

def escribir_candidata(
    escenario,
    candidato,
    particiones,
):

    RAIZ_CANDIDATAS.mkdir(

        parents=True,

        exist_ok=True,

    )

    exigir(
        not RAIZ_CANDIDATAS.is_symlink(),
        "Raiz candidata simbolica no permitida.",
    )

    ruta_candidata = (

        RAIZ_CANDIDATAS

        / f"gold_bcrp_{escenario}_v1"

    )

    exigir(
        not ruta_candidata.exists()
        and not ruta_candidata.is_symlink(),
        f"La version candidata ya existe: {ruta_candidata}",
    )

    origen_mercado = (

        GOLD_HISTORICO

        / PRODUCTOS_GOLD["mercado_valor_diario"]

    )

    rutas_excluidas = {

        origen_mercado

        / f"anio={particion['anio']}"

        / f"mes={particion['mes']}"

        for particion in particiones

    }

    exigir(
        all(
            ruta.is_dir()
            for ruta in rutas_excluidas
        ),
        "Una particion candidata no existe en Gold base.",
    )

    def ignorar_particiones(directorio, nombres):

        directorio = Path(directorio)

        return [

            nombre

            for nombre in nombres

            if directorio / nombre
            in rutas_excluidas

        ]

    # Copiamos Gold historico, excluyendo solamente
    # las particiones de mercado que deben escribirse.

    shutil.copytree(

        GOLD_HISTORICO,

        ruta_candidata,

        ignore=ignorar_particiones,

    )

    destino_mercado = (

        ruta_candidata

        / PRODUCTOS_GOLD["mercado_valor_diario"]

    )

    particiones_escritas = []

    for particion in particiones:

        anio = particion["anio"]

        mes = particion["mes"]

        destino_particion = (

            destino_mercado

            / f"anio={anio}"

            / f"mes={mes}"

        )

        exigir(
            not destino_particion.exists(),
            "La particion destino ya existe.",
        )

        datos_particion = candidato.filter(

            (F.col("anio") == anio)

            & (F.col("mes") == mes)

        )

        cantidad = datos_particion.count()

        exigir(
            cantidad > 0,
            "La particion candidata esta vacia.",
        )

        (

            datos_particion

            .drop("anio", "mes")

            .coalesce(1)

            .write

            .mode("errorifexists")

            .option("compression", "snappy")

            .parquet(str(destino_particion))
        )

        particiones_escritas.append({

            "anio": anio,

            "mes": mes,

            "registros_particion": cantidad,

        })

        print(

            f"Particion escrita: {anio}/{mes} "

            f"-> {cantidad} registros"

        )

    return ruta_candidata, particiones_escritas


# ============================================================
# 9. VERIFICAR PRODUCTOS Y PARTICIONES CONSERVADAS
# ============================================================

def verificar_productos_conservados(
    ruta_candidata,
    inventario_gold,
):

    conservados = []

    for producto, relativa in PRODUCTOS_GOLD.items():

        if producto == "mercado_valor_diario":
            continue

        actual = inventariar_dataset(

            producto,

            ruta_candidata / relativa,

        )

        esperado = inventario_gold[producto]

        exigir(
            actual["huella_dataset_sha256"]
            == esperado["huella_dataset_sha256"],
            f"Producto no afectado modificado: {producto}",
        )

        conservados.append(producto)

    return conservados


def verificar_particiones_conservadas(
    ruta_candidata,
    particiones,
):

    from version_registry import construir_integridad

    origen = (

        GOLD_HISTORICO

        / PRODUCTOS_GOLD["mercado_valor_diario"]

    )

    destino = (

        ruta_candidata

        / PRODUCTOS_GOLD["mercado_valor_diario"]

    )

    afectadas = {

        f"anio={item['anio']}/mes={item['mes']}"

        for item in particiones

    }

    verificadas = 0

    for ruta in sorted(

        origen.glob("anio=*/mes=*")

    ):

        relativa = ruta.relative_to(origen)

        if relativa.as_posix() in afectadas:
            continue

        exigir(
            construir_integridad(ruta)

            == construir_integridad(
                destino / relativa
            ),

            f"Particion no afectada modificada: {relativa}",
        )

        verificadas += 1

    return verificadas


# ============================================================
# 10. EJECUTAR ESCENARIO
# ============================================================

def ejecutar_escenario(
    spark,
    escenario,
):

    imprimir_seccion(

        "EJECUCION BCRP: " + escenario.upper()

    )

    registro, huella_registro = cargar_registro(

        RUTA_REGISTRO

    )

    inventario_gold = inventariar_gold()

    inventario_silver = inventariar_fuentes_silver()

    alcance, huella_alcance, evidencia = (

        cargar_alcance(

            escenario,

            registro,

            huella_registro,

            inventario_gold,

            inventario_silver,

        )

    )

    ruta_reporte = (

        RAIZ_REPORTES

        / f"ejecucion_gold_bcrp_{escenario}.json"

    )

    # Control de idempotencia de ejecuciones locales.

    exigir(
        not ruta_reporte.is_symlink(),
        "No se permite un reporte simbolico.",
    )

    if ruta_reporte.exists():

        imprimir_seccion(
            "CONTROL DE IDEMPOTENCIA"
        )

        previo, huella_previo = cargar_json(
            ruta_reporte
        )

        controles = {
            "entorno": "PRUEBA",
            "escenario": escenario,
            "run_id": alcance["run_id"],
            "huella_manifest_sha256": (
                alcance["huella_manifest_sha256"]
            ),
            "huella_registro_sha256": huella_registro,
            "huella_alcance_sha256": huella_alcance,
            "version_silver_evaluada": (
                alcance["version_silver_publicada"]
            ),
            "version_gold_base": (
                alcance["version_gold_base"]
            ),
            "inventario_gold_base": inventario_gold,
        }

        for campo, esperado in controles.items():

            exigir(
                previo.get(campo) == esperado,
                f"Conflicto de idempotencia: {campo}",
            )

        exigir(
            previo.get("estado") in {
                "GOLD_SIN_CAMBIOS",
                "GOLD_CANDIDATO_PRUEBA_VALIDADO",
            },
            "La ejecucion anterior no fue exitosa.",
        )

        for campo in [
            "validacion_operativa_aprobada",
            "alcance_efectivo_resuelto",
            "equivalencia_aprobada",
            "gold_historico_conservado",
            "fuentes_silver_conservadas",
            "ejecucion_experimental_completada",
        ]:

            exigir(
                previo.get(campo) is True,
                f"Control anterior no aprobado: {campo}",
            )

        exigir(
            previo.get("publicacion_productiva")
            is False,
            "Publicacion productiva no permitida.",
        )

        exigir(
            previo.get("autorizacion_publicacion_gold")
            is False,
            "Autorizacion de publicacion incompatible.",
        )

        comparaciones = previo.get("comparaciones")

        exigir(
            isinstance(comparaciones, list)
            and len(comparaciones) >= 2,
            "Comparaciones anteriores incompletas.",
        )

        for comparacion in comparaciones:

            exigir(
                comparacion.get("equivalencia_aprobada")
                is True
                and comparacion.get("solo_izquierda") == 0
                and comparacion.get("solo_derecha") == 0,
                "Equivalencia anterior no aprobada.",
            )

        ruta_esperada = (
            RAIZ_CANDIDATAS
            / f"gold_bcrp_{escenario}_v1"
        )

        if previo["estado"] == (
            "GOLD_CANDIDATO_PRUEBA_VALIDADO"
        ):

            exigir(
                previo.get("ruta_gold_candidata")
                == ruta_esperada.as_uri(),
                "Ruta candidata incompatible.",
            )

            exigir(
                previo.get("version_gold_candidata")
                == ruta_esperada.name,
                "Version candidata incompatible.",
            )

            exigir(
                ruta_esperada.is_dir()
                and not ruta_esperada.is_symlink(),
                "La candidata anterior no esta disponible.",
            )

            inventario_actual = {

                producto: inventariar_dataset(
                    producto,
                    ruta_esperada / relativa,
                )

                for producto, relativa
                in PRODUCTOS_GOLD.items()

            }

            exigir(
                inventario_actual
                == previo.get("inventario_gold_candidata"),
                "La candidata anterior fue modificada.",
            )

            print(
                "OK: integridad Gold candidata verificada."
            )

        else:

            exigir(
                previo.get("version_gold_candidata")
                is None
                and previo.get("ruta_gold_candidata")
                is None
                and previo.get("inventario_gold_candidata")
                is None,
                "El escenario sin cambios tiene una candidata.",
            )

            exigir(
                previo.get("registros_mercado_modificados")
                == 0,
                "Resultado anterior incompatible.",
            )

            exigir(
                not ruta_esperada.exists()
                and not ruta_esperada.is_symlink(),
                "Existe una candidata inesperada.",
            )

        exigir(
            inventariar_gold() == inventario_gold,
            "Gold historico fue modificado.",
        )

        exigir(
            inventariar_fuentes_silver()
            == inventario_silver,
            "Las fuentes Silver fueron modificadas.",
        )

        verificar_integridad_version(
            evidencia["version_anterior"]
        )

        verificar_integridad_version(
            evidencia["version_publicada"]
        )

        _, huella_final = cargar_json(
            ruta_reporte
        )

        exigir(
            huella_final == huella_previo,
            "El reporte anterior fue modificado.",
        )

        resultado = dict(previo)

        resultado["estado_anterior"] = previo["estado"]

        resultado["estado"] = (
            "GOLD_OMITIDO_IDEMPOTENCIA"
        )

        resultado["registros_mercado_recalculados"] = 0

        resultado["registros_mercado_modificados"] = 0

        resultado["particiones_escritas"] = []

        resultado["ejecucion_omitida"] = True

        resultado["huella_reporte_anterior_sha256"] = (
            huella_previo
        )

        print("OK: run_id coincidente.")
        print("OK: huella manifest coincidente.")
        print("OK: ejecucion anterior validada.")
        print("OK: integridad de fuentes conservada.")
        print("OK: reporte anterior conservado.")
        print("Registros recalculados: 0")
        print("Registros modificados: 0")
        print("ESTADO: GOLD_OMITIDO_IDEMPOTENCIA")

        return resultado

    if alcance["cantidad_claves_mercado"] > 0:

        posible_candidata = (

            RAIZ_CANDIDATAS

            / f"gold_bcrp_{escenario}_v1"

        )

        exigir(
            not posible_candidata.exists()
            and not posible_candidata.is_symlink(),
            "Ya existe una candidata. "
            "Inspeccionar antes de continuar.",
        )

    dataframes = []

    comparaciones = []

    ruta_candidata = None

    particiones_escritas = []

    particiones_conservadas = 0

    productos_conservados = []

    try:

        cotizaciones = spark.read.parquet(

            inventario_silver["cotizaciones"]["ruta"]

        ).persist()

        dataframes.append(cotizaciones)

        embig = spark.read.parquet(

            inventario_silver["embig"]["ruta"]

        ).persist()

        dataframes.append(embig)

        puente = spark.read.parquet(

            inventario_silver[
                "puente_ruc_nemonico"
            ]["ruta"]

        ).persist()

        dataframes.append(puente)

        tc_nuevo = leer_version(

            spark,

            evidencia["version_publicada"],

        ).persist()

        dataframes.append(tc_nuevo)

        mercado_base = spark.read.parquet(

            inventario_gold[
                "mercado_valor_diario"
            ]["ruta"]

        ).persist()

        dataframes.append(mercado_base)

        hechos_base = spark.read.parquet(

            inventario_gold[
                "hechos_empresa"
            ]["ruta"]

        ).persist()

        dataframes.append(hechos_base)

        impacto_base = spark.read.parquet(

            inventario_gold[
                "impacto_hechos_valor"
            ]["ruta"]

        ).persist()

        dataframes.append(impacto_base)

        cantidad_base = mercado_base.count()

        claves, tarea = construir_claves_mercado(

            spark,

            alcance,

        )

        validar_alcance_mercado(

            claves,

            cotizaciones,

            mercado_base,

            alcance,

        )

        cantidad_candidatas = claves.count()

        cantidad_recalculadas = 0

        cambios_efectivos = 0

        # ====================================================
        # RECALCULO INCREMENTAL
        # ====================================================

        if cantidad_candidatas > 0:

            imprimir_seccion(

                "RECALCULO INCREMENTAL DE MERCADO"

            )

            (

                mercado_candidato,

                recalculados,

                cambios_efectivos,

            ) = recalcular_mercado(

                cotizaciones,

                tc_nuevo,

                embig,

                puente,

                mercado_base,

                claves,

            )

            dataframes.extend([

                mercado_candidato,

                recalculados,

            ])

            cantidad_recalculadas = (

                recalculados.count()

            )

        else:

            imprimir_seccion(

                "SIN CLAVES GOLD AFECTADAS"

            )

            mercado_candidato = mercado_base

        validar_clave(

            mercado_candidato,

            CLAVE_MERCADO,

            "mercado candidato",

        )

        cantidad_candidata = (

            mercado_candidato.count()

        )

        exigir(
            cantidad_candidata == cantidad_base,
            "Cambio inesperado de cardinalidad Gold.",
        )

        # ====================================================
        # REFERENCIA COMPLETA EXCLUSIVA PARA VALIDACION
        # ====================================================

        imprimir_seccion(

            "VALIDACION CONTRA RECONSTRUCCION COMPLETA"

        )

        mercado_referencia = construir_mercado(

            cotizaciones,

            tc_nuevo,

            embig,

            puente,

        ).persist()

        dataframes.append(mercado_referencia)

        comparaciones.append(

            comprobar_equivalencia(

                mercado_candidato,

                mercado_referencia,

                "Mercado incremental vs referencia completa",

            )

        )

        # Comprobamos que la ausencia de candidatos
        # de impacto realmente conserva el producto.

        candidatos_impacto = preparar_candidatos(

            hechos_base

        )

        impacto_referencia = construir_impacto(

            candidatos_impacto,

            mercado_referencia,

        ).persist()

        dataframes.append(impacto_referencia)

        comparaciones.append(

            comprobar_equivalencia(

                impacto_base,

                impacto_referencia,

                "Impacto historico vs referencia completa",

            )

        )

        # ====================================================
        # GENERAR CANDIDATA SOLO SI EXISTEN CAMBIOS
        # ====================================================

        if cambios_efectivos > 0:

            imprimir_seccion(

                "ESCRITURA DE VERSION GOLD CANDIDATA"

            )

            (

                ruta_candidata,

                particiones_escritas,

            ) = escribir_candidata(

                escenario,

                mercado_candidato,

                tarea["particiones_gold_afectadas"],

            )

            mercado_fisico = spark.read.parquet(

                (

                    ruta_candidata

                    / PRODUCTOS_GOLD[
                        "mercado_valor_diario"
                    ]

                ).as_uri()

            )

            comparaciones.append(

                comprobar_equivalencia(

                    mercado_fisico,

                    mercado_referencia,

                    "Mercado Parquet candidato vs referencia",

                )

            )

            validar_clave(

                mercado_fisico,

                CLAVE_MERCADO,

                "mercado candidato fisico",

            )

            productos_conservados = (

                verificar_productos_conservados(

                    ruta_candidata,

                    inventario_gold,

                )

            )

            particiones_conservadas = (

                verificar_particiones_conservadas(

                    ruta_candidata,

                    tarea["particiones_gold_afectadas"],

                )

            )

            estado = (

                "GOLD_CANDIDATO_PRUEBA_VALIDADO"

            )

        else:

            productos_conservados = list(

                PRODUCTOS_GOLD

            )

            estado = "GOLD_SIN_CAMBIOS"

        # ====================================================
        # VERIFICACION FINAL DE FUENTES
        # ====================================================

        imprimir_seccion(

            "VERIFICACION FINAL DE INTEGRIDAD"

        )

        exigir(
            inventariar_gold() == inventario_gold,
            "Gold historico fue modificado.",
        )

        exigir(
            inventariar_fuentes_silver()
            == inventario_silver,
            "Las fuentes Silver fueron modificadas.",
        )

        verificar_integridad_version(

            evidencia["version_anterior"]

        )

        verificar_integridad_version(

            evidencia["version_publicada"]

        )

        _, huella_registro_final = cargar_registro(

            RUTA_REGISTRO

        )

        exigir(
            huella_registro_final == huella_registro,
            "El registro fue modificado.",
        )

        _, huella_alcance_final = cargar_json(

            RAIZ_REPORTES

            / f"alcance_gold_bcrp_{escenario}.json"

        )

        exigir(
            huella_alcance_final == huella_alcance,
            "El alcance fue modificado.",
        )

        cargar_evidencia(

            escenario,

            registro,

            huella_registro,

        )

        inventario_candidata = None

        if ruta_candidata is not None:

            inventario_candidata = {

                producto: inventariar_dataset(

                    producto,

                    ruta_candidata / relativa,

                )

                for producto, relativa

                in PRODUCTOS_GOLD.items()

            }

        reporte = {

            "version_ejecutor": VERSION_EJECUTOR,

            "estado": estado,

            "entorno": "PRUEBA",

            "fecha_ejecucion": fecha_actual_utc(),

            "escenario": escenario,

            "run_id": alcance["run_id"],

            "modo_laboratorio": (
                alcance["modo_laboratorio"]
            ),

            "estado_silver_original": (
                alcance["estado_silver_original"]
            ),

            "huella_manifest_sha256": (
                alcance["huella_manifest_sha256"]
            ),

            "huella_registro_sha256": (
                huella_registro
            ),

            "huella_alcance_sha256": (
                huella_alcance
            ),

            "version_silver_evaluada": (
                alcance["version_silver_publicada"]
            ),

            "version_gold_base": (
                alcance["version_gold_base"]
            ),

            "version_gold_candidata": (

                ruta_candidata.name

                if ruta_candidata

                else None

            ),

            "ruta_gold_candidata": (

                ruta_candidata.as_uri()

                if ruta_candidata

                else None

            ),

            "registros_mercado_base": (
                cantidad_base
            ),

            "registros_mercado_candidato": (
                cantidad_candidata
            ),

            "claves_mercado_candidatas": (
                cantidad_candidatas
            ),

            "registros_mercado_recalculados": (
                cantidad_recalculadas
            ),

            "registros_mercado_modificados": (
                cambios_efectivos
            ),

            "registros_mercado_no_modificados": (

                cantidad_base - cambios_efectivos

            ),

            "registros_impacto_recalculados_incrementalmente": 0,

            "particiones_escritas": (
                particiones_escritas
            ),

            "particiones_mercado_conservadas": (
                particiones_conservadas
            ),

            "productos_gold_conservados": (
                productos_conservados
            ),

            "comparaciones": comparaciones,

            "inventario_gold_base": (
                inventario_gold
            ),

            "inventario_gold_candidata": (
                inventario_candidata
            ),

            "reconstruccion_completa_solo_referencia": True,

            "validacion_operativa_aprobada": True,

            "alcance_efectivo_resuelto": True,

            "equivalencia_aprobada": True,

            "gold_historico_conservado": True,

            "fuentes_silver_conservadas": True,

            "ejecucion_experimental_completada": True,

            "publicacion_productiva": False,

            "autorizacion_publicacion_gold": False,

        }

        guardar_reporte(

            reporte,

            ruta_reporte,

        )

        imprimir_seccion(

            "RESULTADO FINAL: " + escenario.upper()

        )

        print(

            "Registros Gold base:",

            cantidad_base,

        )

        print(

            "Registros recalculados:",

            cantidad_recalculadas,

        )

        print(

            "Registros modificados:",

            cambios_efectivos,

        )

        print(

            "Registros conservados:",

            cantidad_base - cambios_efectivos,

        )

        print(

            "Particiones escritas:",

            particiones_escritas,

        )

        print(

            "Equivalencia aprobada: True"

        )

        print(

            "Reporte:", ruta_reporte

        )

        print(

            "ESTADO:", estado

        )

        return reporte

    finally:

        for dataframe in dataframes:

            dataframe.unpersist()


# ============================================================
# 11. INTERFAZ DE LINEA DE COMANDOS
# ============================================================

def main():

    parser = argparse.ArgumentParser(

        description=(

            "Ejecutor incremental Gold BCRP "

            "para pruebas locales."

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

        required=True,

    )

    parser.add_argument(

        "--ejecutar-prueba",

        action="store_true",

        help=(

            "Autorizar explicitamente la ejecucion "

            "experimental local, sin publicar Gold."

        ),

    )

    argumentos = parser.parse_args()

    spark = None

    try:

        exigir(
            argumentos.ejecutar_prueba is True,
            "Debe indicar --ejecutar-prueba.",
        )

        if argumentos.escenario == "todos":

            escenarios = list(ESCENARIOS)

        else:

            escenarios = [

                argumentos.escenario

            ]

        spark = crear_spark()

        resultados = []

        for escenario in escenarios:

            resultado = ejecutar_escenario(

                spark,

                escenario,

            )

            resultados.append(resultado)

        imprimir_seccion(

            "RESUMEN GENERAL DE EJECUCION"

        )

        for resultado in resultados:

            print(

                resultado["escenario"],

                "->",

                resultado["estado"],

                "| recalculados:",

                resultado[
                    "registros_mercado_recalculados"
                ],

                "| modificados:",

                resultado[
                    "registros_mercado_modificados"
                ],

            )

        print(

            "\nESTADO: EJECUCION_INCREMENTAL_PRUEBA_COMPLETADA"

        )

        print(

            "Gold historico conservado."

        )

        print(

            "No se realizo ninguna publicacion productiva."

        )

        return 0

    except Exception as exc:

        error = {

            "estado": (

                "EJECUCION_INCREMENTAL_PRUEBA_FALLIDA"

            ),

            "entorno": "PRUEBA",

            "fecha": fecha_actual_utc(),

            "publicacion_productiva": False,

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

    finally:

        if spark is not None:

            spark.stop()


if __name__ == "__main__":

    sys.exit(main())
