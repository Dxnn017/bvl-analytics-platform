#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics
Generador de manifests operativos de prueba
para el procesamiento incremental Silver -> Gold.

Utiliza el reporte generado mediante Spark por
preparar_versiones_bcrp.py.

Escenarios:
- Insercion de tipo de cambio.
- Correccion de tipo de cambio.

Este componente:
- Lee el reporte de preparacion.
- Verifica versiones, rutas y cambios declarados.
- Genera manifests compatibles con el contrato v0.1.
- Ejecuta la validacion estructural.
- Calcula las huellas SHA-256 de los manifests.
- Genera un reporte de control.

No modifica Silver ni Gold.
No ejecuta Spark.
No autoriza publicaciones productivas.
"""

import argparse
import json
import sys

from datetime import date, datetime, timezone
from pathlib import Path


# ============================================================
# 1. CONFIGURACION
# ============================================================

ROOT = Path(__file__).resolve().parents[3]

INCREMENTAL_DIR = (
    ROOT / "src" / "gold" / "incremental"
)

sys.path.insert(
    0,
    str(INCREMENTAL_DIR),
)

from manifest_validator import (
    ManifestError,
    cargar_manifest,
    validar_manifest,
    procesar_manifest,
)


HOME = Path.home()

DIRECTORIO_ENTREGAS = (
    HOME / "bvl_entregas"
)

DIRECTORIO_PRUEBAS = (
    DIRECTORIO_ENTREGAS
    / "incremental_pruebas_20261114"
)

DIRECTORIO_SILVER_ORIGINAL = (
    DIRECTORIO_ENTREGAS
    / "silver_20261109"
    / "silver"
)

RUTA_DATASET = Path(
    "macroeconomia/tipo_cambio"
)

VERSION_MANIFEST = "0.1"

VERSION_ESQUEMA = "tipo_cambio_prueba_v1"

VERSION_BASE = "snapshot_20261109"

DATASET = "tipo_cambio"

REGISTROS_BASE = 7751

ESQUEMA_ESPERADO = {
    "fecha": "date",
    "tipo_cambio_venta": "double",
    "anio": "int",
    "mes": "int",
}

ESCENARIOS = {

    "insercion": {

        "campo_reporte": "version_insercion",

        "version": "bcrp_insercion_v1",

        "run_id": "prueba_bcrp_insercion_001",

        "archivo": (
            "manifest_bcrp_insercion_operativo.json"
        ),

        "operacion": "INSERT",

        "fecha": "2026-09-18",

        "valor_anterior": None,

        "valor_nuevo": 3.37,

        "registros_despues": 7752,

        "conteos": {
            "INSERT": 1,
            "UPDATE": 0,
            "DELETE": 0,
        },

    },

    "correccion": {

        "campo_reporte": "version_correccion",

        "version": "bcrp_correccion_v1",

        "run_id": "prueba_bcrp_correccion_001",

        "archivo": (
            "manifest_bcrp_correccion_operativo.json"
        ),

        "operacion": "UPDATE",

        "fecha": "2025-12-30",

        "valor_anterior": 3.36435714285714,

        "valor_nuevo": 3.4,

        "registros_despues": 7751,

        "conteos": {
            "INSERT": 0,
            "UPDATE": 1,
            "DELETE": 0,
        },

    },

}


# ============================================================
# 2. EXCEPCIONES Y FUNCIONES AUXILIARES
# ============================================================

class GeneracionManifestError(Exception):
    """Error durante la generacion de manifests."""


def exigir(condicion, mensaje):

    if not condicion:

        raise GeneracionManifestError(
            mensaje
        )


def imprimir_seccion(titulo):

    print(
        "\n" + "=" * 70
    )

    print(titulo)

    print(
        "=" * 70
    )


def fecha_actual_utc():

    return datetime.now(
        timezone.utc
    ).isoformat()


def ruta_uri(ruta):

    return ruta.resolve().as_uri()


def comprobar_destino_nuevo(ruta):

    exigir(
        not ruta.exists()
        and not ruta.is_symlink(),
        f"El destino ya existe: {ruta}",
    )


def comprobar_directorio_seguro(
    directorio,
    raiz,
):

    exigir(
        directorio.is_dir(),
        f"No existe el directorio: {directorio}",
    )

    exigir(
        not directorio.is_symlink(),
        f"No se permiten enlaces simbolicos: {directorio}",
    )

    exigir(
        directorio.resolve().is_relative_to(
            raiz.resolve()
        ),
        f"Directorio fuera de la raiz autorizada: {directorio}",
    )


def comprobar_entorno(pruebas_root):

    exigir(
        pruebas_root.is_dir(),
        f"No existe el entorno de pruebas: {pruebas_root}",
    )

    exigir(
        pruebas_root.resolve()
        == DIRECTORIO_PRUEBAS.resolve(),
        "La raiz no coincide con el entorno autorizado.",
    )

    for nombre in [
        "silver_versiones",
        "manifests",
        "reportes",
    ]:

        comprobar_directorio_seguro(

            pruebas_root / nombre,

            pruebas_root,

        )


def guardar_json(
    contenido,
    destino,
):

    comprobar_destino_nuevo(
        destino
    )

    texto = json.dumps(

        contenido,

        indent=4,

        ensure_ascii=False,

        allow_nan=False,

    ) + "\n"

    with destino.open(

        "x",

        encoding="utf-8",

    ) as archivo:

        archivo.write(
            texto
        )


# ============================================================
# 3. LECTURA Y VALIDACION DEL REPORTE
# ============================================================

def cargar_reporte(ruta):

    exigir(
        ruta.is_file(),
        f"No existe el reporte: {ruta}",
    )

    exigir(
        not ruta.is_symlink(),
        "El reporte no puede ser un enlace simbolico.",
    )

    # Reutilizamos el lector JSON reforzado.
    # No aplicamos aqui las reglas estructurales
    # propias de un manifest.

    reporte, huella = cargar_manifest(
        ruta
    )

    return reporte, huella


def validar_reporte_general(reporte):

    exigir(

        reporte.get("estado")
        == "VERSIONES_PRUEBA_PREPARADAS",

        "El reporte no tiene un estado aprobado de prueba.",

    )

    exigir(
        reporte.get("entorno") == "PRUEBA",
        "El reporte debe pertenecer al entorno PRUEBA.",
    )

    exigir(
        reporte.get("dataset") == DATASET,
        "Dataset inesperado.",
    )

    exigir(
        reporte.get("datos_simulados") is True,
        "El reporte debe identificar los datos como simulados.",
    )

    exigir(
        reporte.get("publicacion_productiva") is False,
        "No se permiten publicaciones productivas.",
    )

    exigir(
        reporte.get("autorizacion_ejecucion_gold") is False,
        "El reporte no debe autorizar ejecucion Gold.",
    )

    exigir(
        reporte.get("clave_negocio") == ["fecha"],
        "Clave de negocio inesperada.",
    )

    exigir(
        reporte.get("columnas_particion")
        == ["anio", "mes"],
        "Columnas de particion inesperadas.",
    )

    verificacion = reporte.get(
        "verificacion_final_original"
    )

    exigir(
        isinstance(verificacion, dict),
        "Falta la verificacion del snapshot original.",
    )

    exigir(

        verificacion.get("estado")
        == "SIN_CAMBIOS_DETECTADOS",

        "El snapshot original no fue conservado.",

    )

    exigir(
        verificacion.get("metricas_conservadas") is True,
        "Las metricas originales no fueron conservadas.",
    )

    exigir(

        verificacion.get(
            "registros_prueba_conservados"
        ) is True,

        "Los registros originales no fueron conservados.",

    )


def validar_snapshot_original(reporte):

    original = reporte.get(
        "snapshot_original"
    )

    exigir(
        isinstance(original, dict),
        "Falta la informacion del snapshot original.",
    )

    exigir(
        original.get("version") == VERSION_BASE,
        "Version base inesperada.",
    )

    exigir(
        original.get("esquema") == ESQUEMA_ESPERADO,
        "Esquema original incompatible.",
    )

    origen = (
        DIRECTORIO_SILVER_ORIGINAL
        / RUTA_DATASET
    )

    exigir(
        origen.is_dir(),
        f"No existe el dataset original: {origen}",
    )

    exigir(
        (origen / "_SUCCESS").is_file(),
        "El dataset original no tiene _SUCCESS.",
    )

    exigir(

        original.get("ruta_silver")
        == ruta_uri(origen),

        "La ruta original no coincide con la ruta autorizada.",

    )

    metricas = original.get(
        "metricas"
    )

    exigir(
        isinstance(metricas, dict),
        "No existen las metricas originales.",
    )

    validar_metricas(

        metricas,

        REGISTROS_BASE,

    )

    exigir(
        metricas.get("fecha_minima") == "1997-01-02",
        "Fecha minima original inesperada.",
    )

    exigir(
        metricas.get("fecha_maxima") == "2026-09-17",
        "Fecha maxima original inesperada.",
    )

    exigir(
        original.get("registro_insercion") is None,
        "La fecha de insercion ya existia.",
    )

    registro = original.get(
        "registro_correccion"
    )

    exigir(
        isinstance(registro, dict),
        "Falta el registro original de correccion.",
    )

    exigir(
        registro.get("fecha") == "2025-12-30",
        "Fecha original de correccion inesperada.",
    )

    exigir(

        registro.get("tipo_cambio_venta")
        == 3.36435714285714,

        "Valor original de correccion inesperado.",

    )

    return original


def validar_metricas(
    metricas,
    registros_esperados,
):

    exigir(
        isinstance(metricas, dict),
        "Metricas invalidas.",
    )

    campos_enteros = [

        "total_registros",

        "fechas_unicas",

        "fechas_nulas",

        "particiones_inconsistentes",

        "valores_nulos",

        "valores_no_finitos",

    ]

    for campo in campos_enteros:

        valor = metricas.get(
            campo
        )

        exigir(

            type(valor) is int
            and valor >= 0,

            f"Metrica invalida: {campo}",

        )

    exigir(

        metricas["total_registros"]
        == registros_esperados,

        "Conteo total inesperado.",

    )

    exigir(

        metricas["fechas_unicas"]
        == registros_esperados,

        "Existen claves duplicadas.",

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
        metricas["valores_no_finitos"] == 0,
        "Existen valores no finitos.",
    )

    exigir(
        metricas["valores_nulos"] == 352,
        "Cantidad inesperada de valores nulos.",
    )


# ============================================================
# 4. VALIDACION DE VERSIONES SIMULADAS
# ============================================================

def validar_version(

    reporte,

    original,

    escenario,

    pruebas_root,

):

    configuracion = ESCENARIOS[
        escenario
    ]

    version = reporte.get(
        configuracion["campo_reporte"]
    )

    exigir(
        isinstance(version, dict),
        f"Falta la version: {escenario}",
    )

    exigir(
        version.get("estado") == "VERSION_PRUEBA_VALIDADA",
        f"Version no validada: {escenario}",
    )

    exigir(

        version.get("version")
        == configuracion["version"],

        f"Identificador inesperado: {escenario}",

    )

    exigir(

        version.get("version_anterior")
        == original["version"],

        "La version anterior no coincide.",

    )

    raiz_version = (

        pruebas_root

        / "silver_versiones"

        / configuracion["version"]

    )

    comprobar_directorio_seguro(

        raiz_version,

        pruebas_root,

    )

    destino = (

        raiz_version

        / RUTA_DATASET

    )

    comprobar_directorio_seguro(

        destino,

        raiz_version,

    )

    exigir(

        (destino / "_SUCCESS").is_file(),

        f"No existe _SUCCESS: {destino}",

    )

    exigir(

        version.get("ruta_silver")
        == ruta_uri(destino),

        f"Ruta inesperada: {escenario}",

    )

    metricas = version.get(
        "metricas"
    )

    validar_metricas(

        metricas,

        configuracion["registros_despues"],

    )

    exigir(

        metricas["valores_nulos"]
        == original["metricas"]["valores_nulos"],

        "Los valores nulos no fueron conservados.",

    )

    exigir(

        metricas.get("fecha_minima")
        == original["metricas"]["fecha_minima"],

        "La fecha minima fue modificada.",

    )

    fecha_maxima = (

        configuracion["fecha"]

        if escenario == "insercion"

        else original["metricas"]["fecha_maxima"]

    )

    exigir(

        metricas.get("fecha_maxima")
        == fecha_maxima,

        "Fecha maxima inesperada.",

    )

    conteos = version.get(
        "conteos_cambios"
    )

    exigir(
        isinstance(conteos, dict),
        "Conteos de cambios invalidos.",
    )

    exigir(

        all(
            type(valor) is int
            for valor in conteos.values()
        ),

        "Los conteos deben ser enteros.",

    )

    exigir(

        conteos == configuracion["conteos"],

        f"Conteos de cambios inesperados: {conteos}",

    )

    detalle = version.get(
        "detalle_cambios"
    )

    exigir(
        isinstance(detalle, list)
        and len(detalle) == 1,
        "Se esperaba exactamente un cambio.",
    )

    cambio = detalle[0]

    exigir(
        isinstance(cambio, dict),
        "Detalle de cambio invalido.",
    )

    validar_detalle_cambio(

        cambio,

        configuracion,

    )

    return version


def validar_detalle_cambio(
    cambio,
    configuracion,
):

    fecha = date.fromisoformat(
        configuracion["fecha"]
    )

    exigir(

        cambio.get("operacion")
        == configuracion["operacion"],

        "Operacion inesperada.",

    )

    exigir(

        cambio.get("fecha")
        == configuracion["fecha"],

        "Fecha de cambio inesperada.",

    )

    exigir(

        cambio.get("fecha_nueva")
        == configuracion["fecha"],

        "Clave nueva inesperada.",

    )

    exigir(

        cambio.get("valor_nuevo")
        == configuracion["valor_nuevo"],

        "Valor nuevo inesperado.",

    )

    exigir(

        cambio.get("valor_anterior")
        == configuracion["valor_anterior"],

        "Valor anterior inesperado.",

    )

    exigir(

        cambio.get("anio_nuevo") == fecha.year
        and cambio.get("mes_nuevo") == fecha.month,

        "Particion nueva inesperada.",

    )

    if configuracion["operacion"] == "INSERT":

        exigir(

            cambio.get("fecha_anterior") is None,

            "INSERT no debe tener clave anterior.",

        )

        exigir(

            cambio.get("anio_anterior") is None
            and cambio.get("mes_anterior") is None,

            "INSERT no debe tener particion anterior.",

        )

    elif configuracion["operacion"] == "UPDATE":

        exigir(

            cambio.get("fecha_anterior")
            == configuracion["fecha"],

            "Clave anterior inesperada.",

        )

        exigir(

            cambio.get("anio_anterior") == fecha.year
            and cambio.get("mes_anterior") == fecha.month,

            "Particion anterior inesperada.",

        )


# ============================================================
# 5. CONSTRUCCION DE CLAVES Y PARTICIONES
# ============================================================

def construir_claves_afectadas(detalle):

    operaciones = []

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

        operaciones.append({

            "operacion": cambio["operacion"],

            "clave_anterior": anterior,

            "clave_nueva": nueva,

        })

    return operaciones


def construir_particiones_afectadas(detalle):

    particiones = set()

    for cambio in detalle:

        for campo in [

            "fecha_anterior",

            "fecha_nueva",

        ]:

            valor = cambio[campo]

            if valor is None:

                continue

            fecha = date.fromisoformat(
                valor
            )

            particiones.add(
                (fecha.year, fecha.month)
            )

    return [

        {
            "anio": anio,
            "mes": mes,
        }

        for anio, mes in sorted(
            particiones
        )

    ]


# ============================================================
# 6. CONSTRUCCION DEL MANIFEST
# ============================================================

def construir_manifest(

    original,

    version,

    escenario,

    fecha_publicacion,

):

    configuracion = ESCENARIOS[
        escenario
    ]

    detalle = version[
        "detalle_cambios"
    ]

    conteos = version[
        "conteos_cambios"
    ]

    dataset = {

        "dataset": DATASET,

        "ruta_silver": version["ruta_silver"],

        "version_esquema": VERSION_ESQUEMA,

        "version_anterior": original["version"],

        "version_publicada": version["version"],

        "clave": ["fecha"],

        "particiones_afectadas": (
            construir_particiones_afectadas(
                detalle
            )
        ),

        "claves_afectadas": (
            construir_claves_afectadas(
                detalle
            )
        ),

        "ruta_detalle_cambios": None,

        "registros_nuevos": conteos["INSERT"],

        "registros_corregidos": conteos["UPDATE"],

        "registros_eliminados": conteos["DELETE"],

        "registros_antes": (
            original["metricas"]["total_registros"]
        ),

        "registros_despues": (
            version["metricas"]["total_registros"]
        ),

        "autorizacion_eliminaciones": False,

    }

    manifest = {

        "version_manifest": VERSION_MANIFEST,

        "entorno": "PRUEBA",

        "run_id": configuracion["run_id"],

        # Estado simulado para satisfacer
        # el contrato provisional de pruebas.

        "estado": "SILVER_PUBLICADO_APROBADO",

        "fecha_publicacion": fecha_publicacion,

        "tipo_publicacion": "INCREMENTAL",

        "validacion_aprobada": True,

        "datasets": [
            dataset
        ],

    }

    return manifest


# ============================================================
# 7. VALIDACION ESTRUCTURAL
# ============================================================

def validar_manifest_generado(manifest):

    errores, advertencias = validar_manifest(

        manifest,

        entorno_esperado="PRUEBA",

    )

    exigir(

        not errores,

        "Manifest estructuralmente invalido: "
        + json.dumps(
            errores,
            ensure_ascii=False,
        ),

    )

    return advertencias


def guardar_y_verificar_manifest(

    manifest,

    destino,

):

    advertencias = validar_manifest_generado(
        manifest
    )

    guardar_json(

        manifest,

        destino,

    )

    resultado = procesar_manifest(

        destino,

        entorno_esperado="PRUEBA",

    )

    exigir(

        resultado["estado"]
        == "MANIFEST_ESTRUCTURA_APROBADA",

        f"Manifest rechazado: {resultado['errores']}",

    )

    exigir(

        resultado["validacion_estructural_aprobada"]
        is True,

        "La validacion estructural no fue aprobada.",

    )

    exigir(

        resultado["validacion_operativa_aprobada"]
        is False,

        "No corresponde aprobar la validacion operativa.",

    )

    exigir(

        resultado["autorizacion_ejecucion_gold"]
        is False,

        "No se debe autorizar ejecucion Gold.",

    )

    exigir(

        resultado["run_id"]
        == manifest["run_id"],

        "El run_id persistido no coincide.",

    )

    manifest_guardado, huella = cargar_manifest(
        destino
    )

    exigir(

        manifest_guardado == manifest,

        "El manifest persistido no coincide.",

    )

    exigir(

        huella == resultado["huella_sha256"],

        "La huella SHA-256 no coincide.",

    )

    print(
        f"OK: manifest generado: {destino}"
    )

    print(
        f"OK: run_id: {manifest['run_id']}"
    )

    print(
        f"OK: SHA-256: {huella}"
    )

    print(
        "OK: validacion estructural aprobada."
    )

    return {

        "archivo": str(destino),

        "ruta_manifest": ruta_uri(destino),

        "run_id": manifest["run_id"],

        "version_manifest": (
            manifest["version_manifest"]
        ),

        "version_anterior": (
            manifest["datasets"][0]["version_anterior"]
        ),

        "version_publicada": (
            manifest["datasets"][0]["version_publicada"]
        ),

        "huella_sha256": huella,

        "estado": resultado["estado"],

        "validacion_estructural_aprobada": True,

        "validacion_operativa_aprobada": False,

        "autorizacion_ejecucion_gold": False,

        "advertencias": resultado["advertencias"],

    }


# ============================================================
# 8. GENERACION PRINCIPAL
# ============================================================

def generar_manifests(pruebas_root):

    comprobar_entorno(
        pruebas_root
    )

    pruebas_root = pruebas_root.resolve()

    ruta_reporte = (

        pruebas_root

        / "reportes"

        / "preparacion_versiones_bcrp.json"

    )

    destino_reporte = (

        pruebas_root

        / "reportes"

        / "generacion_manifests_operativos_bcrp.json"

    )

    destinos = {}

    for escenario, configuracion in ESCENARIOS.items():

        destino = (

            pruebas_root

            / "manifests"

            / configuracion["archivo"]

        )

        comprobar_destino_nuevo(
            destino
        )

        destinos[escenario] = destino

    comprobar_destino_nuevo(
        destino_reporte
    )

    imprimir_seccion(
        "LECTURA DEL REPORTE DE PREPARACION"
    )

    reporte, huella_reporte = cargar_reporte(
        ruta_reporte
    )

    validar_reporte_general(
        reporte
    )

    original = validar_snapshot_original(
        reporte
    )

    print(
        "OK: reporte de preparacion validado."
    )

    print(
        f"OK: version base: {original['version']}"
    )

    print(
        f"OK: SHA-256 del reporte: {huella_reporte}"
    )

    versiones = {}

    manifests = {}

    # Utilizamos la fecha de generacion del reporte
    # como fecha de publicacion simulada.

    fecha_publicacion = reporte.get(
        "fecha_generacion"
    )

    exigir(

        isinstance(fecha_publicacion, str),

        "Fecha de generacion invalida.",

    )

    # Validamos ambos escenarios antes de escribir
    # cualquiera de los manifests.

    for escenario in ESCENARIOS:

        imprimir_seccion(

            "VALIDACION DE VERSION: "
            + escenario.upper()

        )

        version = validar_version(

            reporte,

            original,

            escenario,

            pruebas_root,

        )

        versiones[escenario] = version

        manifest = construir_manifest(

            original,

            version,

            escenario,

            fecha_publicacion,

        )

        validar_manifest_generado(
            manifest
        )

        manifests[escenario] = manifest

        print(
            "OK: version validada."
        )

        print(
            "OK: manifest construido."
        )

    resultados = {}

    for escenario in ESCENARIOS:

        imprimir_seccion(

            "GENERACION DE MANIFEST: "
            + escenario.upper()

        )

        resultado = guardar_y_verificar_manifest(

            manifests[escenario],

            destinos[escenario],

        )

        resultados[escenario] = resultado

    reporte_final = {

        "estado": (
            "MANIFESTS_OPERATIVOS_PRUEBA_GENERADOS"
        ),

        "entorno": "PRUEBA",

        "fecha_generacion": fecha_actual_utc(),

        "dataset": DATASET,

        "datos_simulados": True,

        "publicacion_productiva": False,

        "reporte_preparacion": {

            "ruta": ruta_uri(ruta_reporte),

            "huella_sha256": huella_reporte,

        },

        "version_base": original["version"],

        "version_esquema": VERSION_ESQUEMA,

        "cantidad_manifests": len(resultados),

        "manifests": resultados,

        "validacion_estructural_aprobada": True,

        "validacion_operativa_aprobada": False,

        "autorizacion_ejecucion_gold": False,

        "autorizacion_publicacion_gold": False,

    }

    guardar_json(

        reporte_final,

        destino_reporte,

    )

    imprimir_seccion(
        "RESULTADO FINAL"
    )

    print(
        "OK: reporte de preparacion verificado."
    )

    print(
        "OK: manifest de insercion generado."
    )

    print(
        "OK: manifest de correccion generado."
    )

    print(
        "OK: ambos manifests aprobados estructuralmente."
    )

    print(
        "OK: huellas SHA-256 verificadas."
    )

    print(
        f"OK: reporte generado: {destino_reporte}"
    )

    print(
        "\nESTADO: MANIFESTS_OPERATIVOS_PRUEBA_GENERADOS"
    )

    print(
        "Validacion operativa de Silver pendiente."
    )

    print(
        "No se autoriza ejecutar ni publicar Gold."
    )

    return reporte_final


# ============================================================
# 9. INTERFAZ DE LINEA DE COMANDOS
# ============================================================

def main():

    parser = argparse.ArgumentParser(

        description=(

            "Generar manifests operativos simulados "
            "para pruebas incrementales BCRP."

        )

    )

    parser.add_argument(

        "--pruebas-root",

        default=str(
            DIRECTORIO_PRUEBAS
        ),

        help="Directorio aislado de pruebas operativas.",

    )

    argumentos = parser.parse_args()

    pruebas_root = Path(
        argumentos.pruebas_root
    )

    try:

        generar_manifests(
            pruebas_root
        )

        return 0

    except (

        GeneracionManifestError,

        ManifestError,

        OSError,

        ValueError,

        TypeError,

        KeyError,

    ) as exc:

        print(

            f"ERROR: {exc}",

            file=sys.stderr,

        )

        print(

            "ESTADO: GENERACION_MANIFESTS_FALLIDA",

            file=sys.stderr,

        )

        return 1


if __name__ == "__main__":

    sys.exit(
        main()
    )
