#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics
Inicializador del registro local de versiones Silver BCRP.

Registra tres versiones experimentales:

1. snapshot_20261109
2. bcrp_insercion_v1
3. bcrp_correccion_v1

Utiliza rutas autorizadas independientes de los manifests.

Responsabilidades:
- Validar los reportes de preparacion y generacion.
- Construir registros de versiones Silver.
- Inventariar archivos Parquet.
- Calcular huellas SHA-256.
- Verificar relaciones entre versiones.
- Contrastar los manifests operativos.
- Persistir el registro local de versiones.

No modifica Silver ni Gold.
No ejecuta Spark.
No autoriza procesamiento productivo.
"""

import json
import sys

from pathlib import Path
from datetime import datetime, timezone


# ============================================================
# 1. IMPORTACIONES DEL SISTEMA INCREMENTAL
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
)

from version_registry import (
    VersionRegistryError,
    DIRECTORIO_PRUEBAS,
    SILVER_ORIGINAL,
    SILVER_VERSIONES,
    RUTA_REGISTRO,
    crear_registro_vacio,
    construir_version,
    validar_registro,
    guardar_registro_nuevo,
    cargar_registro,
    verificar_integridad_version,
    resolver_manifest,
    resumir_registro,
)


# ============================================================
# 2. CONFIGURACION
# ============================================================

DATASET = "tipo_cambio"

VERSION_ESQUEMA = "tipo_cambio_prueba_v1"

VERSION_BASE = "snapshot_20261109"

VERSION_INSERCION = "bcrp_insercion_v1"

VERSION_CORRECCION = "bcrp_correccion_v1"

RUTA_DATASET = Path(
    "macroeconomia/tipo_cambio"
)

ESQUEMA_ESPERADO = {

    "fecha": "date",

    "tipo_cambio_venta": "double",

    "anio": "int",

    "mes": "int",

}

RUTA_REPORTE_PREPARACION = (

    DIRECTORIO_PRUEBAS

    / "reportes"

    / "preparacion_versiones_bcrp.json"

)

RUTA_REPORTE_MANIFESTS = (

    DIRECTORIO_PRUEBAS

    / "reportes"

    / "generacion_manifests_operativos_bcrp.json"

)

RUTA_REPORTE_REGISTRO = (

    DIRECTORIO_PRUEBAS

    / "reportes"

    / "inicializacion_registro_versiones_bcrp.json"

)

# Huellas obtenidas durante las ejecuciones anteriores.
# Constituyen referencias locales de esta prueba.

HUELLA_REPORTE_PREPARACION = (
    "d2c239534659300effab3e3e4aadef14e"
    "382fb3a6ddfa99ef61e64c34de7a95c"
)

CONFIGURACION_VERSIONES = {

    VERSION_BASE: {

        "version_anterior": None,

        "ruta": (
            SILVER_ORIGINAL
            / RUTA_DATASET
        ),

        "registros": 7751,

        "campo_reporte": "snapshot_original",

    },

    VERSION_INSERCION: {

        "version_anterior": VERSION_BASE,

        "ruta": (

            SILVER_VERSIONES

            / VERSION_INSERCION

            / RUTA_DATASET

        ),

        "registros": 7752,

        "campo_reporte": "version_insercion",

    },

    VERSION_CORRECCION: {

        "version_anterior": VERSION_BASE,

        "ruta": (

            SILVER_VERSIONES

            / VERSION_CORRECCION

            / RUTA_DATASET

        ),

        "registros": 7751,

        "campo_reporte": "version_correccion",

    },

}

CONFIGURACION_MANIFESTS = {

    "insercion": {

        "archivo": (
            "manifest_bcrp_insercion_operativo.json"
        ),

        "run_id": "prueba_bcrp_insercion_001",

        "version": VERSION_INSERCION,

        "sha256": (

            "5af0dcbf0378be517f6918eac8ccc7a661"

            "8ebe7e5d6db8d1a5e782e542f2a51b"

        ),

    },

    "correccion": {

        "archivo": (
            "manifest_bcrp_correccion_operativo.json"
        ),

        "run_id": "prueba_bcrp_correccion_001",

        "version": VERSION_CORRECCION,

        "sha256": (

            "0e818f8bc1b9da8a8f345dc8ce8d4ed"

            "ce91f5474831098f5a8681dad601655ba"

        ),

    },

}


# ============================================================
# 3. EXCEPCIONES Y FUNCIONES AUXILIARES
# ============================================================

class InicializacionRegistroError(Exception):

    """Error al inicializar el registro local."""


def exigir(condicion, mensaje):

    if not condicion:

        raise InicializacionRegistroError(
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


def comprobar_archivo_nuevo(ruta):

    exigir(

        not ruta.exists()

        and not ruta.is_symlink(),

        f"El archivo de destino ya existe: {ruta}",

    )


def comprobar_archivo_existente(ruta):

    exigir(

        ruta.is_file(),

        f"No existe el archivo requerido: {ruta}",

    )

    for componente in [

        ruta,

        *ruta.parents,

    ]:

        exigir(

            not componente.is_symlink(),

            f"Enlace simbolico no permitido: {componente}",

        )


def cargar_json_control(ruta):

    comprobar_archivo_existente(
        ruta
    )

    contenido, huella = cargar_manifest(
        ruta
    )

    exigir(

        isinstance(contenido, dict),

        f"Contenido JSON invalido: {ruta}",

    )

    return contenido, huella


def guardar_json_nuevo(
    contenido,
    ruta,
):

    comprobar_archivo_nuevo(
        ruta
    )

    texto = json.dumps(

        contenido,

        indent=4,

        ensure_ascii=False,

        allow_nan=False,

    ) + "\n"

    with ruta.open(

        "x",

        encoding="utf-8",

    ) as archivo:

        archivo.write(
            texto
        )


# ============================================================
# 4. COMPROBACION DEL ENTORNO
# ============================================================

def comprobar_entorno():

    exigir(

        DIRECTORIO_PRUEBAS.is_dir(),

        "No existe el entorno de pruebas.",

    )

    for nombre in [

        "silver_versiones",

        "manifests",

        "control",

        "reportes",

    ]:

        directorio = (

            DIRECTORIO_PRUEBAS

            / nombre

        )

        exigir(

            directorio.is_dir(),

            f"No existe el directorio: {directorio}",

        )

        for componente in [

            directorio,

            *directorio.parents,

        ]:

            exigir(

                not componente.is_symlink(),

                f"Directorio simbolico no permitido: {componente}",

            )

    comprobar_archivo_nuevo(
        RUTA_REGISTRO
    )

    comprobar_archivo_nuevo(
        RUTA_REPORTE_REGISTRO
    )


# ============================================================
# 5. VALIDAR REPORTE DE PREPARACION
# ============================================================

def validar_reporte_preparacion():

    reporte, huella = cargar_json_control(
        RUTA_REPORTE_PREPARACION
    )

    exigir(

        huella == HUELLA_REPORTE_PREPARACION,

        "La huella del reporte de preparacion no coincide.",

    )

    exigir(

        reporte.get("estado")

        == "VERSIONES_PRUEBA_PREPARADAS",

        "Reporte de preparacion no aprobado.",

    )

    exigir(

        reporte.get("entorno") == "PRUEBA",

        "Entorno de preparacion incompatible.",

    )

    exigir(

        reporte.get("dataset") == DATASET,

        "Dataset de preparacion incompatible.",

    )

    exigir(

        reporte.get("datos_simulados") is True,

        "Los datos deben estar identificados como simulados.",

    )

    exigir(

        reporte.get("publicacion_productiva") is False,

        "No se permite publicacion productiva.",

    )

    exigir(

        reporte.get("autorizacion_ejecucion_gold") is False,

        "El reporte no debe autorizar ejecutar Gold.",

    )

    exigir(

        reporte.get("clave_negocio") == ["fecha"],

        "Clave de negocio incompatible.",

    )

    exigir(

        reporte.get("columnas_particion")

        == ["anio", "mes"],

        "Particiones incompatibles.",

    )

    original = reporte[
        "snapshot_original"
    ]

    exigir(

        original["esquema"] == ESQUEMA_ESPERADO,

        "El esquema original no coincide.",

    )

    verificacion = reporte[
        "verificacion_final_original"
    ]

    exigir(

        verificacion["estado"]

        == "SIN_CAMBIOS_DETECTADOS",

        "No se ha confirmado la conservacion del snapshot.",

    )

    exigir(

        verificacion["metricas_conservadas"] is True,

        "Metricas originales no conservadas.",

    )

    exigir(

        verificacion["registros_prueba_conservados"] is True,

        "Registros de prueba no conservados.",

    )

    for version, configuracion in CONFIGURACION_VERSIONES.items():

        detalle = reporte[
            configuracion["campo_reporte"]
        ]

        exigir(

            detalle["version"] == version,

            f"Identificador de version incompatible: {version}",

        )

        ruta_esperada = configuracion[
            "ruta"
        ].absolute().as_uri()

        exigir(

            detalle["ruta_silver"] == ruta_esperada,

            f"Ruta Silver incompatible: {version}",

        )

        metricas = detalle[
            "metricas"
        ]

        exigir(

            type(metricas["total_registros"]) is int

            and metricas["total_registros"]

            == configuracion["registros"],

            f"Conteo incompatible: {version}",

        )

        exigir(

            metricas["fechas_unicas"]

            == configuracion["registros"],

            f"Claves duplicadas: {version}",

        )

        exigir(

            metricas["fechas_nulas"] == 0,

            f"Claves nulas: {version}",

        )

        exigir(

            metricas["particiones_inconsistentes"] == 0,

            f"Particiones inconsistentes: {version}",

        )

        exigir(

            metricas["valores_no_finitos"] == 0,

            f"Valores no finitos: {version}",

        )

        exigir(

            metricas["valores_nulos"] == 352,

            f"Cantidad inesperada de nulos: {version}",

        )

        if version != VERSION_BASE:

            exigir(

                detalle["estado"]

                == "VERSION_PRUEBA_VALIDADA",

                f"Version no validada: {version}",

            )

            exigir(

                detalle["version_anterior"]

                == configuracion["version_anterior"],

                f"Version anterior incompatible: {version}",

            )

    return reporte, huella


# ============================================================
# 6. VALIDAR REPORTE DE MANIFESTS
# ============================================================

def validar_reporte_manifests(
    huella_preparacion,
):

    reporte, huella = cargar_json_control(
        RUTA_REPORTE_MANIFESTS
    )

    exigir(

        reporte["estado"]

        == "MANIFESTS_OPERATIVOS_PRUEBA_GENERADOS",

        "Reporte de manifests no aprobado.",

    )

    exigir(

        reporte["entorno"] == "PRUEBA",

        "Entorno de manifests incompatible.",

    )

    exigir(

        reporte["dataset"] == DATASET,

        "Dataset de manifests incompatible.",

    )

    exigir(

        reporte["datos_simulados"] is True,

        "Los manifests deben pertenecer a una simulacion.",

    )

    exigir(

        reporte["publicacion_productiva"] is False,

        "No se permite publicacion productiva.",

    )

    exigir(

        reporte["version_base"] == VERSION_BASE,

        "Version base incompatible.",

    )

    exigir(

        reporte["version_esquema"] == VERSION_ESQUEMA,

        "Version de esquema incompatible.",

    )

    exigir(

        reporte["cantidad_manifests"] == 2,

        "Se esperaban dos manifests operativos.",

    )

    exigir(

        reporte["validacion_estructural_aprobada"] is True,

        "Validacion estructural pendiente.",

    )

    exigir(

        reporte["validacion_operativa_aprobada"] is False,

        "La validacion operativa debe seguir pendiente.",

    )

    exigir(

        reporte["autorizacion_ejecucion_gold"] is False,

        "No se permite ejecutar Gold todavia.",

    )

    exigir(

        reporte["autorizacion_publicacion_gold"] is False,

        "No se permite publicar Gold todavia.",

    )

    referencia = reporte[
        "reporte_preparacion"
    ]

    exigir(

        referencia["ruta"]

        == RUTA_REPORTE_PREPARACION.absolute().as_uri(),

        "Ruta de reporte de preparacion incompatible.",

    )

    exigir(

        referencia["huella_sha256"]

        == huella_preparacion,

        "Huella de reporte de preparacion incompatible.",

    )

    exigir(

        set(reporte["manifests"])

        == set(CONFIGURACION_MANIFESTS),

        "Escenarios de manifests incompatibles.",

    )

    manifests = {}

    for escenario, configuracion in CONFIGURACION_MANIFESTS.items():

        detalle = reporte["manifests"][
            escenario
        ]

        ruta = (

            DIRECTORIO_PRUEBAS

            / "manifests"

            / configuracion["archivo"]

        )

        manifest, huella_manifest = cargar_json_control(
            ruta
        )

        exigir(

            huella_manifest

            == configuracion["sha256"],

            f"Huella original incompatible: {escenario}",

        )

        exigir(

            huella_manifest

            == detalle["huella_sha256"],

            f"Huella del reporte incompatible: {escenario}",

        )

        exigir(

            detalle["ruta_manifest"]

            == ruta.absolute().as_uri(),

            f"Ruta de manifest incompatible: {escenario}",

        )

        exigir(

            detalle["estado"]

            == "MANIFEST_ESTRUCTURA_APROBADA",

            f"Manifest no aprobado: {escenario}",

        )

        exigir(

            detalle["validacion_estructural_aprobada"]

            is True,

            f"Validacion estructural pendiente: {escenario}",

        )

        exigir(

            detalle["validacion_operativa_aprobada"]

            is False,

            f"Validacion operativa inesperada: {escenario}",

        )

        exigir(

            detalle["autorizacion_ejecucion_gold"]

            is False,

            f"Autorizacion Gold inesperada: {escenario}",

        )

        exigir(

            manifest["run_id"]

            == configuracion["run_id"]

            == detalle["run_id"],

            f"run_id incompatible: {escenario}",

        )

        exigir(

            manifest["entorno"] == "PRUEBA",

            f"Entorno de manifest incompatible: {escenario}",

        )

        exigir(

            manifest["estado"]

            == "SILVER_PUBLICADO_APROBADO",

            f"Estado de manifest incompatible: {escenario}",

        )

        exigir(

            manifest["validacion_aprobada"] is True,

            f"Manifest no aprobado: {escenario}",

        )

        exigir(

            len(manifest["datasets"]) == 1,

            f"Cantidad inesperada de datasets: {escenario}",

        )

        dataset = manifest["datasets"][0]

        exigir(

            dataset["dataset"] == DATASET,

            f"Dataset incompatible: {escenario}",

        )

        exigir(

            dataset["version_anterior"] == VERSION_BASE,

            f"Version base incompatible: {escenario}",

        )

        exigir(

            dataset["version_publicada"]

            == configuracion["version"],

            f"Version publicada incompatible: {escenario}",

        )

        manifests[escenario] = {

            "manifest": manifest,

            "ruta": str(ruta),

            "huella_sha256": huella_manifest,

        }

    return reporte, huella, manifests


# ============================================================
# 7. CONSTRUIR REGISTRO DE VERSIONES
# ============================================================

def construir_registro_bcrp():

    registro = crear_registro_vacio()

    for version, configuracion in CONFIGURACION_VERSIONES.items():

        imprimir_seccion(

            "REGISTRANDO VERSION: " + version

        )

        ruta = configuracion[
            "ruta"
        ]

        exigir(

            ruta.is_dir(),

            f"No existe la version Silver: {ruta}",

        )

        version_registrada = construir_version(

            dataset=DATASET,

            version=version,

            version_anterior=(

                configuracion["version_anterior"]

            ),

            version_esquema=VERSION_ESQUEMA,

            ruta_silver=ruta.absolute().as_uri(),

            esquema=ESQUEMA_ESPERADO,

            cantidad_registros=(

                configuracion["registros"]

            ),

        )

        registro["versiones"].append(
            version_registrada
        )

        integridad = version_registrada[
            "integridad"
        ]

        print(

            f"OK: version registrada: {version}"

        )

        print(

            f"OK: registros esperados: "

            f"{version_registrada['cantidad_registros']}"

        )

        print(

            f"OK: archivos Parquet: "

            f"{integridad['cantidad_archivos_parquet']}"

        )

        print(

            f"OK: bytes Parquet: "

            f"{integridad['total_bytes_parquet']}"

        )

        print(

            f"OK: SHA-256 del dataset: "

            f"{integridad['huella_dataset_sha256']}"

        )

    indice = validar_registro(
        registro
    )

    exigir(

        len(indice) == 3,

        "Cantidad inesperada de versiones registradas.",

    )

    exigir(

        set(indice)

        == {

            (DATASET, VERSION_BASE),

            (DATASET, VERSION_INSERCION),

            (DATASET, VERSION_CORRECCION),

        },

        "El indice no contiene las versiones esperadas.",

    )

    return registro


# ============================================================
# 8. VERIFICAR INTEGRIDAD DE VERSIONES
# ============================================================

def verificar_todas_las_versiones(
    registro,
):

    resultados = []

    for version in registro["versiones"]:

        resultado = verificar_integridad_version(
            version
        )

        exigir(

            resultado["integridad_verificada"] is True,

            f"Integridad no verificada: {version['version']}",

        )

        resultados.append(
            resultado
        )

        print(

            f"OK: integridad verificada: "

            f"{version['version']}"

        )

    exigir(

        len(resultados) == 3,

        "No se verificaron todas las versiones.",

    )

    return resultados


# ============================================================
# 9. RESOLVER MANIFESTS CONTRA EL REGISTRO
# ============================================================

def verificar_manifests_registrados(

    registro,

    manifests,

):

    resultados = {}

    for escenario, contenido in manifests.items():

        manifest = contenido[
            "manifest"
        ]

        resultado = resolver_manifest(

            registro,

            manifest,

            verificar_integridad=True,

        )

        exigir(

            resultado["estado"] == "VERSIONES_RESUELTAS",

            f"No se resolvieron las versiones: {escenario}",

        )

        exigir(

            resultado["versiones_resueltas"] is True,

            f"Resolucion incompleta: {escenario}",

        )

        exigir(

            resultado["integridad_verificada"] is True,

            f"Integridad pendiente: {escenario}",

        )

        exigir(

            resultado["validacion_operativa_aprobada"]

            is False,

            "No corresponde aprobar validacion operativa.",

        )

        exigir(

            resultado["autorizacion_ejecucion_gold"]

            is False,

            "No corresponde autorizar ejecucion Gold.",

        )

        exigir(

            resultado["autorizacion_publicacion_gold"]

            is False,

            "No corresponde autorizar publicacion Gold.",

        )

        dataset_resuelto = resultado[
            "datasets"
        ][0]

        anterior = dataset_resuelto[
            "version_anterior"
        ]

        publicada = dataset_resuelto[
            "version_publicada"
        ]

        resultados[escenario] = {

            "run_id": resultado["run_id"],

            "ruta_manifest": contenido["ruta"],

            "huella_manifest_sha256": (

                contenido["huella_sha256"]

            ),

            "estado": resultado["estado"],

            "version_anterior": anterior["version"],

            "version_publicada": publicada["version"],

            "ruta_anterior": anterior["ruta_silver"],

            "ruta_publicada": publicada["ruta_silver"],

            "integridad_verificada": True,

            "esquema_fisico_verificado": False,

            "conteos_fisicos_verificados": False,

            "cambios_efectivos_verificados": False,

            "validacion_operativa_aprobada": False,

            "autorizacion_ejecucion_gold": False,

        }

        print(

            f"OK: manifest resuelto: "

            f"{resultado['run_id']}"

        )

    return resultados


# ============================================================
# 10. INICIALIZACION PRINCIPAL
# ============================================================

def inicializar_registro():

    comprobar_entorno()

    imprimir_seccion(
        "VALIDACION DE REPORTES DE CONTROL"
    )

    _, huella_preparacion = (
        validar_reporte_preparacion()
    )

    print(
        "OK: reporte de preparacion verificado."
    )

    print(

        f"OK: SHA-256 preparacion: "

        f"{huella_preparacion}"

    )

    _, huella_manifests, manifests = (
        validar_reporte_manifests(
            huella_preparacion
        )
    )

    print(
        "OK: reporte de manifests verificado."
    )

    print(

        f"OK: SHA-256 reporte manifests: "

        f"{huella_manifests}"

    )

    print(
        "OK: huellas de ambos manifests verificadas."
    )

    imprimir_seccion(
        "CONSTRUCCION DEL REGISTRO LOCAL"
    )

    registro = construir_registro_bcrp()

    imprimir_seccion(
        "VERIFICACION DE INTEGRIDAD DE VERSIONES"
    )

    integridades = verificar_todas_las_versiones(
        registro
    )

    imprimir_seccion(
        "RESOLUCION DE MANIFESTS OPERATIVOS"
    )

    resoluciones = verificar_manifests_registrados(

        registro,

        manifests,

    )

    # Persistimos el registro solamente cuando
    # todos los controles anteriores han terminado.

    imprimir_seccion(
        "PERSISTENCIA DEL REGISTRO LOCAL"
    )

    huella_guardada = guardar_registro_nuevo(

        registro,

        RUTA_REGISTRO,

    )

    registro_persistido, huella_registro = (
        cargar_registro(
            RUTA_REGISTRO
        )
    )

    exigir(

        huella_guardada == huella_registro,

        "La huella del registro persistido no coincide.",

    )

    exigir(

        registro_persistido == registro,

        "El registro persistido no coincide.",

    )

    resumen = resumir_registro(
        registro_persistido
    )

    print(
        f"OK: registro guardado: {RUTA_REGISTRO}"
    )

    print(
        f"OK: SHA-256 del registro: {huella_registro}"
    )

    print(
        "OK: registro persistido y recuperado."
    )

    reporte_final = {

        "estado": (

            "REGISTRO_VERSIONES_BCRP_INICIALIZADO"

        ),

        "entorno": "PRUEBA",

        "fecha_generacion": fecha_actual_utc(),

        "dataset": DATASET,

        "datos_simulados": True,

        "publicacion_productiva": False,

        "registro_versiones": {

            "ruta": RUTA_REGISTRO.as_uri(),

            "huella_sha256": huella_registro,

            "cantidad_versiones": 3,

        },

        "reportes_origen": {

            "preparacion": {

                "ruta": (

                    RUTA_REPORTE_PREPARACION.as_uri()

                ),

                "huella_sha256": huella_preparacion,

            },

            "manifests": {

                "ruta": (

                    RUTA_REPORTE_MANIFESTS.as_uri()

                ),

                "huella_sha256": huella_manifests,

            },

        },

        "versiones": resumen["versiones"],

        "integridades": integridades,

        "resoluciones": resoluciones,

        "validacion_estructural_aprobada": True,

        "registro_versiones_validado": True,

        "integridad_versiones_verificada": True,

        "versiones_manifests_resueltas": True,

        "esquemas_fisicos_verificados": False,

        "conteos_fisicos_verificados": False,

        "cambios_efectivos_verificados": False,

        "validacion_operativa_aprobada": False,

        "autorizacion_ejecucion_gold": False,

        "autorizacion_publicacion_gold": False,

    }

    guardar_json_nuevo(

        reporte_final,

        RUTA_REPORTE_REGISTRO,

    )

    imprimir_seccion(
        "RESULTADO FINAL"
    )

    print(
        "OK: reportes de origen verificados."
    )

    print(
        "OK: tres versiones Silver registradas."
    )

    print(
        "OK: inventarios Parquet generados."
    )

    print(
        "OK: integridad de versiones verificada."
    )

    print(
        "OK: relaciones entre versiones verificadas."
    )

    print(
        "OK: ambos manifests operativos resueltos."
    )

    print(
        "OK: registro persistido correctamente."
    )

    print(

        f"OK: reporte generado: "

        f"{RUTA_REPORTE_REGISTRO}"

    )

    print(

        "\nESTADO: REGISTRO_VERSIONES_BCRP_INICIALIZADO"

    )

    print(
        "Validacion fisica con Spark pendiente."
    )

    print(
        "No se autoriza ejecutar ni publicar Gold."
    )

    return reporte_final


# ============================================================
# 11. PUNTO DE ENTRADA
# ============================================================

def main():

    try:

        inicializar_registro()

        return 0

    except (

        InicializacionRegistroError,

        VersionRegistryError,

        ManifestError,

        OSError,

        ValueError,

        TypeError,

        KeyError,

        OverflowError,

        RecursionError,

    ) as exc:

        print(

            f"ERROR: {exc}",

            file=sys.stderr,

        )

        print(

            "ESTADO: INICIALIZACION_REGISTRO_FALLIDA",

            file=sys.stderr,

        )

        return 1


if __name__ == "__main__":

    sys.exit(
        main()
    )
