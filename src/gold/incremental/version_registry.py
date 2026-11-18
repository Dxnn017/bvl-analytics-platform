#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics
Registro local de versiones Silver.

Contrato provisional Silver -> Gold v0.1.

Responsabilidades:
- Registrar versiones Silver conocidas.
- Resolver versiones por dataset e identificador.
- Verificar rutas locales autorizadas.
- Registrar esquemas y claves de negocio.
- Registrar relaciones entre versiones.
- Calcular huellas SHA-256 de archivos Parquet.
- Verificar integridad de versiones registradas.
- Resolver las versiones declaradas en un manifest.

Limitaciones actuales:
- Solo entorno PRUEBA.
- Solo almacenamiento local.
- No publica versiones productivas.
- No modifica datasets Silver ni Gold.
- No ejecuta transformaciones Spark.
- No autoriza por si solo la ejecucion Gold.
"""

import argparse
import hashlib
import json
import re
import sys

from datetime import datetime, timezone
from pathlib import Path

if __package__:

    from .storage_config import (
        HOME,
        DIRECTORIO_ENTREGAS,
        DIRECTORIO_PRUEBAS,
        SILVER_ORIGINAL,
        SILVER_VERSIONES,
        RAICES_AUTORIZADAS,
        RUTA_REGISTRO_ORIGINAL,
    )

else:

    from storage_config import (
        HOME,
        DIRECTORIO_ENTREGAS,
        DIRECTORIO_PRUEBAS,
        SILVER_ORIGINAL,
        SILVER_VERSIONES,
        RAICES_AUTORIZADAS,
        RUTA_REGISTRO_ORIGINAL,
    )

if __package__:

    from .manifest_validator import (
        CATALOGO_DATASETS,
        ManifestError,
        cargar_manifest,
        validar_manifest,
        validar_compatibilidad_json,
    )

else:

    from manifest_validator import (
        CATALOGO_DATASETS,
        ManifestError,
        cargar_manifest,
        validar_manifest,
        validar_compatibilidad_json,
    )


# ============================================================
# 1. CONFIGURACION
# ============================================================

VERSION_REGISTRO = "0.1"

ENTORNO_REGISTRO = "PRUEBA"

ESTADO_VERSION = "VERSION_PRUEBA_REGISTRADA"

ESTADO_REGISTRO = "REGISTRO_VERSIONES_PRUEBA"

RUTA_REGISTRO = RUTA_REGISTRO_ORIGINAL

PATRON_IDENTIFICADOR = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}"
)

PATRON_SHA256 = re.compile(
    r"[a-f0-9]{64}"
)

CAMPOS_REGISTRO = {
    "version_registro",
    "entorno",
    "estado",
    "fecha_creacion",
    "publicacion_productiva",
    "versiones",
}

CAMPOS_VERSION = {
    "dataset",
    "version",
    "version_anterior",
    "version_esquema",
    "estado",
    "entorno",
    "ruta_silver",
    "esquema",
    "clave_negocio",
    "columnas_particion",
    "cantidad_registros",
    "fecha_registro",
    "integridad",
}

CAMPOS_INTEGRIDAD = {
    "algoritmo",
    "cantidad_archivos_parquet",
    "total_bytes_parquet",
    "huella_dataset_sha256",
    "archivos",
}

CAMPOS_ARCHIVO = {
    "ruta_relativa",
    "tamano_bytes",
    "sha256",
}


# ============================================================
# 2. EXCEPCIONES
# ============================================================

class VersionRegistryError(Exception):
    """Error del registro local de versiones."""


# ============================================================
# 3. FUNCIONES AUXILIARES
# ============================================================

def exigir(condicion, mensaje):

    if not condicion:

        raise VersionRegistryError(
            mensaje
        )


def fecha_actual_utc():

    return datetime.now(
        timezone.utc
    ).isoformat()


def identificador_valido(valor):

    return (

        isinstance(valor, str)

        and PATRON_IDENTIFICADOR.fullmatch(
            valor
        ) is not None

    )


def sha256_valido(valor):

    return (

        isinstance(valor, str)

        and PATRON_SHA256.fullmatch(
            valor
        ) is not None

    )


def entero_no_negativo(valor):

    return (

        type(valor) is int

        and valor >= 0

    )


def timestamp_valido(valor):

    if not isinstance(valor, str):

        return False

    try:

        fecha = datetime.fromisoformat(
            valor.replace("Z", "+00:00")
        )

        return (

            "T" in valor

            and fecha.tzinfo is not None

            and fecha.utcoffset() is not None

        )

    except ValueError:

        return False


def json_canonico(contenido):

    return json.dumps(

        contenido,

        sort_keys=True,

        ensure_ascii=False,

        separators=(",", ":"),

        allow_nan=False,

    )


def calcular_sha256_bytes(contenido):

    return hashlib.sha256(
        contenido
    ).hexdigest()


def calcular_sha256_archivo(ruta):

    huella = hashlib.sha256()

    with ruta.open("rb") as archivo:

        while True:

            bloque = archivo.read(
                1024 * 1024
            )

            if not bloque:

                break

            huella.update(
                bloque
            )

    return huella.hexdigest()


def validar_campos(
    objeto,
    esperados,
    contexto,
):

    exigir(

        isinstance(objeto, dict),

        f"{contexto}: se esperaba objeto JSON.",

    )

    exigir(

        set(objeto) == esperados,

        f"{contexto}: campos incorrectos. "
        f"Faltantes={sorted(esperados - set(objeto))}, "
        f"adicionales={sorted(set(objeto) - esperados)}",

    )


# ============================================================
# 4. VALIDACION DE RUTAS LOCALES
# ============================================================

def rechazar_enlaces_simbolicos(ruta):

    ruta = Path(ruta).absolute()

    for componente in [
        ruta,
        *ruta.parents,
    ]:

        exigir(

            not componente.is_symlink(),

            f"Enlace simbolico no permitido: {componente}",

        )


def verificar_raiz_autorizada(
    ruta,
    raices_autorizadas,
):

    ruta_real = Path(ruta).resolve()

    for raiz in raices_autorizadas:

        raiz_real = Path(raiz).resolve()

        if ruta_real.is_relative_to(
            raiz_real
        ):

            return raiz_real

    raise VersionRegistryError(

        f"Ruta fuera de las raices autorizadas: {ruta}"

    )


def resolver_uri_local(
    uri,
    raices_autorizadas=RAICES_AUTORIZADAS,
):

    exigir(

        isinstance(uri, str),

        "La ruta Silver debe ser una cadena.",

    )

    exigir(

        uri.startswith("file:///"),

        "Solo se permiten URI locales file:///.",

    )

    exigir(

        uri == uri.strip(),

        "Espacios externos no permitidos en la ruta.",

    )

    exigir(

        not any(
            ord(caracter) < 32
            or ord(caracter) == 127
            for caracter in uri
        ),

        "Caracteres de control no permitidos.",

    )

    exigir(

        not any(
            caracter in uri
            for caracter in [
                "%",
                "\\",
                "?",
                "#",
            ]
        ),

        "Caracteres no permitidos en la URI.",

    )

    ruta_texto = uri[
        len("file://"):
    ]

    segmentos = ruta_texto.split("/")

    exigir(

        not any(
            segmento in {".", ".."}
            for segmento in segmentos
        ),

        "Segmentos de ruta no permitidos.",

    )

    ruta = Path(
        ruta_texto
    )

    exigir(

        ruta.is_absolute(),

        "Se requiere una ruta absoluta.",

    )

    rechazar_enlaces_simbolicos(
        ruta
    )

    exigir(

        ruta.is_dir(),

        f"No existe el directorio Silver: {ruta}",

    )

    ruta_real = ruta.resolve(
        strict=True
    )

    exigir(

        ruta_real.as_uri() == uri,

        "La URI debe utilizar su forma canonica.",

    )

    verificar_raiz_autorizada(

        ruta_real,

        raices_autorizadas,

    )

    return ruta_real


def validar_ruta_dataset(
    dataset,
    uri,
    raices_autorizadas=RAICES_AUTORIZADAS,
):

    exigir(

        isinstance(dataset, str)

        and dataset in CATALOGO_DATASETS,

        f"Dataset desconocido: {dataset}",

    )

    ruta = resolver_uri_local(

        uri,

        raices_autorizadas,

    )

    sufijo = CATALOGO_DATASETS[
        dataset
    ]["ruta"]

    componentes = Path(
        sufijo
    ).parts

    exigir(

        tuple(
            ruta.parts[-len(componentes):]
        ) == componentes,

        f"Ruta incompatible con dataset: {dataset}",

    )

    marcador = ruta / "_SUCCESS"

    exigir(

        marcador.is_file()

        and not marcador.is_symlink(),

        f"No existe un _SUCCESS valido: {ruta}",

    )

    return ruta


def validar_ruta_registro(ruta):

    ruta = Path(ruta).absolute()

    raiz_control = (

        DIRECTORIO_PRUEBAS

        / "control"

    )

    rechazar_enlaces_simbolicos(
        ruta
    )

    exigir(

        raiz_control.is_dir(),

        f"No existe el directorio de control: {raiz_control}",

    )

    exigir(

        ruta.parent.resolve()
        == raiz_control.resolve(),

        "El registro debe estar dentro del directorio control.",

    )

    exigir(

        ruta.suffix == ".json",

        "El registro debe ser un archivo JSON.",

    )

    return ruta


# ============================================================
# 5. INVENTARIO E INTEGRIDAD DE ARCHIVOS
# ============================================================

def inventariar_archivos_parquet(ruta):

    ruta = Path(ruta)

    exigir(

        ruta.is_dir(),

        f"No existe el dataset: {ruta}",

    )

    rechazar_enlaces_simbolicos(
        ruta
    )

    archivos = []

    for elemento in sorted(
        ruta.rglob("*")
    ):

        exigir(

            not elemento.is_symlink(),

            f"Enlace simbolico dentro del dataset: {elemento}",

        )

        if not elemento.is_file():

            continue

        if elemento.suffix != ".parquet":

            continue

        relativa = elemento.relative_to(
            ruta
        ).as_posix()

        estado_antes = elemento.stat()

        huella = calcular_sha256_archivo(
            elemento
        )

        estado_despues = elemento.stat()

        exigir(

            estado_antes.st_size
            == estado_despues.st_size

            and estado_antes.st_mtime_ns
            == estado_despues.st_mtime_ns

            and estado_antes.st_ino
            == estado_despues.st_ino,

            f"Archivo modificado durante la lectura: {elemento}",

        )

        exigir(

            estado_despues.st_size > 0,

            f"Archivo Parquet vacio: {elemento}",

        )

        archivos.append({

            "ruta_relativa": relativa,

            "tamano_bytes": (
                estado_despues.st_size
            ),

            "sha256": huella,

        })

    exigir(

        bool(archivos),

        f"No se encontraron archivos Parquet: {ruta}",

    )

    return sorted(

        archivos,

        key=lambda item: item["ruta_relativa"],

    )


def construir_integridad(ruta):

    archivos = inventariar_archivos_parquet(
        ruta
    )

    huella_dataset = calcular_sha256_bytes(

        json_canonico(
            archivos
        ).encode("utf-8")

    )

    return {

        "algoritmo": "SHA-256",

        "cantidad_archivos_parquet": len(
            archivos
        ),

        "total_bytes_parquet": sum(

            item["tamano_bytes"]

            for item in archivos

        ),

        "huella_dataset_sha256": (
            huella_dataset
        ),

        "archivos": archivos,

    }


def validar_integridad_declarada(
    integridad,
):

    validar_campos(

        integridad,

        CAMPOS_INTEGRIDAD,

        "integridad",

    )

    exigir(

        integridad["algoritmo"] == "SHA-256",

        "Algoritmo de integridad incompatible.",

    )

    exigir(

        entero_no_negativo(
            integridad["cantidad_archivos_parquet"]
        ),

        "Cantidad de archivos invalida.",

    )

    exigir(

        entero_no_negativo(
            integridad["total_bytes_parquet"]
        ),

        "Tamano total invalido.",

    )

    exigir(

        sha256_valido(
            integridad["huella_dataset_sha256"]
        ),

        "Huella de dataset invalida.",

    )

    archivos = integridad[
        "archivos"
    ]

    exigir(

        isinstance(archivos, list)

        and bool(archivos),

        "Inventario de archivos invalido.",

    )

    rutas_vistas = set()

    for archivo in archivos:

        validar_campos(

            archivo,

            CAMPOS_ARCHIVO,

            "archivo Parquet",

        )

        relativa = archivo[
            "ruta_relativa"
        ]

        exigir(

            isinstance(relativa, str)

            and bool(relativa),

            "Ruta relativa invalida.",

        )

        exigir(

            not Path(relativa).is_absolute(),

            "No se permiten rutas absolutas en el inventario.",

        )

        exigir(

            not any(

                segmento in {
                    "",
                    ".",
                    "..",
                }

                for segmento in relativa.split("/")

            ),

            "Segmentos invalidos en ruta relativa.",

        )

        exigir(

            "\\" not in relativa,

            "Barra invertida no permitida.",

        )

        exigir(

            relativa.endswith(".parquet"),

            "Archivo no reconocido como Parquet.",

        )

        exigir(

            relativa not in rutas_vistas,

            f"Archivo duplicado: {relativa}",

        )

        rutas_vistas.add(
            relativa
        )

        exigir(

            entero_no_negativo(
                archivo["tamano_bytes"]
            )

            and archivo["tamano_bytes"] > 0,

            f"Tamano invalido: {relativa}",

        )

        exigir(

            sha256_valido(
                archivo["sha256"]
            ),

            f"Huella invalida: {relativa}",

        )

    exigir(

        archivos == sorted(

            archivos,

            key=lambda item: item["ruta_relativa"],

        ),

        "El inventario debe estar ordenado.",

    )

    exigir(

        len(archivos)

        == integridad["cantidad_archivos_parquet"],

        "Cantidad de archivos inconsistente.",

    )

    exigir(

        sum(

            item["tamano_bytes"]

            for item in archivos

        )

        == integridad["total_bytes_parquet"],

        "Tamano total inconsistente.",

    )

    esperada = calcular_sha256_bytes(

        json_canonico(
            archivos
        ).encode("utf-8")

    )

    exigir(

        esperada == integridad["huella_dataset_sha256"],

        "Huella de inventario inconsistente.",

    )


# ============================================================
# 6. CREACION DE REGISTROS
# ============================================================

def crear_registro_vacio():

    return {

        "version_registro": VERSION_REGISTRO,

        "entorno": ENTORNO_REGISTRO,

        "estado": ESTADO_REGISTRO,

        "fecha_creacion": fecha_actual_utc(),

        "publicacion_productiva": False,

        "versiones": [],

    }


def construir_version(

    dataset,

    version,

    version_anterior,

    version_esquema,

    ruta_silver,

    esquema,

    cantidad_registros,

    raices_autorizadas=RAICES_AUTORIZADAS,

):

    ruta = validar_ruta_dataset(

        dataset,

        ruta_silver,

        raices_autorizadas,

    )

    configuracion = CATALOGO_DATASETS[
        dataset
    ]

    exigir(

        configuracion["particiones"] is not None,

        "Las particiones de este dataset "
        "todavia no estan confirmadas.",

    )

    registro = {

        "dataset": dataset,

        "version": version,

        "version_anterior": version_anterior,

        "version_esquema": version_esquema,

        "estado": ESTADO_VERSION,

        "entorno": ENTORNO_REGISTRO,

        "ruta_silver": ruta.as_uri(),

        "esquema": dict(esquema),

        "clave_negocio": list(
            configuracion["clave"]
        ),

        "columnas_particion": list(
            configuracion["particiones"]
        ),

        "cantidad_registros": cantidad_registros,

        "fecha_registro": fecha_actual_utc(),

        "integridad": construir_integridad(
            ruta
        ),

    }

    validar_version(

        registro,

        raices_autorizadas,

    )

    return registro


# ============================================================
# 7. VALIDACION DE UNA VERSION
# ============================================================

def validar_version(

    version,

    raices_autorizadas=RAICES_AUTORIZADAS,

):

    validar_campos(

        version,

        CAMPOS_VERSION,

        "version Silver",

    )

    dataset = version[
        "dataset"
    ]

    exigir(

        isinstance(dataset, str)

        and dataset in CATALOGO_DATASETS,

        f"Dataset desconocido: {dataset}",

    )

    configuracion = CATALOGO_DATASETS[
        dataset
    ]

    exigir(

        identificador_valido(
            version["version"]
        ),

        "Identificador de version invalido.",

    )

    anterior = version[
        "version_anterior"
    ]

    exigir(

        anterior is None

        or identificador_valido(anterior),

        "Identificador de version anterior invalido.",

    )

    exigir(

        anterior != version["version"],

        "Una version no puede ser su propio antecedente.",

    )

    exigir(

        identificador_valido(
            version["version_esquema"]
        ),

        "Identificador de esquema invalido.",

    )

    exigir(

        version["entorno"] == ENTORNO_REGISTRO,

        "Entorno de version incompatible.",

    )

    exigir(

        version["estado"] == ESTADO_VERSION,

        "Estado de version incompatible.",

    )

    exigir(

        timestamp_valido(
            version["fecha_registro"]
        ),

        "Fecha de registro invalida.",

    )

    exigir(

        entero_no_negativo(
            version["cantidad_registros"]
        ),

        "Cantidad de registros invalida.",

    )

    esquema = version[
        "esquema"
    ]

    exigir(

        isinstance(esquema, dict)

        and bool(esquema),

        "Esquema invalido.",

    )

    for nombre, tipo in esquema.items():

        exigir(

            isinstance(nombre, str)

            and bool(nombre.strip())

            and isinstance(tipo, str)

            and bool(tipo.strip()),

            "Columna o tipo de esquema invalido.",

        )

    exigir(

        version["clave_negocio"]
        == configuracion["clave"],

        "Clave de negocio incompatible.",

    )

    exigir(

        configuracion["particiones"] is not None,

        "Particiones pendientes de confirmacion.",

    )

    exigir(

        version["columnas_particion"]
        == configuracion["particiones"],

        "Columnas de particion incompatibles.",

    )

    for columna in (

        version["clave_negocio"]

        + version["columnas_particion"]

    ):

        exigir(

            columna in esquema,

            f"Columna ausente del esquema: {columna}",

        )

    validar_ruta_dataset(

        dataset,

        version["ruta_silver"],

        raices_autorizadas,

    )

    validar_integridad_declarada(

        version["integridad"]

    )


# ============================================================
# 8. VALIDACION GENERAL DEL REGISTRO
# ============================================================

def construir_indice(registro):

    indice = {}

    for version in registro["versiones"]:

        clave = (

            version["dataset"],

            version["version"],

        )

        exigir(

            clave not in indice,

            f"Version duplicada: {clave}",

        )

        indice[clave] = version

    return indice


def validar_relaciones_versiones(indice):

    for clave, version in indice.items():

        anterior = version[
            "version_anterior"
        ]

        if anterior is None:

            continue

        clave_anterior = (

            version["dataset"],

            anterior,

        )

        exigir(

            clave_anterior in indice,

            f"Version anterior desconocida: {clave_anterior}",

        )

        version_anterior = indice[
            clave_anterior
        ]

        exigir(

            version["version_esquema"]
            == version_anterior["version_esquema"],

            f"Cambio de esquema no soportado: {clave}",

        )

        exigir(

            version["esquema"]
            == version_anterior["esquema"],

            f"Esquemas incompatibles: {clave}",

        )

    # Verificar que no existan ciclos.

    for clave in indice:

        visitadas = set()

        actual = clave

        while actual is not None:

            exigir(

                actual not in visitadas,

                f"Ciclo de versiones detectado: {actual}",

            )

            visitadas.add(
                actual
            )

            version = indice[
                actual
            ]

            anterior = version[
                "version_anterior"
            ]

            actual = (

                (
                    version["dataset"],
                    anterior,
                )

                if anterior is not None

                else None

            )


def validar_registro(

    registro,

    raices_autorizadas=RAICES_AUTORIZADAS,

):

    errores_json = validar_compatibilidad_json(
        registro
    )

    exigir(

        not errores_json,

        f"Registro JSON invalido: {errores_json}",

    )

    validar_campos(

        registro,

        CAMPOS_REGISTRO,

        "registro",

    )

    exigir(

        registro["version_registro"]
        == VERSION_REGISTRO,

        "Version de registro no soportada.",

    )

    exigir(

        registro["entorno"]
        == ENTORNO_REGISTRO,

        "Entorno de registro incompatible.",

    )

    exigir(

        registro["estado"]
        == ESTADO_REGISTRO,

        "Estado de registro incompatible.",

    )

    exigir(

        registro["publicacion_productiva"]
        is False,

        "El registro no permite produccion.",

    )

    exigir(

        timestamp_valido(
            registro["fecha_creacion"]
        ),

        "Fecha de creacion invalida.",

    )

    versiones = registro[
        "versiones"
    ]

    exigir(

        isinstance(versiones, list),

        "versiones debe ser una lista.",

    )

    rutas_vistas = set()

    for version in versiones:

        validar_version(

            version,

            raices_autorizadas,

        )

        ruta = version[
            "ruta_silver"
        ]

        exigir(

            ruta not in rutas_vistas,

            f"Ruta reutilizada por distintas versiones: {ruta}",

        )

        rutas_vistas.add(
            ruta
        )

    indice = construir_indice(
        registro
    )

    validar_relaciones_versiones(
        indice
    )

    return indice


# ============================================================
# 9. CARGA Y ESCRITURA DEL REGISTRO
# ============================================================

def guardar_registro_nuevo(

    registro,

    ruta=RUTA_REGISTRO,

):

    ruta = validar_ruta_registro(
        ruta
    )

    validar_registro(
        registro
    )

    exigir(

        not ruta.exists()

        and not ruta.is_symlink(),

        f"El registro ya existe: {ruta}",

    )

    contenido = json.dumps(

        registro,

        indent=4,

        ensure_ascii=False,

        allow_nan=False,

    ) + "\n"

    with ruta.open(

        "x",

        encoding="utf-8",

    ) as archivo:

        archivo.write(
            contenido
        )

    registro_guardado, huella = cargar_registro(
        ruta
    )

    exigir(

        registro_guardado == registro,

        "El registro persistido no coincide.",

    )

    return huella


def cargar_registro(

    ruta=RUTA_REGISTRO,

):

    ruta = validar_ruta_registro(
        ruta
    )

    exigir(

        ruta.is_file(),

        f"No existe el registro: {ruta}",

    )

    registro, huella = cargar_manifest(
        ruta
    )

    validar_registro(
        registro
    )

    return registro, huella


# ============================================================
# 10. RESOLUCION DE VERSIONES
# ============================================================

def obtener_version(

    registro,

    dataset,

    version,

    raices_autorizadas=RAICES_AUTORIZADAS,

):

    exigir(

        isinstance(dataset, str)

        and isinstance(version, str),

        "Dataset o version invalida.",

    )

    indice = validar_registro(

        registro,

        raices_autorizadas,

    )

    clave = (
        dataset,
        version,
    )

    exigir(

        clave in indice,

        f"Version no registrada: {clave}",

    )

    return json.loads(

        json_canonico(
            indice[clave]
        )

    )


def verificar_integridad_version(

    version,

    raices_autorizadas=RAICES_AUTORIZADAS,

):

    validar_version(

        version,

        raices_autorizadas,

    )

    ruta = validar_ruta_dataset(

        version["dataset"],

        version["ruta_silver"],

        raices_autorizadas,

    )

    integridad_actual = construir_integridad(
        ruta
    )

    exigir(

        integridad_actual == version["integridad"],

        "Integridad de version incompatible: "
        f"{version['dataset']} / {version['version']}",

    )

    return {

        "dataset": version["dataset"],

        "version": version["version"],

        "estado": "INTEGRIDAD_VERIFICADA",

        "ruta_silver": version["ruta_silver"],

        "cantidad_archivos_parquet": (
            integridad_actual[
                "cantidad_archivos_parquet"
            ]
        ),

        "total_bytes_parquet": (
            integridad_actual[
                "total_bytes_parquet"
            ]
        ),

        "huella_dataset_sha256": (
            integridad_actual[
                "huella_dataset_sha256"
            ]
        ),

        "integridad_verificada": True,

    }


# ============================================================
# 11. RESOLVER VERSIONES DE UN MANIFEST
# ============================================================

def resolver_dataset_manifest(

    registro,

    dataset_manifest,

    verificar_integridad=True,

):

    nombre = dataset_manifest[
        "dataset"
    ]

    anterior = obtener_version(

        registro,

        nombre,

        dataset_manifest[
            "version_anterior"
        ],

    )

    publicada = obtener_version(

        registro,

        nombre,

        dataset_manifest[
            "version_publicada"
        ],

    )

    exigir(

        dataset_manifest["ruta_silver"]
        == publicada["ruta_silver"],

        f"{nombre}: ruta publicada incompatible.",

    )

    exigir(

        dataset_manifest["version_esquema"]
        == anterior["version_esquema"]
        == publicada["version_esquema"],

        f"{nombre}: version de esquema incompatible.",

    )

    exigir(

        dataset_manifest["clave"]
        == anterior["clave_negocio"]
        == publicada["clave_negocio"],

        f"{nombre}: clave de negocio incompatible.",

    )

    exigir(

        dataset_manifest["registros_antes"]
        == anterior["cantidad_registros"],

        f"{nombre}: conteo anterior incompatible.",

    )

    exigir(

        dataset_manifest["registros_despues"]
        == publicada["cantidad_registros"],

        f"{nombre}: conteo publicado incompatible.",

    )

    total_cambios = (

        dataset_manifest["registros_nuevos"]

        + dataset_manifest["registros_corregidos"]

        + dataset_manifest["registros_eliminados"]

    )

    if anterior["version"] != publicada["version"]:

        exigir(

            publicada["version_anterior"]
            == anterior["version"],

            f"{nombre}: relacion entre versiones incompatible.",

        )

    else:

        exigir(

            total_cambios == 0,

            f"{nombre}: existen cambios sin nueva version.",

        )

    integridad_anterior = None

    integridad_publicada = None

    if verificar_integridad:

        integridad_anterior = (
            verificar_integridad_version(
                anterior
            )
        )

        if (
            anterior["version"]
            == publicada["version"]
        ):

            integridad_publicada = (
                integridad_anterior.copy()
            )

        else:

            integridad_publicada = (
                verificar_integridad_version(
                    publicada
                )
            )

    return {

        "dataset": nombre,

        "version_anterior": anterior,

        "version_publicada": publicada,

        "integridad_anterior": integridad_anterior,

        "integridad_publicada": integridad_publicada,

        "versiones_resueltas": True,

        "integridad_verificada": (
            verificar_integridad
        ),

        "esquema_fisico_verificado": False,

        "conteos_fisicos_verificados": False,

        "cambios_efectivos_verificados": False,

    }


def resolver_manifest(

    registro,

    manifest,

    verificar_integridad=True,

):

    errores, advertencias = validar_manifest(

        manifest,

        entorno_esperado="PRUEBA",

    )

    exigir(

        not errores,

        f"Manifest estructuralmente invalido: {errores}",

    )

    validar_registro(
        registro
    )

    resultados = []

    for dataset in manifest["datasets"]:

        resultado = resolver_dataset_manifest(

            registro,

            dataset,

            verificar_integridad,

        )

        resultados.append(
            resultado
        )

    return {

        "estado": "VERSIONES_RESUELTAS",

        "entorno": "PRUEBA",

        "run_id": manifest["run_id"],

        "version_registro": VERSION_REGISTRO,

        "validacion_estructural_aprobada": True,

        "versiones_resueltas": True,

        "integridad_verificada": verificar_integridad,

        "validacion_operativa_aprobada": False,

        "autorizacion_ejecucion_gold": False,

        "autorizacion_publicacion_gold": False,

        "datasets": resultados,

        "advertencias": advertencias,

    }


# ============================================================
# 12. RESUMEN DEL REGISTRO
# ============================================================

def resumir_registro(registro):

    validar_registro(
        registro
    )

    versiones = []

    for version in registro["versiones"]:

        integridad = version[
            "integridad"
        ]

        versiones.append({

            "dataset": version["dataset"],

            "version": version["version"],

            "version_anterior": (
                version["version_anterior"]
            ),

            "version_esquema": (
                version["version_esquema"]
            ),

            "ruta_silver": version["ruta_silver"],

            "cantidad_registros": (
                version["cantidad_registros"]
            ),

            "cantidad_archivos_parquet": (
                integridad[
                    "cantidad_archivos_parquet"
                ]
            ),

            "huella_dataset_sha256": (
                integridad[
                    "huella_dataset_sha256"
                ]
            ),

        })

    return {

        "estado": "REGISTRO_ESTRUCTURA_VALIDADA",

        "version_registro": (
            registro["version_registro"]
        ),

        "entorno": registro["entorno"],

        "cantidad_versiones": len(
            versiones
        ),

        "versiones": versiones,

        "autorizacion_ejecucion_gold": False,

        "autorizacion_publicacion_gold": False,

    }


def resumir_resolucion(resultado):

    datasets = []

    for item in resultado["datasets"]:

        anterior = item[
            "version_anterior"
        ]

        publicada = item[
            "version_publicada"
        ]

        datasets.append({

            "dataset": item["dataset"],

            "version_anterior": anterior["version"],

            "version_publicada": publicada["version"],

            "ruta_anterior": anterior["ruta_silver"],

            "ruta_publicada": publicada["ruta_silver"],

            "registros_antes_declarados": (
                anterior["cantidad_registros"]
            ),

            "registros_despues_declarados": (
                publicada["cantidad_registros"]
            ),

            "integridad_verificada": (
                item["integridad_verificada"]
            ),

            "cambios_efectivos_verificados": False,

        })

    return {

        "estado": resultado["estado"],

        "run_id": resultado["run_id"],

        "entorno": resultado["entorno"],

        "versiones_resueltas": (
            resultado["versiones_resueltas"]
        ),

        "integridad_verificada": (
            resultado["integridad_verificada"]
        ),

        "validacion_operativa_aprobada": False,

        "autorizacion_ejecucion_gold": False,

        "autorizacion_publicacion_gold": False,

        "datasets": datasets,

        "advertencias": resultado["advertencias"],

    }


# ============================================================
# 13. INTERFAZ CLI
# ============================================================

def main():

    parser = argparse.ArgumentParser(

        description=(

            "Registro local de versiones Silver "
            "para BVL Analytics."

        )

    )

    parser.add_argument(

        "--registro",

        default=str(
            RUTA_REGISTRO
        ),

        help="Ruta del registro local de versiones.",

    )

    acciones = parser.add_mutually_exclusive_group(

        required=True

    )

    acciones.add_argument(

        "--mostrar-registro",

        action="store_true",

        help="Mostrar resumen de versiones registradas.",

    )

    acciones.add_argument(

        "--verificar-integridad",

        action="store_true",

        help="Verificar los archivos de todas las versiones.",

    )

    acciones.add_argument(

        "--manifest",

        help="Resolver las versiones de un manifest.",

    )

    parser.add_argument(

        "--resumen",

        action="store_true",

        help="Mostrar una resolucion resumida.",

    )

    argumentos = parser.parse_args()

    try:

        registro, huella_registro = cargar_registro(

            argumentos.registro

        )

        if argumentos.mostrar_registro:

            resultado = resumir_registro(
                registro
            )

        elif argumentos.verificar_integridad:

            versiones = []

            for version in registro["versiones"]:

                versiones.append(

                    verificar_integridad_version(
                        version
                    )

                )

            resultado = {

                "estado": "INTEGRIDAD_REGISTRO_VERIFICADA",

                "entorno": "PRUEBA",

                "cantidad_versiones": len(
                    versiones
                ),

                "versiones": versiones,

                "integridad_verificada": True,

                "validacion_operativa_aprobada": False,

                "autorizacion_ejecucion_gold": False,

            }

        else:

            manifest, huella_manifest = cargar_manifest(

                argumentos.manifest

            )

            resultado = resolver_manifest(

                registro,

                manifest,

                verificar_integridad=True,

            )

            if argumentos.resumen:

                resultado = resumir_resolucion(
                    resultado
                )

            resultado["huella_manifest_sha256"] = (
                huella_manifest
            )

        resultado["huella_registro_sha256"] = (
            huella_registro
        )

        print(

            json.dumps(

                resultado,

                indent=4,

                ensure_ascii=False,

                allow_nan=False,

            )

        )

        return 0

    except (

        VersionRegistryError,

        ManifestError,

        OSError,

        ValueError,

        TypeError,

        KeyError,

        OverflowError,

        RecursionError,

    ) as exc:

        resultado = {

            "estado": "GOLD_RECHAZADO",

            "entorno": "PRUEBA",

            "versiones_resueltas": False,

            "integridad_verificada": False,

            "validacion_operativa_aprobada": False,

            "autorizacion_ejecucion_gold": False,

            "autorizacion_publicacion_gold": False,

            "errores": [
                str(exc)
            ],

        }

        print(

            json.dumps(

                resultado,

                indent=4,

                ensure_ascii=True,

                allow_nan=False,

            )

        )

        return 1


if __name__ == "__main__":

    sys.exit(
        main()
    )
