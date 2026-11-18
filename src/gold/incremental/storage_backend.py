#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics Platform
Abstraccion de almacenamiento LOCAL y GCS.

Responsabilidades:
- Leer archivos y objetos.
- Escribir archivos nuevos sin sobrescribir.
- Leer y escribir documentos JSON.
- Inventariar archivos Parquet.
- Calcular huellas SHA-256.
- Copiar archivos entre rutas autorizadas.
- Conservar compatibilidad con el contrato v0.1.

Limitaciones:
- Solo entorno PRUEBA.
- No publica versiones productivas.
- No elimina datasets.
- No crea buckets.
- No ejecuta Spark.
"""

import hashlib
import json
import os
import stat
import tempfile

from pathlib import Path
from urllib.parse import urlsplit

if __package__:

    from .storage_config import (
        BACKEND_LOCAL,
        BACKEND_GCS,
        StorageConfig,
        cargar_configuracion,
        validar_uri,
        validar_ruta_relativa,
        uri_bajo_raiz,
        unir_uri,
        uri_local,
    )

else:

    from storage_config import (
        BACKEND_LOCAL,
        BACKEND_GCS,
        StorageConfig,
        cargar_configuracion,
        validar_uri,
        validar_ruta_relativa,
        uri_bajo_raiz,
        unir_uri,
        uri_local,
    )


# ============================================================
# 1. CONSTANTES
# ============================================================

VERSION_BACKEND = "0.1"

TAMANO_BLOQUE = 1024 * 1024


# ============================================================
# 2. EXCEPCIONES
# ============================================================

class StorageBackendError(Exception):
    """Error de almacenamiento Gold."""


def exigir(condicion, mensaje):

    if not condicion:

        raise StorageBackendError(mensaje)


# ============================================================
# 3. FUNCIONES DE INTEGRIDAD
# ============================================================

def calcular_sha256_bytes(contenido):

    exigir(
        isinstance(contenido, bytes),
        "Se esperaba contenido binario.",
    )

    return hashlib.sha256(
        contenido
    ).hexdigest()


def calcular_sha256_stream(archivo):

    huella = hashlib.sha256()

    cantidad_bytes = 0

    while True:

        bloque = archivo.read(
            TAMANO_BLOQUE
        )

        if not bloque:

            break

        huella.update(bloque)

        cantidad_bytes += len(bloque)

    return {

        "sha256": huella.hexdigest(),

        "tamano_bytes": cantidad_bytes,

    }


def construir_integridad_archivos(archivos):

    exigir(
        isinstance(archivos, list)
        and bool(archivos),
        "El inventario Parquet esta vacio.",
    )

    ordenados = sorted(

        archivos,

        key=lambda item: item["ruta_relativa"],

    )

    rutas = set()

    for archivo in ordenados:

        relativa = validar_ruta_relativa(
            archivo["ruta_relativa"]
        )

        exigir(
            relativa.endswith(".parquet"),
            "Archivo incompatible con Parquet.",
        )

        exigir(
            relativa not in rutas,
            f"Archivo duplicado: {relativa}",
        )

        exigir(
            type(archivo["tamano_bytes"]) is int
            and archivo["tamano_bytes"] > 0,
            f"Archivo vacio o invalido: {relativa}",
        )

        rutas.add(relativa)

    contenido = json.dumps(

        ordenados,

        sort_keys=True,

        ensure_ascii=False,

        separators=(",", ":"),

        allow_nan=False,

    ).encode("utf-8")

    return {

        "algoritmo": "SHA-256",

        "cantidad_archivos_parquet": len(
            ordenados
        ),

        "total_bytes_parquet": sum(

            item["tamano_bytes"]

            for item in ordenados

        ),

        "huella_dataset_sha256": (
            calcular_sha256_bytes(contenido)
        ),

        "archivos": ordenados,

    }


# ============================================================
# 4. LECTURA JSON ESTRICTA
# ============================================================

def rechazar_constante_json(valor):

    raise StorageBackendError(
        f"Constante JSON no permitida: {valor}"
    )


def verificar_pares_json(pares):

    resultado = {}

    for clave, valor in pares:

        exigir(
            clave not in resultado,
            f"Clave JSON duplicada: {clave}",
        )

        resultado[clave] = valor

    return resultado


# ============================================================
# 5. INTERFAZ COMUN
# ============================================================

class StorageBackend:

    def __init__(self, config):

        exigir(
            isinstance(config, StorageConfig),
            "Configuracion de almacenamiento invalida.",
        )

        exigir(
            config.entorno == "PRUEBA",
            "Solo se permite el entorno PRUEBA.",
        )

        exigir(
            config.publicacion_productiva is False,
            "La publicacion productiva esta bloqueada.",
        )

        self.config = config

    @property
    def backend(self):

        return self.config.backend

    @property
    def raices_lectura(self):

        return (

            self.config.silver_original,

            self.config.silver_versiones,

            self.config.gold_historico,

            self.config.gold_candidatas,

            self.config.control,

            self.config.manifests,

            self.config.reportes,

        )

    @property
    def raices_escritura(self):

        return (

            self.config.gold_candidatas,

            self.config.reportes,

        )

    @property
    def raices_protegidas(self):

        return (

            self.config.silver_original,

            self.config.silver_versiones,

            self.config.gold_historico,

            self.config.manifests,

            self.config.registro_original,

            self.config.registro_integracion,

        )

    def verificar_lectura(self, uri):

        validar_uri(
            uri,
            self.backend,
        )

        exigir(

            any(

                uri_bajo_raiz(
                    uri,
                    raiz,
                    self.backend,
                )

                for raiz in self.raices_lectura

            ),

            f"Lectura fuera de rutas autorizadas: {uri}",

        )

        return uri

    def verificar_escritura(self, uri):

        validar_uri(
            uri,
            self.backend,
        )

        exigir(

            any(

                uri != raiz

                and uri_bajo_raiz(
                    uri,
                    raiz,
                    self.backend,
                )

                for raiz in self.raices_escritura

            ),

            f"Escritura fuera de rutas autorizadas: {uri}",

        )

        exigir(

            not any(

                uri_bajo_raiz(
                    uri,
                    raiz,
                    self.backend,
                )

                for raiz in self.raices_protegidas

            ),

            f"Escritura sobre ruta protegida: {uri}",

        )

        return uri

    def leer_bytes(self, uri):

        raise NotImplementedError

    def escribir_bytes_nuevo(self, uri, contenido):

        raise NotImplementedError

    def existe_archivo(self, uri):

        raise NotImplementedError

    def calcular_integridad_archivo(self, uri):

        raise NotImplementedError

    def listar_parquet(self, raiz):

        raise NotImplementedError

    def copiar_archivo_nuevo(self, origen, destino):

        raise NotImplementedError

    def leer_json(self, uri):

        contenido = self.leer_bytes(uri)

        documento = json.loads(

            contenido.decode("utf-8"),

            object_pairs_hook=verificar_pares_json,

            parse_constant=rechazar_constante_json,

        )

        exigir(
            isinstance(documento, dict),
            "Se esperaba un objeto JSON.",
        )

        return (

            documento,

            calcular_sha256_bytes(contenido),

        )

    def escribir_json_nuevo(self, uri, documento):

        exigir(
            uri.endswith(".json"),
            "La salida debe ser un archivo JSON.",
        )

        exigir(
            isinstance(documento, dict),
            "Se esperaba un objeto JSON.",
        )

        contenido = (

            json.dumps(

                documento,

                indent=4,

                ensure_ascii=False,

                allow_nan=False,

            ) + "\n"

        ).encode("utf-8")

        self.escribir_bytes_nuevo(

            uri,

            contenido,

        )

        guardado, huella = self.leer_json(uri)

        exigir(
            guardado == documento,
            "El documento persistido no coincide.",
        )

        exigir(
            huella == calcular_sha256_bytes(contenido),
            "La huella del documento no coincide.",
        )

        return huella

    def verificar_success(self, raiz):

        marcador = unir_uri(
            raiz,
            "_SUCCESS",
        )

        exigir(

            self.existe_archivo(marcador),

            f"No existe marcador _SUCCESS: {raiz}",

        )

        return True

    def construir_integridad(self, raiz):

        return construir_integridad_archivos(

            self.listar_parquet(raiz)

        )

    def verificar_integridad(self, raiz, esperada):

        actual = self.construir_integridad(raiz)

        exigir(
            actual == esperada,
            f"Integridad incompatible: {raiz}",
        )

        return {

            "estado": "INTEGRIDAD_VERIFICADA",

            "ruta": raiz,

            "cantidad_archivos_parquet": (
                actual["cantidad_archivos_parquet"]
            ),

            "total_bytes_parquet": (
                actual["total_bytes_parquet"]
            ),

            "huella_dataset_sha256": (
                actual["huella_dataset_sha256"]
            ),

            "integridad_verificada": True,

        }


# ============================================================
# 6. BACKEND LOCAL
# ============================================================

class LocalStorageBackend(StorageBackend):

    def __init__(self, config):

        super().__init__(config)

        exigir(
            config.backend == BACKEND_LOCAL,
            "Se requiere configuracion LOCAL.",
        )

    @staticmethod
    def rechazar_enlaces_simbolicos(ruta):

        for componente in (

            ruta,

            *ruta.parents,

        ):

            exigir(
                not componente.is_symlink(),
                f"Enlace simbolico no permitido: {componente}",
            )

    def resolver_ruta(self, uri, escritura=False):

        if escritura:

            self.verificar_escritura(uri)

        else:

            self.verificar_lectura(uri)

        ruta = Path(

            urlsplit(uri).path

        )

        self.rechazar_enlaces_simbolicos(
            ruta
        )

        exigir(
            ruta.is_absolute(),
            "La ruta debe ser absoluta.",
        )

        exigir(
            uri_local(ruta) == uri,
            "La URI local no es canonica.",
        )

        return ruta

    @staticmethod
    def firma_archivo(estado):

        return (

            estado.st_dev,

            estado.st_ino,

            estado.st_size,

            estado.st_mtime_ns,

            estado.st_ctime_ns,

        )

    def existe_archivo(self, uri):

        ruta = self.resolver_ruta(uri)

        return ruta.is_file()

    def leer_bytes(self, uri):

        ruta = self.resolver_ruta(uri)

        exigir(
            ruta.is_file(),
            f"No existe el archivo: {ruta}",
        )

        antes = ruta.stat()

        with ruta.open("rb") as archivo:

            contenido = archivo.read()

            durante = os.fstat(
                archivo.fileno()
            )

        despues = ruta.stat()

        exigir(

            self.firma_archivo(antes)

            == self.firma_archivo(durante)

            == self.firma_archivo(despues),

            f"Archivo modificado durante lectura: {ruta}",

        )

        return contenido

    def escribir_bytes_nuevo(self, uri, contenido):

        exigir(
            isinstance(contenido, bytes),
            "Se esperaba contenido binario.",
        )

        destino = self.resolver_ruta(

            uri,

            escritura=True,

        )

        exigir(
            not destino.exists(),
            f"El archivo ya existe: {destino}",
        )

        destino.parent.mkdir(

            parents=True,

            exist_ok=True,

        )

        self.rechazar_enlaces_simbolicos(
            destino
        )

        temporal = None

        try:

            with tempfile.NamedTemporaryFile(

                mode="wb",

                prefix=".bvl_temporal_",

                dir=destino.parent,

                delete=False,

            ) as archivo:

                temporal = Path(archivo.name)

                archivo.write(contenido)

                archivo.flush()

                os.fsync(
                    archivo.fileno()
                )

            # Crea el destino sin reemplazar uno existente.
            os.link(
                temporal,
                destino,
            )

        finally:

            if temporal is not None:

                temporal.unlink(
                    missing_ok=True
                )

        return uri

    def calcular_integridad_archivo(self, uri):

        ruta = self.resolver_ruta(uri)

        exigir(
            ruta.is_file(),
            f"No existe el archivo: {ruta}",
        )

        antes = ruta.stat()

        exigir(
            stat.S_ISREG(antes.st_mode),
            "Se esperaba un archivo regular.",
        )

        with ruta.open("rb") as archivo:

            resultado = calcular_sha256_stream(
                archivo
            )

            durante = os.fstat(
                archivo.fileno()
            )

        despues = ruta.stat()

        exigir(

            self.firma_archivo(antes)

            == self.firma_archivo(durante)

            == self.firma_archivo(despues),

            f"Archivo modificado durante lectura: {ruta}",

        )

        exigir(

            resultado["tamano_bytes"]
            == despues.st_size,

            "Tamano de archivo inconsistente.",

        )

        return resultado

    def inventario_firmas(self, raiz):

        resultado = {}

        for elemento in sorted(
            raiz.rglob("*")
        ):

            self.rechazar_enlaces_simbolicos(
                elemento
            )

            if not elemento.is_file():

                continue

            if elemento.suffix != ".parquet":

                continue

            relativa = elemento.relative_to(
                raiz
            ).as_posix()

            validar_ruta_relativa(relativa)

            resultado[relativa] = (

                self.firma_archivo(
                    elemento.stat()
                )

            )

        return resultado

    def listar_parquet(self, raiz):

        directorio = self.resolver_ruta(raiz)

        exigir(
            directorio.is_dir(),
            f"No existe el directorio: {directorio}",
        )

        firmas_antes = self.inventario_firmas(
            directorio
        )

        archivos = []

        for relativa in firmas_antes:

            uri = unir_uri(
                raiz,
                relativa,
            )

            integridad = (
                self.calcular_integridad_archivo(uri)
            )

            archivos.append({

                "ruta_relativa": relativa,

                "tamano_bytes": (
                    integridad["tamano_bytes"]
                ),

                "sha256": integridad["sha256"],

            })

        firmas_despues = self.inventario_firmas(
            directorio
        )

        exigir(

            firmas_antes == firmas_despues,

            "El dataset cambio durante el inventario.",

        )

        exigir(
            bool(archivos),
            "No se encontraron archivos Parquet.",
        )

        return archivos

    def copiar_archivo_nuevo(self, origen, destino):

        self.verificar_lectura(origen)

        self.verificar_escritura(destino)

        exigir(
            origen != destino,
            "Origen y destino deben ser distintos.",
        )

        ruta_origen = self.resolver_ruta(origen)

        ruta_destino = self.resolver_ruta(
            destino,
            escritura=True,
        )

        exigir(
            ruta_origen.is_file(),
            "No existe el archivo de origen.",
        )

        exigir(
            not ruta_destino.exists(),
            "El archivo de destino ya existe.",
        )

        ruta_destino.parent.mkdir(

            parents=True,

            exist_ok=True,

        )

        self.rechazar_enlaces_simbolicos(
            ruta_destino
        )

        antes = ruta_origen.stat()

        huella = hashlib.sha256()

        cantidad = 0

        temporal = None

        try:

            with ruta_origen.open("rb") as entrada:

                with tempfile.NamedTemporaryFile(

                    mode="wb",

                    prefix=".bvl_copia_",

                    dir=ruta_destino.parent,

                    delete=False,

                ) as salida:

                    temporal = Path(salida.name)

                    while True:

                        bloque = entrada.read(
                            TAMANO_BLOQUE
                        )

                        if not bloque:

                            break

                        salida.write(bloque)

                        huella.update(bloque)

                        cantidad += len(bloque)

                    salida.flush()

                    os.fsync(salida.fileno())

                durante = os.fstat(
                    entrada.fileno()
                )

            despues = ruta_origen.stat()

            exigir(

                self.firma_archivo(antes)

                == self.firma_archivo(durante)

                == self.firma_archivo(despues),

                "El archivo origen cambio durante la copia.",

            )

            exigir(
                cantidad == antes.st_size,
                "Tamano copiado inconsistente.",
            )

            os.link(
                temporal,
                ruta_destino,
            )

        finally:

            if temporal is not None:

                temporal.unlink(
                    missing_ok=True
                )

        integridad_destino = (
            self.calcular_integridad_archivo(destino)
        )

        exigir(

            integridad_destino == {

                "sha256": huella.hexdigest(),

                "tamano_bytes": cantidad,

            },

            "La copia local no conserva su integridad.",

        )

        return {

            "estado": "ARCHIVO_COPIADO",

            "origen": origen,

            "destino": destino,

            **integridad_destino,

        }


# ============================================================
# 7. BACKEND GOOGLE CLOUD STORAGE
# ============================================================

class GCSStorageBackend(StorageBackend):

    def __init__(self, config, cliente=None):

        super().__init__(config)

        exigir(
            config.backend == BACKEND_GCS,
            "Se requiere configuracion GCS.",
        )

        if cliente is None:

            try:

                from google.cloud import storage

            except ImportError as error:

                raise StorageBackendError(

                    "Falta instalar google-cloud-storage."

                ) from error

            cliente = storage.Client(

                project=config.proyecto_gcp

            )

        self.cliente = cliente

    @staticmethod
    def separar_uri(uri):

        partes = urlsplit(uri)

        return (

            partes.netloc,

            partes.path.lstrip("/"),

        )

    def obtener_blob(self, uri, escritura=False):

        if escritura:

            self.verificar_escritura(uri)

        else:

            self.verificar_lectura(uri)

        bucket, nombre = self.separar_uri(uri)

        exigir(
            bool(nombre),
            "Se esperaba una ruta de objeto GCS.",
        )

        return self.cliente.bucket(

            bucket

        ).blob(nombre)

    def obtener_blob_existente(self, uri):

        self.verificar_lectura(uri)

        bucket, nombre = self.separar_uri(uri)

        exigir(
            bool(nombre),
            "Se esperaba un objeto GCS.",
        )

        blob = self.cliente.bucket(

            bucket

        ).get_blob(nombre)

        exigir(
            blob is not None,
            f"No existe el objeto GCS: {uri}",
        )

        exigir(
            blob.generation is not None,
            "El objeto no tiene generacion.",
        )

        return blob

    def existe_archivo(self, uri):

        blob = self.obtener_blob(uri)

        return blob.exists()

    def verificar_generacion_actual(self, uri, generacion):

        actual = self.obtener_blob_existente(uri)

        exigir(

            int(actual.generation)
            == int(generacion),

            f"El objeto cambio de generacion: {uri}",

        )

    def leer_bytes(self, uri):

        blob = self.obtener_blob_existente(uri)

        generacion = int(blob.generation)

        contenido = blob.download_as_bytes(

            raw_download=True,

            if_generation_match=generacion,

            checksum="auto",

        )

        exigir(

            len(contenido) == int(blob.size),

            f"Tamano de objeto inconsistente: {uri}",

        )

        self.verificar_generacion_actual(

            uri,

            generacion,

        )

        return contenido

    def escribir_bytes_nuevo(self, uri, contenido):

        exigir(
            isinstance(contenido, bytes),
            "Se esperaba contenido binario.",
        )

        blob = self.obtener_blob(

            uri,

            escritura=True,

        )

        blob.upload_from_string(

            contenido,

            content_type=(

                "application/json"

                if uri.endswith(".json")

                else "application/octet-stream"

            ),

            if_generation_match=0,

            checksum="auto",

        )

        return uri

    def calcular_integridad_archivo(self, uri):

        blob = self.obtener_blob_existente(uri)

        generacion = int(blob.generation)

        with blob.open(

            "rb",

            chunk_size=8 * TAMANO_BLOQUE,

            raw_download=True,

            if_generation_match=generacion,

        ) as archivo:

            resultado = calcular_sha256_stream(
                archivo
            )

        exigir(

            resultado["tamano_bytes"]
            == int(blob.size),

            f"Tamano de objeto inconsistente: {uri}",

        )

        self.verificar_generacion_actual(

            uri,

            generacion,

        )

        return resultado

    def inventario_generaciones(self, raiz):

        self.verificar_lectura(raiz)

        bucket, prefijo = self.separar_uri(raiz)

        prefijo = (

            prefijo + "/"

            if prefijo

            else ""

        )

        objetos = {}

        for blob in self.cliente.list_blobs(

            bucket,

            prefix=prefijo,

        ):

            if not blob.name.endswith(".parquet"):

                continue

            relativa = blob.name[
                len(prefijo):
            ]

            validar_ruta_relativa(
                relativa
            )

            exigir(
                relativa not in objetos,
                f"Objeto duplicado: {relativa}",
            )

            exigir(
                blob.generation is not None,
                "Objeto sin generacion.",
            )

            objetos[relativa] = (

                int(blob.generation),

                int(blob.size),

            )

        return objetos

    def listar_parquet(self, raiz):

        generaciones_antes = (
            self.inventario_generaciones(raiz)
        )

        archivos = []

        for relativa in sorted(
            generaciones_antes
        ):

            generacion, tamano = (
                generaciones_antes[relativa]
            )

            uri = unir_uri(

                raiz,

                relativa,

            )

            self.verificar_generacion_actual(

                uri,

                generacion,

            )

            integridad = (
                self.calcular_integridad_archivo(uri)
            )

            self.verificar_generacion_actual(

                uri,

                generacion,

            )

            exigir(
                integridad["tamano_bytes"] == tamano,
                f"Tamano incompatible: {uri}",
            )

            archivos.append({

                "ruta_relativa": relativa,

                "tamano_bytes": tamano,

                "sha256": integridad["sha256"],

            })

        generaciones_despues = (
            self.inventario_generaciones(raiz)
        )

        exigir(

            generaciones_antes
            == generaciones_despues,

            "El dataset GCS cambio durante el inventario.",

        )

        exigir(
            bool(archivos),
            "No se encontraron objetos Parquet.",
        )

        return archivos

    def copiar_archivo_nuevo(self, origen, destino):

        self.verificar_lectura(origen)

        self.verificar_escritura(destino)

        exigir(
            origen != destino,
            "Origen y destino deben ser distintos.",
        )

        blob_origen = (
            self.obtener_blob_existente(origen)
        )

        generacion = int(
            blob_origen.generation
        )

        integridad_origen = (
            self.calcular_integridad_archivo(origen)
        )

        self.verificar_generacion_actual(

            origen,

            generacion,

        )

        blob_destino = self.obtener_blob(

            destino,

            escritura=True,

        )

        token = None

        while True:

            token, _, _ = blob_destino.rewrite(

                blob_origen,

                token=token,

                if_generation_match=0,

                if_source_generation_match=generacion,

            )

            if token is None:

                break

        integridad_destino = (
            self.calcular_integridad_archivo(destino)
        )

        exigir(

            integridad_origen
            == integridad_destino,

            "La copia GCS no conserva su integridad.",

        )

        self.verificar_generacion_actual(

            origen,

            generacion,

        )

        return {

            "estado": "ARCHIVO_COPIADO",

            "origen": origen,

            "destino": destino,

            **integridad_destino,

        }


# ============================================================
# 8. FACTORIA DE BACKENDS
# ============================================================

def crear_backend(config=None, cliente=None):

    if config is None:

        config = cargar_configuracion()

    if config.backend == BACKEND_LOCAL:

        return LocalStorageBackend(config)

    if config.backend == BACKEND_GCS:

        return GCSStorageBackend(

            config,

            cliente=cliente,

        )

    raise StorageBackendError(

        f"Backend no soportado: {config.backend}"

    )


# ============================================================
# 9. INFORMACION DEL BACKEND
# ============================================================

def main():

    config = cargar_configuracion()

    resultado = {

        "version_backend": VERSION_BACKEND,

        "backend": config.backend,

        "entorno": config.entorno,

        "configuracion": config.resumen(),

        "publicacion_productiva": False,

        "conexion_gcs_verificada": False,

    }

    print(

        json.dumps(

            resultado,

            indent=4,

            ensure_ascii=False,

        )

    )


if __name__ == "__main__":

    main()
