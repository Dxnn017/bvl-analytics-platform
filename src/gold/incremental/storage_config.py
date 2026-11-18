#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics Platform
Configuracion centralizada de almacenamiento Gold.

Responsabilidades:
- Centralizar las rutas locales existentes.
- Permitir configuraciones independientes LOCAL y GCS.
- Mantener separados los registros Silver.
- Validar URI y raices de almacenamiento.
- Conservar compatibilidad con las pruebas historicas.

Este modulo:
- No crea buckets.
- No realiza operaciones de escritura.
- No solicita credenciales.
- No ejecuta Spark.
- No autoriza publicaciones productivas.
"""

import os
import re

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


# ============================================================
# 1. CONSTANTES GENERALES
# ============================================================

VERSION_CONFIGURACION = "0.1"

BACKEND_LOCAL = "LOCAL"

BACKEND_GCS = "GCS"

ENTORNO_PRUEBA = "PRUEBA"

PROYECTO_GCP_DEFAULT = "bvl-analytics-2026"

BACKENDS_PERMITIDOS = (
    BACKEND_LOCAL,
    BACKEND_GCS,
)

PATRON_BUCKET = re.compile(
    r"[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]"
)


# ============================================================
# 2. EXCEPCIONES
# ============================================================

class StorageConfigError(Exception):
    """Error de configuracion de almacenamiento."""


def exigir(condicion, mensaje):

    if not condicion:

        raise StorageConfigError(mensaje)


# ============================================================
# 3. CONFIGURACION LOCAL HISTORICA
# ============================================================

HOME = Path.home()

DIRECTORIO_ENTREGAS = (
    HOME / "bvl_entregas"
)

DIRECTORIO_PRUEBAS = (
    DIRECTORIO_ENTREGAS
    / "incremental_pruebas_20261114"
)

SILVER_ORIGINAL = (
    DIRECTORIO_ENTREGAS
    / "silver_20261109"
    / "silver"
)

SILVER_VERSIONES = (
    DIRECTORIO_PRUEBAS
    / "silver_versiones"
)

GOLD_HISTORICO = (
    DIRECTORIO_ENTREGAS
    / "gold_staging_20261114"
)

RAIZ_REPORTES = (
    DIRECTORIO_PRUEBAS
    / "reportes"
)

RAIZ_CONTROL = (
    DIRECTORIO_PRUEBAS
    / "control"
)

RAIZ_MANIFESTS = (
    DIRECTORIO_PRUEBAS
    / "manifests"
)

RAIZ_CANDIDATAS = (
    DIRECTORIO_PRUEBAS
    / "gold_candidatas"
)

RUTA_REGISTRO_ORIGINAL = (
    RAIZ_CONTROL
    / "registro_versiones_silver.json"
)

RUTA_REGISTRO_INTEGRACION = (
    RAIZ_CONTROL
    / "registro_versiones_silver_integracion.json"
)

RAICES_AUTORIZADAS = (
    SILVER_ORIGINAL,
    SILVER_VERSIONES,
)


# ============================================================
# 4. CATALOGO DE RUTAS RELATIVAS
# ============================================================

PRODUCTOS_GOLD_LOCALES = {

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


FUENTES_SILVER_LOCALES = {

    "cotizaciones": (
        "mercado_diario/cotizaciones"
    ),

    "montos_sab": (
        "mercado_diario/montos_sab"
    ),

    "tipo_cambio": (
        "macroeconomia/tipo_cambio"
    ),

    "embig": (
        "macroeconomia/embig"
    ),

    "principales_cuentas": (
        "finanzas_empresariales/principales_cuentas"
    ),

    "eventos": (
        "hechos_importancia/eventos"
    ),

    "documentos": (
        "hechos_importancia/documentos"
    ),

    "contenido_textual": (
        "hechos_importancia/contenido_textual"
    ),

    "identidad_empresarial": (
        "hechos_importancia/identidad_empresarial"
    ),

    "puente_ruc_nemonico": (
        "referencias_smv/puente_ruc_nemonico"
    ),

    "indices": (
        "mercado_diario/indices"
    ),

}


# ============================================================
# 5. VALIDACION DE RUTAS
# ============================================================

def validar_texto_ruta(valor):

    exigir(
        isinstance(valor, str),
        "La ruta debe ser una cadena.",
    )

    exigir(
        bool(valor),
        "La ruta no puede estar vacia.",
    )

    exigir(
        valor == valor.strip(),
        "La ruta contiene espacios exteriores.",
    )

    exigir(
        not any(
            ord(caracter) < 32
            or ord(caracter) == 127
            for caracter in valor
        ),
        "La ruta contiene caracteres de control.",
    )

    exigir(
        "\\" not in valor,
        "No se permiten barras invertidas.",
    )

    exigir(
        "%" not in valor,
        "No se permiten rutas codificadas.",
    )

    exigir(
        "?" not in valor
        and "#" not in valor,
        "No se permiten consultas ni fragmentos.",
    )

    return valor


def validar_ruta_relativa(ruta):

    ruta = validar_texto_ruta(ruta)

    exigir(
        not ruta.startswith("/"),
        "Se esperaba una ruta relativa.",
    )

    exigir(
        not ruta.endswith("/"),
        "La ruta relativa termina en una barra.",
    )

    exigir(
        ":" not in ruta,
        "No se permiten esquemas en rutas relativas.",
    )

    componentes = ruta.split("/")

    exigir(
        all(
            componente not in ("", ".", "..")
            for componente in componentes
        ),
        "La ruta contiene segmentos no permitidos.",
    )

    return ruta


def validar_uri_local(uri):

    uri = validar_texto_ruta(uri)

    exigir(
        uri.startswith("file:///"),
        "Se esperaba una URI local file:///.",
    )

    partes = urlsplit(uri)

    exigir(
        partes.scheme == "file"
        and not partes.netloc,
        "La URI local no es valida.",
    )

    ruta = partes.path

    exigir(
        ruta.startswith("/"),
        "La ruta local debe ser absoluta.",
    )

    exigir(
        ruta != "/",
        "No se permite la raiz del sistema.",
    )

    validar_ruta_relativa(
        ruta[1:]
    )

    exigir(
        Path(ruta).as_uri() == uri,
        "La URI local no es canonica.",
    )

    return uri


def validar_uri_gcs(uri):

    uri = validar_texto_ruta(uri)

    exigir(
        uri.startswith("gs://"),
        "Se esperaba una URI gs://.",
    )

    partes = urlsplit(uri)

    exigir(
        partes.scheme == "gs",
        "Esquema GCS incorrecto.",
    )

    bucket = partes.netloc

    exigir(
        bool(
            PATRON_BUCKET.fullmatch(bucket)
        ),
        "Nombre de bucket GCS no valido.",
    )

    exigir(
        ".." not in bucket,
        "El bucket contiene segmentos invalidos.",
    )

    exigir(
        not partes.query
        and not partes.fragment,
        "La URI GCS contiene elementos no permitidos.",
    )

    if partes.path:

        exigir(
            partes.path.startswith("/"),
            "Prefijo GCS incorrecto.",
        )

        validar_ruta_relativa(
            partes.path[1:]
        )

    return uri


def validar_uri(uri, backend):

    exigir(
        backend in BACKENDS_PERMITIDOS,
        f"Backend no permitido: {backend}",
    )

    if backend == BACKEND_LOCAL:

        return validar_uri_local(uri)

    return validar_uri_gcs(uri)


# ============================================================
# 6. CONSTRUCCION DE URI
# ============================================================

def uri_local(ruta):

    ruta = Path(ruta)

    exigir(
        ruta.is_absolute(),
        "La ruta local debe ser absoluta.",
    )

    return validar_uri_local(
        ruta.as_uri()
    )


def unir_uri(raiz, *componentes):

    raiz = validar_texto_ruta(raiz)

    if raiz.startswith("file:///"):

        backend = BACKEND_LOCAL

    elif raiz.startswith("gs://"):

        backend = BACKEND_GCS

    else:

        raise StorageConfigError(
            "Esquema de almacenamiento no permitido."
        )

    validar_uri(
        raiz,
        backend,
    )

    relativas = [

        validar_ruta_relativa(componente)

        for componente in componentes

    ]

    resultado = "/".join(
        [raiz] + relativas
    )

    return validar_uri(
        resultado,
        backend,
    )


def uri_bajo_raiz(uri, raiz, backend):

    validar_uri(uri, backend)

    validar_uri(raiz, backend)

    return (
        uri == raiz
        or uri.startswith(
            raiz + "/"
        )
    )


def exigir_bajo_raiz(uri, raiz, backend):

    exigir(
        uri_bajo_raiz(
            uri,
            raiz,
            backend,
        ),
        f"Ruta fuera de la raiz autorizada: {uri}",
    )


# ============================================================
# 7. MODELO DE CONFIGURACION
# ============================================================

@dataclass(frozen=True)
class StorageConfig:

    backend: str

    entorno: str

    proyecto_gcp: str | None

    region: str | None

    silver_original: str

    silver_versiones: str

    gold_historico: str

    gold_candidatas: str

    control: str

    manifests: str

    reportes: str

    registro_original: str

    registro_integracion: str

    publicacion_productiva: bool = False

    def __post_init__(self):

        exigir(
            self.backend in BACKENDS_PERMITIDOS,
            "Backend de almacenamiento no permitido.",
        )

        exigir(
            self.entorno == ENTORNO_PRUEBA,
            "Solo se permite el entorno PRUEBA.",
        )

        exigir(
            self.publicacion_productiva is False,
            "La publicacion productiva esta bloqueada.",
        )

        for campo in (

            "silver_original",

            "silver_versiones",

            "gold_historico",

            "gold_candidatas",

            "control",

            "manifests",

            "reportes",

            "registro_original",

            "registro_integracion",

        ):

            validar_uri(
                getattr(self, campo),
                self.backend,
            )

        exigir(
            self.registro_original
            != self.registro_integracion,
            "Los registros Silver deben ser independientes.",
        )

        exigir_bajo_raiz(
            self.registro_original,
            self.control,
            self.backend,
        )

        exigir_bajo_raiz(
            self.registro_integracion,
            self.control,
            self.backend,
        )

        exigir(
            self.registro_original.endswith(".json")
            and self.registro_integracion.endswith(".json"),
            "Los registros deben ser archivos JSON.",
        )

        pares_independientes = (

            (
                self.silver_original,
                self.silver_versiones,
            ),

            (
                self.gold_historico,
                self.gold_candidatas,
            ),

        )

        for primera, segunda in pares_independientes:

            exigir(
                not uri_bajo_raiz(
                    primera,
                    segunda,
                    self.backend,
                )
                and not uri_bajo_raiz(
                    segunda,
                    primera,
                    self.backend,
                ),
                "Las raices independientes se superponen.",
            )

        if self.backend == BACKEND_GCS:

            exigir(
                bool(self.proyecto_gcp),
                "Falta el proyecto GCP.",
            )

            exigir(
                bool(self.region),
                "Falta la region GCP.",
            )

            buckets_silver = {

                urlsplit(
                    self.silver_original
                ).netloc,

                urlsplit(
                    self.silver_versiones
                ).netloc,

            }

            buckets_gold = {

                urlsplit(
                    self.gold_historico
                ).netloc,

                urlsplit(
                    self.gold_candidatas
                ).netloc,

            }

            bucket_control = urlsplit(
                self.control
            ).netloc

            exigir(
                len(buckets_silver) == 1,
                "Las rutas Silver deben compartir bucket.",
            )

            exigir(
                len(buckets_gold) == 1,
                "Las rutas Gold deben compartir bucket.",
            )

            exigir(
                not buckets_silver.intersection(
                    buckets_gold
                ),
                "Silver y Gold deben usar buckets distintos.",
            )

            exigir(
                bucket_control not in (
                    buckets_silver | buckets_gold
                ),
                "Control debe usar un bucket independiente.",
            )

            for ruta in (
                self.manifests,
                self.reportes,
            ):

                exigir(
                    urlsplit(ruta).netloc
                    == bucket_control,
                    "Manifests y reportes deben usar Control.",
                )

    @property
    def raices_autorizadas_silver(self):

        return (

            self.silver_original,

            self.silver_versiones,

        )

    def resumen(self):

        return {

            "version_configuracion": (
                VERSION_CONFIGURACION
            ),

            "backend": self.backend,

            "entorno": self.entorno,

            "proyecto_gcp": self.proyecto_gcp,

            "region": self.region,

            "silver_original": self.silver_original,

            "silver_versiones": self.silver_versiones,

            "gold_historico": self.gold_historico,

            "gold_candidatas": self.gold_candidatas,

            "control": self.control,

            "manifests": self.manifests,

            "reportes": self.reportes,

            "registro_original": (
                self.registro_original
            ),

            "registro_integracion": (
                self.registro_integracion
            ),

            "publicacion_productiva": (
                self.publicacion_productiva
            ),

        }


# ============================================================
# 8. CONFIGURACION LOCAL
# ============================================================

def crear_configuracion_local(
    directorio_entregas=None,
):

    if directorio_entregas is None:

        entregas = DIRECTORIO_ENTREGAS

    else:

        entregas = Path(
            directorio_entregas
        )

    exigir(
        entregas.is_absolute(),
        "El directorio de entregas debe ser absoluto.",
    )

    pruebas = (
        entregas
        / "incremental_pruebas_20261114"
    )

    control = (
        pruebas / "control"
    )

    return StorageConfig(

        backend=BACKEND_LOCAL,

        entorno=ENTORNO_PRUEBA,

        proyecto_gcp=None,

        region=None,

        silver_original=uri_local(
            entregas
            / "silver_20261109"
            / "silver"
        ),

        silver_versiones=uri_local(
            pruebas
            / "silver_versiones"
        ),

        gold_historico=uri_local(
            entregas
            / "gold_staging_20261114"
        ),

        gold_candidatas=uri_local(
            pruebas
            / "gold_candidatas"
        ),

        control=uri_local(
            control
        ),

        manifests=uri_local(
            pruebas
            / "manifests"
        ),

        reportes=uri_local(
            pruebas
            / "reportes"
        ),

        registro_original=uri_local(
            control
            / "registro_versiones_silver.json"
        ),

        registro_integracion=uri_local(
            control
            / "registro_versiones_silver_integracion.json"
        ),

        publicacion_productiva=False,

    )


# ============================================================
# 9. CONFIGURACION GCS
# ============================================================

def crear_configuracion_gcs(

    *,

    proyecto_gcp,

    region,

    silver_original,

    silver_versiones,

    gold_historico,

    gold_candidatas,

    control,

    manifests,

    reportes,

    registro_original,

    registro_integracion,

):

    return StorageConfig(

        backend=BACKEND_GCS,

        entorno=ENTORNO_PRUEBA,

        proyecto_gcp=proyecto_gcp,

        region=region,

        silver_original=silver_original,

        silver_versiones=silver_versiones,

        gold_historico=gold_historico,

        gold_candidatas=gold_candidatas,

        control=control,

        manifests=manifests,

        reportes=reportes,

        registro_original=registro_original,

        registro_integracion=registro_integracion,

        publicacion_productiva=False,

    )


# ============================================================
# 10. VARIABLES DE ENTORNO
# ============================================================

def variable_requerida(nombre, variables):

    valor = variables.get(nombre)

    exigir(
        isinstance(valor, str)
        and bool(valor.strip()),
        f"Falta la variable de entorno {nombre}.",
    )

    exigir(
        valor == valor.strip(),
        f"Valor incorrecto para {nombre}.",
    )

    return valor


def cargar_configuracion(variables=None):

    if variables is None:

        variables = os.environ

    backend = variables.get(
        "BVL_STORAGE_BACKEND",
        BACKEND_LOCAL,
    )

    entorno = variables.get(
        "BVL_ENTORNO",
        ENTORNO_PRUEBA,
    )

    exigir(
        entorno == ENTORNO_PRUEBA,
        "La configuracion productiva esta bloqueada.",
    )

    exigir(
        backend in BACKENDS_PERMITIDOS,
        f"Backend no permitido: {backend}",
    )

    if backend == BACKEND_LOCAL:

        directorio_entregas = variables.get(
            "BVL_DIRECTORIO_ENTREGAS"
        )

        return crear_configuracion_local(
            directorio_entregas
        )

    return crear_configuracion_gcs(

        proyecto_gcp=variable_requerida(
            "BVL_GCP_PROJECT",
            variables,
        ),

        region=variable_requerida(
            "BVL_GCP_REGION",
            variables,
        ),

        silver_original=variable_requerida(
            "BVL_SILVER_ORIGINAL_URI",
            variables,
        ),

        silver_versiones=variable_requerida(
            "BVL_SILVER_VERSIONES_URI",
            variables,
        ),

        gold_historico=variable_requerida(
            "BVL_GOLD_HISTORICO_URI",
            variables,
        ),

        gold_candidatas=variable_requerida(
            "BVL_GOLD_CANDIDATAS_URI",
            variables,
        ),

        control=variable_requerida(
            "BVL_CONTROL_URI",
            variables,
        ),

        manifests=variable_requerida(
            "BVL_MANIFESTS_URI",
            variables,
        ),

        reportes=variable_requerida(
            "BVL_REPORTES_URI",
            variables,
        ),

        registro_original=variable_requerida(
            "BVL_REGISTRO_ORIGINAL_URI",
            variables,
        ),

        registro_integracion=variable_requerida(
            "BVL_REGISTRO_INTEGRACION_URI",
            variables,
        ),

    )


# ============================================================
# 11. CONFIGURACION LOCAL PREDETERMINADA
# ============================================================

CONFIGURACION_LOCAL = (
    crear_configuracion_local()
)


# ============================================================
# 12. EJECUCION INDEPENDIENTE
# ============================================================

def main():

    import json

    configuracion = cargar_configuracion()

    print(
        json.dumps(
            configuracion.resumen(),
            indent=4,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":

    main()
