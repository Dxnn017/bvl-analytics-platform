#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics
Validador estructural del manifest Silver -> Gold.

Contrato provisional: v0.1

Este componente:
- Lee manifests JSON.
- Valida estructura y reglas de negocio declaradas.
- Identifica datasets reconocidos.
- Verifica claves, operaciones y conteos.
- Calcula la huella SHA-256.

No modifica Silver ni Gold.
No ejecuta transformaciones Spark.
No autoriza por sí solo una publicación productiva.
"""
import argparse
import hashlib
import json
import math
import re
import sys

from collections import Counter
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

if __package__:
    from .storage_config import (
        BACKEND_GCS,
        validar_uri_gcs,
        uri_bajo_raiz,
        StorageConfigError,
        cargar_configuracion,
    )
    from .storage_backend import crear_backend
else:
    from storage_config import (
        BACKEND_GCS,
        validar_uri_gcs,
        uri_bajo_raiz,
        StorageConfigError,
        cargar_configuracion,
    )
    from storage_backend import crear_backend


# ============================================================
# 1. CONFIGURACION
# ============================================================

VERSIONES_SOPORTADAS = {"0.1"}

ESTADO_SILVER_APROBADO = "SILVER_PUBLICADO_APROBADO"

ENTORNOS_VALIDOS = {"PRUEBA", "PRODUCCION"}

OPERACIONES_VALIDAS = {"INSERT", "UPDATE", "DELETE"}

TIPOS_PUBLICACION_VALIDOS = {"INCREMENTAL"}


# ============================================================
# 2. CATALOGO PROVISIONAL DE DATASETS SILVER
# ============================================================

CATALOGO_DATASETS = {
    "cotizaciones": {
        "ruta": "mercado_diario/cotizaciones",
        "clave": ["valor", "fecha_cotizacion"],
        "particiones": ["anio", "mes"],
        "fechas_clave": ["fecha_cotizacion"],
        "consumido_gold": True,
    },
    "montos_sab": {
        "ruta": "mercado_diario/montos_sab",
        "clave": ["codigo_sab", "fecha_negociacion"],
        "particiones": ["anio", "mes"],
        "fechas_clave": ["fecha_negociacion"],
        "consumido_gold": True,
    },
    "tipo_cambio": {
        "ruta": "macroeconomia/tipo_cambio",
        "clave": ["fecha"],
        "particiones": ["anio", "mes"],
        "fechas_clave": ["fecha"],
        "consumido_gold": True,
    },
    "embig": {
        "ruta": "macroeconomia/embig",
        "clave": ["fecha"],
        "particiones": ["anio", "mes"],
        "fechas_clave": ["fecha"],
        "consumido_gold": True,
    },
    "principales_cuentas": {
        "ruta": "finanzas_empresariales/principales_cuentas",
        "clave": ["id_registro"],
        "particiones": ["ejercicio", "trimestre_num"],
        "fechas_clave": [],
        "consumido_gold": True,
    },
    "eventos": {
        "ruta": "hechos_importancia/eventos",
        "clave": ["numero_expediente"],
        "particiones": ["anio_presentacion"],
        "fechas_clave": [],
        "consumido_gold": True,
    },
    "documentos": {
        "ruta": "hechos_importancia/documentos",
        "clave": ["guid_documento"],
        "particiones": None,
        "fechas_clave": [],
        "consumido_gold": True,
    },
    "contenido_textual": {
        "ruta": "hechos_importancia/contenido_textual",
        "clave": ["guid_documento"],
        "particiones": None,
        "fechas_clave": [],
        "consumido_gold": True,
    },
    "identidad_empresarial": {
        "ruta": "hechos_importancia/identidad_empresarial",
        "clave": ["numero_expediente"],
        "particiones": None,
        "fechas_clave": [],
        "consumido_gold": True,
    },
    "puente_ruc_nemonico": {
        "ruta": "referencias_smv/puente_ruc_nemonico",
        "clave": ["nemonico_valor"],
        "particiones": None,
        "fechas_clave": [],
        "consumido_gold": True,
    },
    "indices": {
        "ruta": "mercado_diario/indices",
        "clave": ["indice", "fecha"],
        "particiones": ["anio", "mes"],
        "fechas_clave": ["fecha"],
        "consumido_gold": False,
    },
}


# ============================================================
# 3. CAMPOS OBLIGATORIOS
# ============================================================

CAMPOS_MANIFEST = {
    "version_manifest",
    "entorno",
    "run_id",
    "estado",
    "fecha_publicacion",
    "tipo_publicacion",
    "validacion_aprobada",
    "datasets",
}

CAMPOS_DATASET = {
    "dataset",
    "ruta_silver",
    "version_esquema",
    "version_anterior",
    "version_publicada",
    "clave",
    "particiones_afectadas",
    "claves_afectadas",
    "ruta_detalle_cambios",
    "registros_nuevos",
    "registros_corregidos",
    "registros_eliminados",
    "registros_antes",
    "registros_despues",
    "autorizacion_eliminaciones",
}

CAMPOS_OPERACION = {
    "operacion",
    "clave_anterior",
    "clave_nueva",
}

CAMPOS_CONTEOS = (
    "registros_nuevos",
    "registros_corregidos",
    "registros_eliminados",
    "registros_antes",
    "registros_despues",
)


# ============================================================
# 4. EXCEPCIONES
# ============================================================

class ManifestError(Exception):
    """Error de lectura o validacion del manifest."""


# ============================================================
# 5. FUNCIONES AUXILIARES
# ============================================================

def texto_valido(valor):
    return isinstance(valor, str) and bool(valor.strip())


def entero_no_negativo(valor):
    return type(valor) is int and valor >= 0


def fecha_iso_valida(valor):
    if not isinstance(valor, str):
        return False

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", valor):
        return False

    try:
        date.fromisoformat(valor)
        return True
    except ValueError:
        return False


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


def identificador_valido(valor):
    if not texto_valido(valor):
        return False

    return bool(
        re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}",
            valor,
        )
    )


def json_sin_duplicados(pares):
    resultado = {}

    for clave, valor in pares:

        if clave in resultado:
            raise ManifestError(
                f"Clave JSON duplicada: {clave}"
            )

        resultado[clave] = valor

    return resultado

def rechazar_constante_json(valor):
    raise ManifestError(
        f"Constante JSON no permitida: {valor}"
    )


def parsear_float_finito(valor):
    """
    Convierte un numero JSON a float y rechaza
    valores que excedan el rango finito de Python.
    """

    try:
        numero = float(valor)

    except (ValueError, OverflowError) as exc:
        raise ManifestError(
            f"Numero JSON invalido: {valor}"
        ) from exc

    if not math.isfinite(numero):
        raise ManifestError(
            f"Numero JSON no finito: {valor}"
        )

    return numero

def comprobar_campos(
    objeto,
    campos_obligatorios,
    contexto,
    errores,
):
    faltantes = campos_obligatorios - set(objeto)

    adicionales = set(objeto) - campos_obligatorios

    if faltantes:
        errores.append(
            f"{contexto}: campos faltantes: "
            f"{', '.join(sorted(faltantes))}"
        )

    if adicionales:
        errores.append(
            f"{contexto}: campos no reconocidos: "
            f"{', '.join(sorted(adicionales))}"
        )

def clave_canonica(clave):
    return json.dumps(
        clave,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def validar_compatibilidad_json(manifest):
    """
    Verifica que un objeto Python sea compatible
    con una estructura JSON valida.

    Rechaza:
    - Numeros no finitos.
    - Objetos no serializables.
    - Referencias circulares.
    - Anidamiento excesivo.
    - Claves de diccionario que no sean cadenas.
    - Caracteres Unicode invalidos.

    No modifica el objeto recibido.
    """

    profundidad_maxima = 64
    objetos_activos = set()

    def inspeccionar(valor, profundidad=0):

        if profundidad > profundidad_maxima:
            raise ManifestError(
                "Anidamiento JSON excesivo."
            )

        if valor is None:
            return

        if type(valor) in (bool, int):
            return

        if type(valor) is float:

            if not math.isfinite(valor):
                raise ManifestError(
                    "Numero JSON no finito."
                )

            return

        if type(valor) is str:

            try:
                valor.encode("utf-8")

            except UnicodeEncodeError as exc:
                raise ManifestError(
                    "Cadena con caracteres Unicode invalidos."
                ) from exc

            return

        if type(valor) not in (dict, list):

            raise ManifestError(
                "Tipo Python no compatible con JSON: "
                f"{type(valor).__name__}."
            )

        identidad = id(valor)

        if identidad in objetos_activos:

            raise ManifestError(
                "Referencia circular en estructura JSON."
            )

        objetos_activos.add(identidad)

        try:

            if type(valor) is dict:

                for clave, contenido in valor.items():

                    if type(clave) is not str:

                        raise ManifestError(
                            "Las claves JSON deben ser cadenas."
                        )

                    inspeccionar(
                        clave,
                        profundidad + 1,
                    )

                    inspeccionar(
                        contenido,
                        profundidad + 1,
                    )

            else:

                for contenido in valor:

                    inspeccionar(
                        contenido,
                        profundidad + 1,
                    )

        finally:

            objetos_activos.remove(
                identidad
            )

    try:

        inspeccionar(manifest)

        json.dumps(
            manifest,
            ensure_ascii=False,
            allow_nan=False,
        )

    except ManifestError as exc:

        return [
            f"Estructura JSON invalida: {exc}"
        ]

    except (
        ValueError,
        TypeError,
        OverflowError,
        RecursionError,
    ) as exc:

        return [
            f"Estructura JSON invalida: {exc}"
        ]

    return []


# ============================================================
# 6. LECTURA DEL MANIFEST Y HUELLA SHA-256
# ============================================================
def cargar_manifest(ruta, backend=None):

    try:
        if backend is not None:
            contenido = backend.leer_bytes(str(ruta))
        elif str(ruta).startswith("gs://"):
            config = cargar_configuracion()
            if config.backend != BACKEND_GCS:
                raise ManifestError(
                    "La lectura GCS requiere configuracion GCS."
                )
            contenido = crear_backend(config).leer_bytes(str(ruta))
        else:
            contenido = Path(ruta).read_bytes()

    except Exception as exc:
        raise ManifestError(
            f"No se pudo leer el manifest: {exc}"
        ) from exc

    huella = hashlib.sha256(contenido).hexdigest()

    try:
        manifest = json.loads(
            contenido.decode("utf-8"),
            object_pairs_hook=json_sin_duplicados,
            parse_constant=rechazar_constante_json,
            parse_float=parsear_float_finito,
        )

    except ManifestError:
        raise

    except RecursionError as exc:
        raise ManifestError(
            "JSON invalido: nivel de anidamiento excesivo."
        ) from exc

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        ValueError,
        OverflowError,
    ) as exc:
        raise ManifestError(
            f"JSON invalido: {exc}"
        ) from exc

    if not isinstance(manifest, dict):
        raise ManifestError(
            "El manifest debe ser un objeto JSON."
        )

    return manifest, huella


# ============================================================
# 7. VALIDACION ESTRUCTURAL DE RUTAS
# ============================================================

def validar_ruta(
    ruta,
    entorno,
    contexto,
    errores,
    sufijo_esperado=None,
    config=None,
):
    if not texto_valido(ruta):
        errores.append(
            f"{contexto}: ruta vacia o invalida."
        )
        return

    if ruta != ruta.strip():
        errores.append(
            f"{contexto}: espacios al inicio "
            "o final de la ruta no permitidos."
        )
        return

    if any(
        ord(caracter) < 32 or ord(caracter) == 127
        for caracter in ruta
    ):
        errores.append(
            f"{contexto}: caracteres de control "
            "no permitidos en la ruta."
        )
        return

    if "\\" in ruta:
        errores.append(
            f"{contexto}: barras invertidas "
            "no permitidas en la ruta."
        )
        return

    if "%" in ruta:
        errores.append(
            f"{contexto}: codificacion porcentual "
            "no permitida en la ruta."
        )
        return

    try:
        componentes = urlparse(ruta)

    except ValueError:
        errores.append(
            f"{contexto}: ruta malformada."
        )
        return

    esquema = componentes.scheme

    if entorno == "PRUEBA":

        if config is not None and config.backend == BACKEND_GCS:

            try:
                validar_uri_gcs(ruta)

                raices = (
                    config.raices_autorizadas_silver
                    if sufijo_esperado
                    else (config.control, config.manifests)
                )

                if not any(
                    uri_bajo_raiz(ruta, raiz, BACKEND_GCS)
                    for raiz in raices
                ):
                    errores.append(
                        f"{contexto}: ruta GCS no autorizada."
                    )

            except StorageConfigError as exc:
                errores.append(f"{contexto}: {exc}")

        elif esquema != "file":
            errores.append(
                f"{contexto}: PRUEBA solo acepta "
                "rutas locales file://."
            )

    elif entorno == "PRODUCCION":

        if esquema not in {"hdfs", "gs"}:
            errores.append(
                f"{contexto}: esquema de "
                "produccion no permitido."
            )

    if esquema == "file":

        if componentes.netloc not in {"", "localhost"}:
            errores.append(
                f"{contexto}: ruta file:// no local."
            )

    elif esquema in {"hdfs", "gs"}:

        if not componentes.netloc:
            errores.append(
                f"{contexto}: falta autoridad o bucket."
            )

    if not componentes.path.startswith("/"):
        errores.append(
            f"{contexto}: se requiere ruta absoluta."
        )

    if componentes.query or componentes.fragment:
        errores.append(
            f"{contexto}: query o fragment no permitido."
        )

    if any(
        segmento in {".", ".."}
        for segmento in componentes.path.split("/")
    ):
        errores.append(
            f"{contexto}: segmentos de ruta no permitidos."
        )

    if sufijo_esperado:

        ruta_normalizada = componentes.path.rstrip("/")

        if not ruta_normalizada.endswith(
            "/" + sufijo_esperado
        ):
            errores.append(
                f"{contexto}: ruta incompatible "
                f"con el catalogo: {sufijo_esperado}"
            )


# ============================================================
# 8. VALIDACION DE PARTICIONES
# ============================================================

def validar_particiones(
    particiones,
    configuracion,
    total_cambios,
    contexto,
    errores,
):
    if not isinstance(particiones, list):
        errores.append(
            f"{contexto}: particiones_afectadas "
            "debe ser una lista."
        )
        return set()

    campos_esperados = configuracion["particiones"]

    if (
        campos_esperados
        and total_cambios > 0
        and not particiones
    ):
        errores.append(
            f"{contexto}: faltan particiones afectadas."
        )

    particiones_vistas = set()

    for indice, particion in enumerate(particiones):

        prefijo = (
            f"{contexto}.particiones_afectadas[{indice}]"
        )

        if not isinstance(particion, dict):
            errores.append(
                f"{prefijo}: debe ser un objeto."
            )
            continue

        if not particion:
            errores.append(
                f"{prefijo}: particion vacia."
            )
            continue

        if (
            campos_esperados is not None
            and set(particion) != set(campos_esperados)
        ):
            errores.append(
                f"{prefijo}: campos de particion "
                f"esperados: {campos_esperados}"
            )

        for campo, valor in particion.items():

            if not entero_no_negativo(valor):
                errores.append(
                    f"{prefijo}.{campo}: entero invalido."
                )
                continue

            if campo in {
                "anio",
                "ejercicio",
                "anio_presentacion",
            }:

                if not 1900 <= valor <= 9999:
                    errores.append(
                        f"{prefijo}.{campo}: anio invalido."
                    )

            elif campo == "mes":

                if not 1 <= valor <= 12:
                    errores.append(
                        f"{prefijo}: mes fuera de rango."
                    )

            elif campo in {
                "trimestre",
                "trimestre_num",
            }:

                if not 1 <= valor <= 4:
                    errores.append(
                        f"{prefijo}: trimestre invalido."
                    )

            elif campos_esperados is None:
                errores.append(
                    f"{prefijo}: particion no confirmada "
                    f"en el contrato: {campo}"
                )

        canonica = clave_canonica(particion)

        if canonica in particiones_vistas:
            errores.append(
                f"{prefijo}: particion duplicada."
            )

        particiones_vistas.add(canonica)

    return particiones_vistas


# ============================================================
# 9. VALIDACION DE CLAVES DE NEGOCIO
# ============================================================

def validar_clave_operacion(
    clave,
    configuracion,
    contexto,
    errores,
):
    if not isinstance(clave, dict):
        errores.append(
            f"{contexto}: se esperaba objeto de clave."
        )
        return False

    campos_esperados = set(configuracion["clave"])

    if set(clave) != campos_esperados:
        errores.append(
            f"{contexto}: campos de clave incorrectos. "
            f"Esperados: {sorted(campos_esperados)}"
        )
        return False

    valido = True

    for campo, valor in clave.items():

        if campo in configuracion["fechas_clave"]:

            if not fecha_iso_valida(valor):
                errores.append(
                    f"{contexto}.{campo}: fecha invalida."
                )
                valido = False

        else:

            if not (
                texto_valido(valor)
                or type(valor) is int
            ):
                errores.append(
                    f"{contexto}.{campo}: valor "
                    "de clave invalido."
                )
                valido = False

    return valido


def validar_particion_de_clave(
    clave,
    configuracion,
    particiones_vistas,
    contexto,
    errores,
):
    if configuracion["particiones"] != ["anio", "mes"]:
        return

    for campo in configuracion["fechas_clave"]:

        valor = clave.get(campo)

        if not fecha_iso_valida(valor):
            continue

        fecha = date.fromisoformat(valor)

        particion = {
            "anio": fecha.year,
            "mes": fecha.month,
        }

        if clave_canonica(
            particion
        ) not in particiones_vistas:

            errores.append(
                f"{contexto}: la particion {particion} "
                "de la clave no fue declarada."
            )


# ============================================================
# 10. VALIDACION DEL DETALLE DE CAMBIOS
# ============================================================

def validar_operaciones(
    operaciones,
    configuracion,
    particiones_vistas,
    contexto,
    errores,
):
    conteos = Counter()

    if not isinstance(operaciones, list):
        errores.append(
            f"{contexto}: claves_afectadas "
            "debe ser una lista."
        )
        return conteos

    claves_origen = set()
    claves_destino = set()

    operaciones_por_clave = {}

    for indice, cambio in enumerate(operaciones):

        prefijo = (
            f"{contexto}.claves_afectadas[{indice}]"
        )

        if not isinstance(cambio, dict):
            errores.append(
                f"{prefijo}: operacion invalida."
            )
            continue

        comprobar_campos(
            cambio,
            CAMPOS_OPERACION,
            prefijo,
            errores,
        )

        operacion = cambio.get("operacion")

        if (
            not isinstance(operacion, str)
            or operacion not in OPERACIONES_VALIDAS
        ):
            errores.append(
                f"{prefijo}: operacion no reconocida."
            )
            continue

        conteos[operacion] += 1

        anterior = cambio.get("clave_anterior")
        nueva = cambio.get("clave_nueva")

        anterior_valida = False
        nueva_valida = False

        if operacion == "INSERT":

            if anterior is not None:
                errores.append(
                    f"{prefijo}: INSERT requiere "
                    "clave_anterior null."
                )

            nueva_valida = validar_clave_operacion(
                nueva,
                configuracion,
                prefijo + ".clave_nueva",
                errores,
            )

        elif operacion == "UPDATE":

            anterior_valida = validar_clave_operacion(
                anterior,
                configuracion,
                prefijo + ".clave_anterior",
                errores,
            )

            nueva_valida = validar_clave_operacion(
                nueva,
                configuracion,
                prefijo + ".clave_nueva",
                errores,
            )

        elif operacion == "DELETE":

            anterior_valida = validar_clave_operacion(
                anterior,
                configuracion,
                prefijo + ".clave_anterior",
                errores,
            )

            if nueva is not None:
                errores.append(
                    f"{prefijo}: DELETE requiere "
                    "clave_nueva null."
                )

        claves_cambio = set()

        if anterior_valida:

            canonica = clave_canonica(anterior)

            if canonica in claves_origen:
                errores.append(
                    f"{prefijo}: clave de origen repetida."
                )

            claves_origen.add(canonica)
            claves_cambio.add(canonica)

            validar_particion_de_clave(
                anterior,
                configuracion,
                particiones_vistas,
                prefijo + ".clave_anterior",
                errores,
            )

        if nueva_valida:

            canonica = clave_canonica(nueva)

            if canonica in claves_destino:
                errores.append(
                    f"{prefijo}: clave de destino repetida."
                )

            claves_destino.add(canonica)
            claves_cambio.add(canonica)

            validar_particion_de_clave(
                nueva,
                configuracion,
                particiones_vistas,
                prefijo + ".clave_nueva",
                errores,
            )

        for canonica in claves_cambio:

            if canonica in operaciones_por_clave:
                errores.append(
                    f"{prefijo}: clave presente "
                    "en multiples operaciones."
                )

            else:
                operaciones_por_clave[canonica] = indice

    return conteos


# ============================================================
# 11. VALIDACION DE UN DATASET
# ============================================================

def validar_dataset(
    dataset,
    indice,
    entorno,
    errores,
    advertencias,
    config=None,
):
    contexto = f"datasets[{indice}]"

    if not isinstance(dataset, dict):
        errores.append(
            f"{contexto}: debe ser un objeto."
        )
        return

    comprobar_campos(
        dataset,
        CAMPOS_DATASET,
        contexto,
        errores,
    )

    nombre = dataset.get("dataset")

    if not texto_valido(nombre):
        errores.append(
            f"{contexto}: identificador invalido."
        )
        return

    contexto = f"dataset '{nombre}'"

    if nombre not in CATALOGO_DATASETS:
        errores.append(
            f"{contexto}: dataset desconocido."
        )
        return

    configuracion = CATALOGO_DATASETS[nombre]

    validar_ruta(
        dataset.get("ruta_silver"),
        entorno,
        contexto + ".ruta_silver",
        errores,
        sufijo_esperado=configuracion["ruta"],
        config=config,
    )

    for campo in (
        "version_esquema",
        "version_anterior",
        "version_publicada",
    ):

        if not texto_valido(dataset.get(campo)):
            errores.append(
                f"{contexto}.{campo}: version invalida."
            )

    clave = dataset.get("clave")

    if clave != configuracion["clave"]:
        errores.append(
            f"{contexto}: clave incorrecta. "
            f"Esperada: {configuracion['clave']}"
        )

    conteos_validos = True

    for campo in CAMPOS_CONTEOS:

        if not entero_no_negativo(dataset.get(campo)):
            errores.append(
                f"{contexto}.{campo}: debe ser "
                "un entero no negativo."
            )
            conteos_validos = False

    if type(
        dataset.get("autorizacion_eliminaciones")
    ) is not bool:

        errores.append(
            f"{contexto}: autorizacion_eliminaciones "
            "debe ser booleana."
        )

    total_cambios = 0

    if conteos_validos:

        nuevos = dataset["registros_nuevos"]
        corregidos = dataset["registros_corregidos"]
        eliminados = dataset["registros_eliminados"]

        antes = dataset["registros_antes"]
        despues = dataset["registros_despues"]

        total_cambios = nuevos + corregidos + eliminados

        esperado = antes + nuevos - eliminados

        if despues != esperado:
            errores.append(
                f"{contexto}: conteo final inconsistente. "
                f"Esperado={esperado}, recibido={despues}."
            )

        if corregidos + eliminados > antes:
            errores.append(
                f"{contexto}: modificaciones exceden "
                "los registros anteriores."
            )

        if (
            eliminados > 0
            and dataset.get(
                "autorizacion_eliminaciones"
            ) is not True
        ):
            errores.append(
                f"{contexto}: eliminacion no autorizada."
            )

        if (
            total_cambios > 0
            and dataset.get("version_anterior")
            == dataset.get("version_publicada")
        ):
            errores.append(
                f"{contexto}: existen cambios sin "
                "una nueva version publicada."
            )

    particiones_vistas = validar_particiones(
        dataset.get("particiones_afectadas"),
        configuracion,
        total_cambios,
        contexto,
        errores,
    )

    operaciones = dataset.get("claves_afectadas")

    ruta_detalle = dataset.get("ruta_detalle_cambios")

    tiene_operaciones = operaciones is not None
    tiene_ruta = ruta_detalle is not None

    if tiene_operaciones and tiene_ruta:

        errores.append(
            f"{contexto}: se debe utilizar una sola "
            "modalidad de detalle de cambios."
        )

    if (
        total_cambios > 0
        and not tiene_operaciones
        and not tiene_ruta
    ):
        errores.append(
            f"{contexto}: falta el detalle de cambios."
        )

    if tiene_operaciones:

        conteos_operaciones = validar_operaciones(
            operaciones,
            configuracion,
            particiones_vistas,
            contexto,
            errores,
        )

        if conteos_validos:

            esperados = {
                "INSERT": nuevos,
                "UPDATE": corregidos,
                "DELETE": eliminados,
            }

            for operacion, esperado in esperados.items():

                recibido = conteos_operaciones[operacion]

                if recibido != esperado:
                    errores.append(
                        f"{contexto}: conteo {operacion} "
                        f"inconsistente. "
                        f"Esperado={esperado}, "
                        f"recibido={recibido}."
                    )

    if tiene_ruta:

        validar_ruta(
            ruta_detalle,
            entorno,
            contexto + ".ruta_detalle_cambios",
            errores,
            config=config,
        )

        advertencias.append(
            f"{contexto}: detalle externo pendiente "
            "de validacion operativa."
        )

    if not configuracion["consumido_gold"]:

        advertencias.append(
            f"{contexto}: dataset reconocido, "
            "pero no consumido por Gold actualmente."
        )

    if nombre == "principales_cuentas":

        advertencias.append(
            f"{contexto}: estabilidad de id_registro "
            "pendiente de confirmacion con Silver."
        )

    if configuracion["particiones"] is None:

        advertencias.append(
            f"{contexto}: contrato de particiones "
            "pendiente de confirmacion operativa."
        )


# ============================================================
# 12. VALIDACION GENERAL DEL MANIFEST
# ============================================================
def validar_manifest(
    manifest,
    entorno_esperado="PRUEBA",
    config=None,
):
    errores = []
    advertencias = []

    if not isinstance(manifest, dict):
        return (
            ["El manifest debe ser un objeto JSON."],
            [],
        )

    errores_json = validar_compatibilidad_json(
        manifest
    )

    if errores_json:

        return (
            errores_json,
            advertencias,
        )

    comprobar_campos(
        manifest,
        CAMPOS_MANIFEST,
        "manifest",
        errores,
    )

    version = manifest.get("version_manifest")

    if (
        not isinstance(version, str)
        or version not in VERSIONES_SOPORTADAS
    ):
        errores.append(
            "Version del manifest no soportada."
        )

    entorno = manifest.get("entorno")

    if (
        not isinstance(entorno, str)
        or entorno not in ENTORNOS_VALIDOS
    ):
        errores.append(
            "Entorno del manifest invalido."
        )

    elif entorno != entorno_esperado:

        errores.append(
            f"Entorno incompatible: "
            f"esperado={entorno_esperado}, "
            f"recibido={entorno}."
        )

    if entorno == "PRODUCCION":
        errores.append(
            "Produccion bloqueada: contrato provisional "
            "v0.1 pendiente de aprobacion."
        )

    if not identificador_valido(
        manifest.get("run_id")
    ):
        errores.append(
            "run_id invalido."
        )

    if manifest.get(
        "estado"
    ) != ESTADO_SILVER_APROBADO:

        errores.append(
            "Silver no se encuentra publicado y aprobado."
        )

    if manifest.get(
        "validacion_aprobada"
    ) is not True:

        errores.append(
            "validacion_aprobada debe ser true."
        )

    if not timestamp_valido(
        manifest.get("fecha_publicacion")
    ):
        errores.append(
            "fecha_publicacion invalida."
        )

    tipo_publicacion = manifest.get(
        "tipo_publicacion"
    )

    if (
        not isinstance(tipo_publicacion, str)
        or tipo_publicacion
        not in TIPOS_PUBLICACION_VALIDOS
    ):
        errores.append(
            "Tipo de publicacion no soportado."
        )

    datasets = manifest.get("datasets")

    if not isinstance(datasets, list):

        errores.append(
            "datasets debe ser una lista."
        )

        return errores, advertencias

    if not datasets:

        errores.append(
            "El manifest no contiene datasets."
        )

        return errores, advertencias

    datasets_vistos = set()

    for indice, dataset in enumerate(datasets):

        if isinstance(dataset, dict):

            nombre = dataset.get("dataset")

            if texto_valido(nombre):

                if nombre in datasets_vistos:
                    errores.append(
                        f"Dataset duplicado: {nombre}"
                    )

                datasets_vistos.add(nombre)

        validar_dataset(
            dataset,
            indice,
            entorno,
            errores,
            advertencias,
            config=config,
        )

    return errores, advertencias


# ============================================================
# 13. CONSTRUIR RESULTADO
# ============================================================

def procesar_manifest(
    ruta,
    entorno_esperado="PRUEBA",
    backend=None,
):
    resultado = {
        "archivo": str(ruta),
        "run_id": None,
        "version_manifest": None,
        "entorno": None,
        "huella_sha256": None,
        "validacion_estructural_aprobada": False,
        "validacion_operativa_aprobada": False,
        "autorizacion_ejecucion_gold": False,
        "estado": "GOLD_RECHAZADO",
        "datasets": [],
        "errores": [],
        "advertencias": [],
    }

    try:
        if backend is None and str(ruta).startswith("gs://"):
            try:
                config = cargar_configuracion()
                if config.backend != BACKEND_GCS:
                    raise ManifestError(
                        "Se requiere configuracion GCS."
                    )
                backend = crear_backend(config)
            except Exception as exc:
                raise ManifestError(str(exc)) from exc

        manifest, huella = cargar_manifest(
            ruta,
            backend=backend,
        )

        resultado["huella_sha256"] = huella

        resultado["run_id"] = manifest.get(
            "run_id"
        )

        resultado["version_manifest"] = manifest.get(
            "version_manifest"
        )

        resultado["entorno"] = manifest.get(
            "entorno"
        )

        errores, advertencias = validar_manifest(
            manifest,
            entorno_esperado,
            config=backend.config if backend is not None else None,
        )

        resultado["errores"] = errores
        resultado["advertencias"] = advertencias

        datasets = manifest.get("datasets")

        if isinstance(datasets, list):

            resultado["datasets"] = [
                item.get("dataset")
                for item in datasets
                if isinstance(item, dict)
            ]

        if not errores:

            resultado[
                "validacion_estructural_aprobada"
            ] = True

            resultado["estado"] = (
                "MANIFEST_ESTRUCTURA_APROBADA"
            )

            resultado["advertencias"].append(
                "Validacion operativa de Silver pendiente. "
                "No se autoriza ejecutar Gold todavia."
            )

    except ManifestError as exc:

        resultado["errores"].append(str(exc))

    return resultado


# ============================================================
# 14. INTERFAZ DE LINEA DE COMANDOS
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Validador estructural de manifests "
            "Silver -> Gold para BVL Analytics."
        )
    )

    parser.add_argument(
        "--manifest",
        required=True,
        help="Ruta del manifest JSON.",
    )

    parser.add_argument(
        "--entorno",
        choices=["PRUEBA", "PRODUCCION"],
        default="PRUEBA",
        help="Entorno esperado. Default: PRUEBA.",
    )

    argumentos = parser.parse_args()

    resultado = procesar_manifest(
        argumentos.manifest,
        argumentos.entorno,
    )

    print(
        json.dumps(
            resultado,
            indent=4,
            ensure_ascii=False,
            allow_nan=False,
        )
    )

    if resultado[
        "validacion_estructural_aprobada"
    ]:
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
