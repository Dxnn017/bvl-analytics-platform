"""Puente local Gold -> staging incremental BigQuery.

No ejecuta GCP, SQL ni MERGE.
No concede autorizaciones operativas.
"""

import hashlib
import json
import math
import re

from datetime import date
from decimal import Decimal
from pathlib import Path

from preparador_carga_historica import (
    cargar_contratos,
    PRODUCTOS,
    DECIMALES_SAB,
    exigir,
)

from trazabilidad_carga_historica import (
    HASHES_CONTRATOS,
    validar_sha,
    validar_version,
    huella_documento,
)

from src.gold.bigquery.verificar_payload_local import (
    calcular_sha256,
    verificar_sha256,
)


CONTROLES = [
    ("operation", "STRING"),
    ("delete_authorized", "BOOL"),
    ("batch_id", "STRING"),
    ("payload_sha256", "STRING"),
    ("run_id", "STRING"),
    ("source_version", "STRING"),
]

COLUMNAS_STAGING = {
    "mercado_valor_diario": 47,
    "actividad_sab_diaria": 47,
    "empresa_trimestre": 64,
}

HASHES_STAGING = {
    "mercado_valor_diario":
        "a72e4bff7bfd917e56cc80759d7f35e09"
        "884e943d8d52afa72f7922ee8761dee",
    "actividad_sab_diaria":
        "f0108bd6997573322ccb5bdd3c687d6da"
        "32bb8564ae5cfdac01efce5b065dc86",
    "empresa_trimestre":
        "a54cd803ea86043da68a471991f107fe"
        "f4c138e88a3c0eaf370b8567a42c463c",
}


def validar_identidad(valor):

    exigir(
        isinstance(valor, str)
        and re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}",
            valor,
        ) is not None,
        "Identidad incremental incompatible"
    )


def cargar_contratos_incrementales(raiz):

    raiz = Path(raiz).resolve()

    _, contratos = cargar_contratos(raiz)

    for producto, contrato in contratos.items():

        exigir(
            contrato["sha256"]
            == HASHES_CONTRATOS[producto],
            "Contrato Gold alterado"
        )

        ruta = (
            raiz / "docs/bigquery"
            / f"{producto}_staging.sql"
        )

        contenido = ruta.read_bytes()

        exigir(
            hashlib.sha256(contenido).hexdigest()
            == HASHES_STAGING[producto],
            "Contrato staging alterado"
        )

        texto = contenido.decode("utf-8")

        columnas_sql = re.findall(
            r"^\s*`([A-Za-z_][A-Za-z0-9_]*)`"
            r"\s+([A-Z0-9]+)"
            r"(\s+NOT NULL)?\s*,?\s*$",
            texto,
            re.MULTILINE,
        )

        observadas = [
            (nombre, tipo, bool(obligatoria))
            for nombre, tipo, obligatoria
            in columnas_sql
        ]

        esperadas = [
            (
                campo["name"],
                campo["bigquery_type"],
                False,
            )
            for campo in contrato["gold"]["columns"]
        ]

        esperadas.extend(
            (nombre, tipo, True)
            for nombre, tipo in CONTROLES
        )

        exigir(
            observadas == esperadas,
            f"Esquema staging incompatible: {producto}"
        )

        exigir(
            len(observadas)
            == COLUMNAS_STAGING[producto],
            "Cantidad staging incompatible"
        )

        contrato["staging_sha256"] = (
            HASHES_STAGING[producto]
        )

        contrato["staging_columns"] = [
            nombre for nombre, _, _ in observadas
        ]

    return contratos


def normalizar_fecha(valor):

    exigir(
        type(valor) in (date, str),
        "DATE incompatible"
    )

    texto = (
        valor.isoformat()
        if type(valor) is date
        else valor
    )

    fecha = date.fromisoformat(texto)

    exigir(
        fecha.isoformat() == texto,
        "DATE no canonico"
    )

    return texto


def normalizar_decimal(valor, tipo):

    exigir(
        isinstance(valor, (Decimal, str)),
        "Decimal debe recibirse sin FLOAT64"
    )

    texto = str(valor)

    exigir(
        re.fullmatch(
            r"-?[0-9]+(?:\.[0-9]+)?",
            texto,
        ) is not None,
        "Formato decimal incompatible"
    )

    numero = Decimal(texto)

    exigir(
        numero.is_finite(),
        "Decimal no finito"
    )

    precision, escala = map(
        int,
        re.fullmatch(
            r"decimal\(([0-9]+),([0-9]+)\)",
            tipo,
        ).groups(),
    )

    # Comprobacion exacta sin redondeo.
    coeficiente = numero.as_tuple()

    digitos = coeficiente.digits

    exponente = coeficiente.exponent

    if exponente < -escala:

        exceso = -escala - exponente

        exigir(
            all(d == 0 for d in digitos[-exceso:]),
            "Escala decimal incompatible"
        )

    enteros = (
        max(numero.adjusted() + 1, 0)
        if numero != 0
        else 0
    )

    exigir(
        enteros <= precision - escala,
        "Desbordamiento decimal Gold"
    )

    exigir(
        enteros <= 29,
        "Desbordamiento NUMERIC BigQuery"
    )

    # Representacion fija, sin perdida monetaria.
    normalizado = format(numero, f".{escala}f")

    exigir(
        Decimal(normalizado) == numero,
        "Conversion decimal no exacta"
    )

    return normalizado


def normalizar_valor(valor, campo):

    nombre = campo["name"]

    tipo = campo["spark_type"]

    if valor is None:

        exigir(
            campo["nullable"] is True,
            "Columna obligatoria nula: " + nombre
        )

        return None

    if tipo == "string":

        exigir(
            type(valor) is str,
            "STRING incompatible: " + nombre
        )

        return valor

    if tipo == "boolean":

        exigir(
            type(valor) is bool,
            "BOOL incompatible: " + nombre
        )

        return valor

    if tipo == "date":

        return normalizar_fecha(valor)

    if tipo in (
        "int", "integer", "long", "bigint"
    ):

        exigir(
            type(valor) is int,
            "INT incompatible: " + nombre
        )

        bits = (
            32
            if tipo in ("int", "integer")
            else 64
        )

        exigir(
            -(2 ** (bits - 1))
            <= valor
            < 2 ** (bits - 1),
            "Entero fuera de rango: " + nombre
        )

        return valor

    if tipo == "double":

        exigir(
            type(valor) is float
            and math.isfinite(valor),
            "FLOAT64 incompatible: " + nombre
        )

        return valor

    if tipo.startswith("decimal("):

        return normalizar_decimal(
            valor, tipo
        )

    raise ValueError(
        "Tipo Gold no contemplado: " + tipo
    )


def validar_claves(fila, contrato):

    valores = []

    for nombre in contrato["gold"]["business_keys"]:

        valor = fila[nombre]

        exigir(
            valor is not None
            and (
                not isinstance(valor, str)
                or bool(valor.strip())
            ),
            "Clave nula o vacia: " + nombre
        )

        valores.append(valor)

    return tuple(valores)


def validar_particiones(
    producto, fila, contrato,
):

    nombre = contrato["configuracion"][
        "bigquery_partition"
    ]

    exigir(
        fila[nombre] is not None,
        "Fecha de particion nula"
    )

    fecha = date.fromisoformat(
        fila[nombre]
    )

    exigir(
        fila["anio"] == fecha.year,
        "Anio de particion incompatible"
    )

    if producto == "empresa_trimestre":

        periodo = (
            (fecha.month - 1) // 3 + 1
        )

        exigir(
            fila["trimestre"] == periodo,
            "Trimestre de particion incompatible"
        )

    else:

        exigir(
            fila["mes"] == fecha.month,
            "Mes de particion incompatible"
        )


def validar_mercado(fila):

    original = fila["nemonico_original"]

    exigir(
        original is not None
        and fila["nemonico"]
        == original.strip().upper(),
        "Nemonico canonico incompatible"
    )


def validar_cobertura_sab(fila):

    estado = fila["estado_cobertura"]

    exigir(
        estado in (
            "COMPLETA",
            "PARCIAL",
            "DESCONOCIDA",
        ),
        "Estado de cobertura incompatible"
    )

    exigir(
        fila["cobertura_completa"]
        == (estado == "COMPLETA"),
        "Cobertura SAB inconsistente"
    )

    elegibles = fila["registros_elegibles"]

    cuarentena = fila["registros_cuarentena"]

    excluidos = fila["archivos_excluidos"]

    if elegibles is not None:

        exigir(
            elegibles >= 0
            and elegibles
            == fila["cantidad_sab_diaria"],
            "Registros elegibles incompatibles"
        )

    for valor in (cuarentena, excluidos):

        exigir(
            valor is None or valor >= 0,
            "Conteo SAB negativo"
        )

    if estado in ("COMPLETA", "PARCIAL"):

        exigir(
            fila["trazabilidad_verificada"] is True
            and all(
                isinstance(fila[c], str)
                and bool(fila[c].strip())
                for c in (
                    "version_silver",
                    "scope_quality_id",
                )
            )
            and all(
                v is not None
                for v in (
                    elegibles,
                    cuarentena,
                    excluidos,
                )
            ),
            "Cobertura SAB no sustentada"
        )

    if estado == "COMPLETA":

        exigir(
            cuarentena == 0 and excluidos == 0,
            "Cobertura completa incompatible"
        )

    if estado == "PARCIAL":

        exigir(
            cuarentena > 0 or excluidos > 0,
            "Cobertura parcial incompatible"
        )


def normalizar_fila(
    producto,
    fila,
    contrato,
):

    exigir(
        isinstance(fila, dict),
        "Fila incremental incompatible"
    )

    campos = contrato["gold"]["columns"]

    nombres = [
        campo["name"] for campo in campos
    ]

    exigir(
        set(fila) == set(nombres) | {"operation"},
        "Columnas delta incompatibles"
    )

    exigir(
        fila["operation"] == "UPSERT",
        "DELETE u operacion no admitida"
    )

    normalizada = {
        campo["name"]: normalizar_valor(
            fila[campo["name"]],
            campo,
        )
        for campo in campos
    }

    clave = validar_claves(
        normalizada, contrato
    )

    validar_particiones(
        producto,
        normalizada,
        contrato,
    )

    if producto == "mercado_valor_diario":

        validar_mercado(normalizada)

    if producto == "actividad_sab_diaria":

        validar_cobertura_sab(normalizada)

    normalizada["operation"] = "UPSERT"

    return clave, normalizada


def preparar_batch_incremental(
    producto,
    filas,
    contrato,
    *,
    batch_id,
    run_id,
    source_version,
    previous_gold_version,
    new_gold_version,
    current_gold_version,
    source_manifest_sha256,
    expected_rows,
    payload_sha256_declarado,
):

    exigir(
        producto in PRODUCTOS
        and contrato["gold"]["product"] == producto,
        "Producto incremental incompatible"
    )

    exigir(
        contrato["sha256"]
        == HASHES_CONTRATOS[producto],
        "Contrato Gold incompatible"
    )

    exigir(
        contrato["staging_sha256"]
        == HASHES_STAGING[producto],
        "Contrato staging incompatible"
    )

    validar_identidad(batch_id)

    validar_identidad(run_id)

    for version in (
        source_version,
        previous_gold_version,
        new_gold_version,
        current_gold_version,
    ):

        validar_version(version)

    exigir(
        previous_gold_version
        == current_gold_version,
        "Continuidad de version incompatible"
    )

    exigir(
        previous_gold_version != new_gold_version,
        "Version Gold sin avance"
    )

    validar_sha(source_manifest_sha256)

    validar_sha(payload_sha256_declarado)

    exigir(
        type(expected_rows) is int
        and expected_rows > 0,
        "Conteo incremental incompatible"
    )

    exigir(
        isinstance(filas, list)
        and len(filas) == expected_rows,
        "Batch vacio o conteo incompatible"
    )

    payload = []

    vistas = set()

    for fila in filas:

        clave, normalizada = normalizar_fila(
            producto,
            fila,
            contrato,
        )

        exigir(
            clave not in vistas,
            "Clave incremental duplicada"
        )

        vistas.add(clave)

        payload.append(normalizada)

    # No reordenar la lista: el hash existente
    # depende del orden de sus filas.
    #
    # El hash se calcula antes de incorporar
    # las cinco columnas de control restantes.
    huella = calcular_sha256(payload)

    verificar_sha256(
        payload,
        payload_sha256_declarado,
    )

    exigir(
        huella == payload_sha256_declarado,
        "Huella incremental incompatible"
    )

    staging = []

    for fila in payload:

        transferida = {
            **fila,
            "delete_authorized": False,
            "batch_id": batch_id,
            "payload_sha256": huella,
            "run_id": run_id,
            "source_version": source_version,
        }

        exigir(
            list(transferida)
            == contrato["staging_columns"],
            "Orden staging incompatible"
        )

        staging.append(transferida)

    evidencia = {
        "status":
            "LOCAL_INCREMENTAL_CANDIDATE_ONLY",
        "product": producto,
        "batch_id": batch_id,
        "run_id": run_id,
        "source_version": source_version,
        "previous_gold_version":
            previous_gold_version,
        "new_gold_version": new_gold_version,
        "source_manifest_sha256":
            source_manifest_sha256,
        "payload_sha256": huella,
        "staging_content_sha256":
            huella_documento(staging),
        "gold_contract_sha256":
            contrato["sha256"],
        "staging_contract_sha256":
            contrato["staging_sha256"],
        "records_count": len(staging),
        "staging_columns":
            len(contrato["staging_columns"]),
        "operations": {
            "UPSERT": len(staging),
            "DELETE": 0,
        },
        "decimal_transfer_policy":
            "EXACT_NUMERIC_SCALE_2",
        "official_source_verified": False,
        "official_version_verified": False,
        "quality_authorization_verified": False,
        "delete_authorized": False,
        "execution_authorized": False,
        "production_write_authorized": False,
        "publication_authorized": False,
    }

    return {
        "payload": payload,
        "staging": staging,
        "evidence": evidencia,
        "evidence_sha256":
            huella_documento(evidencia),
    }


def exigir_autorizacion_operativa(*args, **kwargs):

    raise PermissionError(
        "CARGA_INCREMENTAL_NO_AUTORIZADA"
    )
