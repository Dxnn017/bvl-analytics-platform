"""Materializacion local de staging incremental.

No carga datos a GCS ni BigQuery.
No modifica los productos Gold.
"""

import re

from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from preparador_incremental import (
    CONTROLES,
    HASHES_STAGING,
    normalizar_fila,
    normalizar_decimal,
    validar_identidad,
)

from preparador_carga_historica import (
    exigir,
    DECIMALES_SAB,
)

from trazabilidad_carga_historica import (
    HASHES_CONTRATOS,
    huella_archivo,
    huella_documento,
    validar_sha,
    validar_version,
)

from src.gold.bigquery.verificar_payload_local import (
    calcular_sha256,
)


def tipo_arrow(campo):

    nombre = campo["name"]
    tipo = campo["spark_type"]

    directos = {
        "string": pa.string(),
        "boolean": pa.bool_(),
        "date": pa.date32(),
        "double": pa.float64(),
        "int": pa.int32(),
        "integer": pa.int32(),
        "bigint": pa.int64(),
        "long": pa.int64(),
    }

    if tipo in directos:

        return directos[tipo]

    if tipo.startswith("decimal("):

        precision, escala = map(
            int,
            re.fullmatch(
                r"decimal\(([0-9]+),([0-9]+)\)",
                tipo,
            ).groups(),
        )

        if nombre in DECIMALES_SAB:

            exigir(
                tipo == "decimal(34,2)",
                "Tipo decimal SAB incompatible"
            )

            precision = 31

        return pa.decimal128(
            precision, escala
        )

    raise ValueError(
        "Tipo fisico no admitido: " + tipo
    )


def esquema_staging(contrato):

    campos = [
        pa.field(
            campo["name"],
            tipo_arrow(campo),
            nullable=True,
        )
        for campo in contrato["gold"]["columns"]
    ]

    for nombre, tipo in CONTROLES:

        campos.append(
            pa.field(
                nombre,
                pa.bool_()
                if tipo == "BOOL"
                else pa.string(),
                nullable=False,
            )
        )

    esquema = pa.schema(campos)

    exigir(
        esquema.names
        == contrato["staging_columns"],
        "Esquema fisico incompatible"
    )

    return esquema


def validar_paquete(
    producto,
    paquete,
    contrato,
):

    exigir(
        set(paquete) == {
            "payload",
            "staging",
            "evidence",
            "evidence_sha256",
        },
        "Paquete incremental incompatible"
    )

    evidencia = paquete["evidence"]

    exigir(
        huella_documento(evidencia)
        == paquete["evidence_sha256"],
        "Evidencia alterada"
    )

    exigir(
        evidencia["product"] == producto
        == contrato["gold"]["product"],
        "Producto incompatible"
    )

    exigir(
        evidencia["status"]
        == "LOCAL_INCREMENTAL_CANDIDATE_ONLY",
        "Estado incompatible"
    )

    exigir(
        evidencia["gold_contract_sha256"]
        == contrato["sha256"]
        == HASHES_CONTRATOS[producto],
        "Contrato Gold incompatible"
    )

    exigir(
        evidencia["staging_contract_sha256"]
        == contrato["staging_sha256"]
        == HASHES_STAGING[producto],
        "Contrato staging incompatible"
    )

    for bandera in (
        "official_source_verified",
        "official_version_verified",
        "quality_authorization_verified",
        "delete_authorized",
        "execution_authorized",
        "production_write_authorized",
        "publication_authorized",
    ):

        exigir(
            evidencia[bandera] is False,
            "Alcance operativo incompatible"
        )

    validar_identidad(evidencia["batch_id"])
    validar_identidad(evidencia["run_id"])

    for nombre in (
        "source_version",
        "previous_gold_version",
        "new_gold_version",
    ):

        validar_version(evidencia[nombre])

    exigir(
        evidencia["previous_gold_version"]
        != evidencia["new_gold_version"],
        "Version sin avance"
    )

    validar_sha(
        evidencia["source_manifest_sha256"]
    )

    validar_sha(
        evidencia["payload_sha256"]
    )

    payload = paquete["payload"]

    staging = paquete["staging"]

    exigir(
        isinstance(payload, list)
        and isinstance(staging, list)
        and type(evidencia["records_count"]) is int
        and len(payload)
        == len(staging)
        == evidencia["records_count"]
        and len(payload) > 0,
        "Conteo de transferencia incompatible"
    )

    exigir(
        evidencia["operations"] == {
            "UPSERT": len(payload),
            "DELETE": 0,
        },
        "Operaciones incompatibles"
    )

    exigir(
        evidencia["staging_columns"]
        == len(contrato["staging_columns"]),
        "Columnas staging incompatibles"
    )

    exigir(
        calcular_sha256(payload)
        == evidencia["payload_sha256"],
        "Payload alterado"
    )

    exigir(
        huella_documento(staging)
        == evidencia["staging_content_sha256"],
        "Staging alterado"
    )

    vistas = set()

    for fila, transferida in zip(
        payload, staging
    ):

        clave, normalizada = normalizar_fila(
            producto, fila, contrato
        )

        exigir(
            normalizada == fila,
            "Payload no normalizado"
        )

        exigir(
            clave not in vistas,
            "Clave duplicada"
        )

        vistas.add(clave)

        esperada = {
            **fila,
            "delete_authorized": False,
            "batch_id": evidencia["batch_id"],
            "payload_sha256":
                evidencia["payload_sha256"],
            "run_id": evidencia["run_id"],
            "source_version":
                evidencia["source_version"],
        }

        exigir(
            transferida == esperada,
            "Fila staging incompatible con payload"
        )

    return evidencia


def fila_fisica(fila, contrato):

    resultado = dict(fila)

    for campo in contrato["gold"]["columns"]:

        nombre = campo["name"]

        valor = resultado[nombre]

        if valor is None:

            continue

        tipo = campo["spark_type"]

        if tipo == "date":

            resultado[nombre] = (
                date.fromisoformat(valor)
            )

        elif tipo.startswith("decimal("):

            resultado[nombre] = Decimal(
                normalizar_decimal(valor, tipo)
            )

    return resultado


def fila_logica(fila, contrato):

    resultado = dict(fila)

    for campo in contrato["gold"]["columns"]:

        nombre = campo["name"]

        valor = resultado[nombre]

        if valor is None:

            continue

        tipo = campo["spark_type"]

        if tipo == "date":

            resultado[nombre] = (
                valor.isoformat()
            )

        elif tipo.startswith("decimal("):

            resultado[nombre] = (
                normalizar_decimal(valor, tipo)
            )

    return resultado


def validar_parquet(
    ruta,
    producto,
    paquete,
    contrato,
):

    evidencia = validar_paquete(
        producto, paquete, contrato
    )

    ruta = Path(ruta)

    exigir(
        ruta.is_file()
        and not ruta.is_symlink(),
        "Archivo Parquet incompatible"
    )

    tabla = pq.ParquetFile(ruta).read()

    exigir(
        tabla.schema.equals(
            esquema_staging(contrato),
            check_metadata=False,
        ),
        "Esquema Parquet incompatible"
    )

    exigir(
        tabla.num_rows
        == evidencia["records_count"],
        "Conteo Parquet incompatible"
    )

    reconstruidas = [
        fila_logica(fila, contrato)
        for fila in tabla.to_pylist()
    ]

    exigir(
        reconstruidas == paquete["staging"],
        "Contenido Parquet alterado"
    )

    exigir(
        huella_documento(reconstruidas)
        == evidencia["staging_content_sha256"],
        "Huella de contenido incompatible"
    )

    return {
        "records_count": tabla.num_rows,
        "columns_count": tabla.num_columns,
        **huella_archivo(ruta),
    }


def materializar_parquet(
    destino,
    producto,
    paquete,
    contrato,
):

    evidencia = validar_paquete(
        producto, paquete, contrato
    )

    destino = Path(destino).resolve()

    exigir(
        destino.is_dir(),
        "Directorio destino inexistente"
    )

    ruta = destino / (
        producto + ".parquet"
    )

    esquema = esquema_staging(contrato)

    tabla = pa.Table.from_pylist(
        [
            fila_fisica(fila, contrato)
            for fila in paquete["staging"]
        ],
        schema=esquema,
    )

    tabla.validate(full=True)

    # Creacion exclusiva: no sobrescribir
    # archivos existentes.

    with ruta.open("xb") as archivo:

        pq.write_table(
            tabla,
            archivo,
            compression="snappy",
            version="2.6",
        )

    comprobacion = validar_parquet(
        ruta, producto, paquete, contrato
    )

    informe = {
        "status":
            "LOCAL_PARQUET_CANDIDATE_ONLY",
        "product": producto,
        "relative_path": ruta.name,
        "batch_id": evidencia["batch_id"],
        "payload_sha256":
            evidencia["payload_sha256"],
        "staging_content_sha256":
            evidencia["staging_content_sha256"],
        "evidence_sha256":
            paquete["evidence_sha256"],
        **comprobacion,
        "roundtrip_verified": True,
        "official_source_verified": False,
        "cloud_write_authorized": False,
        "production_write_authorized": False,
        "publication_authorized": False,
    }

    return {
        "transfer": informe,
        "transfer_sha256":
            huella_documento(informe),
    }
