"""Integracion local del detector Gold con staging BigQuery.

No ejecuta constructores, no modifica Gold y no
autoriza operaciones cloud.
"""

from pyspark.sql import functions as F

from src.gold.incremental.detectar_cambios_gold import (
    detectar_cambios_gold,
    CLAVES_PRODUCTO,
)

from preparador_carga_historica import (
    PRODUCTOS,
    exigir,
    validar_esquema,
)

from preparador_incremental import (
    preparar_batch_incremental,
    normalizar_fila,
    validar_identidad,
)

from trazabilidad_carga_historica import (
    HASHES_CONTRATOS,
    validar_sha,
    validar_version,
    huella_documento,
)

from src.gold.bigquery.verificar_payload_local import (
    calcular_sha256,
)


def validar_snapshot(producto, datos, contrato):

    exigir(
        len(datos.columns)
        == len(set(datos.columns)),
        "Columnas Gold duplicadas"
    )

    esperado = {
        campo["name"]: campo["spark_type"]
        for campo in contrato["gold"]["columns"]
    }

    observado = {
        campo.name: campo.dataType.simpleString()
        for campo in datos.schema.fields
    }

    aliases = {
        "integer": "int",
        "long": "bigint",
    }

    esperado = {
        nombre: aliases.get(tipo, tipo)
        for nombre, tipo in esperado.items()
    }

    exigir(
        esperado == observado,
        "Esquema Gold incompatible"
    )

    claves = contrato["gold"]["business_keys"]

    exigir(
        claves == CLAVES_PRODUCTO[producto],
        "Claves contractuales incompatibles"
    )

    for clave in claves:

        invalidos = datos.filter(
            F.col(clave).isNull()
            | (
                F.trim(
                    F.col(clave).cast("string")
                ) == ""
            )
        ).limit(1).count()

        exigir(
            invalidos == 0,
            "Clave Gold nula o vacia"
        )

    return datos


def extraer_delta_local(
    producto,
    anterior,
    vigente,
    contrato,
    *,
    max_rows=100000,
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
        type(max_rows) is int
        and 0 < max_rows <= 100000,
        "Limite local incompatible"
    )

    validar_snapshot(
        producto, anterior, contrato
    )

    validar_snapshot(
        producto, vigente, contrato
    )

    claves = contrato["gold"]["business_keys"]

    columnas = [
        campo["name"]
        for campo in contrato["gold"]["columns"]
    ]

    cambios = detectar_cambios_gold(
        anterior,
        vigente,
        producto,
    )

    exigir(
        cambios["producto_gold"] == producto,
        "Detector devolvio producto incompatible"
    )

    anteriores = cambios["claves_anteriores"]
    nuevas = cambios["claves_nuevas"]

    eliminadas = anteriores.join(
        nuevas,
        claves,
        "left_anti",
    )

    cantidad_eliminadas = eliminadas.count()

    exigir(
        cantidad_eliminadas == 0,
        "DELETE detectado: lote completo bloqueado"
    )

    insertadas = nuevas.join(
        anteriores,
        claves,
        "left_anti",
    )

    modificadas = nuevas.join(
        anteriores,
        claves,
        "inner",
    )

    cantidad_insertadas = insertadas.count()
    cantidad_modificadas = modificadas.count()

    cantidad = (
        cantidad_insertadas
        + cantidad_modificadas
    )

    exigir(
        cantidad <= max_rows,
        "Delta excede limite local en memoria"
    )

    if cantidad == 0:

        return {
            "status": "LOCAL_NO_CHANGES",
            "product": producto,
            "rows": [],
            "inserted_keys": 0,
            "modified_keys": 0,
            "deleted_keys": 0,
            "payload_sha256": None,
            "version_advance_authorized": False,
            "execution_authorized": False,
        }

    seleccionadas = (
        vigente.join(
            nuevas,
            claves,
            "inner",
        )
        .select(*columnas)
        .orderBy(*claves)
        .limit(max_rows + 1)
        .collect()
    )

    exigir(
        len(seleccionadas) == cantidad,
        "Conteo delta incompatible"
    )

    filas = []

    for registro in seleccionadas:

        fila = registro.asDict(recursive=True)

        fila["operation"] = "UPSERT"

        _, normalizada = normalizar_fila(
            producto,
            fila,
            contrato,
        )

        filas.append(normalizada)

    return {
        "status": "LOCAL_DELTA_CANDIDATE",
        "product": producto,
        "rows": filas,
        "inserted_keys": cantidad_insertadas,
        "modified_keys": cantidad_modificadas,
        "deleted_keys": 0,
        "payload_sha256":
            calcular_sha256(filas),
        "version_advance_authorized": False,
        "execution_authorized": False,
    }


def preparar_desde_detector(
    producto,
    anterior,
    vigente,
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
    max_rows=100000,
):

    validar_identidad(batch_id)
    validar_identidad(run_id)

    for version in (
        source_version,
        previous_gold_version,
        new_gold_version,
        current_gold_version,
    ):

        validar_version(version)

    validar_sha(source_manifest_sha256)

    exigir(
        previous_gold_version
        == current_gold_version,
        "Continuidad de version incompatible"
    )

    exigir(
        previous_gold_version
        != new_gold_version,
        "Version Gold sin avance"
    )

    exigir(
        type(expected_rows) is int
        and expected_rows >= 0,
        "Conteo incremental incompatible"
    )

    delta = extraer_delta_local(
        producto,
        anterior,
        vigente,
        contrato,
        max_rows=max_rows,
    )

    exigir(
        len(delta["rows"]) == expected_rows,
        "Conteo declarado incompatible"
    )

    if delta["status"] == "LOCAL_NO_CHANGES":

        exigir(
            payload_sha256_declarado is None,
            "Delta vacio con hash incompatible"
        )

        evidencia = {
            "status": "LOCAL_NO_CHANGES",
            "product": producto,
            "batch_id": batch_id,
            "run_id": run_id,
            "source_version": source_version,
            "previous_gold_version":
                previous_gold_version,
            "new_gold_version":
                new_gold_version,
            "source_manifest_sha256":
                source_manifest_sha256,
            "records_count": 0,
            "staging_created": False,
            "version_advance_authorized": False,
            "execution_authorized": False,
        }

        return {
            "delta": delta,
            "batch": None,
            "evidence": evidencia,
            "evidence_sha256":
                huella_documento(evidencia),
        }

    validar_sha(payload_sha256_declarado)

    exigir(
        delta["payload_sha256"]
        == payload_sha256_declarado,
        "Hash del detector incompatible"
    )

    paquete = preparar_batch_incremental(
        producto,
        delta["rows"],
        contrato,
        batch_id=batch_id,
        run_id=run_id,
        source_version=source_version,
        previous_gold_version=previous_gold_version,
        new_gold_version=new_gold_version,
        current_gold_version=current_gold_version,
        source_manifest_sha256=source_manifest_sha256,
        expected_rows=expected_rows,
        payload_sha256_declarado=(
            payload_sha256_declarado
        ),
    )

    evidencia = {
        "status":
            "LOCAL_DETECTOR_INTEGRATION_ONLY",
        "product": producto,
        "inserted_keys": delta["inserted_keys"],
        "modified_keys": delta["modified_keys"],
        "deleted_keys": 0,
        "records_count": expected_rows,
        "payload_sha256":
            delta["payload_sha256"],
        "batch_evidence_sha256":
            paquete["evidence_sha256"],
        "official_source_verified": False,
        "version_advance_authorized": False,
        "execution_authorized": False,
        "publication_authorized": False,
    }

    return {
        "delta": delta,
        "batch": paquete,
        "evidence": evidencia,
        "evidence_sha256":
            huella_documento(evidencia),
    }
