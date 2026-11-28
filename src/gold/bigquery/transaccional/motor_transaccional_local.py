"""Modelo transaccional candidato de BigQuery.

Exclusivamente SQLite local.
No conecta con BigQuery ni GCS.
No concede autorizaciones operativas.
"""

import json
import sqlite3

from copy import deepcopy

from preparador_carga_historica import (
    PRODUCTOS,
    exigir,
)

from preparador_incremental import (
    normalizar_fila,
)

from transferencia_incremental_local import (
    validar_paquete,
)

from trazabilidad_carga_historica import (
    validar_version,
    huella_documento,
)

from src.gold.bigquery.comparador_negocio_local import (
    METADATOS_TECNICOS,
)


ESTADO = "LOCAL_TRANSACTION_CANDIDATE_ONLY"


def json_canonico(valor):

    return json.dumps(
        valor,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def exigir_conexion_local(db):

    exigir(
        isinstance(db, sqlite3.Connection),
        "Solo se admite SQLite local"
    )

    exigir(
        not db.in_transaction,
        "Conexion con transaccion activa"
    )


def inicializar_control_local(db):

    exigir_conexion_local(db)

    db.executescript("""
        CREATE TABLE IF NOT EXISTS bq_versiones (
            product TEXT PRIMARY KEY,
            current_version TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bq_datos (
            product TEXT NOT NULL,
            business_key TEXT NOT NULL,
            payload TEXT NOT NULL,
            PRIMARY KEY(product, business_key)
        );

        CREATE TABLE IF NOT EXISTS bq_batches (
            product TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            source_version TEXT NOT NULL,
            previous_version TEXT NOT NULL,
            new_version TEXT NOT NULL,
            payload_sha256 TEXT NOT NULL,
            staging_sha256 TEXT NOT NULL,
            evidence_sha256 TEXT NOT NULL,
            status TEXT NOT NULL,
            PRIMARY KEY(product, batch_id),
            UNIQUE(product, new_version)
        );

        CREATE TABLE IF NOT EXISTS bq_auditoria (
            product TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            inserted_rows INTEGER NOT NULL,
            updated_rows INTEGER NOT NULL,
            business_updates INTEGER NOT NULL,
            technical_updates INTEGER NOT NULL,
            unchanged_rows INTEGER NOT NULL,
            deleted_rows INTEGER NOT NULL,
            received_rows INTEGER NOT NULL,
            report_sha256 TEXT NOT NULL,
            PRIMARY KEY(product, batch_id),
            FOREIGN KEY(product, batch_id)
                REFERENCES bq_batches(product, batch_id),
            CHECK(deleted_rows = 0),
            CHECK(
                updated_rows =
                    business_updates + technical_updates
            ),
            CHECK(
                received_rows =
                    inserted_rows +
                    updated_rows +
                    unchanged_rows
            )
        );

        CREATE TABLE IF NOT EXISTS bq_version_history (
            product TEXT NOT NULL,
            version TEXT NOT NULL,
            PRIMARY KEY(product, version)
        );
    """)


def iniciar_transaccion(db):

    exigir_conexion_local(db)

    db.execute("BEGIN IMMEDIATE")


def contenido_gold(fila, contrato):

    return {
        campo["name"]: fila[campo["name"]]
        for campo in contrato["gold"]["columns"]
    }


def clave_canonica(fila, contrato):

    return json_canonico([
        fila[nombre]
        for nombre in contrato["gold"]["business_keys"]
    ])


def inicializar_snapshot_sintetico(
    db,
    producto,
    version,
    filas,
    contrato,
):

    exigir(
        producto in PRODUCTOS
        and contrato["gold"]["product"] == producto,
        "Producto incompatible"
    )

    validar_version(version)

    exigir(
        isinstance(filas, list)
        and len(filas) > 0,
        "Snapshot local vacio"
    )

    normalizadas = []

    vistas = set()

    for fila in filas:

        clave, normalizada = normalizar_fila(
            producto,
            {
                **fila,
                "operation": "UPSERT",
            },
            contrato,
        )

        exigir(
            clave not in vistas,
            "Snapshot con claves duplicadas"
        )

        vistas.add(clave)

        normalizadas.append(normalizada)

    iniciar_transaccion(db)

    try:

        existente = db.execute(
            """
            SELECT current_version
            FROM bq_versiones
            WHERE product = ?
            """,
            (producto,),
        ).fetchone()

        exigir(
            existente is None,
            "Snapshot ya inicializado"
        )

        for tabla in (
            "bq_datos",
            "bq_batches",
            "bq_auditoria",
            "bq_version_history",
        ):

            cantidad = db.execute(
                f"""
                SELECT COUNT(*) FROM {tabla}
                WHERE product = ?
                """,
                (producto,),
            ).fetchone()[0]

            exigir(
                cantidad == 0,
                "Estado previo incompatible"
            )

        for fila in normalizadas:

            db.execute(
                """
                INSERT INTO bq_datos
                VALUES (?, ?, ?)
                """,
                (
                    producto,
                    clave_canonica(fila, contrato),
                    json_canonico(
                        contenido_gold(fila, contrato)
                    ),
                ),
            )

        db.execute(
            "INSERT INTO bq_versiones VALUES (?, ?)",
            (producto, version),
        )

        db.execute(
            "INSERT INTO bq_version_history VALUES (?, ?)",
            (producto, version),
        )

        db.commit()

    except BaseException:

        db.rollback()
        raise

    return {
        "status": "SYNTHETIC_SNAPSHOT_INITIALIZED",
        "product": producto,
        "version": version,
        "records_count": len(normalizadas),
        "official_snapshot_verified": False,
        "execution_authorized": False,
    }


def clasificar_cambio(
    anterior,
    vigente,
    contrato,
):

    campos = [
        campo["name"]
        for campo in contrato["gold"]["columns"]
    ]

    exigir(
        set(anterior) == set(vigente) == set(campos),
        "Contenido Gold incompleto"
    )

    for clave in contrato["gold"]["business_keys"]:

        exigir(
            anterior[clave] == vigente[clave],
            "Modificacion de clave no permitida"
        )

    modificadas = [
        campo for campo in campos
        if anterior[campo] != vigente[campo]
    ]

    if not modificadas:

        return "NOOP"

    if all(
        campo in METADATOS_TECNICOS
        for campo in modificadas
    ):

        return "UPDATE_TECHNICAL"

    return "UPDATE_BUSINESS"


def identidad_batch(evidencia, paquete):

    return (
        evidencia["run_id"],
        evidencia["source_version"],
        evidencia["previous_gold_version"],
        evidencia["new_gold_version"],
        evidencia["payload_sha256"],
        evidencia["staging_content_sha256"],
        paquete["evidence_sha256"],
    )


def construir_informe(
    producto,
    evidencia,
    paquete,
    metricas,
):

    exigir(
        metricas["updated_rows"]
        == metricas["business_updates"]
        + metricas["technical_updates"],
        "Metricas UPDATE inconsistentes"
    )

    exigir(
        metricas["inserted_rows"]
        + metricas["updated_rows"]
        + metricas["unchanged_rows"]
        == evidencia["records_count"],
        "Metricas del batch inconsistentes"
    )

    exigir(
        metricas["deleted_rows"] == 0,
        "DELETE no permitido"
    )

    return {
        "status": ESTADO,
        "result": "COMPLETED",
        "product": producto,
        "batch_id": evidencia["batch_id"],
        "previous_version":
            evidencia["previous_gold_version"],
        "new_version":
            evidencia["new_gold_version"],
        "payload_sha256":
            evidencia["payload_sha256"],
        "batch_evidence_sha256":
            paquete["evidence_sha256"],
        "received_rows":
            evidencia["records_count"],
        **metricas,
        "official_source_verified": False,
        "cloud_execution_authorized": False,
        "production_write_authorized": False,
        "publication_authorized": False,
    }


def procesar_incremental_local(
    db,
    producto,
    paquete,
    contrato,
    *,
    fallo_simulado=None,
):

    exigir_conexion_local(db)

    exigir(
        fallo_simulado in (
            None,
            "AFTER_DATA",
            "AFTER_AUDIT",
            "AFTER_VERSION",
        ),
        "Punto de fallo incompatible"
    )

    # Trabajar con una copia estable del paquete.
    paquete = deepcopy(paquete)

    evidencia = validar_paquete(
        producto,
        paquete,
        contrato,
    )

    batch = evidencia["batch_id"]

    identidad = identidad_batch(
        evidencia, paquete
    )

    iniciar_transaccion(db)

    try:

        anterior_batch = db.execute(
            """
            SELECT
                run_id,
                source_version,
                previous_version,
                new_version,
                payload_sha256,
                staging_sha256,
                evidence_sha256,
                status
            FROM bq_batches
            WHERE product = ?
              AND batch_id = ?
            """,
            (producto, batch),
        ).fetchone()

        # Resolver REPLAY antes de validar continuidad:
        # un lote completado puede pertenecer a una
        # version anterior ya aplicada.

        if anterior_batch is not None:

            exigir(
                anterior_batch
                == (*identidad, "COMPLETED"),
                "Batch reutilizado con identidad distinta"
            )

            auditoria = db.execute(
                """
                SELECT
                    inserted_rows,
                    updated_rows,
                    business_updates,
                    technical_updates,
                    unchanged_rows,
                    deleted_rows,
                    received_rows,
                    report_sha256
                FROM bq_auditoria
                WHERE product = ?
                  AND batch_id = ?
                """,
                (producto, batch),
            ).fetchone()

            exigir(
                auditoria is not None,
                "Replay sin auditoria acreditada"
            )

            metricas_originales = dict(zip(
                (
                    "inserted_rows",
                    "updated_rows",
                    "business_updates",
                    "technical_updates",
                    "unchanged_rows",
                    "deleted_rows",
                ),
                auditoria[:6],
            ))

            informe_original = construir_informe(
                producto,
                evidencia,
                paquete,
                metricas_originales,
            )

            exigir(
                auditoria[6]
                == evidencia["records_count"]
                and auditoria[7]
                == huella_documento(informe_original),
                "Auditoria de replay incompatible"
            )

            version_registrada = db.execute(
                """
                SELECT 1
                FROM bq_version_history
                WHERE product = ?
                  AND version = ?
                """,
                (
                    producto,
                    evidencia["new_gold_version"],
                ),
            ).fetchone()

            exigir(
                version_registrada is not None,
                "Replay sin historial de version"
            )

            db.commit()

            return {
                "status": ESTADO,
                "result": "REPLAY_NOOP",
                "product": producto,
                "batch_id": batch,
                "applied_rows": 0,
                "original_report_sha256":
                    auditoria[7],
                "execution_authorized": False,
            }

        version = db.execute(
            """
            SELECT current_version
            FROM bq_versiones
            WHERE product = ?
            """,
            (producto,),
        ).fetchone()

        exigir(
            version is not None,
            "Carga historica local no inicializada"
        )

        exigir(
            version[0]
            == evidencia["previous_gold_version"],
            "Continuidad de version rechazada"
        )

        repetida = db.execute(
            """
            SELECT 1
            FROM bq_version_history
            WHERE product = ?
              AND version = ?
            """,
            (
                producto,
                evidencia["new_gold_version"],
            ),
        ).fetchone()

        exigir(
            repetida is None,
            "Version Gold previamente aplicada"
        )

        metricas = {
            "inserted_rows": 0,
            "updated_rows": 0,
            "business_updates": 0,
            "technical_updates": 0,
            "unchanged_rows": 0,
            "deleted_rows": 0,
        }

        for fila in paquete["payload"]:

            clave = clave_canonica(
                fila, contrato
            )

            vigente = contenido_gold(
                fila, contrato
            )

            actual = db.execute(
                """
                SELECT payload
                FROM bq_datos
                WHERE product = ?
                  AND business_key = ?
                """,
                (producto, clave),
            ).fetchone()

            if actual is None:

                db.execute(
                    """
                    INSERT INTO bq_datos
                    VALUES (?, ?, ?)
                    """,
                    (
                        producto,
                        clave,
                        json_canonico(vigente),
                    ),
                )

                metricas["inserted_rows"] += 1

                continue

            anterior = json.loads(actual[0])

            cambio = clasificar_cambio(
                anterior,
                vigente,
                contrato,
            )

            if cambio == "NOOP":

                metricas["unchanged_rows"] += 1

                continue

            db.execute(
                """
                UPDATE bq_datos
                SET payload = ?
                WHERE product = ?
                  AND business_key = ?
                """,
                (
                    json_canonico(vigente),
                    producto,
                    clave,
                ),
            )

            metricas["updated_rows"] += 1

            if cambio == "UPDATE_TECHNICAL":

                metricas["technical_updates"] += 1

            else:

                metricas["business_updates"] += 1

        if fallo_simulado == "AFTER_DATA":

            raise RuntimeError(
                "FALLO_SIMULADO_AFTER_DATA"
            )

        informe = construir_informe(
            producto,
            evidencia,
            paquete,
            metricas,
        )

        informe_sha = huella_documento(
            informe
        )

        db.execute(
            """
            INSERT INTO bq_batches
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                producto,
                batch,
                *identidad,
                "COMPLETED",
            ),
        )

        db.execute(
            """
            INSERT INTO bq_auditoria
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                producto,
                batch,
                metricas["inserted_rows"],
                metricas["updated_rows"],
                metricas["business_updates"],
                metricas["technical_updates"],
                metricas["unchanged_rows"],
                metricas["deleted_rows"],
                evidencia["records_count"],
                informe_sha,
            ),
        )

        if fallo_simulado == "AFTER_AUDIT":

            raise RuntimeError(
                "FALLO_SIMULADO_AFTER_AUDIT"
            )

        cursor = db.execute(
            """
            UPDATE bq_versiones
            SET current_version = ?
            WHERE product = ?
              AND current_version = ?
            """,
            (
                evidencia["new_gold_version"],
                producto,
                evidencia["previous_gold_version"],
            ),
        )

        exigir(
            cursor.rowcount == 1,
            "Avance de version incompatible"
        )

        db.execute(
            """
            INSERT INTO bq_version_history
            VALUES (?, ?)
            """,
            (
                producto,
                evidencia["new_gold_version"],
            ),
        )

        if fallo_simulado == "AFTER_VERSION":

            raise RuntimeError(
                "FALLO_SIMULADO_AFTER_VERSION"
            )

        db.commit()

    except BaseException:

        db.rollback()
        raise

    return {
        "report": informe,
        "report_sha256": informe_sha,
    }


def exigir_autorizacion_operativa(*args, **kwargs):

    raise PermissionError(
        "MERGE_BIGQUERY_NO_AUTORIZADO"
    )
