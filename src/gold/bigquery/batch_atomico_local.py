"""Batch Gold atomico local. No conecta con BigQuery."""

import hashlib
import json
import sqlite3

from src.gold.bigquery.escritor_unico_local import escritor_unico
from src.gold.bigquery.autorizacion_previa_local import validar_preflight
from src.gold.bigquery.conteos_incrementales_local import (
    CLAVES, contar_cambios
)


def procesar_batch(
    db, producto, batch_id, run_id, version, filas,
    *,
    hash_declarado,
    versiones_oficiales,
    autorizaciones_quality,
):
    if producto not in CLAVES:
        raise ValueError("Producto no permitido")
    if not all(isinstance(x, str) and x.strip()
               for x in (batch_id, run_id, version)):
        raise ValueError("Identidad incompleta")
    if not isinstance(filas, list) or not filas:
        raise ValueError("Batch vacio")

    payload = json.dumps(
        filas, sort_keys=True, ensure_ascii=False,
        separators=(",", ":"), default=str
    )
    huella = hashlib.sha256(payload.encode("utf-8")).hexdigest()

    with escritor_unico(db):
        validar_preflight(
            filas,
            hash_declarado,
            version,
            versiones_oficiales,
            autorizaciones_quality,
            producto,
            batch_id,
        )

        if huella.lower() != hash_declarado.lower():
            raise ValueError("Hash del batch inconsistente")

        db.execute("""
            CREATE TABLE IF NOT EXISTS batches (
                product TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                version TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                PRIMARY KEY(product, batch_id)
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS datos (
                product TEXT NOT NULL,
                clave TEXT NOT NULL,
                contenido TEXT NOT NULL,
                PRIMARY KEY(product, clave)
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS auditoria (
                product TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                inserted_rows INTEGER NOT NULL,
                updated_rows INTEGER NOT NULL,
                unchanged_rows INTEGER NOT NULL,
                deleted_rows INTEGER NOT NULL,
                PRIMARY KEY(product, batch_id)
            )
        """)

        anterior = db.execute(
            "SELECT run_id, version, sha256 FROM batches "
            "WHERE product=? AND batch_id=?",
            (producto, batch_id)
        ).fetchone()

        if anterior:
            if anterior != (run_id, version, huella):
                raise ValueError("Batch reutilizado con otro contenido")

            auditado = db.execute(
                "SELECT 1 FROM auditoria "
                "WHERE product=? AND batch_id=?",
                (producto, batch_id)
            ).fetchone()

            if not auditado:
                raise ValueError("Batch completado sin auditoria")

            return {"status": "REPLAY_NOOP"}

        conteos = contar_cambios(db, producto, filas)

        for fila in filas:
            clave = json.dumps(
                tuple(fila[c] for c in CLAVES[producto]),
                ensure_ascii=False
            )
            contenido = json.dumps(
                fila, sort_keys=True, ensure_ascii=False
            )

            db.execute("""
                INSERT INTO datos(product, clave, contenido)
                VALUES (?, ?, ?)
                ON CONFLICT(product, clave)
                DO UPDATE SET contenido=excluded.contenido
            """, (producto, clave, contenido))

        db.execute(
            "INSERT INTO batches VALUES (?, ?, ?, ?, ?)",
            (producto, batch_id, run_id, version, huella)
        )

        db.execute("""
            INSERT INTO auditoria VALUES (?, ?, ?, ?, ?, ?)
        """, (
            producto, batch_id,
            conteos["inserted_rows"],
            conteos["updated_rows"],
            conteos["unchanged_rows"],
            conteos["deleted_rows"],
        ))

        return {"status": "COMPLETED", **conteos}


if __name__ == "__main__":
    casos = {
        "mercado_valor_diario":
            {"nemonico": "ABC", "fecha": "2026-11-27"},
        "actividad_sab_diaria":
            {"fecha": "2026-11-27", "codigo_sab": "S01"},
        "empresa_trimestre":
            {"id_registro": "REG-001"},
    }

    for producto, clave in casos.items():
        db = sqlite3.connect(":memory:")

        fila = [{**clave, "operation": "UPSERT", "valor": 100}]

        from src.gold.bigquery.verificar_payload_local import calcular_sha256
        huella_prueba = calcular_sha256(fila)

        primero = procesar_batch(
            db, producto, "batch-1", "run-1", "v1", fila,
            hash_declarado=huella_prueba,
            versiones_oficiales={producto: "v1"},
            autorizaciones_quality={(producto, "batch-1"): "v1"},
        )
        segundo = procesar_batch(
            db, producto, "batch-1", "run-1", "v1", fila,
            hash_declarado=huella_prueba,
            versiones_oficiales={producto: "v1"},
            autorizaciones_quality={(producto, "batch-1"): "v1"},
        )

        assert primero["status"] == "COMPLETED"
        assert primero["inserted_rows"] == 1
        assert segundo["status"] == "REPLAY_NOOP"

        assert db.execute(
            "SELECT COUNT(*) FROM auditoria"
        ).fetchone()[0] == 1

        assert db.execute(
            "SELECT COUNT(*) FROM batches"
        ).fetchone()[0] == 1

        print(f"{producto}: BATCH_ATOMICO_OK")
        db.close()

    print("TRES_BATCHES_ATOMICOS_LOCALES_OK")
