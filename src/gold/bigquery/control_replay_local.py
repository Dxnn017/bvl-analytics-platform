"""Resolución local de batches. No conecta con GCP."""

import sqlite3


def resolver_batch(
    conexion, producto, batch_id, run_id,
    source_version, payload_sha256
):
    if not all(
        isinstance(x, str) and x.strip()
        for x in (
            producto, batch_id, run_id,
            source_version, payload_sha256
        )
    ):
        raise ValueError("Identidad del batch incompleta")

    registro = conexion.execute(
        """
        SELECT run_id, version, sha256
        FROM batches
        WHERE product = ? AND batch_id = ?
        """,
        (producto, batch_id),
    ).fetchone()

    if registro is None:
        return "PROCESS"

    if registro != (
        run_id, source_version, payload_sha256
    ):
        raise ValueError(
            "REJECT: batch_id reutilizado con identidad diferente"
        )

    return "REPLAY_NOOP"


def preparar_prueba():
    db = sqlite3.connect(":memory:")
    db.execute("""
        CREATE TABLE batches (
            product TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            version TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            PRIMARY KEY(product, batch_id)
        )
    """)
    return db


if __name__ == "__main__":
    db = preparar_prueba()

    identidad = (
        "mercado_valor_diario",
        "batch-001",
        "run-001",
        "silver-v1",
        "a" * 64,
    )

    assert resolver_batch(db, *identidad) == "PROCESS"

    db.execute(
        "INSERT INTO batches VALUES (?, ?, ?, ?, ?)",
        identidad,
    )
    db.commit()

    assert resolver_batch(db, *identidad) == "REPLAY_NOOP"

    try:
        resolver_batch(db, *identidad[:-1], "b" * 64)
    except ValueError:
        print("BATCH_CON_HASH_DIFERENTE_RECHAZADO_OK")
    else:
        raise AssertionError("No se rechazó el hash diferente")

    print("REPLAY_NOOP_LOCAL_VALIDADO_OK")
    db.close()
