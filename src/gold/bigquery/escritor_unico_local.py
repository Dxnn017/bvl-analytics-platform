"""Coordinación transaccional local. No utiliza BigQuery."""

import sqlite3
from contextlib import contextmanager


@contextmanager
def escritor_unico(conexion):
    if conexion.in_transaction:
        raise RuntimeError("Ya existe una transacción activa")

    conexion.execute("BEGIN IMMEDIATE")

    try:
        yield conexion
        conexion.commit()
    except BaseException:
        conexion.rollback()
        raise


def inicializar(conexion):
    conexion.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            product TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            version TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            PRIMARY KEY (product, batch_id)
        )
    """)
    conexion.commit()


if __name__ == "__main__":
    from control_replay_local import resolver_batch

    db = sqlite3.connect(":memory:")
    inicializar(db)

    identidad = (
        "mercado_valor_diario",
        "batch-001",
        "run-001",
        "silver-v1",
        "a" * 64,
    )

    with escritor_unico(db):
        assert resolver_batch(db, *identidad) == "PROCESS"

        db.execute(
            "INSERT INTO batches VALUES (?, ?, ?, ?, ?)",
            identidad,
        )

    with escritor_unico(db):
        assert resolver_batch(db, *identidad) == "REPLAY_NOOP"

    assert db.execute(
        "SELECT COUNT(*) FROM batches"
    ).fetchone()[0] == 1

    print("ESCRITOR_UNICO_LOCAL_OK")
    print("REPLAY_SIN_DUPLICACION_OK")
    db.close()
