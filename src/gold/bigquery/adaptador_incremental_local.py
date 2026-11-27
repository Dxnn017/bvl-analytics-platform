"""Simulador transaccional Gold -> BigQuery. Sin acceso a GCP."""

import hashlib
import json
import sqlite3

CLAVES = {
    "mercado_valor_diario": ("nemonico", "fecha"),
    "actividad_sab_diaria": ("fecha", "codigo_sab"),
    "empresa_trimestre": ("id_registro",),
}


def aplicar_batch(conexion, producto, batch_id, run_id, version, filas):
    if producto not in CLAVES:
        raise ValueError("Producto Gold no permitido")
    if not all(isinstance(x, str) and x.strip()
               for x in (batch_id, run_id, version)):
        raise ValueError("Trazabilidad incompleta")
    if not isinstance(filas, list) or not filas:
        raise ValueError("Batch vacío")

    claves = CLAVES[producto]
    vistos = set()
    for fila in filas:
        if not isinstance(fila, dict):
            raise ValueError("Fila inválida")
        if fila.get("operation") != "UPSERT":
            raise ValueError("DELETE bloqueado")
        valores = tuple(fila.get(c) for c in claves)
        if any(v is None or v == "" for v in valores):
            raise ValueError("Clave nula")
        identidad = json.dumps(valores, ensure_ascii=False)
        if identidad in vistos:
            raise ValueError("Clave duplicada")
        vistos.add(identidad)

    contenido = json.dumps(
        filas, sort_keys=True, ensure_ascii=False, default=str
    )
    huella = hashlib.sha256(contenido.encode()).hexdigest()

    with conexion:
        conexion.execute("""
            CREATE TABLE IF NOT EXISTS batches (
                product TEXT, batch_id TEXT, run_id TEXT,
                version TEXT, sha256 TEXT,
                PRIMARY KEY (product, batch_id)
            )
        """)
        conexion.execute("""
            CREATE TABLE IF NOT EXISTS datos (
                product TEXT, clave TEXT, contenido TEXT,
                PRIMARY KEY (product, clave)
            )
        """)

        anterior = conexion.execute(
            "SELECT run_id, version, sha256 FROM batches "
            "WHERE product=? AND batch_id=?",
            (producto, batch_id),
        ).fetchone()

        if anterior:
            if anterior != (run_id, version, huella):
                raise ValueError("Batch ID reutilizado con otro contenido")
            return {"status": "REPLAY", "changes": 0}

        cambios = 0
        for fila in filas:
            identidad = json.dumps(
                [fila[c] for c in claves], ensure_ascii=False
            )
            datos = json.dumps(fila, sort_keys=True, default=str)
            actual = conexion.execute(
                "SELECT contenido FROM datos WHERE product=? AND clave=?",
                (producto, identidad),
            ).fetchone()

            if actual is None or actual[0] != datos:
                cambios += 1

            conexion.execute("""
                INSERT INTO datos (product, clave, contenido)
                VALUES (?, ?, ?)
                ON CONFLICT(product, clave)
                DO UPDATE SET contenido=excluded.contenido
            """, (producto, identidad, datos))

        conexion.execute(
            "INSERT INTO batches VALUES (?, ?, ?, ?, ?)",
            (producto, batch_id, run_id, version, huella),
        )

    return {"status": "COMPLETED", "changes": cambios}
