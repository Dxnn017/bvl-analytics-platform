"""Prueba reproducible de concurrencia SQLite. Sin acceso GCP."""

import multiprocessing as mp
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from src.gold.bigquery.escritor_unico_local import (
    escritor_unico,
    inicializar,
)
from src.gold.bigquery.control_replay_local import resolver_batch


CASOS = {
    "mercado_valor_diario": {
        "nemonico": "ABC",
        "fecha": "2026-11-27",
    },
    "actividad_sab_diaria": {
        "fecha": "2026-11-27",
        "codigo_sab": "S01",
    },
    "empresa_trimestre": {
        "id_registro": "REG-001",
    },
}


def trabajador(ruta, identidad, inicio, resultados):
    db = sqlite3.connect(ruta, timeout=15)

    try:
        if not inicio.wait(timeout=10):
            raise TimeoutError("Inicio no confirmado")

        with escritor_unico(db):
            estado = resolver_batch(db, *identidad)

            if estado == "PROCESS":
                time.sleep(0.2)
                db.execute(
                    "INSERT INTO batches VALUES (?, ?, ?, ?, ?)",
                    identidad,
                )
                estado = "COMPLETED"

        resultados.put(estado)

    except Exception as error:
        resultados.put(
            f"ERROR:{type(error).__name__}:{error}"
        )

    finally:
        db.close()


class TestConcurrenciaReproducible(unittest.TestCase):

    def test_dos_procesos_tres_productos(self):
        ctx = mp.get_context("fork")

        with tempfile.TemporaryDirectory() as carpeta:
            for producto in CASOS:
                with self.subTest(producto=producto):
                    ruta = str(Path(carpeta) / f"{producto}.db")

                    db = sqlite3.connect(ruta)
                    inicializar(db)
                    db.close()

                    identidad = (
                        producto,
                        "batch-001",
                        "run-001",
                        "silver-simulada-v1",
                        "a" * 64,
                    )

                    inicio = ctx.Event()
                    resultados = ctx.Queue()

                    procesos = [
                        ctx.Process(
                            target=trabajador,
                            args=(
                                ruta,
                                identidad,
                                inicio,
                                resultados,
                            ),
                        )
                        for _ in range(2)
                    ]

                    try:
                        for proceso in procesos:
                            proceso.start()

                        inicio.set()

                        respuestas = [
                            resultados.get(timeout=20)
                            for _ in procesos
                        ]

                        for proceso in procesos:
                            proceso.join(timeout=20)

                        self.assertTrue(
                            all(p.exitcode == 0 for p in procesos)
                        )

                        self.assertEqual(
                            sorted(respuestas),
                            ["COMPLETED", "REPLAY_NOOP"],
                        )

                        db = sqlite3.connect(ruta)
                        cantidad = db.execute(
                            "SELECT COUNT(*) FROM batches"
                        ).fetchone()[0]
                        db.close()

                        self.assertEqual(cantidad, 1)
                        print(
                            f"{producto}: "
                            "CONCURRENCIA_REPRODUCIBLE_OK"
                        )

                    finally:
                        for proceso in procesos:
                            if proceso.is_alive():
                                proceso.terminate()
                            proceso.join(timeout=5)
                        resultados.close()


if __name__ == "__main__":
    unittest.main()
