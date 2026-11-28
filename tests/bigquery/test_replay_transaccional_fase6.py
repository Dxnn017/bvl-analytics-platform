import sqlite3
import unittest
from pathlib import Path

from src.gold.bigquery.batch_atomico_local import procesar_batch
from src.gold.bigquery.verificar_payload_local import calcular_sha256

PRODUCTOS = {
    "mercado_valor_diario": {
        "nemonico": "ABC",
        "fecha": "2026-01-15",
        "precio_cierre": 10.0,
    },
    "actividad_sab_diaria": {
        "codigo_sab": "S01",
        "fecha": "2026-01-15",
        "monto_total": 100,
    },
    "empresa_trimestre": {
        "id_registro": "REG-001",
        "activo_total": 1000,
    },
}


class TestReplayTransaccional(unittest.TestCase):

    def test_01_sql_replay_condicional(self):
        for producto in PRODUCTOS:
            sql = Path(
                f"docs/bigquery/{producto}_transaccional.sql"
            ).read_text()

            self.assertIn("ASSERT FALSE AS", sql)

            self.assertLess(
                sql.index("SELECT 'REPLAY_NOOP'"),
                sql.index("MERGE `")
            )

            self.assertLess(
                sql.index("MERGE `"),
                sql.index("END IF;")
            )

            self.assertLess(
                sql.index("END IF;"),
                sql.index("COMMIT TRANSACTION;")
            )

            self.assertIn(
                "run_id IS DISTINCT FROM @run_id", sql
            )

            self.assertIn(
                "source_version IS DISTINCT FROM @source_version",
                sql
            )

            self.assertIn(
                "payload_sha256 IS DISTINCT FROM @payload_sha256",
                sql
            )

            self.assertIn(
                "status IS DISTINCT FROM 'COMPLETED'", sql
            )

    def test_02_replay_y_version_incompatible(self):
        for producto, datos in PRODUCTOS.items():
            with self.subTest(producto=producto):
                with sqlite3.connect(":memory:") as db:

                    filas = [{**datos, "operation": "UPSERT"}]

                    def ejecutar(version, run="R1"):
                        return procesar_batch(
                            db, producto, "B1", run,
                            version, filas,
                            hash_declarado=calcular_sha256(filas),
                            versiones_oficiales={
                                producto: version
                            },
                            autorizaciones_quality={
                                (producto, "B1"): version
                            },
                        )

                    primero = ejecutar("V1")

                    self.assertEqual(
                        primero["status"], "COMPLETED"
                    )

                    self.assertEqual(
                        ejecutar("V1")["status"], "REPLAY_NOOP"
                    )

                    with self.assertRaises(ValueError):
                        ejecutar("V2")

                    with self.assertRaises(ValueError):
                        ejecutar("V1", run="R2")

                    for tabla in ("datos", "batches", "auditoria"):
                        self.assertEqual(
                            db.execute(
                                f"SELECT COUNT(*) FROM {tabla}"
                            ).fetchone()[0],
                            1
                        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
