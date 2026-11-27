import sqlite3
import unittest

from src.gold.bigquery.adaptador_incremental_local import (
    aplicar_batch,
)


class TestBatchIdempotencia(unittest.TestCase):
    def test_tres_productos(self):
        casos = {
            "mercado_valor_diario":
                {"nemonico": "ABC", "fecha": "2026-11-27"},
            "actividad_sab_diaria":
                {"fecha": "2026-11-27", "codigo_sab": "S01"},
            "empresa_trimestre":
                {"id_registro": "REG-001"},
        }

        for producto, clave in casos.items():
            with self.subTest(producto=producto):
                db = sqlite3.connect(":memory:")
                fila = [{**clave, "operation": "UPSERT", "valor": 10}]
                args = (db, producto, "batch-1", "run-1", "v1")

                primero = aplicar_batch(*args, fila)
                repetido = aplicar_batch(*args, fila)

                self.assertEqual(primero["status"], "COMPLETED")
                self.assertEqual(repetido["status"], "REPLAY")

                with self.assertRaises(ValueError):
                    aplicar_batch(
                        *args,
                        [{**clave, "operation": "UPSERT", "valor": 20}],
                    )

                actualizado = aplicar_batch(
                    db, producto, "batch-2", "run-2", "v2",
                    [{**clave, "operation": "UPSERT", "valor": 20}],
                )
                self.assertEqual(actualizado["changes"], 1)

                with self.assertRaises(ValueError):
                    aplicar_batch(
                        db, producto, "batch-3", "run-3", "v3",
                        [{**clave, "operation": "DELETE"}],
                    )

                self.assertEqual(
                    db.execute("SELECT COUNT(*) FROM batches")
                    .fetchone()[0], 2
                )
                db.close()
                print(f"{producto}: BATCH_IDEMPOTENTE_OK")


if __name__ == "__main__":
    unittest.main()
