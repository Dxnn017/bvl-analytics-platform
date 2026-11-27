"""Regresión de seguridad y atomicidad local Gold -> BigQuery."""

import sqlite3
import unittest

from src.gold.bigquery.batch_atomico_local import procesar_batch
from src.gold.bigquery.verificar_payload_local import calcular_sha256


CASOS = {
    "mercado_valor_diario": {
        "nemonico": "ABC", "fecha": "2026-11-27"
    },
    "actividad_sab_diaria": {
        "fecha": "2026-11-27", "codigo_sab": "S01"
    },
    "empresa_trimestre": {
        "id_registro": "REG-001"
    },
}


class TestSeguridadAtomica(unittest.TestCase):

    def test_rechazos_sin_mutaciones(self):
        for producto, clave in CASOS.items():
            with self.subTest(producto=producto):
                db = sqlite3.connect(":memory:")
                try:
                    filas = [{**clave, "operation": "UPSERT", "valor": 100}]
                    version = "silver-simulada-v1"
                    huella = calcular_sha256(filas)

                    base = {
                        "hash_declarado": huella,
                        "versiones_oficiales": {producto: version},
                        "autorizaciones_quality": {
                            (producto, "batch-1"): version
                        },
                    }

                    casos_invalidos = [
                        {**base, "autorizaciones_quality": {}},
                        {**base, "versiones_oficiales": {
                            producto: "version-distinta"
                        }},
                        {**base, "hash_declarado": "0" * 64},
                    ]

                    for argumentos in casos_invalidos:
                        with self.assertRaises(
                            (PermissionError, ValueError)
                        ):
                            procesar_batch(
                                db, producto, "batch-1", "run-1",
                                version, filas, **argumentos
                            )

                        for tabla in ("datos", "batches", "auditoria"):
                            existe = db.execute(
                                "SELECT 1 FROM sqlite_master "
                                "WHERE type='table' AND name=?",
                                (tabla,),
                            ).fetchone()
                            if existe:
                                cantidad = db.execute(
                                    f"SELECT COUNT(*) FROM {tabla}"
                                ).fetchone()[0]
                                self.assertEqual(cantidad, 0)

                    print(f"{producto}: RECHAZOS_SIN_MUTACIONES_OK")
                finally:
                    db.close()

    def test_rollback_por_fallo_auditoria(self):
        for producto, clave in CASOS.items():
            with self.subTest(producto=producto):
                db = sqlite3.connect(":memory:")
                try:
                    version = "silver-simulada-v1"

                    def ejecutar(batch, valor):
                        filas = [{
                            **clave,
                            "operation": "UPSERT",
                            "valor": valor,
                        }]
                        return procesar_batch(
                            db, producto, batch, batch, version, filas,
                            hash_declarado=calcular_sha256(filas),
                            versiones_oficiales={producto: version},
                            autorizaciones_quality={
                                (producto, batch): version
                            },
                        )

                    ejecutar("batch-inicial", 100)

                    antes = {
                        tabla: db.execute(
                            f"SELECT * FROM {tabla} ORDER BY 1, 2"
                        ).fetchall()
                        for tabla in ("datos", "batches", "auditoria")
                    }

                    db.execute("""
                        CREATE TRIGGER fallar_auditoria
                        BEFORE INSERT ON auditoria
                        BEGIN
                          SELECT RAISE(ABORT, 'FALLO_SIMULADO');
                        END;
                    """)

                    with self.assertRaises(sqlite3.IntegrityError):
                        ejecutar("batch-fallido", 200)

                    for tabla, esperado in antes.items():
                        actual = db.execute(
                            f"SELECT * FROM {tabla} ORDER BY 1, 2"
                        ).fetchall()
                        self.assertEqual(actual, esperado)

                    print(f"{producto}: ROLLBACK_ATOMICO_OK")
                finally:
                    db.close()


if __name__ == "__main__":
    unittest.main()
