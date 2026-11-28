import json
import re
import sqlite3
import unittest
from pathlib import Path

from src.gold.bigquery.adaptador_incremental_local import aplicar_batch
from src.gold.bigquery.comparador_negocio_local import cambio_negocio
from src.gold.bigquery.verificar_payload_local import calcular_sha256

BASE = Path("docs/bigquery")

PRODUCTOS = {
    "mercado_valor_diario": ["nemonico_original"],
    "actividad_sab_diaria": [
        "registros_elegibles",
        "registros_cuarentena",
        "archivos_excluidos",
        "estado_cobertura",
        "version_silver",
        "scope_quality_id",
        "trazabilidad_verificada",
        "cobertura_completa",
    ],
}


class TestCompatibilidadFase6(unittest.TestCase):

    def test_01_contratos_ddl(self):
        for producto, nuevos in PRODUCTOS.items():
            contrato = json.loads(
                (BASE / f"{producto}.json").read_text()
            )

            columnas = contrato["columns"]
            nombres = [c["name"] for c in columnas]

            self.assertEqual(len(nombres), 41)
            self.assertEqual(len(set(nombres)), 41)
            self.assertFalse(contrato["production_authorized"])

            ddl = (BASE / f"{producto}.sql").read_text()
            staging = (
                BASE / f"{producto}_staging.sql"
            ).read_text()

            for columna in columnas:
                nombre = columna["name"]
                tipo = columna["bigquery_type"]

                patron = (
                    rf"(?m)^\s*`{nombre}` {tipo}"
                    + ("" if columna["nullable"] else " NOT NULL")
                    + r",?$"
                )

                self.assertRegex(ddl, patron)
                self.assertRegex(
                    staging,
                    rf"(?m)^\s*`{nombre}` {tipo},?$"
                )

            self.assertTrue(set(nuevos).issubset(nombres))

    def test_02_merge_y_transaccional(self):
        for producto, nuevos in PRODUCTOS.items():
            contrato = json.loads(
                (BASE / f"{producto}.json").read_text()
            )
            nombres = [c["name"] for c in contrato["columns"]]

            for sufijo in (
                "_merge.sql", "_transaccional.sql"
            ):
                sql = (
                    BASE / f"{producto}{sufijo}"
                ).read_text()

                insert = re.search(
                    r"THEN INSERT \((.*?)\)\s+VALUES \((.*?)\);",
                    sql, re.S
                )

                self.assertIsNotNone(insert)

                self.assertEqual(
                    re.findall(r"`([^`]+)`", insert.group(1)),
                    nombres
                )

                self.assertEqual(
                    re.findall(r"S\.`([^`]+)`", insert.group(2)),
                    nombres
                )

                for c in nuevos:
                    self.assertIn(
                        f"T.`{c}` IS DISTINCT FROM S.`{c}`", sql
                    )
                    self.assertIn(f"`{c}` = S.`{c}`", sql)

                self.assertIn("ASSERT FALSE AS", sql)
                self.assertIn(
                    "DELETE bloqueado hasta autorización oficial",
                    sql
                )

    def test_03_incremental_nemonico(self):
        anterior = {
            "nemonico": "DEMO1",
            "fecha": "2026-01-15",
            "nemonico_original": "DEMO1",
            "precio_cierre": 10.0,
            "operation": "UPSERT",
        }

        nuevo = dict(
            anterior,
            nemonico_original=" demo1 "
        )

        self.assertTrue(cambio_negocio(
            "mercado_valor_diario", anterior, nuevo
        ))

        self.validar_actualizacion(
            "mercado_valor_diario", anterior, nuevo
        )

    def test_04_incremental_cobertura(self):
        anterior = {
            "fecha": "2026-01-15",
            "codigo_sab": "S01",
            "monto_total": 100.0,
            "actividad_diaria_valida": True,
            "estado_cobertura": "DESCONOCIDA",
            "cobertura_completa": False,
            "trazabilidad_verificada": False,
            "operation": "UPSERT",
        }

        nuevo = dict(
            anterior,
            estado_cobertura="PARCIAL",
            registros_elegibles=1,
            registros_cuarentena=1,
            archivos_excluidos=1,
            version_silver="SINTETICO-V1",
            scope_quality_id="TEST-SCOPE",
            trazabilidad_verificada=True,
        )

        self.assertTrue(cambio_negocio(
            "actividad_sab_diaria", anterior, nuevo
        ))

        self.validar_actualizacion(
            "actividad_sab_diaria", anterior, nuevo
        )

    def validar_actualizacion(self, producto, anterior, nuevo):
        with sqlite3.connect(":memory:") as db:
            resultado = aplicar_batch(
                db, producto, "B1", "R1", "V1", [anterior]
            )
            self.assertEqual(resultado["changes"], 1)

            resultado = aplicar_batch(
                db, producto, "B2", "R2", "V2", [nuevo]
            )
            self.assertEqual(resultado["changes"], 1)

            replay = aplicar_batch(
                db, producto, "B2", "R2", "V2", [nuevo]
            )
            self.assertEqual(replay["status"], "REPLAY")
            self.assertEqual(replay["changes"], 0)

            noop = aplicar_batch(
                db, producto, "B3", "R3", "V2", [nuevo]
            )
            self.assertEqual(noop["changes"], 0)

            filas = db.execute(
                "SELECT contenido FROM datos WHERE product=?",
                (producto,)
            ).fetchall()

            self.assertEqual(len(filas), 1)
            self.assertEqual(json.loads(filas[0][0]), nuevo)

            self.assertNotEqual(
                calcular_sha256([anterior]),
                calcular_sha256([nuevo])
            )

            with self.assertRaises(ValueError):
                aplicar_batch(
                    db, producto, "B2", "R2", "V2", [anterior]
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
