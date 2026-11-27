"""Clasificación de cambios de negocio frente a metadatos."""

import sqlite3
import unittest

from src.gold.bigquery.batch_atomico_local import procesar_batch
from src.gold.bigquery.conteos_incrementales_local import contar_cambios
from src.gold.bigquery.verificar_payload_local import calcular_sha256


CASOS = {
    "mercado_valor_diario": (
        {"nemonico": "ABC", "fecha": "2026-11-27"},
        "precio_cierre",
    ),
    "actividad_sab_diaria": (
        {"fecha": "2026-11-27", "codigo_sab": "S01"},
        "monto_total",
    ),
    "empresa_trimestre": (
        {"id_registro": "REG-001"},
        "activo_total",
    ),
}


class TestConteosNegocio(unittest.TestCase):

    def test_metadata_no_es_update(self):
        for producto, (clave, campo) in CASOS.items():
            with self.subTest(producto=producto):
                db = sqlite3.connect(":memory:")

                try:
                    version = "silver-simulada-v1"

                    original = [{
                        **clave,
                        campo: 100.0,
                        "fecha_ingesta": "2026-11-26",
                        "operation": "UPSERT",
                    }]

                    procesar_batch(
                        db, producto, "batch-1", "run-1",
                        version, original,
                        hash_declarado=calcular_sha256(original),
                        versiones_oficiales={producto: version},
                        autorizaciones_quality={
                            (producto, "batch-1"): version
                        },
                    )

                    solo_metadata = [{
                        **original[0],
                        "fecha_ingesta": "2026-11-27",
                    }]

                    conteos_metadata = contar_cambios(
                        db, producto, solo_metadata
                    )

                    self.assertEqual(
                        conteos_metadata["updated_rows"], 0
                    )
                    self.assertEqual(
                        conteos_metadata["unchanged_rows"], 1
                    )

                    cambio_real = [{
                        **solo_metadata[0],
                        campo: 200.0,
                    }]

                    conteos_negocio = contar_cambios(
                        db, producto, cambio_real
                    )

                    self.assertEqual(
                        conteos_negocio["updated_rows"], 1
                    )
                    self.assertEqual(
                        conteos_negocio["unchanged_rows"], 0
                    )

                    print(
                        f"{producto}: METADATA_NOOP_NEGOCIO_UPDATE_OK"
                    )
                finally:
                    db.close()


if __name__ == "__main__":
    unittest.main()
