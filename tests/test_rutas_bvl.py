import unittest
from unittest.mock import patch

from scripts.pipeline import rutas_bvl


class TestRutasBVL(unittest.TestCase):

    def test_bronze_hdfs(self):
        resultado = rutas_bvl.ruta(
            "bronze", "bcrp/tipo_cambio"
        )
        self.assertEqual(
            resultado,
            "/datalake/bvl/bronze/bcrp/tipo_cambio"
        )

    def test_silver_hdfs(self):
        resultado = rutas_bvl.ruta(
            "silver", "macroeconomia/tipo_cambio"
        )
        self.assertEqual(
            resultado,
            "/datalake/bvl/silver/macroeconomia/tipo_cambio"
        )

    def test_ruta_insegura(self):
        with self.assertRaises(ValueError):
            rutas_bvl.ruta("silver", "../bronze")

    def test_zona_invalida(self):
        with self.assertRaises(ValueError):
            rutas_bvl.ruta("desconocida")

    def test_gcs_sin_configurar(self):
        with self.assertRaises(ValueError):
            rutas_bvl.ruta(
                "silver", entorno="gcs"
            )

    def test_gcs_simulado(self):
        cfg = rutas_bvl.cargar()

        cfg["gcs"]["silver"] = (
            "gs://bucket-simulado/silver"
        )

        with patch.object(
            rutas_bvl, "cargar", return_value=cfg
        ):
            resultado = rutas_bvl.ruta(
                "silver",
                "versions/tipo_cambio",
                entorno="gcs"
            )

        self.assertEqual(
            resultado,
            "gs://bucket-simulado/silver/versions/tipo_cambio"
        )


if __name__ == "__main__":
    unittest.main()
