"""Pruebas locales de clasificación financiera. No ejecuta GCP."""

import unittest


def clasificar_cambio_financiero(anterior, nuevo):
    if anterior["id_registro"] == nuevo["id_registro"]:
        return "MISMO_REGISTRO"

    if anterior["clave_negocio"] != nuevo["clave_negocio"]:
        return "NEGOCIOS_DISTINTOS"

    if (
        anterior.get("colision_clave_negocio")
        or nuevo.get("colision_clave_negocio")
    ):
        return "COLISION_REQUIERE_REVISION"

    return "SUSTITUCION_POTENCIAL_BLOQUEADA"


class TestSustitucionFinancieraLocal(unittest.TestCase):

    def setUp(self):
        self.original = {
            "id_registro": "HASH_A",
            "clave_negocio": "EMPRESA_TEST_2025_T4",
            "activo_total": 100000,
            "colision_clave_negocio": False,
        }

    def test_correccion_bloqueada(self):
        corregido = {
            **self.original,
            "id_registro": "HASH_B",
            "activo_total": 120000,
        }

        resultado = clasificar_cambio_financiero(
            self.original, corregido
        )

        self.assertEqual(
            resultado, "SUSTITUCION_POTENCIAL_BLOQUEADA"
        )

    def test_colision_requiere_revision(self):
        colision = {
            **self.original,
            "id_registro": "HASH_C",
            "colision_clave_negocio": True,
        }

        resultado = clasificar_cambio_financiero(
            self.original, colision
        )

        self.assertEqual(
            resultado, "COLISION_REQUIERE_REVISION"
        )


if __name__ == "__main__":
    unittest.main()
