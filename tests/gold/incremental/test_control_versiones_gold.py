import unittest
from copy import deepcopy

from src.gold.incremental.control_versiones_gold import (
    preparar_actualizacion_gold,
    ControlVersionesError,
)


class TestControlVersionesGold(unittest.TestCase):

    def setUp(self):
        self.producto = "mercado_valor_diario"

        self.vigentes = {
            self.producto: "G1",
            "actividad_sab_diaria": "S1",
            "empresa_trimestre": "E1",
        }

        self.cambios = {
            self.producto: {
                "version_anterior": "G1",
                "version_nueva": "G2",
            }
        }

    def test_01_actualizacion_y_conservacion(self):
        antes = deepcopy(self.vigentes)

        resultado = preparar_actualizacion_gold(
            self.vigentes, self.cambios, {}, "RUN1"
        )

        self.assertEqual(
            resultado["estado"],
            "ACTUALIZACION_PREPARADA"
        )

        self.assertEqual(
            resultado["versiones_resultantes"][self.producto],
            "G2"
        )

        self.assertEqual(
            resultado["productos_conservados"],
            ["actividad_sab_diaria", "empresa_trimestre"]
        )

        self.assertEqual(self.vigentes, antes)

        self.assertFalse(resultado["publicacion_autorizada"])

    def test_02_replay_despues_de_actualizacion(self):
        vigentes = dict(self.vigentes)
        vigentes[self.producto] = "G2"

        resultado = preparar_actualizacion_gold(
            vigentes,
            self.cambios,
            {"RUN1": self.cambios},
            "RUN1"
        )

        self.assertEqual(
            resultado["estado"],
            "EJECUCION_REPETIDA"
        )

        self.assertEqual(
            resultado["versiones_resultantes"],
            vigentes
        )

        self.assertFalse(resultado["publicacion_autorizada"])

    def test_03_replay_no_retrocede_versiones(self):
        vigentes = dict(self.vigentes)
        vigentes[self.producto] = "G3"

        resultado = preparar_actualizacion_gold(
            vigentes,
            self.cambios,
            {"RUN1": self.cambios},
            "RUN1"
        )

        self.assertEqual(
            resultado["estado"],
            "EJECUCION_REPETIDA"
        )

        self.assertEqual(
            resultado["versiones_resultantes"][self.producto],
            "G3"
        )

    def test_04_run_id_reutilizado_rechazado(self):
        otros = deepcopy(self.cambios)
        otros[self.producto]["version_nueva"] = "G9"

        with self.assertRaises(ControlVersionesError):
            preparar_actualizacion_gold(
                self.vigentes,
                otros,
                {"RUN1": self.cambios},
                "RUN1"
            )

    def test_05_continuidad_incorrecta_rechazada(self):
        vigentes = dict(self.vigentes)
        vigentes[self.producto] = "G3"

        with self.assertRaises(ControlVersionesError):
            preparar_actualizacion_gold(
                vigentes,
                self.cambios,
                {},
                "RUN2"
            )

    def test_06_producto_desconocido_rechazado(self):
        with self.assertRaises(ControlVersionesError):
            preparar_actualizacion_gold(
                self.vigentes,
                {
                    "desconocido": {
                        "version_anterior": "V1",
                        "version_nueva": "V2",
                    }
                },
                {},
                "RUN1"
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
