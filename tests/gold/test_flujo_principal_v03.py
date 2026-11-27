"""Verifica la integración efectiva del marcador con el flujo principal."""

import unittest
from unittest.mock import patch

from flujo_silver_gold_v03 import (
    preparar_silver_v03,
    FlujoSilverV03Error,
)


class TestFlujoPrincipalV03(unittest.TestCase):

    def test_conjunto_rechazado_si_verificador_falla(self):
        autorizacion = {
            "product": "referencias_smv/valores_inscritos",
            "current": {},
            "manifest": {},
            "manifest_signature": {},
        }

        import json
        contenido = json.dumps(autorizacion).encode()

        with (
            patch(
                "flujo_silver_gold_v03.validar_autorizacion",
                return_value={"firma_quality_verificada": True},
            ),
            patch(
                "verificar_conjunto_silver_v03.verificar_conjunto_silver",
                side_effect=FlujoSilverV03Error(
                    "Marcador conjunto inválido"
                ),
            ),
        ):
            with self.assertRaisesRegex(
                FlujoSilverV03Error, "Marcador conjunto inválido"
            ):
                preparar_silver_v03(
                    None, contenido, b"firma-fixture",
                    b"clave-fixture", "mercado_valor_diario"
                )

    def test_conjunto_no_admite_autorizacion_ausente(self):
        with self.assertRaises(FlujoSilverV03Error):
            preparar_silver_v03(
                None, b"", b"", b"", "mercado_valor_diario"
            )


if __name__ == "__main__":
    unittest.main()
