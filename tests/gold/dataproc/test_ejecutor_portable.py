import unittest
from unittest.mock import patch

from ejecutor_portable import (
    DEPENDENCIAS,
    BLOQUEADOS,
    recalcular_candidato,
)

MODULOS = {
    "mercado_valor_diario": (
        "recalcular_mercado_valor.recalcular_mercado_valor"
    ),
    "actividad_sab_diaria": (
        "recalcular_actividad_sab.recalcular_actividad_sab"
    ),
    "empresa_trimestre": (
        "recalcular_empresa_trimestre.recalcular_empresa_trimestre"
    ),
}


class TestEjecutorPortable(unittest.TestCase):

    def test_01_integracion_tres_recalculadores(self):
        for producto, funcion in MODULOS.items():
            with self.subTest(producto=producto):

                fuentes = {
                    nombre: object()
                    for nombre in DEPENDENCIAS[producto]
                }

                base = object()
                alcance = object()
                candidato = object()

                with patch(
                    funcion, return_value=candidato
                ) as recalcular:

                    with patch(
                        "ejecutor_portable.validar_claves"
                    ) as validar:

                        resultado = recalcular_candidato(
                            producto,
                            fuentes,
                            base,
                            alcance,
                        )

                        self.assertIs(resultado, candidato)

                        argumentos = [
                            fuentes[nombre]
                            for nombre in DEPENDENCIAS[producto]
                        ] + [base, alcance]

                        if producto == "actividad_sab_diaria":
                            argumentos += [None, None]

                        recalcular.assert_called_once_with(
                            *argumentos
                        )

                        validar.assert_called_once_with(
                            candidato, producto
                        )

    def test_02_propagar_version_y_cobertura_sab(self):
        fuentes = {"montos_sab": object()}

        base = object()
        alcance = object()
        cobertura = [{"fecha_negociacion": "2026-01-15"}]

        with patch(
            MODULOS["actividad_sab_diaria"]
        ) as recalcular:

            with patch("ejecutor_portable.validar_claves"):

                recalcular_candidato(
                    "actividad_sab_diaria",
                    fuentes,
                    base,
                    alcance,
                    cobertura=cobertura,
                    version_silver="V2",
                )

                recalcular.assert_called_once_with(
                    fuentes["montos_sab"],
                    base,
                    alcance,
                    cobertura,
                    "V2",
                )

    def test_03_productos_bloqueados(self):
        for producto in BLOQUEADOS:
            with self.assertRaises(PermissionError):
                recalcular_candidato(
                    producto, {}, object(), object()
                )

    def test_04_dependencias_incompletas(self):
        for producto in DEPENDENCIAS:
            with self.assertRaises(ValueError):
                recalcular_candidato(
                    producto, {}, object(), object()
                )

    def test_05_base_o_alcance_ausente(self):
        for producto, dependencias in DEPENDENCIAS.items():

            fuentes = {d: object() for d in dependencias}

            with self.assertRaises(ValueError):
                recalcular_candidato(
                    producto, fuentes, None, object()
                )

            with self.assertRaises(ValueError):
                recalcular_candidato(
                    producto, fuentes, object(), None
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
