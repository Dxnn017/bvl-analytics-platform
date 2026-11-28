import json
import unittest
from unittest.mock import MagicMock, patch

import integracion_quality_v03 as integracion


class TestIntegracionQuality(unittest.TestCase):

    def setUp(self):
        self.cliente = MagicMock()
        self.spark = MagicMock()

        self.producto = "actividad_sab_diaria"

        self.auth = {
            "product": "mercado_diario/montos_sab",
            "version_id": "SINTETICO-V2",
            "current": {"uri": "fixture"},
            "manifest": {"uri": "fixture-manifest"},
            "manifest_signature": {"uri": "fixture-firma"},
            "joint_marker": None,
        }

        self.evidencias = {
            "montos_sab": {
                "autorizacion_bytes":
                    json.dumps(self.auth).encode(),
                "firma_autorizacion_der": b"fixture",
                "tipos_esperados": {"fecha": "date32[day]"},
                "claves": ["fecha"],
                "particiones_por_uri": {},
            }
        }

        self.manifest = {
            "product": self.auth["product"],
            "version_id": self.auth["version_id"],
            "files": [{"uri": "fixture-parquet"}],
            "records_count": 2,
            "bytes_total": 100,
        }

        self.preflight = {
            "product": self.auth["product"],
            "version_id": self.auth["version_id"],
            "esquema_fisico_verificado": True,
            "archivos_parquet_verificados": 1,
            "filas_validadas": 2,
            "consumo_productivo_autorizado": False,
        }

    def test_01_prioridades_y_bloqueos(self):
        for producto in (
            "empresa_trimestre",
            "hechos_empresa",
            "impacto_hechos_valor",
        ):
            with self.assertRaises(PermissionError):
                integracion.validar_producto_quality(producto)

    def test_02_dependencias_incompletas(self):
        with self.assertRaises(ValueError):
            integracion.verificar_fuentes_quality(
                self.cliente,
                "mercado_valor_diario",
                {},
                b"fixture",
            )

    def test_03_autorizacion_de_otra_fuente(self):
        self.auth["product"] = "fuente_incompatible"

        self.evidencias["montos_sab"][
            "autorizacion_bytes"
        ] = json.dumps(self.auth).encode()

        with patch.object(
            integracion,
            "preparar_silver_v03_con_parquet",
        ) as preflight:

            with self.assertRaises(ValueError):
                integracion.verificar_fuentes_quality(
                    self.cliente,
                    self.producto,
                    self.evidencias,
                    b"fixture",
                )

            preflight.assert_not_called()

    def test_04_preflight_rechazado_no_procesa(self):

        with patch.object(
            integracion,
            "preparar_silver_v03_con_parquet",
            side_effect=ValueError("Firma invalida"),
        ):
            with patch.object(
                integracion,
                "construir_candidato",
            ) as construir:

                with self.assertRaises(ValueError):
                    integracion.preparar_candidato_quality(
                        self.spark,
                        self.cliente,
                        self.producto,
                        self.evidencias,
                        b"fixture",
                    )

                construir.assert_not_called()

    def test_05_integracion_preflight_ejecutor(self):

        with patch.multiple(
            integracion,
            preparar_silver_v03_con_parquet=MagicMock(
                return_value=self.preflight
            ),
            leer_verificado=MagicMock(
                return_value=json.dumps(
                    self.manifest
                ).encode()
            ),
            verificar_vigencia=MagicMock(),
            materializar_fuente_verificada=MagicMock(),
            construir_candidato=MagicMock(),
        ):

            df = MagicMock()

            integracion.materializar_fuente_verificada \
                .return_value = df

            candidato = MagicMock()

            candidato.cache.return_value = candidato
            candidato.count.return_value = 2

            integracion.construir_candidato \
                .return_value = candidato

            resultado, reporte = (
                integracion.preparar_candidato_quality(
                    self.spark,
                    self.cliente,
                    self.producto,
                    self.evidencias,
                    b"fixture",
                )
            )

            self.assertIs(resultado, candidato)

            integracion.construir_candidato \
                .assert_called_once_with(
                    self.producto,
                    {"montos_sab": df},
                    cobertura=None,
                    version_silver="SINTETICO-V2",
                )

            self.assertEqual(
                integracion.verificar_vigencia.call_count,
                3
            )

            self.assertEqual(reporte["registros"], 2)

            self.assertFalse(
                reporte["publicacion_autorizada"]
            )

            self.assertFalse(
                reporte["consumo_productivo_autorizado"]
            )

    def test_06_cambio_concurrente_no_procesa(self):

        with patch.multiple(
            integracion,
            preparar_silver_v03_con_parquet=MagicMock(
                return_value=self.preflight
            ),
            leer_verificado=MagicMock(
                return_value=json.dumps(
                    self.manifest
                ).encode()
            ),
            verificar_vigencia=MagicMock(
                side_effect=ValueError(
                    "Registry modificado"
                )
            ),
            construir_candidato=MagicMock(),
        ):

            with self.assertRaises(ValueError):
                integracion.preparar_candidato_quality(
                    self.spark,
                    self.cliente,
                    self.producto,
                    self.evidencias,
                    b"fixture",
                )

            integracion.construir_candidato \
                .assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
