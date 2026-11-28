"""Validacion nueva de auditoria integral v3.

Sin Spark, GCP ni constructores simulados.
Los objetos de evidencia y el transporte de
escritura son fixtures exclusivos de pruebas.
"""

import json
import hashlib
import unittest

from copy import deepcopy
from unittest.mock import patch

import escritura_correctiva_staging as writer

from adaptador_numerico_integral import (
    REGLAS,
    POLITICA,
)


COT = "mercado_diario/cotizaciones"
SAB = "mercado_diario/montos_sab"

CONTEOS = {
    COT: 83130,
    SAB: 21911,
}


def referencia(nombre, caracter):

    return {
        "uri":
            "gs://bvl26-control-7f3a9c/"
            "fixture-pruebas/" + nombre,
        "generation": "1",
        "sha256": caracter * 64,
        "size_bytes": 100,
    }


def fixture():

    fuentes = {
        fuente: {
            "version_id": "fixture-local-v3",
            "physical_contract_sha256": "a" * 64,
        }
        for fuente in REGLAS
    }

    decision = {
        "decision_id": "fixture-numerico",
        "decision":
            "ALLOW_STRICT_NUMERIC_CONVERSIONS",
        "quality_approved": True,
        "policy": POLITICA,
        "sources": {
            fuente: {
                **fuentes[fuente],
                "columns": {
                    columna: {
                        "input_type": "string",
                        "output_type": tipo,
                    }
                    for columna, tipo in reglas.items()
                },
            }
            for fuente, reglas in REGLAS.items()
        },
    }

    cobertura = {
        "policy":
            "SAB_RESTRICTED_RATIFICATION_V1",
        "version_id": fuentes[SAB]["version_id"],
        "payload": referencia("sidecar.json", "b"),
        "signature": referencia("sidecar.sig", "c"),
        "ratification":
            referencia("ratificacion.json", "d"),
        "ratification_signature":
            referencia("ratificacion.sig", "e"),
    }

    decision_null = {
        "decision_id": "fixture-null",
        "decision": "ALLOW_TYPED_NULLS",
    }

    auth = {
        "authorization_id": "fixture-integral-v3",
        "batch_id": "fixture-batch",
        "run_id": "fixture-run",
        "attempt_id": "fixture-attempt",
        "processor_commit": "a" * 40,
        "package_sha256": "b" * 64,
        "launcher_sha256": "c" * 64,
        "staging_uri": (
            "gs://bvl26-gold-7f3a9c/"
            "staging/correccion_fase6/"
            "batch_id=fixture-batch/"
            "run_id=fixture-run/"
            "attempt_id=fixture-attempt/"
        ),
        "quality_approved": True,
        "staging_write_allowed": True,
        "publication_allowed": False,
        "sources": fuentes,
        "output_contracts": {
            producto: "f" * 64
            for producto in writer.ORDEN
        },
        "financial_decision": {
            "decision_id": "fixture-finanzas",
        },
        "numeric_adaptations": decision,
        "cotizaciones_adaptation": decision_null,
        "sab_coverage": cobertura,
    }

    evidencias = {}

    for fuente, reglas in REGLAS.items():

        evidencias[fuente] = {
            "status": "APPLIED",
            "product": fuente,
            "policy": POLITICA,
            "authorization_id":
                auth["authorization_id"],
            "decision_id":
                decision["decision_id"],
            **fuentes[fuente],
            "input_records": CONTEOS[fuente],
            "manifest_reference":
                referencia("manifest.json", "f"),
            "columns": {
                columna: {
                    "input_type": "string",
                    "output_type": tipo,
                    "converted_records":
                        CONTEOS[fuente] - 1,
                    "original_nulls": 1,
                    "invalid_records": 0,
                }
                for columna, tipo in reglas.items()
            },
            "silver_modified": False,
            "constructor_modified": False,
            "publication_allowed": False,
            "rounding_monetary_allowed": False,
            "invalid_replacement_allowed": False,
        }

    evidencia_sab = {
        "policy": cobertura["policy"],
        "status":
            "DOCUMENTARY_RATIFICATION_VERIFIED",
        "product": SAB,
        "version_id": cobertura["version_id"],
        "sidecar_sha256":
            cobertura["payload"]["sha256"],
        "sidecar_signature_sha256":
            cobertura["signature"]["sha256"],
        "ratification_sha256":
            cobertura["ratification"]["sha256"],
        "ratification_signature_sha256":
            cobertura["ratification_signature"]["sha256"],
        "eligible_records": 21911,
        "coverage": "DESCONOCIDA",
        "full_coverage_attested": False,
        "daily_counts_verified_against_dataframe":
            True,
        "production_authorized": False,
        "publication_allowed": False,
    }

    return {
        "autorizacion": auth,
        "numeric_adaptations": evidencias,
        "sab_coverage_evidence": evidencia_sab,
        "cobertura_acreditada": True,
        "cotizaciones_adaptation": {
            "status": "APPLIED",
            "decision": decision_null,
            "version_id": fuentes[COT]["version_id"],
            "authorization_id":
                auth["authorization_id"],
        },
        "candidatos": {
            producto: object()
            for producto in writer.ORDEN
        },
        "excepciones": {
            nombre: object()
            for nombre in writer.EXCEPCIONES
        },
        "verificar_vigencia": lambda: None,
        "publication_allowed": False,
    }


class ClienteProhibido:

    def __getattr__(self, nombre):

        raise AssertionError(
            "Acceso a cliente GCS prohibido: "
            + nombre
        )


class TransporteMemoria:

    def __init__(self):

        self.documentos = {}

        self.orden = []

    def comprobar_prefijo_nuevo(self):

        assert not self.orden

    def escribir_bytes(self, relativo, contenido):

        assert relativo not in self.orden

        self.orden.append(relativo)

        self.documentos[relativo] = json.loads(
            contenido
        )

        return {
            "uri": "fixture://local/" + relativo,
            "generation": "1",
            "sha256": hashlib.sha256(
                contenido
            ).hexdigest(),
            "size_bytes": len(contenido),
        }

    def escribir_archivo(self, relativo, ruta):

        assert relativo not in self.orden

        self.orden.append(relativo)

        return {
            "uri": "fixture://local/" + relativo,
            "generation": "1",
            "sha256": "a" * 64,
            "size_bytes": 100,
        }

    def comprobar_inventario(self):

        assert len(self.orden) == len(
            set(self.orden)
        )


def materializacion_fixture(
    spark, df, producto, destino,
    particiones, cantidad,
):

    return {
        "records_count": cantidad,
        "partition_columns": particiones,
        "content_roundtrip_verified": True,
        "files": [{
            "ruta": destino / "part-000000.parquet",
            "relativo": "part-000000.parquet",
            "records_count": cantidad,
            "size_bytes": 100,
        }],
    }


class TestEvidenciaIntegralV3(unittest.TestCase):

    def rechazar(self, modificar):

        preparado = fixture()

        modificar(preparado)

        with self.assertRaises(ValueError):

            writer.escribir_correccion_supervisada(
                None,
                ClienteProhibido(),
                preparado,
            )

    def test_01_evidencia_valida(self):

        preparado = fixture()

        numerica, sab = (
            writer.validar_evidencia_integral(
                preparado
            )
        )

        self.assertEqual(
            set(numerica), set(REGLAS)
        )

        self.assertFalse(
            sab["full_coverage_attested"]
        )

    def test_02_evidencia_numerica_ausente(self):

        self.rechazar(
            lambda p: p.pop("numeric_adaptations")
        )

    def test_03_fuente_ausente(self):

        self.rechazar(
            lambda p: p[
                "numeric_adaptations"
            ].pop(SAB)
        )

    def test_04_version_incorrecta(self):

        self.rechazar(
            lambda p: p[
                "numeric_adaptations"
            ][COT].update(
                version_id="otra-version"
            )
        )

    def test_05_contrato_incorrecto(self):

        self.rechazar(
            lambda p: p[
                "numeric_adaptations"
            ][SAB].update(
                physical_contract_sha256="0" * 64
            )
        )

    def test_06_invalidos_no_cero(self):

        self.rechazar(
            lambda p: p[
                "numeric_adaptations"
            ][COT]["columns"][
                "cierre_actual"
            ].update(
                invalid_records=1
            )
        )

    def test_07_conteo_inconsistente(self):

        self.rechazar(
            lambda p: p[
                "numeric_adaptations"
            ][SAB]["columns"][
                "MontoCompras"
            ].update(
                original_nulls=2
            )
        )

    def test_08_redondeo_no_autorizado(self):

        self.rechazar(
            lambda p: p[
                "numeric_adaptations"
            ][SAB].update(
                rounding_monetary_allowed=True
            )
        )

    def test_09_tipo_incorrecto(self):

        self.rechazar(
            lambda p: p[
                "numeric_adaptations"
            ][SAB]["columns"][
                "MontoCompras"
            ].update(
                output_type="double"
            )
        )

    def test_10_ratificacion_incorrecta(self):

        self.rechazar(
            lambda p: p[
                "sab_coverage_evidence"
            ].update(
                ratification_sha256="0" * 64
            )
        )

    def test_11_cobertura_inventada(self):

        self.rechazar(
            lambda p: p[
                "sab_coverage_evidence"
            ].update(
                full_coverage_attested=True
            )
        )

    def test_12_conteos_diarios_no_verificados(self):

        self.rechazar(
            lambda p: p[
                "sab_coverage_evidence"
            ].update(
                daily_counts_verified_against_dataframe=False
            )
        )

    def test_13_evidencia_sab_ausente(self):

        self.rechazar(
            lambda p: p.pop(
                "sab_coverage_evidence"
            )
        )

    def test_14_sin_sidecar_expreso(self):

        preparado = fixture()

        preparado[
            "autorizacion"
        ]["sab_coverage"] = None

        preparado["sab_coverage_evidence"] = None

        preparado["cobertura_acreditada"] = False

        numerica, sab = (
            writer.validar_evidencia_integral(
                preparado
            )
        )

        self.assertIsNone(sab)

        self.assertEqual(
            set(numerica), set(REGLAS)
        )

    def test_15_sin_sidecar_con_evidencia(self):

        self.rechazar(
            lambda p: p[
                "autorizacion"
            ].update(
                sab_coverage=None
            )
        )

    def test_16_decision_no_autorizada(self):

        self.rechazar(
            lambda p: p[
                "autorizacion"
            ]["numeric_adaptations"].update(
                quality_approved=False
            )
        )

    def test_17_auditoria_y_marcador_final(self):

        preparado = fixture()

        transporte = TransporteMemoria()

        with patch.object(
            writer,
            "EscritorStagingExclusivo",
            return_value=transporte,
        ), patch.object(
            writer,
            "materializar_parquet_local",
            side_effect=materializacion_fixture,
        ):

            resultado = (
                writer.escribir_correccion_supervisada(
                    None,
                    ClienteProhibido(),
                    preparado,
                )
            )

        auditoria = transporte.documentos[
            "_control/AUDIT.json"
        ]

        self.assertEqual(
            auditoria["numeric_adaptations"],
            preparado["numeric_adaptations"],
        )

        self.assertEqual(
            auditoria["sab_coverage_evidence"],
            preparado["sab_coverage_evidence"],
        )

        self.assertEqual(
            auditoria["cotizaciones_adaptation"],
            preparado["cotizaciones_adaptation"],
        )

        self.assertFalse(
            auditoria["publication_allowed"]
        )

        self.assertFalse(
            auditoria["production_authorized"]
        )

        self.assertEqual(
            transporte.orden[0],
            "_control/STARTED.json",
        )

        self.assertEqual(
            transporte.orden[-1],
            "_control/COMPLETE.json",
        )

        self.assertEqual(
            resultado["status"],
            "STAGING_COMPLETE",
        )

        inventario = transporte.documentos[
            "_control/INVENTORY.json"
        ]["outputs"]

        self.assertEqual(len(inventario), 5)

        for nombre, salida in inventario.items():

            self.assertEqual(
                salida["quality_only"],
                nombre.startswith("calidad/"),
            )

            self.assertFalse(
                salida["production_authorized"]
            )

    def test_18_null_tipado_no_acreditado(self):

        self.rechazar(
            lambda p: p[
                "cotizaciones_adaptation"
            ].update(
                authorization_id="otra-autorizacion"
            )
        )


if __name__ == "__main__":

    suite = unittest.defaultTestLoader \
        .loadTestsFromTestCase(
            TestEvidenciaIntegralV3
        )

    resultado = unittest.TextTestRunner(
        verbosity=2
    ).run(suite)

    assert resultado.testsRun == 18

    assert resultado.wasSuccessful()

    print("VALIDACIONES_NUEVAS_WRITER: 18/18")
    print("AUDITORIA_WRITER_INTEGRAL_PROBADA: True")
    print("TRANSPORTE_ESCRITURA_SIMULADO: True")
    print("MATERIALIZACION_EN_ESTA_PRUEBA_SIMULADA: True")
    print("SPARK_EJECUTADO_EN_ESTA_PRUEBA: False")
    print("CONSTRUCTORES_SIMULADOS: 0")
    print("GCP_EJECUTADO: False")
    print("PUBLICACION_AUTORIZADA: False")
    print("EVIDENCIA_INTEGRAL_WRITER_V3_OK")
