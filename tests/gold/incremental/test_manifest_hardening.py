#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics
Pruebas de regresion del reforzamiento
del validador estructural Silver -> Gold.

Cobertura:
- Lectura segura de JSON.
- Numeros no finitos.
- Objetos Python incompatibles.
- Referencias circulares.
- Anidamiento excesivo.
- Unicode invalido.
- Rutas malformadas.
- Rechazo controlado mediante CLI.

Estas pruebas no modifican Silver ni Gold.
No requieren Spark, Hadoop ni HDFS.
"""

import hashlib
import json
import subprocess
import sys
import tempfile
import unittest

from copy import deepcopy
from datetime import datetime
from pathlib import Path


# ============================================================
# 1. CONFIGURACION
# ============================================================

ROOT = Path(__file__).resolve().parents[3]

INCREMENTAL_DIR = (
    ROOT / "src" / "gold" / "incremental"
)

MANIFEST_DIR = (
    ROOT / "tests" / "gold" / "incremental" / "manifests"
)

VALIDATOR_PATH = (
    INCREMENTAL_DIR / "manifest_validator.py"
)

MANIFEST_BASE = (
    MANIFEST_DIR / "manifest_bcrp_insercion.json"
)

sys.path.insert(
    0,
    str(INCREMENTAL_DIR),
)


from manifest_validator import (
    ManifestError,
    cargar_manifest,
    validar_manifest,
    procesar_manifest,
    validar_compatibilidad_json,
)


# ============================================================
# 2. CLASE BASE
# ============================================================

class BaseHardeningTest(unittest.TestCase):

    def setUp(self):

        self.manifest, self.huella = cargar_manifest(
            MANIFEST_BASE
        )

        self.temp_dir = tempfile.TemporaryDirectory()

        self.addCleanup(
            self.temp_dir.cleanup
        )

        self.ruta_temporal = (
            Path(self.temp_dir.name)
            / "manifest_prueba.json"
        )

    def escribir_texto(self, contenido):

        self.ruta_temporal.write_text(
            contenido,
            encoding="utf-8",
        )

        return self.ruta_temporal

    def escribir_manifest(self, manifest):

        return self.escribir_texto(
            json.dumps(
                manifest,
                ensure_ascii=True,
                allow_nan=False,
            )
        )

    def manifest_con_particion(self, valor):

        manifest = deepcopy(
            self.manifest
        )

        manifest["datasets"][0][
            "particiones_afectadas"
        ][0]["anio"] = valor

        return manifest

    def manifest_con_ruta(
        self,
        ruta,
        campo="ruta_silver",
    ):

        manifest = deepcopy(
            self.manifest
        )

        manifest["datasets"][0][campo] = ruta

        return manifest

    def verificar_rechazo(
        self,
        manifest,
        mensaje_esperado=None,
    ):

        errores, advertencias = validar_manifest(
            manifest
        )

        self.assertTrue(
            errores,
            "El manifest debio ser rechazado.",
        )

        if mensaje_esperado is not None:

            self.assertTrue(
                any(
                    mensaje_esperado in error
                    for error in errores
                ),
                errores,
            )

        return errores

    def verificar_aceptacion(self, manifest):

        errores, advertencias = validar_manifest(
            manifest
        )

        self.assertEqual(
            errores,
            [],
        )

    def verificar_error_lector(
        self,
        contenido,
        mensaje_esperado,
    ):

        ruta = self.escribir_texto(
            contenido
        )

        with self.assertRaises(
            ManifestError
        ) as contexto:

            cargar_manifest(ruta)

        self.assertIn(
            mensaje_esperado,
            str(contexto.exception),
        )

    def ejecutar_cli(self, ruta):

        return subprocess.run(
            [
                sys.executable,
                str(VALIDATOR_PATH),
                "--manifest",
                str(ruta),
                "--entorno",
                "PRUEBA",
            ],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(ROOT),
            timeout=30,
        )


# ============================================================
# 3. LECTURA SEGURA DE JSON
# ============================================================

class TestLecturaJSON(BaseHardeningTest):

    def test_01_rechazar_nan(self):

        self.verificar_error_lector(
            '{"valor": NaN}',
            "Constante JSON no permitida",
        )

    def test_02_rechazar_infinity(self):

        self.verificar_error_lector(
            '{"valor": Infinity}',
            "Constante JSON no permitida",
        )

    def test_03_rechazar_infinity_negativo(self):

        self.verificar_error_lector(
            '{"valor": -Infinity}',
            "Constante JSON no permitida",
        )

    def test_04_rechazar_exponente_desbordado(self):

        self.verificar_error_lector(
            '{"valor": 1e999}',
            "Numero JSON no finito",
        )

    def test_05_rechazar_exponente_negativo_desbordado(self):

        self.verificar_error_lector(
            '{"valor": -1e999}',
            "Numero JSON no finito",
        )

    def test_06_aceptar_numero_finito(self):

        ruta = self.escribir_texto(
            '{"valor": 1.25}'
        )

        manifest, huella = cargar_manifest(
            ruta
        )

        self.assertEqual(
            manifest["valor"],
            1.25,
        )

        self.assertEqual(
            len(huella),
            64,
        )

    def test_07_aceptar_exponente_finito(self):

        ruta = self.escribir_texto(
            '{"valor": 1e100}'
        )

        manifest, _ = cargar_manifest(
            ruta
        )

        self.assertEqual(
            manifest["valor"],
            1e100,
        )

    def test_08_rechazar_no_finito_anidado(self):

        self.verificar_error_lector(
            '{"datos": [{"valor": 1e999}]}',
            "Numero JSON no finito",
        )

    def test_09_rechazar_anidamiento_extremo(self):

        contenido = (
            '{"datos": '
            + "[" * 3000
            + "0"
            + "]" * 3000
            + "}"
        )

        self.verificar_error_lector(
            contenido,
            "anidamiento excesivo",
        )

    def test_10_preservar_huella_sha256(self):

        contenido = (
            b'{"valor": 1.25, "estado": "PRUEBA"}'
        )

        self.ruta_temporal.write_bytes(
            contenido
        )

        manifest, huella = cargar_manifest(
            self.ruta_temporal
        )

        esperada = hashlib.sha256(
            contenido
        ).hexdigest()

        self.assertEqual(
            huella,
            esperada,
        )

    def test_11_rechazar_claves_duplicadas_anidadas(self):

        self.verificar_error_lector(
            '{"datos": {"valor": 1, "valor": 2}}',
            "Clave JSON duplicada",
        )

    def test_12_rechazar_utf8_invalido(self):

        self.ruta_temporal.write_bytes(
            b'{"valor": "\xff"}'
        )

        with self.assertRaises(
            ManifestError
        ):

            cargar_manifest(
                self.ruta_temporal
            )


# ============================================================
# 4. COMPATIBILIDAD DE OBJETOS PYTHON
# ============================================================

class TestObjetosPython(BaseHardeningTest):

    def test_13_rechazar_nan_directo(self):

        manifest = self.manifest_con_particion(
            float("nan")
        )

        self.verificar_rechazo(
            manifest,
            "Numero JSON no finito",
        )

    def test_14_rechazar_infinity_directo(self):

        manifest = self.manifest_con_particion(
            float("inf")
        )

        self.verificar_rechazo(
            manifest,
            "Numero JSON no finito",
        )

    def test_15_rechazar_infinity_negativo_directo(self):

        manifest = self.manifest_con_particion(
            float("-inf")
        )

        self.verificar_rechazo(
            manifest,
            "Numero JSON no finito",
        )

    def test_16_rechazar_set(self):

        manifest = self.manifest_con_particion(
            {1, 2, 3}
        )

        self.verificar_rechazo(
            manifest,
            "Tipo Python no compatible con JSON: set",
        )

    def test_17_rechazar_datetime(self):

        manifest = self.manifest_con_particion(
            datetime(
                2025,
                12,
                30,
            )
        )

        self.verificar_rechazo(
            manifest,
            "Tipo Python no compatible con JSON: datetime",
        )

    def test_18_rechazar_tupla(self):

        manifest = self.manifest_con_particion(
            (2025, 2026)
        )

        self.verificar_rechazo(
            manifest,
            "Tipo Python no compatible con JSON: tuple",
        )

    def test_19_rechazar_bytes(self):

        manifest = self.manifest_con_particion(
            b"2025"
        )

        self.verificar_rechazo(
            manifest,
            "Tipo Python no compatible con JSON: bytes",
        )

    def test_20_rechazar_lista_circular(self):

        circular = []

        circular.append(
            circular
        )

        manifest = self.manifest_con_particion(
            circular
        )

        self.verificar_rechazo(
            manifest,
            "Referencia circular",
        )

    def test_21_rechazar_diccionario_circular(self):

        circular = {}

        circular["referencia"] = circular

        manifest = self.manifest_con_particion(
            circular
        )

        self.verificar_rechazo(
            manifest,
            "Referencia circular",
        )

    def test_22_rechazar_claves_no_textuales(self):

        manifest = self.manifest_con_particion(
            {123: "valor"}
        )

        self.verificar_rechazo(
            manifest,
            "Las claves JSON deben ser cadenas",
        )

    def test_23_rechazar_unicode_invalido(self):

        manifest = self.manifest_con_particion(
            "\ud800"
        )

        self.verificar_rechazo(
            manifest,
            "Caracteres Unicode invalidos".lower()
            if False else "caracteres Unicode invalidos",
        )

    def test_24_rechazar_anidamiento_excesivo(self):

        profundo = []

        for _ in range(70):

            profundo = [
                profundo
            ]

        manifest = self.manifest_con_particion(
            profundo
        )

        self.verificar_rechazo(
            manifest,
            "Anidamiento JSON excesivo",
        )

    def test_25_aceptar_manifest_original(self):

        self.verificar_aceptacion(
            self.manifest
        )

    def test_26_aceptar_objetos_json_compartidos(self):

        compartido = {
            "valor": 123
        }

        datos = {
            "primero": compartido,
            "segundo": compartido,
        }

        errores = validar_compatibilidad_json(
            datos
        )

        self.assertEqual(
            errores,
            [],
        )

    def test_27_no_modificar_manifest(self):

        original = deepcopy(
            self.manifest
        )

        validar_manifest(
            self.manifest
        )

        self.assertEqual(
            self.manifest,
            original,
        )

    def test_28_rechazar_no_finito_en_run_id(self):

        manifest = deepcopy(
            self.manifest
        )

        manifest["run_id"] = float(
            "inf"
        )

        self.verificar_rechazo(
            manifest,
            "Numero JSON no finito",
        )


# ============================================================
# 5. VALIDACION REFORZADA DE RUTAS
# ============================================================

class TestRutas(BaseHardeningTest):

    BASE = "file:///ruta_de_prueba/"

    SUFIJO = "macroeconomia/tipo_cambio"

    def ruta_con_segmento(self, segmento):

        return (
            self.BASE
            + segmento
            + self.SUFIJO
        )

    def test_29_aceptar_ruta_normal(self):

        manifest = self.manifest_con_ruta(
            self.BASE + self.SUFIJO
        )

        self.verificar_aceptacion(
            manifest
        )

    def test_30_rechazar_segmento_punto(self):

        manifest = self.manifest_con_ruta(
            self.ruta_con_segmento("./")
        )

        self.verificar_rechazo(
            manifest,
            "segmentos de ruta no permitidos",
        )

    def test_31_rechazar_segmento_doble_punto(self):

        manifest = self.manifest_con_ruta(
            self.ruta_con_segmento("../")
        )

        self.verificar_rechazo(
            manifest,
            "segmentos de ruta no permitidos",
        )

    def test_32_rechazar_doble_punto_codificado(self):

        manifest = self.manifest_con_ruta(
            self.ruta_con_segmento(
                "%2e%2e/"
            )
        )

        self.verificar_rechazo(
            manifest,
            "codificacion porcentual",
        )

    def test_33_rechazar_doble_codificacion(self):

        manifest = self.manifest_con_ruta(
            self.ruta_con_segmento(
                "%252e%252e/"
            )
        )

        self.verificar_rechazo(
            manifest,
            "codificacion porcentual",
        )

    def test_34_rechazar_barra_codificada(self):

        manifest = self.manifest_con_ruta(
            self.ruta_con_segmento(
                "%2f/"
            )
        )

        self.verificar_rechazo(
            manifest,
            "codificacion porcentual",
        )

    def test_35_rechazar_barra_invertida(self):

        manifest = self.manifest_con_ruta(
            self.ruta_con_segmento(
                "\\/"
            )
        )

        self.verificar_rechazo(
            manifest,
            "barras invertidas",
        )

    def test_36_rechazar_caracter_nulo(self):

        manifest = self.manifest_con_ruta(
            self.ruta_con_segmento(
                "\x00/"
            )
        )

        self.verificar_rechazo(
            manifest,
            "caracteres de control",
        )

    def test_37_rechazar_salto_de_linea(self):

        manifest = self.manifest_con_ruta(
            self.ruta_con_segmento(
                "\n"
            )
        )

        self.verificar_rechazo(
            manifest,
            "caracteres de control",
        )

    def test_38_rechazar_tabulacion(self):

        manifest = self.manifest_con_ruta(
            self.ruta_con_segmento(
                "\t"
            )
        )

        self.verificar_rechazo(
            manifest,
            "caracteres de control",
        )

    def test_39_rechazar_retorno_de_carro(self):

        manifest = self.manifest_con_ruta(
            self.ruta_con_segmento(
                "\r"
            )
        )

        self.verificar_rechazo(
            manifest,
            "caracteres de control",
        )

    def test_40_rechazar_caracter_del(self):

        manifest = self.manifest_con_ruta(
            self.ruta_con_segmento(
                "\x7f/"
            )
        )

        self.verificar_rechazo(
            manifest,
            "caracteres de control",
        )

    def test_41_rechazar_espacio_inicial(self):

        manifest = self.manifest_con_ruta(
            " " + self.BASE + self.SUFIJO
        )

        self.verificar_rechazo(
            manifest,
            "espacios al inicio",
        )

    def test_42_rechazar_espacio_final(self):

        manifest = self.manifest_con_ruta(
            self.BASE + self.SUFIJO + " "
        )

        self.verificar_rechazo(
            manifest,
            "espacios al inicio",
        )

    def test_43_rechazar_porcentaje_en_detalle_externo(self):

        manifest = deepcopy(
            self.manifest
        )

        dataset = manifest["datasets"][0]

        dataset["claves_afectadas"] = None

        dataset["ruta_detalle_cambios"] = (
            "file:///ruta_de_prueba/"
            "%2e%2e/cambios.json"
        )

        self.verificar_rechazo(
            manifest,
            "codificacion porcentual",
        )

    def test_44_rechazar_control_en_detalle_externo(self):

        manifest = deepcopy(
            self.manifest
        )

        dataset = manifest["datasets"][0]

        dataset["claves_afectadas"] = None

        dataset["ruta_detalle_cambios"] = (
            "file:///ruta_de_prueba/"
            "\n/cambios.json"
        )

        self.verificar_rechazo(
            manifest,
            "caracteres de control",
        )


# ============================================================
# 6. RESULTADOS CONTROLADOS
# ============================================================

class TestResultados(BaseHardeningTest):

    def test_45_procesar_exponente_no_finito(self):

        ruta = self.escribir_texto(
            '{"valor": 1e999}'
        )

        resultado = procesar_manifest(
            ruta
        )

        self.assertEqual(
            resultado["estado"],
            "GOLD_RECHAZADO",
        )

        self.assertFalse(
            resultado[
                "validacion_estructural_aprobada"
            ]
        )

        self.assertTrue(
            resultado["errores"]
        )

    def test_46_procesar_ruta_codificada(self):

        manifest = self.manifest_con_ruta(
            "file:///ruta_de_prueba/"
            "%2e%2e/macroeconomia/tipo_cambio"
        )

        ruta = self.escribir_manifest(
            manifest
        )

        resultado = procesar_manifest(
            ruta
        )

        self.assertEqual(
            resultado["estado"],
            "GOLD_RECHAZADO",
        )

        self.assertFalse(
            resultado[
                "autorizacion_ejecucion_gold"
            ]
        )

    def test_47_procesar_manifest_valido(self):

        resultado = procesar_manifest(
            MANIFEST_BASE
        )

        self.assertEqual(
            resultado["estado"],
            "MANIFEST_ESTRUCTURA_APROBADA",
        )

        self.assertTrue(
            resultado[
                "validacion_estructural_aprobada"
            ]
        )

        self.assertFalse(
            resultado[
                "validacion_operativa_aprobada"
            ]
        )

        self.assertFalse(
            resultado[
                "autorizacion_ejecucion_gold"
            ]
        )


# ============================================================
# 7. INTERFAZ CLI
# ============================================================

class TestCLI(BaseHardeningTest):

    def verificar_cli_rechazado(
        self,
        ruta,
        mensaje_esperado,
    ):

        proceso = self.ejecutar_cli(
            ruta
        )

        self.assertEqual(
            proceso.returncode,
            1,
            proceso.stderr,
        )

        self.assertNotIn(
            "Traceback",
            proceso.stderr,
        )

        resultado = json.loads(
            proceso.stdout
        )

        self.assertEqual(
            resultado["estado"],
            "GOLD_RECHAZADO",
        )

        self.assertFalse(
            resultado[
                "validacion_estructural_aprobada"
            ]
        )

        self.assertFalse(
            resultado[
                "autorizacion_ejecucion_gold"
            ]
        )

        self.assertTrue(
            any(
                mensaje_esperado in error
                for error in resultado["errores"]
            ),
            resultado["errores"],
        )

    def test_48_cli_rechazar_nan(self):

        ruta = self.escribir_texto(
            '{"valor": NaN}'
        )

        self.verificar_cli_rechazado(
            ruta,
            "Constante JSON no permitida",
        )

    def test_49_cli_rechazar_exponente_no_finito(self):

        ruta = self.escribir_texto(
            '{"valor": 1e999}'
        )

        self.verificar_cli_rechazado(
            ruta,
            "Numero JSON no finito",
        )

    def test_50_cli_rechazar_ruta_codificada(self):

        manifest = self.manifest_con_ruta(
            "file:///ruta_de_prueba/"
            "%2e%2e/macroeconomia/tipo_cambio"
        )

        ruta = self.escribir_manifest(
            manifest
        )

        self.verificar_cli_rechazado(
            ruta,
            "codificacion porcentual",
        )

    def test_51_cli_rechazar_anidamiento_extremo(self):

        contenido = (
            '{"datos": '
            + "[" * 3000
            + "0"
            + "]" * 3000
            + "}"
        )

        ruta = self.escribir_texto(
            contenido
        )

        self.verificar_cli_rechazado(
            ruta,
            "anidamiento excesivo",
        )

    def test_52_cli_aceptar_manifest_valido(self):

        proceso = self.ejecutar_cli(
            MANIFEST_BASE
        )

        self.assertEqual(
            proceso.returncode,
            0,
            proceso.stderr,
        )

        resultado = json.loads(
            proceso.stdout
        )

        self.assertEqual(
            resultado["estado"],
            "MANIFEST_ESTRUCTURA_APROBADA",
        )

        self.assertTrue(
            resultado[
                "validacion_estructural_aprobada"
            ]
        )

        self.assertFalse(
            resultado[
                "validacion_operativa_aprobada"
            ]
        )

        self.assertFalse(
            resultado[
                "autorizacion_ejecucion_gold"
            ]
        )


# ============================================================
# 8. EJECUCION
# ============================================================

if __name__ == "__main__":

    unittest.main(
        verbosity=2
    )
