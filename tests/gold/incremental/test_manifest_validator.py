#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics
Pruebas automatizadas del validador de manifests Silver -> Gold.

Contrato provisional: v0.1

Estas pruebas no requieren Spark y no modifican
los datos historicos de Silver o Gold.
"""

import copy
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest

from pathlib import Path


# ============================================================
# 1. CONFIGURACION DE RUTAS
# ============================================================

ROOT = Path(__file__).resolve().parents[3]

DIRECTORIO_INCREMENTAL = (
    ROOT / "src" / "gold" / "incremental"
)

DIRECTORIO_MANIFESTS = (
    Path(__file__).resolve().parent / "manifests"
)

SCRIPT_VALIDADOR = (
    DIRECTORIO_INCREMENTAL / "manifest_validator.py"
)

sys.path.insert(0, str(DIRECTORIO_INCREMENTAL))

from manifest_validator import (
    ManifestError,
    cargar_manifest,
    procesar_manifest,
    validar_manifest,
)


# ============================================================
# 2. CLASE DE PRUEBAS
# ============================================================

class TestManifestValidator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.manifest_base, _ = cargar_manifest(
            DIRECTORIO_MANIFESTS
            / "manifest_bcrp_insercion.json"
        )

    def setUp(self):

        self.manifest = copy.deepcopy(
            self.manifest_base
        )

        self.dataset = self.manifest["datasets"][0]

    # ========================================================
    # 3. FUNCIONES AUXILIARES
    # ========================================================

    def verificar_aceptado(self, manifest):

        errores, advertencias = validar_manifest(
            manifest,
            entorno_esperado="PRUEBA",
        )

        self.assertEqual(
            errores,
            [],
            msg=f"Errores inesperados: {errores}",
        )

        return advertencias

    def verificar_rechazado(
        self,
        manifest,
        mensaje_esperado,
        entorno="PRUEBA",
    ):

        errores, _ = validar_manifest(
            manifest,
            entorno_esperado=entorno,
        )

        self.assertTrue(
            errores,
            msg="El manifest debio ser rechazado.",
        )

        self.assertTrue(
            any(
                mensaje_esperado.lower()
                in error.lower()
                for error in errores
            ),
            msg=(
                f"No se encontro el error esperado: "
                f"{mensaje_esperado}. "
                f"Errores obtenidos: {errores}"
            ),
        )

    def verificar_archivo(
        self,
        nombre,
        aceptado,
        error_esperado=None,
    ):

        ruta = DIRECTORIO_MANIFESTS / nombre

        resultado = procesar_manifest(ruta)

        self.assertEqual(
            resultado[
                "validacion_estructural_aprobada"
            ],
            aceptado,
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

        self.assertIsNotNone(
            resultado["huella_sha256"]
        )

        self.assertEqual(
            len(resultado["huella_sha256"]),
            64,
        )

        if aceptado:

            self.assertEqual(
                resultado["estado"],
                "MANIFEST_ESTRUCTURA_APROBADA",
            )

            self.assertEqual(
                resultado["errores"],
                [],
            )

        else:

            self.assertEqual(
                resultado["estado"],
                "GOLD_RECHAZADO",
            )

            self.assertTrue(
                resultado["errores"]
            )

        if error_esperado:

            self.assertTrue(
                any(
                    error_esperado.lower()
                    in error.lower()
                    for error in resultado["errores"]
                )
            )

    # ========================================================
    # 4. MANIFESTS ORIGINALES
    # ========================================================

    def test_01_insercion_bcrp(self):

        self.verificar_archivo(
            "manifest_bcrp_insercion.json",
            aceptado=True,
        )

    def test_02_correccion_bcrp(self):

        self.verificar_archivo(
            "manifest_bcrp_correccion.json",
            aceptado=True,
        )

    def test_03_silver_no_aprobado(self):

        self.verificar_archivo(
            "manifest_no_aprobado.json",
            aceptado=False,
            error_esperado=(
                "Silver no se encuentra publicado"
            ),
        )

    def test_04_dataset_desconocido(self):

        self.verificar_archivo(
            "manifest_dataset_desconocido.json",
            aceptado=False,
            error_esperado="dataset desconocido",
        )

    def test_05_eliminacion_no_autorizada(self):

        self.verificar_archivo(
            "manifest_eliminacion_no_autorizada.json",
            aceptado=False,
            error_esperado=(
                "eliminacion no autorizada"
            ),
        )

    def test_06_correccion_sab(self):

        self.verificar_archivo(
            "manifest_sab_correccion.json",
            aceptado=True,
        )

    # ========================================================
    # 5. VERSIONES, ENTORNO Y ESTADO
    # ========================================================

    def test_07_version_no_soportada(self):

        self.manifest["version_manifest"] = "99.0"

        self.verificar_rechazado(
            self.manifest,
            "Version del manifest no soportada",
        )

    def test_08_entorno_invalido(self):

        self.manifest["entorno"] = "DESCONOCIDO"

        self.verificar_rechazado(
            self.manifest,
            "Entorno del manifest invalido",
        )

    def test_09_entorno_incompatible(self):

        self.verificar_rechazado(
            self.manifest,
            "Entorno incompatible",
            entorno="PRODUCCION",
        )

    def test_10_produccion_bloqueada(self):

        self.manifest["entorno"] = "PRODUCCION"

        self.dataset["ruta_silver"] = (
            "gs://bucket-simulado/silver/"
            "macroeconomia/tipo_cambio"
        )

        self.verificar_rechazado(
            self.manifest,
            "Produccion bloqueada",
            entorno="PRODUCCION",
        )

    def test_11_run_id_vacio(self):

        self.manifest["run_id"] = ""

        self.verificar_rechazado(
            self.manifest,
            "run_id invalido",
        )

    def test_12_run_id_inseguro(self):

        self.manifest["run_id"] = "../ejecucion"

        self.verificar_rechazado(
            self.manifest,
            "run_id invalido",
        )

    def test_13_fecha_publicacion_invalida(self):

        self.manifest["fecha_publicacion"] = (
            "fecha-invalida"
        )

        self.verificar_rechazado(
            self.manifest,
            "fecha_publicacion invalida",
        )

    def test_14_fecha_sin_zona_horaria(self):

        self.manifest["fecha_publicacion"] = (
            "2026-11-14T22:00:00"
        )

        self.verificar_rechazado(
            self.manifest,
            "fecha_publicacion invalida",
        )

    def test_15_aprobacion_no_booleana(self):

        self.manifest["validacion_aprobada"] = 1

        self.verificar_rechazado(
            self.manifest,
            "validacion_aprobada debe ser true",
        )

    def test_16_publicacion_completa(self):

        self.manifest["tipo_publicacion"] = (
            "COMPLETA"
        )

        self.verificar_rechazado(
            self.manifest,
            "Tipo de publicacion no soportado",
        )

    # ========================================================
    # 6. ESTRUCTURA Y CATALOGO
    # ========================================================

    def test_17_campo_obligatorio_ausente(self):

        del self.manifest["run_id"]

        self.verificar_rechazado(
            self.manifest,
            "campos faltantes",
        )

    def test_18_campo_adicional_desconocido(self):

        self.manifest["campo_no_definido"] = True

        self.verificar_rechazado(
            self.manifest,
            "campos no reconocidos",
        )

    def test_19_lista_datasets_vacia(self):

        self.manifest["datasets"] = []

        self.verificar_rechazado(
            self.manifest,
            "no contiene datasets",
        )

    def test_20_datasets_tipo_incorrecto(self):

        self.manifest["datasets"] = {}

        self.verificar_rechazado(
            self.manifest,
            "datasets debe ser una lista",
        )

    def test_21_dataset_duplicado(self):

        self.manifest["datasets"].append(
            copy.deepcopy(self.dataset)
        )

        self.verificar_rechazado(
            self.manifest,
            "Dataset duplicado",
        )

    def test_22_clave_negocio_incorrecta(self):

        self.dataset["clave"] = ["id"]

        self.verificar_rechazado(
            self.manifest,
            "clave incorrecta",
        )

    # ========================================================
    # 7. CONTEOS Y CONSERVACION
    # ========================================================

    def test_23_conteo_negativo(self):

        self.dataset["registros_nuevos"] = -1

        self.verificar_rechazado(
            self.manifest,
            "entero no negativo",
        )

    def test_24_conteo_booleano(self):

        self.dataset["registros_nuevos"] = True

        self.verificar_rechazado(
            self.manifest,
            "entero no negativo",
        )

    def test_25_conteo_decimal(self):

        self.dataset["registros_nuevos"] = 1.5

        self.verificar_rechazado(
            self.manifest,
            "entero no negativo",
        )

    def test_26_conteo_final_inconsistente(self):

        self.dataset["registros_despues"] = 8000

        self.verificar_rechazado(
            self.manifest,
            "conteo final inconsistente",
        )

    def test_27_modificaciones_exceden_historico(self):

        self.dataset["registros_antes"] = 0

        self.dataset["registros_corregidos"] = 1

        self.dataset["registros_despues"] = 1

        self.verificar_rechazado(
            self.manifest,
            "modificaciones exceden",
        )

    def test_28_cambios_sin_nueva_version(self):

        self.dataset["version_publicada"] = (
            self.dataset["version_anterior"]
        )

        self.verificar_rechazado(
            self.manifest,
            "sin una nueva version",
        )

    # ========================================================
    # 8. PARTICIONES
    # ========================================================

    def test_29_mes_invalido(self):

        self.dataset["particiones_afectadas"] = [
            {
                "anio": 2026,
                "mes": 13,
            }
        ]

        self.verificar_rechazado(
            self.manifest,
            "mes fuera de rango",
        )

    def test_30_particion_no_declarada(self):

        self.dataset["particiones_afectadas"] = [
            {
                "anio": 2026,
                "mes": 8,
            }
        ]

        self.verificar_rechazado(
            self.manifest,
            "de la clave no fue declarada",
        )

    def test_31_particiones_vacias(self):

        self.dataset["particiones_afectadas"] = []

        self.verificar_rechazado(
            self.manifest,
            "faltan particiones afectadas",
        )

    def test_32_particiones_duplicadas(self):

        self.dataset[
            "particiones_afectadas"
        ].append(
            {
                "anio": 2026,
                "mes": 9,
            }
        )

        self.verificar_rechazado(
            self.manifest,
            "particion duplicada",
        )

    # ========================================================
    # 9. OPERACIONES Y CLAVES AFECTADAS
    # ========================================================

    def test_33_operacion_desconocida(self):

        self.dataset[
            "claves_afectadas"
        ][0]["operacion"] = "UPSERT"

        self.verificar_rechazado(
            self.manifest,
            "operacion no reconocida",
        )

    def test_34_insert_con_clave_anterior(self):

        self.dataset[
            "claves_afectadas"
        ][0]["clave_anterior"] = {
            "fecha": "2026-09-17"
        }

        self.verificar_rechazado(
            self.manifest,
            "INSERT requiere clave_anterior null",
        )

    def test_35_clave_nueva_incompleta(self):

        self.dataset[
            "claves_afectadas"
        ][0]["clave_nueva"] = {}

        self.verificar_rechazado(
            self.manifest,
            "campos de clave incorrectos",
        )

    def test_36_fecha_clave_invalida(self):

        self.dataset[
            "claves_afectadas"
        ][0]["clave_nueva"]["fecha"] = (
            "2026-02-30"
        )

        self.verificar_rechazado(
            self.manifest,
            "fecha invalida",
        )

    def test_37_conteo_operaciones_inconsistente(self):

        self.dataset["registros_nuevos"] = 2

        self.dataset["registros_despues"] = 7753

        self.verificar_rechazado(
            self.manifest,
            "conteo INSERT inconsistente",
        )

    def test_38_operaciones_duplicadas(self):

        self.dataset["claves_afectadas"].append(
            copy.deepcopy(
                self.dataset["claves_afectadas"][0]
            )
        )

        self.dataset["registros_nuevos"] = 2

        self.dataset["registros_despues"] = 7753

        self.verificar_rechazado(
            self.manifest,
            "clave de destino repetida",
        )

    def test_39_detalle_cambios_ausente(self):

        self.dataset["claves_afectadas"] = None

        self.dataset["ruta_detalle_cambios"] = None

        self.verificar_rechazado(
            self.manifest,
            "falta el detalle de cambios",
        )

    def test_40_dos_modalidades_de_detalle(self):

        self.dataset["ruta_detalle_cambios"] = (
            "file:///ruta_de_prueba/cambios.json"
        )

        self.verificar_rechazado(
            self.manifest,
            "una sola modalidad",
        )

    # ========================================================
    # 10. RUTAS
    # ========================================================

    def test_41_ruta_cloud_en_prueba(self):

        self.dataset["ruta_silver"] = (
            "gs://bucket-simulado/"
            "macroeconomia/tipo_cambio"
        )

        self.verificar_rechazado(
            self.manifest,
            "PRUEBA solo acepta",
        )

    def test_42_ruta_incompatible_dataset(self):

        self.dataset["ruta_silver"] = (
            "file:///ruta_de_prueba/"
            "macroeconomia/embig"
        )

        self.verificar_rechazado(
            self.manifest,
            "ruta incompatible",
        )

    def test_43_ruta_con_segmentos_inseguros(self):

        self.dataset["ruta_silver"] = (
            "file:///ruta_de_prueba/../"
            "macroeconomia/tipo_cambio"
        )

        self.verificar_rechazado(
            self.manifest,
            "segmentos de ruta no permitidos",
        )

    def test_44_ruta_file_no_local(self):

        self.dataset["ruta_silver"] = (
            "file://servidor-externo/"
            "macroeconomia/tipo_cambio"
        )

        self.verificar_rechazado(
            self.manifest,
            "ruta file:// no local",
        )

    # ========================================================
    # 11. ESCENARIOS ESTRUCTURALES POSITIVOS
    # ========================================================

    def test_45_manifest_sin_cambios(self):

        self.dataset["registros_nuevos"] = 0

        self.dataset["registros_despues"] = 7751

        self.dataset["particiones_afectadas"] = []

        self.dataset["claves_afectadas"] = []

        self.dataset["version_publicada"] = (
            self.dataset["version_anterior"]
        )

        self.verificar_aceptado(
            self.manifest
        )

    def test_46_correccion_cambia_particion(self):

        self.dataset["registros_nuevos"] = 0

        self.dataset["registros_corregidos"] = 1

        self.dataset["registros_despues"] = 7751

        self.dataset["particiones_afectadas"] = [
            {
                "anio": 2025,
                "mes": 12,
            },
            {
                "anio": 2026,
                "mes": 1,
            },
        ]

        self.dataset["claves_afectadas"] = [
            {
                "operacion": "UPDATE",
                "clave_anterior": {
                    "fecha": "2025-12-31"
                },
                "clave_nueva": {
                    "fecha": "2026-01-01"
                },
            }
        ]

        self.verificar_aceptado(
            self.manifest
        )

    def test_47_eliminacion_autorizada_estructura(self):

        self.dataset["registros_nuevos"] = 0

        self.dataset["registros_eliminados"] = 1

        self.dataset["registros_despues"] = 7750

        self.dataset[
            "autorizacion_eliminaciones"
        ] = True

        self.dataset["particiones_afectadas"] = [
            {
                "anio": 2025,
                "mes": 12,
            }
        ]

        self.dataset["claves_afectadas"] = [
            {
                "operacion": "DELETE",
                "clave_anterior": {
                    "fecha": "2025-12-30"
                },
                "clave_nueva": None,
            }
        ]

        self.verificar_aceptado(
            self.manifest
        )

    def test_48_detalle_externo_pendiente(self):

        self.dataset["claves_afectadas"] = None

        self.dataset["ruta_detalle_cambios"] = (
            "file:///ruta_de_prueba/cambios.json"
        )

        advertencias = self.verificar_aceptado(
            self.manifest
        )

        self.assertTrue(
            any(
                "detalle externo pendiente"
                in mensaje
                for mensaje in advertencias
            )
        )

    def test_49_dataset_no_consumido(self):

        self.dataset["dataset"] = "indices"

        self.dataset["ruta_silver"] = (
            "file:///ruta_de_prueba/"
            "mercado_diario/indices"
        )

        self.dataset["clave"] = [
            "indice",
            "fecha",
        ]

        self.dataset["registros_antes"] = 16

        self.dataset["registros_despues"] = 17

        self.dataset["claves_afectadas"] = [
            {
                "operacion": "INSERT",
                "clave_anterior": None,
                "clave_nueva": {
                    "indice": "INDICE_SIMULADO",
                    "fecha": "2026-09-18",
                },
            }
        ]

        advertencias = self.verificar_aceptado(
            self.manifest
        )

        self.assertTrue(
            any(
                "no consumido por Gold"
                in mensaje
                for mensaje in advertencias
            )
        )

    # ========================================================
    # 12. LECTURA JSON Y HUELLA
    # ========================================================

    def test_50_json_malformado(self):

        with tempfile.TemporaryDirectory() as tmp:

            ruta = Path(tmp) / "invalido.json"

            ruta.write_text(
                '{"run_id": ',
                encoding="utf-8",
            )

            resultado = procesar_manifest(ruta)

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
                any(
                    "JSON invalido" in error
                    for error in resultado["errores"]
                )
            )

    def test_51_json_clave_duplicada(self):

        with tempfile.TemporaryDirectory() as tmp:

            ruta = Path(tmp) / "duplicado.json"

            ruta.write_text(
                '{"run_id":"a","run_id":"b"}',
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ManifestError,
                "Clave JSON duplicada",
            ):

                cargar_manifest(ruta)

    def test_52_json_constante_nan(self):

        with tempfile.TemporaryDirectory() as tmp:

            ruta = Path(tmp) / "nan.json"

            ruta.write_text(
                '{"valor":NaN}',
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ManifestError,
                "Constante JSON no permitida",
            ):

                cargar_manifest(ruta)

    def test_53_json_raiz_no_objeto(self):

        with tempfile.TemporaryDirectory() as tmp:

            ruta = Path(tmp) / "lista.json"

            ruta.write_text(
                "[]",
                encoding="utf-8",
            )

            resultado = procesar_manifest(ruta)

            self.assertEqual(
                resultado["estado"],
                "GOLD_RECHAZADO",
            )

            self.assertTrue(
                any(
                    "objeto JSON" in error
                    for error in resultado["errores"]
                )
            )

    def test_54_archivo_inexistente(self):

        with tempfile.TemporaryDirectory() as tmp:

            ruta = (
                Path(tmp) / "no_existe.json"
            )

            resultado = procesar_manifest(ruta)

            self.assertEqual(
                resultado["estado"],
                "GOLD_RECHAZADO",
            )

            self.assertTrue(
                any(
                    "No se pudo leer" in error
                    for error in resultado["errores"]
                )
            )

    def test_55_huella_sha256_reproducible(self):

        ruta = (
            DIRECTORIO_MANIFESTS
            / "manifest_bcrp_insercion.json"
        )

        _, primera_huella = cargar_manifest(ruta)

        _, segunda_huella = cargar_manifest(ruta)

        huella_esperada = hashlib.sha256(
            ruta.read_bytes()
        ).hexdigest()

        self.assertEqual(
            primera_huella,
            segunda_huella,
        )

        self.assertEqual(
            primera_huella,
            huella_esperada,
        )

        self.assertEqual(
            len(primera_huella),
            64,
        )

    def test_56_huella_cambia_con_contenido(self):

        with tempfile.TemporaryDirectory() as tmp:

            ruta = Path(tmp) / "manifest.json"

            ruta.write_text(
                json.dumps(self.manifest),
                encoding="utf-8",
            )

            _, primera_huella = cargar_manifest(ruta)

            self.manifest["run_id"] = (
                "simulado_modificado_001"
            )

            ruta.write_text(
                json.dumps(self.manifest),
                encoding="utf-8",
            )

            _, segunda_huella = cargar_manifest(ruta)

            self.assertNotEqual(
                primera_huella,
                segunda_huella,
            )

    # ========================================================
    # 13. INTERFAZ CLI
    # ========================================================

    def test_57_cli_manifest_aceptado(self):

        proceso = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_VALIDADOR),
                "--manifest",
                str(
                    DIRECTORIO_MANIFESTS
                    / "manifest_bcrp_insercion.json"
                ),
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(
            proceso.returncode,
            0,
            msg=proceso.stderr,
        )

        resultado = json.loads(
            proceso.stdout
        )

        self.assertTrue(
            resultado[
                "validacion_estructural_aprobada"
            ]
        )

        self.assertFalse(
            resultado[
                "autorizacion_ejecucion_gold"
            ]
        )

    def test_58_cli_manifest_rechazado(self):

        proceso = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_VALIDADOR),
                "--manifest",
                str(
                    DIRECTORIO_MANIFESTS
                    / "manifest_no_aprobado.json"
                ),
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(
            proceso.returncode,
            1,
            msg=proceso.stderr,
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
                "autorizacion_ejecucion_gold"
            ]
        )


# ============================================================
# 14. EJECUCION DE PRUEBAS
# ============================================================

if __name__ == "__main__":

    unittest.main(verbosity=2)
