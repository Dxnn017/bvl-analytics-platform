#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics

Pruebas automatizadas del registro de dependencias
Silver -> Gold.

Contrato provisional: v0.1.

Estas pruebas:
- No requieren Spark.
- No modifican Silver.
- No modifican Gold historico.
- No realizan publicaciones.
"""

import copy
import json
import subprocess
import sys
import tempfile
import unittest

from pathlib import Path
from unittest.mock import patch


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

SCRIPT_REGISTRO = (
    DIRECTORIO_INCREMENTAL
    / "dependency_registry.py"
)

sys.path.insert(
    0,
    str(DIRECTORIO_INCREMENTAL),
)

import dependency_registry as registro

from manifest_validator import (
    CATALOGO_DATASETS,
    cargar_manifest,
)

from dependency_registry import (
    DependencyError,
    PRODUCTOS_GOLD,
    DEPENDENCIAS_SILVER_GOLD,
    DEPENDENCIAS_GOLD_GOLD,
    REGLAS_PROPAGACION,
    ORDEN_EJECUCION_GOLD,
    validar_registro,
    asegurar_registro_valido,
    obtener_producto,
    obtener_dependencia_silver,
    obtener_dependencias_directas,
    obtener_dependencias_transitivas,
    obtener_productos_afectados,
    cantidad_cambios,
    analizar_dependencias,
    analizar_archivo_manifest,
    obtener_registro_completo,
)


# ============================================================
# 2. CLASE DE PRUEBAS
# ============================================================

class TestDependencyRegistry(unittest.TestCase):

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

        self.dataset = self.manifest[
            "datasets"
        ][0]

    # ========================================================
    # 3. FUNCIONES AUXILIARES
    # ========================================================

    def verificar_resultado(
        self,
        resultado,
        directos,
        indirectos,
        potenciales,
    ):

        self.assertEqual(
            resultado["productos_directos"],
            directos,
        )

        self.assertEqual(
            resultado["productos_indirectos"],
            indirectos,
        )

        self.assertEqual(
            resultado["productos_potenciales"],
            potenciales,
        )

        self.assertEqual(
            resultado[
                "orden_ejecucion_propuesto"
            ],
            potenciales,
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

        self.assertEqual(
            resultado["errores"],
            [],
        )

        if potenciales:

            self.assertEqual(
                resultado["estado"],
                "DEPENDENCIAS_IDENTIFICADAS",
            )

        else:

            self.assertEqual(
                resultado["estado"],
                "SIN_PRODUCTOS_POTENCIALES",
            )

    def verificar_rechazo_archivo(
        self,
        nombre,
        mensaje_esperado,
    ):

        resultado = analizar_archivo_manifest(
            DIRECTORIO_MANIFESTS / nombre
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

        self.assertEqual(
            resultado["productos_directos"],
            [],
        )

        self.assertEqual(
            resultado["productos_indirectos"],
            [],
        )

        self.assertEqual(
            resultado["productos_potenciales"],
            [],
        )

        self.assertEqual(
            resultado[
                "orden_ejecucion_propuesto"
            ],
            [],
        )

        self.assertTrue(
            any(
                mensaje_esperado.lower()
                in error.lower()
                for error in resultado["errores"]
            )
        )

    def cargar_dataset_fixture(
        self,
        nombre,
    ):

        manifest, _ = cargar_manifest(
            DIRECTORIO_MANIFESTS / nombre
        )

        return copy.deepcopy(
            manifest["datasets"][0]
        )

    # ========================================================
    # 4. CONSISTENCIA DEL REGISTRO
    # ========================================================

    def test_01_registro_valido(self):

        self.assertEqual(
            validar_registro(),
            [],
        )

        asegurar_registro_valido()

    def test_02_cantidad_productos_gold(self):

        self.assertEqual(
            len(PRODUCTOS_GOLD),
            5,
        )

    def test_03_cantidad_datasets_silver(self):

        self.assertEqual(
            len(DEPENDENCIAS_SILVER_GOLD),
            11,
        )

    def test_04_catalogo_silver_consistente(self):

        self.assertEqual(
            set(CATALOGO_DATASETS),
            set(DEPENDENCIAS_SILVER_GOLD),
        )

    def test_05_datasets_consumidos(self):

        consumidos = [

            nombre

            for nombre, configuracion
            in CATALOGO_DATASETS.items()

            if configuracion["consumido_gold"]
        ]

        self.assertEqual(
            len(consumidos),
            10,
        )

        self.assertNotIn(
            "indices",
            consumidos,
        )

    def test_06_dependencias_gold_completas(self):

        self.assertEqual(
            set(DEPENDENCIAS_GOLD_GOLD),
            set(PRODUCTOS_GOLD),
        )

    def test_07_orden_sin_duplicados(self):

        self.assertEqual(
            len(ORDEN_EJECUCION_GOLD),
            len(set(ORDEN_EJECUCION_GOLD)),
        )

        self.assertEqual(
            set(ORDEN_EJECUCION_GOLD),
            set(PRODUCTOS_GOLD),
        )

    def test_08_orden_topologico_valido(self):

        posiciones = {

            producto: indice

            for indice, producto
            in enumerate(ORDEN_EJECUCION_GOLD)
        }

        for origen, destinos in (
            DEPENDENCIAS_GOLD_GOLD.items()
        ):

            for destino in destinos:

                self.assertLess(
                    posiciones[origen],
                    posiciones[destino],
                )

    def test_09_reglas_propagacion_completas(self):

        aristas = {

            (origen, destino)

            for origen, destinos
            in DEPENDENCIAS_GOLD_GOLD.items()

            for destino in destinos
        }

        self.assertEqual(
            aristas,
            set(REGLAS_PROPAGACION),
        )

        self.assertEqual(
            len(aristas),
            2,
        )

    def test_10_scripts_gold_existentes(self):

        for nombre, configuracion in (
            PRODUCTOS_GOLD.items()
        ):

            with self.subTest(
                producto=nombre
            ):

                self.assertTrue(
                    (
                        ROOT
                        / configuracion[
                            "constructor"
                        ]
                    ).is_file()
                )

                self.assertTrue(
                    (
                        ROOT
                        / configuracion[
                            "validador"
                        ]
                    ).is_file()
                )

    # ========================================================
    # 5. DEPENDENCIAS DIRECTAS
    # ========================================================

    def test_11_cotizaciones(self):

        self.assertEqual(
            obtener_dependencias_directas(
                "cotizaciones"
            ),
            ["mercado_valor_diario"],
        )

    def test_12_montos_sab(self):

        self.assertEqual(
            obtener_dependencias_directas(
                "montos_sab"
            ),
            ["actividad_sab_diaria"],
        )

    def test_13_tipo_cambio(self):

        self.assertEqual(
            obtener_dependencias_directas(
                "tipo_cambio"
            ),
            ["mercado_valor_diario"],
        )

    def test_14_embig(self):

        self.assertEqual(
            obtener_dependencias_directas(
                "embig"
            ),
            ["mercado_valor_diario"],
        )

    def test_15_principales_cuentas(self):

        self.assertEqual(
            obtener_dependencias_directas(
                "principales_cuentas"
            ),
            ["empresa_trimestre"],
        )

    def test_16_eventos(self):

        self.assertEqual(
            obtener_dependencias_directas(
                "eventos"
            ),
            ["hechos_empresa"],
        )

    def test_17_documentos(self):

        self.assertEqual(
            obtener_dependencias_directas(
                "documentos"
            ),
            ["hechos_empresa"],
        )

    def test_18_contenido_textual(self):

        self.assertEqual(
            obtener_dependencias_directas(
                "contenido_textual"
            ),
            ["hechos_empresa"],
        )

    def test_19_identidad_empresarial(self):

        self.assertEqual(
            obtener_dependencias_directas(
                "identidad_empresarial"
            ),
            ["hechos_empresa"],
        )

    def test_20_puente_ruc_nemonico(self):

        self.assertEqual(
            obtener_dependencias_directas(
                "puente_ruc_nemonico"
            ),
            ["mercado_valor_diario"],
        )

    def test_21_indices_no_consumidos(self):

        self.assertEqual(
            obtener_dependencias_directas(
                "indices"
            ),
            [],
        )

    # ========================================================
    # 6. DEPENDENCIAS TRANSITIVAS
    # ========================================================

    def test_22_propagacion_mercado_impacto(self):

        self.assertEqual(
            obtener_dependencias_transitivas(
                ["mercado_valor_diario"]
            ),
            [
                "mercado_valor_diario",
                "impacto_hechos_valor",
            ],
        )

    def test_23_propagacion_hechos_impacto(self):

        self.assertEqual(
            obtener_dependencias_transitivas(
                ["hechos_empresa"]
            ),
            [
                "hechos_empresa",
                "impacto_hechos_valor",
            ],
        )

    def test_24_sab_sin_propagacion(self):

        self.assertEqual(
            obtener_dependencias_transitivas(
                ["actividad_sab_diaria"]
            ),
            ["actividad_sab_diaria"],
        )

    def test_25_empresa_sin_propagacion(self):

        self.assertEqual(
            obtener_dependencias_transitivas(
                ["empresa_trimestre"]
            ),
            ["empresa_trimestre"],
        )

    def test_26_impacto_sin_propagacion(self):

        self.assertEqual(
            obtener_dependencias_transitivas(
                ["impacto_hechos_valor"]
            ),
            ["impacto_hechos_valor"],
        )

    def test_27_propagacion_sin_duplicados(self):

        self.assertEqual(
            obtener_dependencias_transitivas(
                [
                    "hechos_empresa",
                    "mercado_valor_diario",
                    "mercado_valor_diario",
                ]
            ),
            [
                "mercado_valor_diario",
                "hechos_empresa",
                "impacto_hechos_valor",
            ],
        )

    def test_28_lista_productos_vacia(self):

        self.assertEqual(
            obtener_dependencias_transitivas(
                []
            ),
            [],
        )

    # ========================================================
    # 7. COMBINACION DE DATASETS
    # ========================================================

    def test_29_cotizaciones_y_macro(self):

        resultado = obtener_productos_afectados(
            [
                "cotizaciones",
                "tipo_cambio",
                "embig",
            ]
        )

        self.assertEqual(
            resultado,
            [
                "mercado_valor_diario",
                "impacto_hechos_valor",
            ],
        )

    def test_30_eventos_y_documentos(self):

        resultado = obtener_productos_afectados(
            [
                "eventos",
                "documentos",
                "contenido_textual",
                "identidad_empresarial",
            ]
        )

        self.assertEqual(
            resultado,
            [
                "hechos_empresa",
                "impacto_hechos_valor",
            ],
        )

    def test_31_mercado_y_hechos(self):

        resultado = obtener_productos_afectados(
            [
                "cotizaciones",
                "eventos",
            ]
        )

        self.assertEqual(
            resultado,
            [
                "mercado_valor_diario",
                "hechos_empresa",
                "impacto_hechos_valor",
            ],
        )

    def test_32_todos_los_productos(self):

        resultado = obtener_productos_afectados(
            list(CATALOGO_DATASETS)
        )

        self.assertEqual(
            resultado,
            ORDEN_EJECUCION_GOLD,
        )

    def test_33_dataset_repetido(self):

        resultado = obtener_productos_afectados(
            [
                "tipo_cambio",
                "tipo_cambio",
            ]
        )

        self.assertEqual(
            resultado,
            [
                "mercado_valor_diario",
                "impacto_hechos_valor",
            ],
        )

    def test_34_solo_indices(self):

        self.assertEqual(
            obtener_productos_afectados(
                ["indices"]
            ),
            [],
        )

    def test_35_lista_datasets_vacia(self):

        self.assertEqual(
            obtener_productos_afectados(
                []
            ),
            [],
        )

    # ========================================================
    # 8. MANIFESTS ORIGINALES
    # ========================================================

    def test_36_manifest_insercion_bcrp(self):

        resultado = analizar_archivo_manifest(
            DIRECTORIO_MANIFESTS
            / "manifest_bcrp_insercion.json"
        )

        self.verificar_resultado(
            resultado,
            directos=[
                "mercado_valor_diario"
            ],
            indirectos=[
                "impacto_hechos_valor"
            ],
            potenciales=[
                "mercado_valor_diario",
                "impacto_hechos_valor",
            ],
        )

        self.assertEqual(
            resultado["datasets_analizados"]
            [0]["total_cambios_declarados"],
            1,
        )

    def test_37_manifest_correccion_bcrp(self):

        resultado = analizar_archivo_manifest(
            DIRECTORIO_MANIFESTS
            / "manifest_bcrp_correccion.json"
        )

        self.verificar_resultado(
            resultado,
            directos=[
                "mercado_valor_diario"
            ],
            indirectos=[
                "impacto_hechos_valor"
            ],
            potenciales=[
                "mercado_valor_diario",
                "impacto_hechos_valor",
            ],
        )

        self.assertEqual(
            resultado["datasets_analizados"]
            [0]["registros_corregidos"],
            1,
        )

    def test_38_manifest_correccion_sab(self):

        resultado = analizar_archivo_manifest(
            DIRECTORIO_MANIFESTS
            / "manifest_sab_correccion.json"
        )

        self.verificar_resultado(
            resultado,
            directos=[
                "actividad_sab_diaria"
            ],
            indirectos=[],
            potenciales=[
                "actividad_sab_diaria"
            ],
        )

        self.assertEqual(
            resultado["reglas_directas"]
            [0]["alcance"],
            "FECHAS_SAB_COMPLETAS",
        )

    def test_39_manifest_silver_no_aprobado(self):

        self.verificar_rechazo_archivo(
            "manifest_no_aprobado.json",
            "Silver no se encuentra publicado",
        )

    def test_40_manifest_dataset_desconocido(self):

        self.verificar_rechazo_archivo(
            "manifest_dataset_desconocido.json",
            "dataset desconocido",
        )

    def test_41_manifest_eliminacion_no_autorizada(self):

        self.verificar_rechazo_archivo(
            "manifest_eliminacion_no_autorizada.json",
            "eliminacion no autorizada",
        )

    # ========================================================
    # 9. ESCENARIOS ADICIONALES DE MANIFEST
    # ========================================================

    def test_42_manifest_sin_cambios(self):

        self.dataset["registros_nuevos"] = 0

        self.dataset["registros_despues"] = (
            self.dataset["registros_antes"]
        )

        self.dataset["claves_afectadas"] = []

        self.dataset["particiones_afectadas"] = []

        self.dataset["version_publicada"] = (
            self.dataset["version_anterior"]
        )

        resultado = analizar_dependencias(
            self.manifest
        )

        self.verificar_resultado(
            resultado,
            directos=[],
            indirectos=[],
            potenciales=[],
        )

        self.assertEqual(
            resultado[
                "datasets_sin_cambios_declarados"
            ],
            ["tipo_cambio"],
        )

    def test_43_manifest_dataset_no_consumido(self):

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

        resultado = analizar_dependencias(
            self.manifest
        )

        self.verificar_resultado(
            resultado,
            directos=[],
            indirectos=[],
            potenciales=[],
        )

        self.assertEqual(
            resultado["datasets_no_consumidos"],
            ["indices"],
        )

        self.assertEqual(
            resultado["datasets_analizados"]
            [0]["total_cambios_declarados"],
            1,
        )

    def test_44_manifest_multiple_bcrp_sab(self):

        dataset_sab = (
            self.cargar_dataset_fixture(
                "manifest_sab_correccion.json"
            )
        )

        self.manifest["datasets"].append(
            dataset_sab
        )

        resultado = analizar_dependencias(
            self.manifest
        )

        self.verificar_resultado(
            resultado,
            directos=[
                "mercado_valor_diario",
                "actividad_sab_diaria",
            ],
            indirectos=[
                "impacto_hechos_valor"
            ],
            potenciales=[
                "mercado_valor_diario",
                "actividad_sab_diaria",
                "impacto_hechos_valor",
            ],
        )

        self.assertEqual(
            len(
                resultado["datasets_analizados"]
            ),
            2,
        )

    def test_45_manifest_multiple_macro(self):

        dataset_embig = copy.deepcopy(
            self.dataset
        )

        dataset_embig["dataset"] = "embig"

        dataset_embig["ruta_silver"] = (
            "file:///ruta_de_prueba/"
            "macroeconomia/embig"
        )

        dataset_embig["registros_antes"] = 7481

        dataset_embig["registros_despues"] = 7482

        self.manifest["datasets"].append(
            dataset_embig
        )

        resultado = analizar_dependencias(
            self.manifest
        )

        self.verificar_resultado(
            resultado,
            directos=[
                "mercado_valor_diario"
            ],
            indirectos=[
                "impacto_hechos_valor"
            ],
            potenciales=[
                "mercado_valor_diario",
                "impacto_hechos_valor",
            ],
        )

        self.assertEqual(
            len(resultado["reglas_directas"]),
            2,
        )

        self.assertEqual(
            len(resultado["reglas_indirectas"]),
            1,
        )

    def test_46_manifest_mixto_sin_cambios(self):

        dataset_sab = (
            self.cargar_dataset_fixture(
                "manifest_sab_correccion.json"
            )
        )

        dataset_sab["registros_corregidos"] = 0

        dataset_sab["claves_afectadas"] = []

        dataset_sab["particiones_afectadas"] = []

        dataset_sab["version_publicada"] = (
            dataset_sab["version_anterior"]
        )

        self.manifest["datasets"].append(
            dataset_sab
        )

        resultado = analizar_dependencias(
            self.manifest
        )

        self.verificar_resultado(
            resultado,
            directos=[
                "mercado_valor_diario"
            ],
            indirectos=[
                "impacto_hechos_valor"
            ],
            potenciales=[
                "mercado_valor_diario",
                "impacto_hechos_valor",
            ],
        )

        self.assertEqual(
            resultado[
                "datasets_sin_cambios_declarados"
            ],
            ["montos_sab"],
        )

    def test_47_manifest_detalle_externo(self):

        self.dataset["claves_afectadas"] = None

        self.dataset["ruta_detalle_cambios"] = (
            "file:///ruta_de_prueba/cambios.json"
        )

        resultado = analizar_dependencias(
            self.manifest
        )

        self.assertTrue(
            resultado["datasets_analizados"]
            [0]["detalle_externo"]
        )

        self.assertFalse(
            resultado[
                "autorizacion_ejecucion_gold"
            ]
        )

        self.assertTrue(
            any(
                "detalle externo pendiente"
                in mensaje
                for mensaje
                in resultado["advertencias"]
            )
        )

    def test_48_manifest_entorno_incompatible(self):

        with self.assertRaisesRegex(
            DependencyError,
            "Entorno incompatible",
        ):

            analizar_dependencias(
                self.manifest,
                entorno_esperado="PRODUCCION",
            )

    # ========================================================
    # 10. REGLAS DE ALCANCE INCREMENTAL
    # ========================================================

    def test_49_alcance_sab(self):

        configuracion = (
            obtener_dependencia_silver(
                "montos_sab"
            )
        )

        self.assertEqual(
            configuracion["alcance"],
            "FECHAS_SAB_COMPLETAS",
        )

    def test_50_alcance_financiero(self):

        configuracion = (
            obtener_dependencia_silver(
                "principales_cuentas"
            )
        )

        self.assertEqual(
            configuracion["alcance"],
            "GRUPOS_FINANCIEROS_COMPLETOS",
        )

    def test_51_alcance_puente(self):

        configuracion = (
            obtener_dependencia_silver(
                "puente_ruc_nemonico"
            )
        )

        self.assertEqual(
            configuracion["alcance"],
            "HISTORICO_NEMONICOS_AFECTADOS",
        )

    def test_52_alcance_documentos(self):

        configuracion = (
            obtener_dependencia_silver(
                "documentos"
            )
        )

        self.assertEqual(
            configuracion["alcance"],
            "EXPEDIENTES_DESDE_DOCUMENTOS",
        )

    def test_53_ventana_impacto(self):

        regla = REGLAS_PROPAGACION[
            (
                "mercado_valor_diario",
                "impacto_hechos_valor",
            )
        ]

        self.assertEqual(
            regla["ventana_dias"],
            7,
        )

        self.assertTrue(
            regla["excluir_dia_evento"]
        )

    # ========================================================
    # 11. PROTECCION DEL REGISTRO
    # ========================================================

    def test_54_producto_desconocido(self):

        with self.assertRaisesRegex(
            DependencyError,
            "Producto Gold desconocido",
        ):

            obtener_producto(
                "producto_inexistente"
            )

    def test_55_dataset_desconocido(self):

        with self.assertRaisesRegex(
            DependencyError,
            "Dataset Silver desconocido",
        ):

            obtener_dependencia_silver(
                "dataset_inexistente"
            )

    def test_56_producto_devuelve_copia(self):

        producto = obtener_producto(
            "mercado_valor_diario"
        )

        producto["clave"].append(
            "campo_inexistente"
        )

        original = obtener_producto(
            "mercado_valor_diario"
        )

        self.assertEqual(
            original["clave"],
            [
                "nemonico",
                "fecha",
            ],
        )

    def test_57_dependencia_devuelve_copia(self):

        dependencia = (
            obtener_dependencia_silver(
                "tipo_cambio"
            )
        )

        dependencia["productos"].append(
            "producto_inexistente"
        )

        self.assertEqual(
            obtener_dependencias_directas(
                "tipo_cambio"
            ),
            ["mercado_valor_diario"],
        )

    def test_58_detectar_dependencia_circular(self):

        dependencias = copy.deepcopy(
            DEPENDENCIAS_GOLD_GOLD
        )

        dependencias[
            "impacto_hechos_valor"
        ].append(
            "mercado_valor_diario"
        )

        with patch.object(
            registro,
            "DEPENDENCIAS_GOLD_GOLD",
            dependencias,
        ):

            errores = validar_registro()

            self.assertTrue(
                any(
                    "Orden de ejecucion incompatible"
                    in error
                    for error in errores
                )
            )

            with self.assertRaises(
                DependencyError
            ):

                asegurar_registro_valido()

    def test_59_detectar_dependencia_desconocida(self):

        dependencias = copy.deepcopy(
            DEPENDENCIAS_SILVER_GOLD
        )

        dependencias[
            "tipo_cambio"
        ]["productos"] = [
            "producto_inexistente"
        ]

        with patch.object(
            registro,
            "DEPENDENCIAS_SILVER_GOLD",
            dependencias,
        ):

            errores = validar_registro()

            self.assertTrue(
                any(
                    "producto Gold desconocido"
                    in error
                    for error in errores
                )
            )

    def test_60_detectar_orden_duplicado(self):

        orden = list(
            ORDEN_EJECUCION_GOLD
        )

        orden.append(
            "mercado_valor_diario"
        )

        with patch.object(
            registro,
            "ORDEN_EJECUCION_GOLD",
            orden,
        ):

            errores = validar_registro()

            self.assertTrue(
                any(
                    "productos repetidos"
                    in error
                    for error in errores
                )
            )

    # ========================================================
    # 12. CONTEOS DE CAMBIOS
    # ========================================================

    def test_61_total_cambios(self):

        dataset = {
            "registros_nuevos": 5,
            "registros_corregidos": 3,
            "registros_eliminados": 2,
        }

        self.assertEqual(
            cantidad_cambios(dataset),
            10,
        )

    def test_62_conteo_negativo_rechazado(self):

        self.dataset["registros_nuevos"] = -1

        with self.assertRaisesRegex(
            DependencyError,
            "Conteo invalido",
        ):

            cantidad_cambios(
                self.dataset
            )

    def test_63_conteo_booleano_rechazado(self):

        self.dataset[
            "registros_corregidos"
        ] = True

        with self.assertRaisesRegex(
            DependencyError,
            "Conteo invalido",
        ):

            cantidad_cambios(
                self.dataset
            )

    # ========================================================
    # 13. LECTURA DE ARCHIVOS
    # ========================================================

    def test_64_archivo_manifest_inexistente(self):

        with tempfile.TemporaryDirectory() as tmp:

            resultado = (
                analizar_archivo_manifest(
                    Path(tmp) / "no_existe.json"
                )
            )

            self.assertEqual(
                resultado["estado"],
                "GOLD_RECHAZADO",
            )

            self.assertTrue(
                resultado["errores"]
            )

    def test_65_archivo_manifest_malformado(self):

        with tempfile.TemporaryDirectory() as tmp:

            ruta = Path(tmp) / "invalido.json"

            ruta.write_text(
                '{"datasets": ',
                encoding="utf-8",
            )

            resultado = (
                analizar_archivo_manifest(
                    ruta
                )
            )

            self.assertEqual(
                resultado["estado"],
                "GOLD_RECHAZADO",
            )

            self.assertTrue(
                any(
                    "JSON invalido" in error
                    for error
                    in resultado["errores"]
                )
            )

    def test_66_huella_manifest_conservada(self):

        ruta = (
            DIRECTORIO_MANIFESTS
            / "manifest_bcrp_insercion.json"
        )

        _, huella = cargar_manifest(
            ruta
        )

        resultado = analizar_archivo_manifest(
            ruta
        )

        self.assertEqual(
            resultado["huella_sha256"],
            huella,
        )

    # ========================================================
    # 14. INTERFAZ CLI
    # ========================================================

    def test_67_cli_mostrar_registro(self):

        proceso = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_REGISTRO),
                "--mostrar-registro",
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

        self.assertEqual(
            resultado["estado"],
            "REGISTRO_VALIDO",
        )

        self.assertEqual(
            resultado["cantidad_productos_gold"],
            5,
        )

        self.assertEqual(
            resultado["cantidad_datasets"],
            11,
        )

    def test_68_cli_manifest_aceptado(self):

        proceso = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_REGISTRO),
                "--manifest",
                str(
                    DIRECTORIO_MANIFESTS
                    / "manifest_sab_correccion.json"
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

        self.assertEqual(
            resultado["productos_potenciales"],
            ["actividad_sab_diaria"],
        )

        self.assertFalse(
            resultado[
                "autorizacion_ejecucion_gold"
            ]
        )

    def test_69_cli_manifest_rechazado(self):

        proceso = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_REGISTRO),
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

        self.assertEqual(
            resultado["productos_potenciales"],
            [],
        )

    def test_70_registro_completo_no_autoriza_gold(self):

        resultado = obtener_registro_completo()

        self.assertFalse(
            resultado[
                "autorizacion_ejecucion_gold"
            ]
        )

        self.assertEqual(
            resultado["estado"],
            "REGISTRO_VALIDO",
        )


# ============================================================
# 15. EJECUCION DE PRUEBAS
# ============================================================

if __name__ == "__main__":

    unittest.main(verbosity=2)

