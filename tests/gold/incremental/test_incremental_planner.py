#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BVL Analytics Platform.

Pruebas automatizadas del planificador incremental Gold.

Contrato provisional Silver-Gold v0.1.

No requiere Spark.
No modifica Silver ni Gold.
No ejecuta transformaciones.
No publica resultados.
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

SCRIPT_PLANIFICADOR = (
    DIRECTORIO_INCREMENTAL / "incremental_planner.py"
)

sys.path.insert(
    0,
    str(DIRECTORIO_INCREMENTAL),
)

import incremental_planner as planner

from manifest_validator import cargar_manifest

from dependency_registry import (
    DependencyError,
    PRODUCTOS_GOLD,
    ORDEN_EJECUCION_GOLD,
)

from incremental_planner import (
    PlannerError,
    ESTRATEGIAS_GOLD,
    REQUISITOS_OPERATIVOS,
    validar_configuracion_planificador,
    asegurar_configuracion_valida,
    verificar_json_finito,
    preparar_cambios_dataset,
    obtener_predecesores_gold,
    preparar_fuentes_silver,
    preparar_fuentes_gold,
    validar_orden_tareas,
    construir_plan,
    planificar_archivo_manifest,
    obtener_configuracion,
    obtener_resumen_plan,
)


# ============================================================
# 2. CLASE DE PRUEBAS
# ============================================================

class TestIncrementalPlanner(unittest.TestCase):

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

    def cargar_fixture(self, nombre):

        manifest, _ = cargar_manifest(
            DIRECTORIO_MANIFESTS / nombre
        )

        return manifest

    def plan_fixture(self, nombre):

        return planificar_archivo_manifest(
            DIRECTORIO_MANIFESTS / nombre
        )

    def obtener_tarea(self, plan, producto):

        tareas = [
            tarea
            for tarea in plan["tareas"]
            if tarea["producto_gold"] == producto
        ]

        self.assertEqual(
            len(tareas),
            1,
            msg=f"Tarea esperada: {producto}",
        )

        return tareas[0]

    def verificar_bloqueos(self, plan):

        self.assertFalse(
            plan["validacion_operativa_aprobada"]
        )

        self.assertFalse(
            plan["alcance_efectivo_resuelto"]
        )

        self.assertFalse(
            plan["autorizacion_ejecucion_gold"]
        )

        self.assertFalse(
            plan["autorizacion_publicacion_gold"]
        )

        for tarea in plan.get("tareas", []):

            self.assertFalse(
                tarea["ejecucion_autorizada"]
            )

            self.assertFalse(
                tarea["alcance_efectivo_resuelto"]
            )

            self.assertIsNone(
                tarea["claves_gold_afectadas"]
            )

            self.assertIsNone(
                tarea["particiones_gold_afectadas"]
            )

            self.assertIsNone(
                tarea["cantidad_registros_gold_afectados"]
            )

    def verificar_plan(
        self,
        plan,
        productos_esperados,
    ):

        estado_esperado = (
            "GOLD_PLANIFICADO"
            if productos_esperados
            else "SIN_PRODUCTOS_POTENCIALES"
        )

        self.assertEqual(
            plan["estado"],
            estado_esperado,
        )

        self.assertEqual(
            plan["tipo_plan"],
            "PRELIMINAR",
        )

        self.assertTrue(
            plan["validacion_estructural_aprobada"]
        )

        self.assertEqual(
            plan["errores"],
            [],
        )

        self.assertEqual(
            plan["orden_ejecucion_propuesto"],
            productos_esperados,
        )

        self.assertEqual(
            plan["productos_potenciales"],
            productos_esperados,
        )

        self.assertEqual(
            [
                tarea["producto_gold"]
                for tarea in plan["tareas"]
            ],
            productos_esperados,
        )

        self.assertEqual(
            plan["resumen"]["cantidad_tareas"],
            len(productos_esperados),
        )

        self.assertEqual(
            plan["resumen"]["cantidad_tareas_autorizadas"],
            0,
        )

        self.verificar_bloqueos(plan)

    def verificar_rechazo(
        self,
        plan,
        mensaje_esperado=None,
    ):

        self.assertEqual(
            plan["estado"],
            "GOLD_RECHAZADO",
        )

        self.assertFalse(
            plan["validacion_estructural_aprobada"]
        )

        self.assertEqual(
            plan["productos_potenciales"],
            [],
        )

        self.assertEqual(
            plan["orden_ejecucion_propuesto"],
            [],
        )

        self.assertEqual(
            plan["tareas"],
            [],
        )

        self.assertTrue(
            plan["errores"]
        )

        self.verificar_bloqueos(plan)

        if mensaje_esperado:

            self.assertTrue(
                any(
                    mensaje_esperado.lower()
                    in error.lower()
                    for error in plan["errores"]
                ),
                msg=plan["errores"],
            )

    def dejar_dataset_sin_cambios(self, dataset):

        dataset["registros_nuevos"] = 0

        dataset["registros_corregidos"] = 0

        dataset["registros_eliminados"] = 0

        dataset["registros_despues"] = (
            dataset["registros_antes"]
        )

        dataset["claves_afectadas"] = []

        dataset["particiones_afectadas"] = []

        dataset["ruta_detalle_cambios"] = None

        dataset["version_publicada"] = (
            dataset["version_anterior"]
        )

        dataset["autorizacion_eliminaciones"] = False

    def crear_dataset_simulado(
        self,
        nombre,
        ruta,
        clave,
        clave_nueva,
        particiones,
        registros_antes=10,
    ):

        dataset = copy.deepcopy(
            self.dataset
        )

        dataset.update(
            {
                "dataset": nombre,

                "ruta_silver": (
                    "file:///ruta_de_prueba/" + ruta
                ),

                "clave": clave,

                "particiones_afectadas": particiones,

                "claves_afectadas": [
                    {
                        "operacion": "INSERT",
                        "clave_anterior": None,
                        "clave_nueva": clave_nueva,
                    }
                ],

                "ruta_detalle_cambios": None,

                "registros_nuevos": 1,

                "registros_corregidos": 0,

                "registros_eliminados": 0,

                "registros_antes": registros_antes,

                "registros_despues": (
                    registros_antes + 1
                ),

                "version_anterior": "simulada_v1",

                "version_publicada": "simulada_v2",

                "autorizacion_eliminaciones": False,
            }
        )

        return dataset

    # ========================================================
    # 4. CONFIGURACION DEL PLANIFICADOR
    # ========================================================

    def test_01_configuracion_valida(self):

        self.assertEqual(
            validar_configuracion_planificador(),
            [],
        )

        asegurar_configuracion_valida()

    def test_02_cantidad_estrategias(self):

        self.assertEqual(
            len(ESTRATEGIAS_GOLD),
            5,
        )

    def test_03_productos_consistentes(self):

        self.assertEqual(
            set(ESTRATEGIAS_GOLD),
            set(PRODUCTOS_GOLD),
        )

    def test_04_cantidad_requisitos_operativos(self):

        self.assertEqual(
            len(REQUISITOS_OPERATIVOS),
            12,
        )

    def test_05_requisitos_sin_duplicados(self):

        codigos = [
            requisito["codigo"]
            for requisito in REQUISITOS_OPERATIVOS
        ]

        self.assertEqual(
            len(codigos),
            len(set(codigos)),
        )

    def test_06_configuracion_no_autoriza(self):

        configuracion = obtener_configuracion()

        self.assertEqual(
            configuracion["estado"],
            "CONFIGURACION_PLANIFICADOR_VALIDA",
        )

        self.assertFalse(
            configuracion["autorizacion_ejecucion_gold"]
        )

        self.assertFalse(
            configuracion["autorizacion_publicacion_gold"]
        )

    def test_07_configuracion_devuelve_copia(self):

        configuracion = obtener_configuracion()

        configuracion["estrategias_gold"][
            "mercado_valor_diario"
        ]["unidad_recalculo"].append("invalido")

        original = obtener_configuracion()

        self.assertEqual(
            original["estrategias_gold"][
                "mercado_valor_diario"
            ]["unidad_recalculo"],
            ["nemonico", "fecha"],
        )

    # ========================================================
    # 5. ESTRATEGIAS GOLD
    # ========================================================

    def test_08_estrategia_mercado(self):

        estrategia = ESTRATEGIAS_GOLD[
            "mercado_valor_diario"
        ]

        self.assertEqual(
            estrategia["estrategia"],
            "RECALCULO_MERCADO_POR_CLAVES",
        )

        self.assertEqual(
            estrategia["unidad_recalculo"],
            ["nemonico", "fecha"],
        )

    def test_09_estrategia_sab(self):

        estrategia = ESTRATEGIAS_GOLD[
            "actividad_sab_diaria"
        ]

        self.assertEqual(
            estrategia["estrategia"],
            "RECALCULO_SAB_POR_FECHAS_COMPLETAS",
        )

        self.assertEqual(
            estrategia["unidad_recalculo"],
            ["fecha"],
        )

    def test_10_estrategia_financiera(self):

        estrategia = ESTRATEGIAS_GOLD[
            "empresa_trimestre"
        ]

        self.assertEqual(
            estrategia["unidad_recalculo"],
            ["rpj", "ejercicio", "trimestre_num"],
        )

    def test_11_estrategia_hechos(self):

        estrategia = ESTRATEGIAS_GOLD[
            "hechos_empresa"
        ]

        self.assertEqual(
            estrategia["unidad_recalculo"],
            ["numero_expediente"],
        )

    def test_12_estrategia_impacto(self):

        estrategia = ESTRATEGIAS_GOLD[
            "impacto_hechos_valor"
        ]

        self.assertEqual(
            estrategia["unidad_recalculo"],
            ["numero_expediente", "nemonico"],
        )

        self.assertEqual(
            estrategia["ventana_dias"],
            7,
        )

        self.assertTrue(
            estrategia["excluir_dia_evento"]
        )

    # ========================================================
    # 6. MANIFESTS ORIGINALES
    # ========================================================

    def test_13_insercion_bcrp(self):

        plan = self.plan_fixture(
            "manifest_bcrp_insercion.json"
        )

        self.verificar_plan(
            plan,
            [
                "mercado_valor_diario",
                "impacto_hechos_valor",
            ],
        )

        self.assertEqual(
            plan["resumen"][
                "registros_silver_nuevos_declarados"
            ],
            1,
        )

    def test_14_correccion_bcrp(self):

        plan = self.plan_fixture(
            "manifest_bcrp_correccion.json"
        )

        self.verificar_plan(
            plan,
            [
                "mercado_valor_diario",
                "impacto_hechos_valor",
            ],
        )

        self.assertEqual(
            plan["resumen"][
                "registros_silver_corregidos_declarados"
            ],
            1,
        )

    def test_15_correccion_sab(self):

        plan = self.plan_fixture(
            "manifest_sab_correccion.json"
        )

        self.verificar_plan(
            plan,
            ["actividad_sab_diaria"],
        )

    def test_16_silver_no_aprobado(self):

        plan = self.plan_fixture(
            "manifest_no_aprobado.json"
        )

        self.verificar_rechazo(
            plan,
            "Silver no se encuentra publicado",
        )

    def test_17_dataset_desconocido(self):

        plan = self.plan_fixture(
            "manifest_dataset_desconocido.json"
        )

        self.verificar_rechazo(
            plan,
            "dataset desconocido",
        )

    def test_18_eliminacion_no_autorizada(self):

        plan = self.plan_fixture(
            "manifest_eliminacion_no_autorizada.json"
        )

        self.verificar_rechazo(
            plan,
            "eliminacion no autorizada",
        )

    # ========================================================
    # 7. CONSERVACION DE CAMBIOS DECLARADOS
    # ========================================================

    def test_19_conservar_clave_insercion(self):

        cambio = preparar_cambios_dataset(
            self.dataset
        )

        self.assertEqual(
            cambio["claves_anteriores_declaradas"],
            [],
        )

        self.assertEqual(
            cambio["claves_nuevas_declaradas"],
            [{"fecha": "2026-09-18"}],
        )

    def test_20_conservar_claves_correccion(self):

        manifest = self.cargar_fixture(
            "manifest_bcrp_correccion.json"
        )

        cambio = preparar_cambios_dataset(
            manifest["datasets"][0]
        )

        self.assertEqual(
            cambio["claves_anteriores_declaradas"],
            [{"fecha": "2025-12-30"}],
        )

        self.assertEqual(
            cambio["claves_nuevas_declaradas"],
            [{"fecha": "2025-12-30"}],
        )

    def test_21_conservar_eliminacion_autorizada(self):

        manifest = self.cargar_fixture(
            "manifest_eliminacion_no_autorizada.json"
        )

        manifest["datasets"][0][
            "autorizacion_eliminaciones"
        ] = True

        plan = construir_plan(manifest)

        self.verificar_plan(
            plan,
            [
                "mercado_valor_diario",
                "impacto_hechos_valor",
            ],
        )

        cambio = plan["datasets_analizados"][0]

        self.assertEqual(
            cambio["claves_anteriores_declaradas"],
            [{"fecha": "2025-12-30"}],
        )

        self.assertEqual(
            cambio["claves_nuevas_declaradas"],
            [],
        )

        self.assertTrue(
            cambio["autorizacion_eliminaciones_declarada"]
        )

        self.assertEqual(
            plan["resumen"][
                "registros_silver_eliminados_declarados"
            ],
            1,
        )

    def test_22_conservar_versiones(self):

        cambio = preparar_cambios_dataset(
            self.dataset
        )

        self.assertEqual(
            cambio["version_anterior"],
            self.dataset["version_anterior"],
        )

        self.assertEqual(
            cambio["version_publicada"],
            self.dataset["version_publicada"],
        )

        self.assertEqual(
            cambio["version_esquema"],
            self.dataset["version_esquema"],
        )

    def test_23_conservar_particiones_silver(self):

        cambio = preparar_cambios_dataset(
            self.dataset
        )

        self.assertEqual(
            cambio["particiones_silver_declaradas"],
            [{"anio": 2026, "mes": 9}],
        )

    def test_24_no_modificar_manifest_original(self):

        original = copy.deepcopy(
            self.manifest
        )

        construir_plan(self.manifest)

        self.assertEqual(
            self.manifest,
            original,
        )

    def test_25_operaciones_independientes(self):

        cambio = preparar_cambios_dataset(
            self.dataset
        )

        cambio["operaciones_declaradas"][0][
            "clave_nueva"
        ]["fecha"] = "2000-01-01"

        self.assertEqual(
            self.dataset["claves_afectadas"][0][
                "clave_nueva"
            ]["fecha"],
            "2026-09-18",
        )

    # ========================================================
    # 8. CONSTRUCCION Y ORDEN DE TAREAS
    # ========================================================

    def test_26_identificador_tarea(self):

        plan = construir_plan(self.manifest)

        tarea = self.obtener_tarea(
            plan,
            "mercado_valor_diario",
        )

        self.assertEqual(
            tarea["id_tarea"],
            self.manifest["run_id"]
            + ":mercado_valor_diario",
        )

    def test_27_activacion_directa(self):

        plan = construir_plan(self.manifest)

        tarea = self.obtener_tarea(
            plan,
            "mercado_valor_diario",
        )

        self.assertEqual(
            tarea["tipos_activacion"],
            ["DIRECTA"],
        )

        self.assertEqual(
            tarea["datasets_disparadores"],
            ["tipo_cambio"],
        )

    def test_28_activacion_indirecta(self):

        plan = construir_plan(self.manifest)

        tarea = self.obtener_tarea(
            plan,
            "impacto_hechos_valor",
        )

        self.assertEqual(
            tarea["tipos_activacion"],
            ["INDIRECTA"],
        )

        self.assertEqual(
            tarea["datasets_disparadores"],
            [],
        )

        self.assertTrue(
            tarea[
                "requiere_cambios_efectivos_predecesores"
            ]
        )

    def test_29_predecesores_impacto(self):

        self.assertEqual(
            obtener_predecesores_gold(
                "impacto_hechos_valor"
            ),
            [
                "mercado_valor_diario",
                "hechos_empresa",
            ],
        )

    def test_30_mercado_sin_predecesores(self):

        self.assertEqual(
            obtener_predecesores_gold(
                "mercado_valor_diario"
            ),
            [],
        )

    def test_31_orden_numerico_tareas(self):

        plan = construir_plan(self.manifest)

        self.assertEqual(
            [
                tarea["orden"]
                for tarea in plan["tareas"]
            ],
            [1, 2],
        )

    def test_32_validar_orden_correcto(self):

        plan = construir_plan(self.manifest)

        self.assertEqual(
            validar_orden_tareas(
                plan["tareas"]
            ),
            [],
        )

    def test_33_detectar_orden_invertido(self):

        plan = construir_plan(self.manifest)

        tareas = list(
            reversed(plan["tareas"])
        )

        self.assertTrue(
            validar_orden_tareas(tareas)
        )

    def test_34_detectar_tarea_duplicada(self):

        plan = construir_plan(self.manifest)

        tareas = copy.deepcopy(
            plan["tareas"]
        )

        tareas.append(
            copy.deepcopy(tareas[0])
        )

        self.assertTrue(
            validar_orden_tareas(tareas)
        )

    # ========================================================
    # 9. FUENTES Y VERSIONES REQUERIDAS
    # ========================================================

    def test_35_fuentes_silver_mercado(self):

        fuentes = preparar_fuentes_silver(
            "mercado_valor_diario",
            {
                "tipo_cambio": self.dataset,
            },
        )

        nombres = {
            fuente["dataset"]
            for fuente in fuentes
        }

        self.assertEqual(
            nombres,
            {
                "cotizaciones",
                "tipo_cambio",
                "embig",
                "puente_ruc_nemonico",
            },
        )

    def test_36_fuente_presente_manifest(self):

        fuentes = preparar_fuentes_silver(
            "mercado_valor_diario",
            {
                "tipo_cambio": self.dataset,
            },
        )

        fuente = next(
            fuente
            for fuente in fuentes
            if fuente["dataset"] == "tipo_cambio"
        )

        self.assertTrue(
            fuente["presente_en_manifest"]
        )

        self.assertEqual(
            fuente["version_publicada"],
            self.dataset["version_publicada"],
        )

        self.assertFalse(
            fuente["version_verificada"]
        )

    def test_37_fuentes_no_presentes_manifest(self):

        fuentes = preparar_fuentes_silver(
            "mercado_valor_diario",
            {
                "tipo_cambio": self.dataset,
            },
        )

        ausentes = [
            fuente
            for fuente in fuentes
            if not fuente["presente_en_manifest"]
        ]

        self.assertEqual(
            len(ausentes),
            3,
        )

        for fuente in ausentes:

            self.assertIsNone(
                fuente["version_publicada"]
            )

            self.assertEqual(
                fuente["resolucion"],
                "DESDE_REFERENCIAS_DE_VERSIONES_APROBADAS",
            )

    def test_38_fuente_gold_planificada(self):

        fuentes = preparar_fuentes_gold(
            "impacto_hechos_valor",
            [
                "mercado_valor_diario",
                "impacto_hechos_valor",
            ],
        )

        mercado = next(
            fuente
            for fuente in fuentes
            if fuente["producto"]
            == "mercado_valor_diario"
        )

        self.assertTrue(
            mercado["tiene_tarea_planificada"]
        )

        self.assertEqual(
            mercado["resolucion"],
            "VERSION_CANDIDATA_SI_EXISTE_CAMBIO_EFECTIVO",
        )

    def test_39_fuente_gold_no_planificada(self):

        fuentes = preparar_fuentes_gold(
            "impacto_hechos_valor",
            [
                "mercado_valor_diario",
                "impacto_hechos_valor",
            ],
        )

        hechos = next(
            fuente
            for fuente in fuentes
            if fuente["producto"] == "hechos_empresa"
        )

        self.assertFalse(
            hechos["tiene_tarea_planificada"]
        )

        self.assertEqual(
            hechos["resolucion"],
            "VERSION_GOLD_BASE_APROBADA",
        )

    def test_40_fuentes_no_verificadas(self):

        plan = construir_plan(self.manifest)

        for tarea in plan["tareas"]:

            fuentes = (
                tarea["fuentes_silver_requeridas"]
                + tarea["fuentes_gold_requeridas"]
            )

            for fuente in fuentes:

                self.assertFalse(
                    fuente["version_verificada"]
                )

    # ========================================================
    # 10. MANIFESTS CON MULTIPLES DATASETS
    # ========================================================

    def test_41_manifest_bcrp_sab(self):

        sab = self.cargar_fixture(
            "manifest_sab_correccion.json"
        )

        self.manifest["datasets"].append(
            sab["datasets"][0]
        )

        plan = construir_plan(self.manifest)

        self.verificar_plan(
            plan,
            [
                "mercado_valor_diario",
                "actividad_sab_diaria",
                "impacto_hechos_valor",
            ],
        )

        self.assertEqual(
            plan["resumen"][
                "total_cambios_silver_declarados"
            ],
            2,
        )

    def test_42_manifest_tipo_cambio_embig(self):

        embig = self.crear_dataset_simulado(
            nombre="embig",
            ruta="macroeconomia/embig",
            clave=["fecha"],
            clave_nueva={
                "fecha": "2026-09-18"
            },
            particiones=[
                {
                    "anio": 2026,
                    "mes": 9,
                }
            ],
            registros_antes=7481,
        )

        self.manifest["datasets"].append(
            embig
        )

        plan = construir_plan(self.manifest)

        self.verificar_plan(
            plan,
            [
                "mercado_valor_diario",
                "impacto_hechos_valor",
            ],
        )

        mercado = self.obtener_tarea(
            plan,
            "mercado_valor_diario",
        )

        self.assertEqual(
            set(mercado["datasets_disparadores"]),
            {
                "tipo_cambio",
                "embig",
            },
        )

        self.assertEqual(
            len(mercado["cambios_directos_declarados"]),
            2,
        )

    def test_43_manifest_con_eventos(self):

        eventos = self.crear_dataset_simulado(
            nombre="eventos",
            ruta="hechos_importancia/eventos",
            clave=["numero_expediente"],
            clave_nueva={
                "numero_expediente": "EXP_SIMULADO_001"
            },
            particiones=[
                {
                    "anio_presentacion": 2025,
                }
            ],
            registros_antes=346,
        )

        self.manifest["datasets"] = [
            eventos
        ]

        plan = construir_plan(self.manifest)

        self.verificar_plan(
            plan,
            [
                "hechos_empresa",
                "impacto_hechos_valor",
            ],
        )

    def test_44_manifest_mercado_y_hechos(self):

        eventos = self.crear_dataset_simulado(
            nombre="eventos",
            ruta="hechos_importancia/eventos",
            clave=["numero_expediente"],
            clave_nueva={
                "numero_expediente": "EXP_SIMULADO_002"
            },
            particiones=[
                {
                    "anio_presentacion": 2025,
                }
            ],
            registros_antes=346,
        )

        self.manifest["datasets"].append(
            eventos
        )

        plan = construir_plan(self.manifest)

        self.verificar_plan(
            plan,
            [
                "mercado_valor_diario",
                "hechos_empresa",
                "impacto_hechos_valor",
            ],
        )

        impacto = self.obtener_tarea(
            plan,
            "impacto_hechos_valor",
        )

        self.assertEqual(
            impacto["predecesores_planificados"],
            [
                "mercado_valor_diario",
                "hechos_empresa",
            ],
        )

        self.assertEqual(
            len(
                impacto["reglas_propagacion_indirecta"]
            ),
            2,
        )

    def test_45_manifest_financiero(self):

        financiero = self.crear_dataset_simulado(
            nombre="principales_cuentas",
            ruta=(
                "finanzas_empresariales/"
                "principales_cuentas"
            ),
            clave=["id_registro"],
            clave_nueva={
                "id_registro": "ID_SIMULADO_001"
            },
            particiones=[
                {
                    "ejercicio": 2025,
                    "trimestre_num": 4,
                }
            ],
            registros_antes=5249,
        )

        self.manifest["datasets"] = [
            financiero
        ]

        plan = construir_plan(self.manifest)

        self.verificar_plan(
            plan,
            ["empresa_trimestre"],
        )

        tarea = plan["tareas"][0]

        self.assertEqual(
            tarea["unidad_recalculo"],
            [
                "rpj",
                "ejercicio",
                "trimestre_num",
            ],
        )

    def test_46_manifest_cinco_productos(self):

        sab = self.cargar_fixture(
            "manifest_sab_correccion.json"
        )["datasets"][0]

        financiero = self.crear_dataset_simulado(
            nombre="principales_cuentas",
            ruta=(
                "finanzas_empresariales/"
                "principales_cuentas"
            ),
            clave=["id_registro"],
            clave_nueva={
                "id_registro": "ID_SIMULADO_002"
            },
            particiones=[
                {
                    "ejercicio": 2025,
                    "trimestre_num": 4,
                }
            ],
            registros_antes=5249,
        )

        eventos = self.crear_dataset_simulado(
            nombre="eventos",
            ruta="hechos_importancia/eventos",
            clave=["numero_expediente"],
            clave_nueva={
                "numero_expediente": "EXP_SIMULADO_003"
            },
            particiones=[
                {
                    "anio_presentacion": 2025,
                }
            ],
            registros_antes=346,
        )

        self.manifest["datasets"].extend(
            [
                sab,
                financiero,
                eventos,
            ]
        )

        plan = construir_plan(self.manifest)

        self.verificar_plan(
            plan,
            ORDEN_EJECUCION_GOLD,
        )

        self.assertEqual(
            plan["resumen"]["cantidad_tareas"],
            5,
        )

    # ========================================================
    # 11. ESCENARIOS SIN CAMBIOS
    # ========================================================

    def test_47_manifest_sin_cambios(self):

        self.dejar_dataset_sin_cambios(
            self.dataset
        )

        plan = construir_plan(self.manifest)

        self.verificar_plan(
            plan,
            [],
        )

        self.assertEqual(
            plan["datasets_sin_cambios_declarados"],
            ["tipo_cambio"],
        )

    def test_48_dataset_no_consumido(self):

        indices = self.crear_dataset_simulado(
            nombre="indices",
            ruta="mercado_diario/indices",
            clave=["indice", "fecha"],
            clave_nueva={
                "indice": "INDICE_SIMULADO",
                "fecha": "2026-09-18",
            },
            particiones=[
                {
                    "anio": 2026,
                    "mes": 9,
                }
            ],
            registros_antes=16,
        )

        self.manifest["datasets"] = [
            indices
        ]

        plan = construir_plan(self.manifest)

        self.verificar_plan(
            plan,
            [],
        )

        self.assertEqual(
            plan["datasets_no_consumidos"],
            ["indices"],
        )

    def test_49_manifest_mixto_sin_cambios(self):

        sab = self.cargar_fixture(
            "manifest_sab_correccion.json"
        )["datasets"][0]

        self.dejar_dataset_sin_cambios(sab)

        self.manifest["datasets"].append(
            sab
        )

        plan = construir_plan(self.manifest)

        self.verificar_plan(
            plan,
            [
                "mercado_valor_diario",
                "impacto_hechos_valor",
            ],
        )

        self.assertEqual(
            plan["datasets_sin_cambios_declarados"],
            ["montos_sab"],
        )

    def test_50_sin_productos_no_es_publicacion(self):

        self.dejar_dataset_sin_cambios(
            self.dataset
        )

        plan = construir_plan(self.manifest)

        self.assertNotEqual(
            plan["estado"],
            "GOLD_SIN_CAMBIOS",
        )

        self.assertFalse(
            plan["autorizacion_publicacion_gold"]
        )

    # ========================================================
    # 12. DETALLES EXTERNOS Y CAMBIOS DE PARTICION
    # ========================================================

    def test_51_detalle_externo_pendiente(self):

        self.dataset["claves_afectadas"] = None

        self.dataset["ruta_detalle_cambios"] = (
            "file:///ruta_de_prueba/cambios.json"
        )

        plan = construir_plan(self.manifest)

        self.assertEqual(
            plan["datasets_detalle_externo_pendiente"],
            ["tipo_cambio"],
        )

        self.assertTrue(
            plan["resumen"]["requiere_detalle_externo"]
        )

        cambio = plan["datasets_analizados"][0]

        self.assertIsNone(
            cambio["operaciones_declaradas"]
        )

        self.assertFalse(
            cambio["detalle_externo_resuelto"]
        )

    def test_52_tarea_requiere_detalle_externo(self):

        self.dataset["claves_afectadas"] = None

        self.dataset["ruta_detalle_cambios"] = (
            "file:///ruta_de_prueba/cambios.json"
        )

        plan = construir_plan(self.manifest)

        mercado = self.obtener_tarea(
            plan,
            "mercado_valor_diario",
        )

        self.assertTrue(
            mercado[
                "requiere_resolver_detalle_externo"
            ]
        )

        self.assertFalse(
            mercado["ejecucion_autorizada"]
        )

    def test_53_correccion_entre_particiones(self):

        self.dataset["registros_nuevos"] = 0

        self.dataset["registros_corregidos"] = 1

        self.dataset["registros_despues"] = (
            self.dataset["registros_antes"]
        )

        self.dataset["claves_afectadas"] = [
            {
                "operacion": "UPDATE",

                "clave_anterior": {
                    "fecha": "2025-12-30"
                },

                "clave_nueva": {
                    "fecha": "2026-01-02"
                },
            }
        ]

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

        plan = construir_plan(self.manifest)

        cambio = plan["datasets_analizados"][0]

        self.assertEqual(
            cambio["claves_anteriores_declaradas"],
            [{"fecha": "2025-12-30"}],
        )

        self.assertEqual(
            cambio["claves_nuevas_declaradas"],
            [{"fecha": "2026-01-02"}],
        )

        self.assertEqual(
            len(cambio["particiones_silver_declaradas"]),
            2,
        )

        self.verificar_bloqueos(plan)

    # ========================================================
    # 13. REQUISITOS Y PROTECCIONES
    # ========================================================

    def test_54_requisitos_operativos_pendientes(self):

        plan = construir_plan(self.manifest)

        self.assertEqual(
            len(plan["requisitos_operativos"]),
            12,
        )

        for requisito in plan[
            "requisitos_operativos"
        ]:

            self.assertEqual(
                requisito["estado"],
                "PENDIENTE",
            )

            self.assertFalse(
                requisito["validado"]
            )

    def test_55_alcance_gold_desconocido(self):

        plan = construir_plan(self.manifest)

        self.assertIsNone(
            plan["resumen"][
                "cantidad_registros_gold_afectados"
            ]
        )

        self.assertIsNone(
            plan["resumen"][
                "cantidad_particiones_gold_afectadas"
            ]
        )

    def test_56_versiones_gold_no_resueltas(self):

        plan = construir_plan(self.manifest)

        self.assertIsNone(
            plan["version_gold_base"]
        )

        self.assertIsNone(
            plan["version_gold_candidata"]
        )

    def test_57_staging_no_asignado(self):

        plan = construir_plan(self.manifest)

        for tarea in plan["tareas"]:

            self.assertIsNone(
                tarea["ruta_gold_staging"]
            )

    def test_58_huella_ausente_advertida(self):

        plan = construir_plan(self.manifest)

        self.assertIsNone(
            plan["huella_sha256"]
        )

        self.assertTrue(
            any(
                "huella SHA-256" in mensaje
                for mensaje in plan["advertencias"]
            )
        )

    def test_59_huella_original_conservada(self):

        ruta = (
            DIRECTORIO_MANIFESTS
            / "manifest_bcrp_insercion.json"
        )

        _, huella = cargar_manifest(ruta)

        plan = planificar_archivo_manifest(
            ruta
        )

        self.assertEqual(
            plan["huella_sha256"],
            huella,
        )

    # ========================================================
    # 14. ENTRADAS INVALIDAS Y JSON NO FINITO
    # ========================================================

    def test_60_nan_rechazado(self):

        self.dataset["registros_nuevos"] = float(
            "nan"
        )

        with self.assertRaises(PlannerError):

            construir_plan(self.manifest)

    def test_61_infinito_rechazado(self):

        self.dataset["registros_nuevos"] = float(
            "inf"
        )

        with self.assertRaises(PlannerError):

            construir_plan(self.manifest)

    def test_62_infinito_en_particion_rechazado(self):

        self.dataset["particiones_afectadas"] = [
            {
                "anio": float("inf"),
                "mes": 9,
            }
        ]

        with self.assertRaises(PlannerError):

            construir_plan(self.manifest)

    def test_63_infinito_desde_archivo(self):

        with tempfile.TemporaryDirectory() as tmp:

            ruta = Path(tmp) / "infinito.json"

            self.dataset["registros_nuevos"] = (
                "MARCADOR_INFINITO"
            )

            contenido = json.dumps(
                self.manifest
            )

            contenido = contenido.replace(
                '"MARCADOR_INFINITO"',
                "1e999",
            )

            ruta.write_text(
                contenido,
                encoding="utf-8",
            )

            plan = planificar_archivo_manifest(
                ruta
            )

            self.verificar_rechazo(plan)

    def test_64_json_malformado(self):

        with tempfile.TemporaryDirectory() as tmp:

            ruta = Path(tmp) / "malformado.json"

            ruta.write_text(
                '{"datasets": ',
                encoding="utf-8",
            )

            plan = planificar_archivo_manifest(
                ruta
            )

            self.verificar_rechazo(plan)

    def test_65_archivo_inexistente(self):

        with tempfile.TemporaryDirectory() as tmp:

            ruta = Path(tmp) / "no_existe.json"

            plan = planificar_archivo_manifest(
                ruta
            )

            self.verificar_rechazo(plan)

    def test_66_entorno_incompatible(self):

        with self.assertRaises(
            DependencyError
        ):

            construir_plan(
                self.manifest,
                entorno_esperado="PRODUCCION",
            )

    # ========================================================
    # 15. CONFIGURACIONES INCORRECTAS
    # ========================================================

    def test_67_estrategia_faltante(self):

        estrategias = copy.deepcopy(
            ESTRATEGIAS_GOLD
        )

        del estrategias[
            "actividad_sab_diaria"
        ]

        with patch.object(
            planner,
            "ESTRATEGIAS_GOLD",
            estrategias,
        ):

            errores = (
                validar_configuracion_planificador()
            )

            self.assertTrue(errores)

            with self.assertRaises(
                PlannerError
            ):

                asegurar_configuracion_valida()

    def test_68_fuente_silver_incorrecta(self):

        estrategias = copy.deepcopy(
            ESTRATEGIAS_GOLD
        )

        estrategias[
            "mercado_valor_diario"
        ]["fuentes_silver_requeridas"].append(
            "dataset_inexistente"
        )

        with patch.object(
            planner,
            "ESTRATEGIAS_GOLD",
            estrategias,
        ):

            self.assertTrue(
                validar_configuracion_planificador()
            )

    def test_69_ventana_incompatible(self):

        estrategias = copy.deepcopy(
            ESTRATEGIAS_GOLD
        )

        estrategias[
            "impacto_hechos_valor"
        ]["ventana_dias"] = 30

        with patch.object(
            planner,
            "ESTRATEGIAS_GOLD",
            estrategias,
        ):

            self.assertTrue(
                validar_configuracion_planificador()
            )

    def test_70_predecesor_desconocido(self):

        with self.assertRaises(
            PlannerError
        ):

            obtener_predecesores_gold(
                "producto_inexistente"
            )

    # ========================================================
    # 16. RESUMEN E INTERFAZ CLI
    # ========================================================

    def test_71_resumen_conserva_bloqueos(self):

        plan = construir_plan(self.manifest)

        resumen = obtener_resumen_plan(
            plan
        )

        self.assertFalse(
            resumen["autorizacion_ejecucion_gold"]
        )

        self.assertFalse(
            resumen["autorizacion_publicacion_gold"]
        )

        self.assertEqual(
            resumen["resumen"]["cantidad_tareas"],
            2,
        )

    def test_72_resumen_conserva_orden(self):

        plan = construir_plan(self.manifest)

        resumen = obtener_resumen_plan(
            plan
        )

        self.assertEqual(
            resumen["orden_ejecucion_propuesto"],
            plan["orden_ejecucion_propuesto"],
        )

    def test_73_resumen_plan_rechazado(self):

        plan = self.plan_fixture(
            "manifest_no_aprobado.json"
        )

        resumen = obtener_resumen_plan(
            plan
        )

        self.assertEqual(
            resumen,
            plan,
        )

    def test_74_cli_configuracion(self):

        proceso = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_PLANIFICADOR),
                "--mostrar-configuracion",
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
            "CONFIGURACION_PLANIFICADOR_VALIDA",
        )

    def test_75_cli_plan_completo(self):

        proceso = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_PLANIFICADOR),
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

        plan = json.loads(
            proceso.stdout
        )

        self.verificar_plan(
            plan,
            [
                "mercado_valor_diario",
                "impacto_hechos_valor",
            ],
        )

    def test_76_cli_plan_resumido(self):

        proceso = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_PLANIFICADOR),
                "--manifest",
                str(
                    DIRECTORIO_MANIFESTS
                    / "manifest_sab_correccion.json"
                ),
                "--resumen",
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

        resumen = json.loads(
            proceso.stdout
        )

        self.assertEqual(
            resumen["orden_ejecucion_propuesto"],
            ["actividad_sab_diaria"],
        )

        self.assertFalse(
            resumen["autorizacion_ejecucion_gold"]
        )

    def test_77_cli_plan_rechazado(self):

        proceso = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_PLANIFICADOR),
                "--manifest",
                str(
                    DIRECTORIO_MANIFESTS
                    / "manifest_no_aprobado.json"
                ),
                "--resumen",
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

        self.verificar_rechazo(resultado)

    def test_78_plan_determinista(self):

        plan_a = construir_plan(
            self.manifest,
            huella_sha256="a" * 64,
        )

        plan_b = construir_plan(
            self.manifest,
            huella_sha256="a" * 64,
        )

        self.assertEqual(
            plan_a,
            plan_b,
        )

    def test_79_plan_serializable_json(self):

        plan = construir_plan(self.manifest)

        contenido = json.dumps(
            plan,
            allow_nan=False,
        )

        recuperado = json.loads(
            contenido
        )

        self.assertEqual(
            recuperado,
            plan,
        )

    def test_80_todas_tareas_permanecen_bloqueadas(self):

        nombres = [
            "manifest_bcrp_insercion.json",
            "manifest_bcrp_correccion.json",
            "manifest_sab_correccion.json",
            "manifest_no_aprobado.json",
            "manifest_dataset_desconocido.json",
            "manifest_eliminacion_no_autorizada.json",
        ]

        for nombre in nombres:

            with self.subTest(
                manifest=nombre
            ):

                plan = self.plan_fixture(
                    nombre
                )

                self.verificar_bloqueos(
                    plan
                )


# ============================================================
# 17. EJECUCION DE PRUEBAS
# ============================================================

if __name__ == "__main__":

    unittest.main(verbosity=2)
