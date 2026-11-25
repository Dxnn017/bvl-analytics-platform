import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/ingestion/validar_premigracion_bronze.py"
RUN = "bvl-bronze-inicial-20261124-001"

DATASETS = [
    "bcrp/embig",
    "bcrp/tipo_cambio",
    "smv/cambios_patrimonio",
    "smv/cotizaciones",
    "smv/estado_resultados",
    "smv/flujo_efectivo",
    "smv/hechos_importancia",
    "smv/hechos_valores_consulta",
    "smv/montos_sab",
    "smv/principales_cuentas",
    "smv/situacion_financiera",
    "smv/valores_inscritos",
]


class TestPremigracion(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        operaciones = []

        for i in range(3599):
            dataset = DATASETS[i % 12]
            nombre = f"archivo_{i:05d}.json"
            relativo = f"lote/{nombre}"
            tamano = 362240 if i == 0 else 109300

            operaciones.append({
                "run_id": RUN,
                "dataset": dataset,
                "origen": (
                    f"/datalake/bvl/bronze/{dataset}/{relativo}"
                ),
                "destino": (
                    "gs://bvl26-bronze-7f3a9c/"
                    f"landing/{dataset}/run_id={RUN}/{relativo}"
                ),
                "size_bytes": tamano,
                "sha256": "a" * 64,
                "estado": "PLANIFICADO_NO_EJECUTADO"
            })

        cls.plan = {
            "fase": "3.2.9",
            "tipo": "PLAN_LOCAL_BRONZE",
            "run_id": RUN,
            "datasets": 12,
            "archivos": 3599,
            "bytes": 393623640,
            "transferencias": 0,
            "publicacion": "NO_PUBLICADO",
            "operaciones": operaciones
        }

    def ejecutar(self, plan):
        with tempfile.TemporaryDirectory() as tmp:
            archivo = Path(tmp) / "plan.json"
            archivo.write_text(json.dumps(plan), encoding="utf-8")

            return subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--run-id", RUN,
                    "--plan", str(archivo)
                ],
                capture_output=True,
                text=True
            )

    def test_plan_valido(self):
        resultado = self.ejecutar(self.plan)
        self.assertEqual(
            resultado.returncode, 0,
            resultado.stdout + resultado.stderr
        )
        self.assertIn(
            "PREFLIGHT_MIGRACION_OK",
            resultado.stdout
        )

    def test_rechazo_de_operaciones_inseguras(self):
        casos = [
            (
                "publicacion",
                lambda p: p.update(publicacion="PUBLICADO"),
                "CONTRATO_GLOBAL_INVALIDO"
            ),
            (
                "transferencias",
                lambda p: p.update(transferencias=1),
                "CONTRATO_GLOBAL_INVALIDO"
            ),
            (
                "identidad",
                lambda p: p["operaciones"][0].update(
                    run_id="otro"
                ),
                "IDENTIDAD_INCONSISTENTE"
            ),
            (
                "bucket",
                lambda p: p["operaciones"][0].update(
                    destino="gs://bucket-no-autorizado/archivo"
                ),
                "DESTINO_INVALIDO"
            ),
            (
                "sha256",
                lambda p: p["operaciones"][0].update(
                    sha256="invalido"
                ),
                "SHA256_INVALIDO"
            ),
            (
                "estado",
                lambda p: p["operaciones"][0].update(
                    estado="PUBLICADO"
                ),
                "ESTADO_NO_AUTORIZADO"
            ),
        ]

        for nombre, modificar, esperado in casos:
            with self.subTest(caso=nombre):
                plan = copy.deepcopy(self.plan)
                modificar(plan)

                resultado = self.ejecutar(plan)
                salida = resultado.stdout + resultado.stderr

                self.assertNotEqual(resultado.returncode, 0)
                self.assertIn(esperado, salida)


if __name__ == "__main__":
    unittest.main()
