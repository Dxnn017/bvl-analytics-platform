"""Simulación local de MERGE Gold, sin conexión a BigQuery."""

import json
import unittest
from pathlib import Path


PRODUCTOS = (
    "mercado_valor_diario",
    "actividad_sab_diaria",
    "empresa_trimestre",
)


def aplicar_delta(destino, cambios, claves, autorizaciones):
    claves_vistas = set()
    resultado = dict(destino)
    auditoria = []

    for cambio in cambios:
        fila = cambio["row"]
        clave = tuple(fila.get(c) for c in claves)
        operacion = cambio["operation"]

        if any(v is None or v == "" for v in clave):
            raise ValueError("Clave inválida")

        if clave in claves_vistas:
            raise ValueError("Clave duplicada en delta")
        claves_vistas.add(clave)

        if not cambio.get("run_id") or not cambio.get("source_version"):
            raise ValueError("Trazabilidad incompleta")

        if operacion == "DELETE":
            permiso = (
                cambio["run_id"],
                cambio["source_version"],
                clave,
            )
            if permiso not in autorizaciones:
                raise ValueError("Eliminación no autorizada")

            anterior = resultado.pop(clave, None)
            accion = "DELETE" if anterior is not None else "NOOP"

        elif operacion == "UPSERT":
            anterior = resultado.get(clave)
            accion = (
                "INSERT" if anterior is None
                else "NOOP" if anterior == fila
                else "UPDATE"
            )
            resultado[clave] = dict(fila)

        else:
            raise ValueError("Operación desconocida")

        auditoria.append({
            "key": clave,
            "operation": accion,
            "run_id": cambio["run_id"],
            "source_version": cambio["source_version"],
        })

    return resultado, auditoria


class TestMergeLocal(unittest.TestCase):

    def test_incremental_por_producto(self):
        for producto in PRODUCTOS:
            with self.subTest(producto=producto):
                contrato = json.loads(
                    (
                        Path("docs/bigquery") /
                        f"{producto}.json"
                    ).read_text()
                )
                claves = contrato["business_keys"]

                def fila(numero, valor):
                    datos = {c: f"clave-{numero}" for c in claves}
                    datos["valor_simulado"] = valor
                    return datos

                def cambio(operacion, datos):
                    return {
                        "operation": operacion,
                        "row": datos,
                        "run_id": "run-fixture",
                        "source_version": "silver-fixture-v1",
                    }

                inicial = [cambio("UPSERT", fila(1, 100))]
                estado, registro = aplicar_delta(
                    {}, inicial, claves, set()
                )
                self.assertEqual(registro[0]["operation"], "INSERT")

                repetido, registro = aplicar_delta(
                    estado, inicial, claves, set()
                )
                self.assertEqual(repetido, estado)
                self.assertEqual(registro[0]["operation"], "NOOP")

                actualizado, registro = aplicar_delta(
                    estado,
                    [cambio("UPSERT", fila(1, 200))],
                    claves,
                    set(),
                )
                self.assertEqual(registro[0]["operation"], "UPDATE")

                eliminado = cambio("DELETE", fila(1, 200))

                with self.assertRaises(ValueError):
                    aplicar_delta(
                        actualizado, [eliminado], claves, set()
                    )

                clave = tuple(
                    eliminado["row"][c] for c in claves
                )
                permiso = {
                    ("run-fixture", "silver-fixture-v1", clave)
                }
                final, registro = aplicar_delta(
                    actualizado, [eliminado], claves, permiso
                )

                self.assertEqual(final, {})
                self.assertEqual(registro[0]["operation"], "DELETE")
                self.assertEqual(
                    registro[0]["source_version"],
                    "silver-fixture-v1",
                )

                print(f"{producto}: INCREMENTAL_LOCAL_OK")


if __name__ == "__main__":
    unittest.main()
