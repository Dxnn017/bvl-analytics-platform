"""Comparación de cambios de negocio Gold. Propuesta local."""

import json
from pathlib import Path

BASE = Path("docs/bigquery")

# Exclusiones técnicas candidatas; sujetas a revisión de Daniela.
METADATOS_TECNICOS = {
    "fecha_ingesta",
    "operation",
    "delete_authorized",
    "run_id",
    "source_version",
    "batch_id",
    "payload_sha256",
}


def columnas_negocio(producto):
    contrato = json.loads(
        (BASE / f"{producto}.json").read_text()
    )

    columnas = [
        c["name"]
        for c in contrato["columns"]
        if c["name"] not in METADATOS_TECNICOS
    ]

    if not columnas:
        raise ValueError("Sin columnas de negocio")

    return columnas


def cambio_negocio(producto, anterior, nuevo):
    return any(
        anterior.get(columna) != nuevo.get(columna)
        for columna in columnas_negocio(producto)
    )


if __name__ == "__main__":
    casos = {
        "mercado_valor_diario": {
            "nemonico": "ABC",
            "fecha": "2026-11-27",
            "precio_cierre": 10.0,
        },
        "actividad_sab_diaria": {
            "fecha": "2026-11-27",
            "codigo_sab": "S01",
            "monto_total": 100.0,
        },
        "empresa_trimestre": {
            "id_registro": "REG-001",
            "activo_total": 1000.0,
        },
    }

    for producto, anterior in casos.items():
        solo_metadata = {
            **anterior,
            "fecha_ingesta": "2026-11-28",
        }

        assert not cambio_negocio(
            producto, anterior, solo_metadata
        )

        modificada = dict(anterior)

        campo = {
            "mercado_valor_diario": "precio_cierre",
            "actividad_sab_diaria": "monto_total",
            "empresa_trimestre": "activo_total",
        }[producto]

        modificada[campo] += 1

        assert cambio_negocio(
            producto, anterior, modificada
        )

        print(f"{producto}: COMPARADOR_NEGOCIO_OK")

    print("TRES_COMPARADORES_NEGOCIO_VALIDADOS_OK")
