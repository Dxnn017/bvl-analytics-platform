#!/usr/bin/env python3

import json
from pathlib import Path

BASE = Path("outputs/quality/incremental")

contrato = json.loads(
    (BASE / "contrato_silver_gold_prueba.json")
    .read_text(encoding="utf-8")
)

gold = json.loads(
    Path("outputs/quality/contrato_gold_piero.json")
    .read_text(encoding="utf-8")
)

if (
    contrato["estado"] != "PRUEBA_NO_PUBLICADA"
    or contrato["publicacion_aprobada"] is not False
    or contrato["activar_gold"] is not False
):
    raise SystemExit("ERROR: contrato de prueba inesperado")

dependencias = {
    "tipo_cambio": [
        ("mercado_valor_diario", "DIRECTA"),
        ("impacto_hechos_valor", "INDIRECTA")
    ],
    "embig": [
        ("mercado_valor_diario", "DIRECTA"),
        ("empresa_trimestre", "INDIRECTA")
    ],
    "montos_sab": [
        ("actividad_sab_diaria", "DIRECTA")
    ],
    "principales_cuentas": [
        ("empresa_trimestre", "DIRECTA")
    ]
}

dataset = contrato["dataset"]

if dataset not in dependencias:
    raise SystemExit("ERROR: dependencia no definida")

afectados = []

for nombre, relacion in dependencias[dataset]:
    if nombre not in gold["productos"]:
        raise SystemExit(
            f"ERROR: producto Gold inexistente: {nombre}"
        )

    afectados.append({
        "producto": nombre,
        "tipo_dependencia": relacion,
        "recalculo": "POR_EVALUAR"
    })

    print(f"GOLD_CANDIDATO|{nombre}|{relacion}")

resultado = {
    "dataset_silver": dataset,
    "particiones": contrato["particiones_afectadas"],
    "productos_potenciales": afectados,
    "estado": "SIMULACION",
    "silver_publicado": False,
    "activar_gold": False
}

destino = BASE / "impacto_gold_prueba.json"

destino.write_text(
    json.dumps(resultado, indent=2, ensure_ascii=False),
    encoding="utf-8"
)

print(f"TOTAL_CANDIDATOS|{len(afectados)}")
print("DEPENDENCIAS|PRELIMINARES")
print("GOLD|NO_EJECUTADO")
print("SILVER_OFICIAL|SIN_CAMBIOS")
print(f"REPORTE|{destino}")
