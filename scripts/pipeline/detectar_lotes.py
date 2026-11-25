#!/usr/bin/env python3

import re
import shlex
import subprocess
from collections import defaultdict

RAIZ = "/datalake/bvl/bronze"
EJECUTOR = "./scripts/pipeline/ejecutar_incremental.sh"

FINANZAS = {
    "estado_resultados": "EstadoResultadosSMV.json",
    "flujo_efectivo": "FlujoEfectivoSMV.json",
    "situacion_financiera": "SituacionFinancieraSMV.json",
    "cambios_patrimonio": "CambiosPatrimonioSMV.json",
}

BCRP = {
    "tipo_cambio": "BCRP_Tipo_Cambio_Venta.csv",
    "embig": "BCRP_EMBIG_Peru.csv",
}

PATRON = re.compile(
    r"^/datalake/bvl/bronze/"
    r"(smv|bcrp)/([a-z_]+)/"
    r"ingestion_date=(\d{4}-\d{2}-\d{2})/"
    r"([^/]+)$"
)

def listar_archivos():
    resultado = subprocess.run(
        ["hdfs", "dfs", "-ls", "-R", RAIZ],
        capture_output=True,
        text=True,
        check=True
    )

    archivos = []

    for linea in resultado.stdout.splitlines():
        partes = linea.split()

        if len(partes) >= 8 and partes[0].startswith("-"):
            archivos.append(partes[-1])

    return archivos

def construir_plan(area, fuente, fecha, archivos):
    carpeta = (
        f"{RAIZ}/{area}/{fuente}/"
        f"ingestion_date={fecha}"
    )

    nombres = {a.rsplit("/", 1)[-1] for a in archivos}

    if area == "bcrp" and fuente in BCRP:
        archivo = BCRP[fuente]

        if archivo not in nombres:
            return None

        return [
            EJECUTOR, "plan", "bcrp",
            "--dataset", fuente,
            "--bronze", f"{carpeta}/{archivo}",
            "--current",
            f"/datalake/bvl/silver/macroeconomia/{fuente}"
        ]

    if area == "smv" and fuente in FINANZAS:
        archivo = FINANZAS[fuente]

        if archivo not in nombres:
            return None

        return [
            EJECUTOR, "plan", "finanzas",
            "--dataset", fuente,
            "--bronze", f"{carpeta}/{archivo}",
            "--current",
            f"/datalake/bvl/silver/finanzas_empresariales/{fuente}"
        ]

    if area == "smv" and fuente == "montos_sab":
        if not any(n.endswith(".json") for n in nombres):
            return None

        return [
            EJECUTOR, "plan", "montos_sab",
            "--bronze", carpeta,
            "--ingestion-date", fecha,
            "--current",
            "/datalake/bvl/silver/mercado_diario/montos_sab"
        ]

    if area == "smv" and fuente == "cotizaciones":
        if not any(n.endswith(".json") for n in nombres):
            return None

        return [
            EJECUTOR, "plan", "cotizaciones",
            "--bronze", carpeta,
            "--ingestion-date", fecha,
            "--current",
            "/datalake/bvl/silver/mercado_diario/cotizaciones",
            "--stage-root",
            "/datalake/bvl/staging/cotizaciones_pipeline_daniela"
        ]

    if area == "smv" and fuente == "principales_cuentas":
        if not any(re.fullmatch(r"principales_.*\.json", n) for n in nombres):
            return None

        return [
            EJECUTOR, "plan", "principales",
            "--bronze", f"{carpeta}/principales_*.json",
            "--current",
            "/datalake/bvl/silver/finanzas_empresariales/principales_cuentas",
            "--ingestion-date", fecha
        ]

    return None

def main():
    print("DETECTOR|INICIADO")

    archivos = listar_archivos()
    lotes = defaultdict(list)

    for ruta in archivos:
        encontrado = PATRON.fullmatch(ruta)

        if encontrado:
            area, fuente, fecha, _ = encontrado.groups()
            lotes[(area, fuente, fecha)].append(ruta)

    total = 0

    for (area, fuente, fecha), rutas in sorted(lotes.items()):
        plan = construir_plan(area, fuente, fecha, rutas)

        if plan is None:
            print(f"OMITIDO|{area}/{fuente}|{fecha}")
            continue

        total += 1

        print(
            f"LOTE|{area}/{fuente}|"
            f"fecha={fecha}|archivos={len(rutas)}"
        )
        print("PLAN|" + shlex.join(plan))

    print("HECHOS|REQUIERE_CANDIDATO_STAGING")
    print(f"TOTAL_LOTES_RECONOCIDOS|{total}")
    print("ESTADO|SOLO_PLAN_NO_EJECUTADO")

if __name__ == "__main__":
    main()
