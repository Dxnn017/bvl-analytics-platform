#!/usr/bin/env python3

import copy
import io
import sys
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path("scripts/pipeline").resolve()))

import prevalidar_publicacion_bcrp as validador

base = {
    "dataset": "tipo_cambio",
    "estado": "STAGING_VALIDADO_NO_PUBLICADO",
    "current": "/datalake/bvl/silver/macroeconomia/tipo_cambio",
    "bronze": (
        "/datalake/bvl/bronze/bcrp/tipo_cambio/"
        "ingestion_date=2026-11-15/"
        "BCRP_Tipo_Cambio_Venta.csv"
    ),
    "nuevas": 1,
    "corregidas": 0,
    "particiones": [{
        "anio": 2026,
        "mes": 9,
        "filas": 14,
        "ruta": (
            "/datalake/bvl/staging/"
            "bcrp_tipo_cambio_incremental/"
            "run_20261115T120000_abcdef/"
            "anio=2026/mes=9"
        )
    }]
}

def probar(nombre, cambio, permitido=False):
    datos = copy.deepcopy(base)
    cambio(datos)

    aprobado = True

    try:
        with patch.object(
            validador, "verificar_hdfs",
            return_value=True
        ):
            with redirect_stdout(io.StringIO()):
                validador.validar(datos)

    except (ValueError, KeyError, TypeError):
        aprobado = False

    if aprobado != permitido:
        print(f"FALLO|{nombre}")
        return False

    print(f"OK|{nombre}")
    return True

pruebas = [
    (
        "ESTRUCTURA_VALIDA",
        lambda d: None,
        True
    ),
    (
        "ORIGEN_SINTETICO",
        lambda d: d.update(
            bronze="/datalake/bvl/staging/prueba.csv"
        ),
        False
    ),
    (
        "COMODINES_BLOQUEADOS",
        lambda d: d.update(
            bronze=d["bronze"].replace(
                "BCRP_Tipo_Cambio_Venta.csv", "*.csv"
            )
        ),
        False
    ),
    (
        "DESTINO_NO_AUTORIZADO",
        lambda d: d.update(
            current="/datalake/bvl/silver/otro"
        ),
        False
    ),
    (
        "CORRECCIONES_NO_AUTORIZADAS",
        lambda d: d.update(corregidas=1),
        False
    ),
    (
        "PARTICIONES_DUPLICADAS",
        lambda d: d["particiones"].append(
            copy.deepcopy(d["particiones"][0])
        ),
        False
    ),
    (
        "STAGING_NO_AUTORIZADO",
        lambda d: d["particiones"][0].update(
            ruta="/datalake/bvl/silver/otro"
        ),
        False
    ),
    (
        "SIN_PARTICIONES",
        lambda d: d.update(particiones=[]),
        False
    )
]

resultados = [
    probar(nombre, cambio, permitido)
    for nombre, cambio, permitido in pruebas
]

print(f"TOTAL_PRUEBAS|{len(resultados)}")
print(f"APROBADAS|{sum(resultados)}")
print(f"FALLIDAS|{resultados.count(False)}")
print("PUBLICACION|NO_EJECUTADA")
print("GOLD|NO_ACTIVADO")

sys.exit(0 if all(resultados) else 1)
