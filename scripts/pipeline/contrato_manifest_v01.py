#!/usr/bin/env python3
import json
import sys
from pathlib import Path
from datetime import datetime

VERSION = "0.1"

DEPENDENCIAS = {
    "tipo_cambio": [
        "mercado_valor_diario",
        "impacto_hechos_valor"
    ]
}

CAMPOS_GENERALES = [
    "version_manifest",
    "entorno",
    "run_id",
    "estado",
    "fecha_publicacion",
    "tipo_publicacion",
    "validacion_aprobada",
    "datasets"
]

CAMPOS_DATASET = [
    "dataset",
    "ruta_silver",
    "version_esquema",
    "version_anterior",
    "version_publicada",
    "clave",
    "particiones_afectadas",
    "claves_afectadas",
    "ruta_detalle_cambios",
    "registros_nuevos",
    "registros_corregidos",
    "registros_eliminados",
    "registros_antes",
    "registros_despues",
    "autorizacion_eliminaciones"
]

def validar(datos):
    if not isinstance(datos, dict):
        raise ValueError("Manifest invalido")

    for campo in CAMPOS_GENERALES:
        if campo not in datos:
            raise ValueError(f"Falta {campo}")

    if datos["version_manifest"] != VERSION:
        raise ValueError("Version no compatible")

    if datos["entorno"] not in ("PRUEBA", "PRODUCCION"):
        raise ValueError("Entorno invalido")

    if datos["tipo_publicacion"] != "INCREMENTAL":
        raise ValueError("Tipo no compatible")

    if not isinstance(datos["run_id"], str) or not datos["run_id"]:
        raise ValueError("Run ID invalido")

    fecha = datos["fecha_publicacion"]
    datetime.fromisoformat(fecha.replace("Z", "+00:00"))

    if type(datos["validacion_aprobada"]) is not bool:
        raise ValueError("Validacion invalida")

    datasets = datos["datasets"]
    if not isinstance(datasets, list) or not datasets:
        raise ValueError("Sin datasets")

    vistos = set()

    for d in datasets:
        if not isinstance(d, dict):
            raise ValueError("Dataset invalido")

        for campo in CAMPOS_DATASET:
            if campo not in d:
                raise ValueError(f"Falta {campo}")

        nombre = d["dataset"]

        if nombre not in DEPENDENCIAS:
            raise ValueError("Dataset no reconocido por este modulo")

        if nombre in vistos:
            raise ValueError("Dataset duplicado")

        vistos.add(nombre)

        if d["clave"] != ["fecha"]:
            raise ValueError("Clave BCRP incorrecta")

        for campo in (
            "ruta_silver",
            "version_esquema",
            "version_anterior",
            "version_publicada"
        ):
            if not isinstance(d[campo], str) or not d[campo]:
                raise ValueError(f"Campo invalido: {campo}")

        for campo in (
            "registros_nuevos",
            "registros_corregidos",
            "registros_eliminados",
            "registros_antes",
            "registros_despues"
        ):
            if type(d[campo]) is not int or d[campo] < 0:
                raise ValueError(f"Conteo invalido: {campo}")

        if (
            d["registros_despues"] !=
            d["registros_antes"]
            + d["registros_nuevos"]
            - d["registros_eliminados"]
        ):
            raise ValueError("Conteos inconsistentes")

        if type(d["autorizacion_eliminaciones"]) is not bool:
            raise ValueError("Autorizacion invalida")

        if d["registros_eliminados"] and not d["autorizacion_eliminaciones"]:
            raise ValueError("Eliminaciones no autorizadas")

        if not isinstance(d["particiones_afectadas"], list):
            raise ValueError("Particiones invalidas")

        if d["claves_afectadas"] is not None and not isinstance(
            d["claves_afectadas"], list
        ):
            raise ValueError("Claves afectadas invalidas")

        if d["ruta_detalle_cambios"] is not None and not isinstance(
            d["ruta_detalle_cambios"], str
        ):
            raise ValueError("Ruta detalle invalida")

        print(f"DATASET|{nombre}")
        print(f"NUEVOS|{d['registros_nuevos']}")
        print(f"CORREGIDOS|{d['registros_corregidos']}")
        print(
            "GOLD_CANDIDATOS|" +
            ",".join(DEPENDENCIAS[nombre])
        )

    print("CONTRATO_V01|ESTRUCTURA_COMPATIBLE")
    print("VALIDACION_OPERATIVA|PENDIENTE")

if __name__ == "__main__":
    try:
        datos = json.loads(
            Path(sys.argv[1]).read_text(encoding="utf-8")
        )
        validar(datos)
    except (ValueError, KeyError, IndexError, OSError) as error:
        print(f"ERROR|{error}")
        sys.exit(1)
