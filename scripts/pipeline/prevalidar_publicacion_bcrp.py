#!/usr/bin/env python3

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

DESTINO_OFICIAL = (
    "/datalake/bvl/silver/"
    "macroeconomia/tipo_cambio"
)

ORIGEN_PERMITIDO = (
    r"/datalake/bvl/bronze/bcrp/tipo_cambio/"
    r"ingestion_date=\d{4}-\d{2}-\d{2}/"
    r"[^/]+\.csv"
)

STAGING_PERMITIDO = (
    r"/datalake/bvl/staging/"
    r"bcrp_tipo_cambio_incremental/"
    r"run_[A-Za-z0-9_-]+/"
    r"anio=\d{4}/mes=\d{1,2}"
)

def verificar_hdfs(ruta):
    resultado = subprocess.run(
        ["hdfs", "dfs", "-test", "-d", ruta],
        capture_output=True,
        text=True
    )
    return resultado.returncode == 0

def validar(manifest):
    if manifest.get("dataset") != "tipo_cambio":
        raise ValueError("Dataset incorrecto")

    if manifest.get("estado") != "STAGING_VALIDADO_NO_PUBLICADO":
        raise ValueError("Staging no aprobado")

    if manifest.get("current") != DESTINO_OFICIAL:
        raise ValueError("Destino Silver no autorizado")

    bronze = manifest.get("bronze", "")

    if not re.fullmatch(ORIGEN_PERMITIDO, bronze):
        raise ValueError(
            "ORIGEN_NO_OFICIAL_O_SINTETICO"
        )

    if manifest.get("corregidas") != 0:
        raise ValueError(
            "Correcciones requieren revision"
        )

    if manifest.get("nuevas", 0) <= 0:
        raise ValueError(
            "No existen registros nuevos"
        )

    particiones = manifest.get("particiones", [])

    if not particiones:
        raise ValueError("Sin particiones")

    vistas = set()

    for p in particiones:
        anio = int(p["anio"])
        mes = int(p["mes"])
        ruta = p["ruta"]

        if not 1 <= mes <= 12:
            raise ValueError("Mes invalido")

        if (anio, mes) in vistas:
            raise ValueError("Particion duplicada")

        vistas.add((anio, mes))

        if not re.fullmatch(STAGING_PERMITIDO, ruta):
            raise ValueError(
                "Ruta staging no autorizada"
            )

        if not ruta.endswith(
            f"/anio={anio}/mes={mes}"
        ):
            raise ValueError(
                "Particion inconsistente"
            )

        if not verificar_hdfs(ruta):
            raise ValueError(
                "Particion inexistente en HDFS"
            )

        if int(p["filas"]) <= 0:
            raise ValueError(
                "Particion vacia"
            )

    print("DATASET|VALIDADO")
    print("ORIGEN|OFICIAL")
    print("DESTINO|AUTORIZADO")
    print(f"PARTICIONES|{len(particiones)}")
    print("PLAN|PREVALIDADO")
    print("PUBLICACION|NO_EJECUTADA")
    print("GOLD|NO_ACTIVADO")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args()

    ruta = Path(args.manifest).resolve()

    datos = json.loads(
        ruta.read_text(encoding="utf-8")
    )

    try:
        validar(datos)
    except (ValueError, KeyError, TypeError) as error:
        print(f"PUBLICACION_BLOQUEADA|{error}")
        print("SILVER_OFICIAL|SIN_CAMBIOS")
        sys.exit(2)

if __name__ == "__main__":
    main()
