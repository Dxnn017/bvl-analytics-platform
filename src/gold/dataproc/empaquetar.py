"""Construye un ZIP local de módulos Python para Spark."""

import argparse
import hashlib
from pathlib import Path
from zipfile import ZipFile, ZipInfo, ZIP_DEFLATED


def empaquetar(raiz, salida):
    directorios = (
        "src/gold/mercado",
        "src/gold/empresas",
        "src/gold/eventos",
        "src/gold/incremental",
        "src/gold/dataproc",
    )

    archivos = {}

    for directorio in directorios:
        for ruta in sorted((raiz / directorio).glob("*.py")):
            if ruta.name in {
                "__init__.py",
                "main_gold.py",
                "empaquetar.py",
            }:
                continue

            if ruta.is_symlink():
                raise ValueError("Enlace simbólico no permitido")

            if ruta.name in archivos:
                raise ValueError(
                    "Colisión de módulos: " + ruta.name
                )

            archivos[ruta.name] = ruta

    salida.parent.mkdir(parents=True, exist_ok=True)

    with ZipFile(salida, "w") as zipf:
        for nombre, ruta in sorted(archivos.items()):
            info = ZipInfo(nombre)
            info.date_time = (1980, 1, 1, 0, 0, 0)
            info.compress_type = ZIP_DEFLATED
            info.external_attr = 0o644 << 16

            zipf.writestr(info, ruta.read_bytes())

    with ZipFile(salida) as zipf:
        assert zipf.testzip() is None

    print("MODULOS_EMPAQUETADOS:", len(archivos))
    print("ZIP:", salida)
    print(
        "SHA256:",
        hashlib.sha256(salida.read_bytes()).hexdigest()
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--salida", required=True)
    args = parser.parse_args()

    empaquetar(
        Path.cwd(),
        Path(args.salida),
    )
