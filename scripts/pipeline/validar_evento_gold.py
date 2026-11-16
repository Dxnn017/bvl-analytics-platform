#!/usr/bin/env python3

import argparse
import json
import sys
from pathlib import Path

def validar(evento):
    if evento.get("estado") != "SILVER_PUBLICADO_APROBADO":
        raise ValueError("Silver no publicado oficialmente")

    if evento.get("publicacion_aprobada") is not True:
        raise ValueError("Publicacion no aprobada")

    if evento.get("activar_gold") is not True:
        raise ValueError("Activacion Gold no autorizada")

    if not evento.get("run_id"):
        raise ValueError("Falta identificador de ejecucion")

    if not evento.get("version_publicada"):
        raise ValueError("Falta version publicada")

    if not evento.get("particiones_afectadas"):
        raise ValueError("No hay particiones afectadas")

    ruta = evento.get("ruta_silver_oficial", "")

    if not ruta.startswith("/datalake/bvl/silver/"):
        raise ValueError("Ruta Silver no oficial")

    if "prueba" in ruta.lower():
        raise ValueError("Ruta de laboratorio detectada")

    print("EVENTO|VALIDADO")
    print("GOLD|ELEGIBLE_PARA_PROCESAMIENTO")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--evento", required=True)
    args = parser.parse_args()

    try:
        datos = json.loads(
            Path(args.evento).read_text(encoding="utf-8")
        )
        validar(datos)

    except (ValueError, KeyError, OSError) as error:
        print(f"EVENTO_RECHAZADO|{error}")
        print("GOLD|NO_EJECUTADO")
        sys.exit(2)

if __name__ == "__main__":
    main()
