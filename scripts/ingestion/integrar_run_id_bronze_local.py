#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
import tarfile
from datetime import datetime
from pathlib import Path

def sha256(datos):
    return hashlib.sha256(datos).hexdigest()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    run_id = args.run_id
    patron = r"bvl-bronze-inicial-(\d{8})-(\d{3})"
    coincidencia = re.fullmatch(patron, run_id)

    if not coincidencia:
        parser.error("run_id no cumple la convencion aprobada")

    try:
        datetime.strptime(coincidencia.group(1), "%Y%m%d")
    except ValueError:
        parser.error("Fecha invalida en run_id")

    paquete = (
        Path.home() / "Descargas" /
        "BVL_CONTROL_BRONZE_10356.tar.gz"
    )

    checksum = Path(str(paquete) + ".sha256")

    if not paquete.is_file() or not checksum.is_file():
        raise SystemExit("ERROR: paquete o SHA256 inexistente")

    hash_paquete = sha256(paquete.read_bytes())
    hash_esperado = checksum.read_text().split()[0]

    if hash_paquete != hash_esperado:
        raise SystemExit("ERROR: SHA256 del paquete no coincide")

    with tarfile.open(paquete, "r:gz") as tar:
        def leer(nombre):
            archivo = tar.extractfile(nombre)
            if archivo is None:
                raise ValueError("Archivo inexistente: " + nombre)
            return archivo.read()

        original = leer("CONTROL_LOTE_BRONZE_CANDIDATO.json")
        control = json.loads(original)

        if (
            control["run_id"] is not None
            or control["status"] != "CANDIDATO"
            or control["validation_approved"] is not False
            or control["publication_status"] != "NO_PUBLICADO"
            or len(control["datasets"]) != 12
        ):
            raise ValueError("Control original invalido")

        inventario = leer(control["inventory_file"])

        if sha256(inventario) != control["inventory_sha256"]:
            raise ValueError("Inventario inconsistente")

        nombres = set()
        rutas = set()
        total_archivos = 0
        total_bytes = 0

        for d in control["datasets"]:
            nombre = d["dataset"]

            if nombre in nombres:
                raise ValueError("Dataset duplicado")

            nombres.add(nombre)

            contenido = leer(d["manifest_file"])

            if sha256(contenido) != d["manifest_sha256"]:
                raise ValueError("Manifest modificado: " + nombre)

            manifest = json.loads(contenido)

            if (
                manifest.get("run_id") is not None
                or manifest.get("validation_approved") is not False
                or manifest.get("landing_prefix_template")
                   != d["landing_prefix_template"]
            ):
                raise ValueError("Manifest inconsistente: " + nombre)

            plantilla = d["landing_prefix_template"]

            if plantilla.count("{run_id}") != 1:
                raise ValueError("Plantilla invalida: " + nombre)

            ruta = plantilla.replace("{run_id}", run_id)

            if ruta in rutas:
                raise ValueError("Ruta duplicada")

            rutas.add(ruta)

            d["run_id_operativo"] = run_id
            d["landing_prefix_operativo"] = ruta

            total_archivos += d["expected_file_count"]
            total_bytes += d["expected_size_bytes"]

        if (
            total_archivos != control["file_count"]
            or total_bytes != control["total_size_bytes"]
        ):
            raise ValueError("Totales inconsistentes")

    control["run_id"] = run_id
    control["prueba_local"] = True
    control["paquete_original_sha256"] = hash_paquete
    control["control_original_sha256"] = sha256(original)

    destino = Path(
        "outputs/quality/bronze_runid_lab"
    )
    destino.mkdir(parents=True, exist_ok=True)

    salida = destino / f"CONTROL_OPERATIVO_{run_id}.json"

    if salida.exists():
        anterior = json.loads(salida.read_text(encoding="utf-8"))

        if anterior != control:
            raise SystemExit("CONFLICTO: identidad reutilizada con datos diferentes")

        print("REINTENTO_IDEMPOTENTE: OK")
    else:
        with salida.open("x", encoding="utf-8") as f:
            json.dump(control, f, indent=2, ensure_ascii=False)

        print("REGISTRO_OPERATIVO: CREADO")

    if sha256(paquete.read_bytes()) != hash_paquete:
        raise SystemExit("ERROR: paquete original modificado")

    print("RUN_ID_EXTERNO:", run_id)
    print("DATASETS:", len(nombres))
    print("RUTAS_UNICAS:", len(rutas))
    print("ARCHIVOS:", total_archivos)
    print("MANIFESTS_SHA256: OK")
    print("PAQUETE_ORIGINAL: INTACTO")
    print("PUBLICACION_CLOUD: NO")
    print("RESULTADO: INTEGRACION LOCAL OK")

if __name__ == "__main__":
    main()
