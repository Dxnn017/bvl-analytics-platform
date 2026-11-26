#!/usr/bin/env python3
"""Audita referencias Bronze oficiales incluidas en un paquete Quality.

SOLO OFFLINE. No accede a GCS, no verifica firmas de SILVER_AUTORIZADO,
no reserva consumo unico, no inicia Spark y NO HABILITA SILVER.
"""
import argparse
import hashlib
import json
import re
import tarfile
from pathlib import Path

SHA = re.compile(r"^[0-9a-f]{64}$")
GEN = re.compile(r"^[1-9][0-9]*$")


def exigir(condicion, mensaje):
    if not condicion:
        raise ValueError(mensaje)


def leer(tar, nombre):
    candidatos = tar.getmembers()
    nombres = [m.name for m in candidatos]
    exigir(nombres.count(nombre) == 1, f"Miembro ausente o duplicado: {nombre}")
    miembro = tar.getmember(nombre)
    exigir(miembro.isfile(), f"No es archivo regular: {nombre}")
    archivo = tar.extractfile(miembro)
    exigir(archivo is not None, f"No se puede leer: {nombre}")
    crudo = archivo.read()
    try:
        return crudo, json.loads(crudo.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"JSON invalido: {nombre}: {exc}") from exc


def verificar(ruta):
    exigir(ruta.is_file(), f"Paquete no encontrado: {ruta}")
    with tarfile.open(ruta, mode="r:gz") as tar:
        _, handoff = leer(tar, "entrega_bronze_silver_preparada.json")
        _, publication = leer(tar, "resultado_publicacion_bronze.json")
        exigir(handoff.get("schema_version") == "BVL-BRONZE-SILVER-HANDOFF-v1", "Schema handoff inesperado")
        exigir(publication.get("schema_version") == "BVL-BRONZE-PUBLICATION-RESULT-v1", "Schema publicacion inesperado")
        exigir(handoff.get("status") == "HANDOFF_PREPARED_PENDING_AUTHORIZATION", "Estado handoff inesperado")
        exigir(handoff.get("silver_consumption_allowed") is False, "Handoff habilita Silver inesperadamente")
        exigir(publication.get("status") == "BRONZE_PUBLICADO" and publication.get("published") is True, "Bronze no publicado segun evidencia")
        exigir(publication.get("silver_consumption_allowed") is False, "Publicacion habilita Silver inesperadamente")
        run_id = handoff.get("run_id")
        proyecto = handoff.get("project_id")
        exigir(isinstance(run_id, str) and run_id, "run_id vacio")
        exigir(proyecto == publication.get("project_id") and run_id == publication.get("run_id"), "Proyecto/run_id no coincide")
        datasets = handoff.get("datasets")
        reporte = publication.get("manifests")
        exigir(isinstance(datasets, list) and isinstance(reporte, list), "Listas de datasets/manifests invalidas")
        exigir(len(datasets) == len(reporte) == handoff.get("datasets_count") == publication.get("manifests_published"), "Conteo de datasets no coincide")
        exigir(len(datasets) == 12, "Se esperaban 12 datasets")
        publ_por_dataset = {r.get("dataset"): r for r in reporte}
        exigir(len(publ_por_dataset) == 12, "Dataset duplicado en publicacion")
        total_archivos = total_bytes = 0
        ds_vistos = set()
        objetos_vistos = set()
        resumen = []
        for entry in datasets:
            ds = entry.get("dataset")
            exigir(isinstance(ds, str) and re.fullmatch(r"[a-z0-9_]+/[a-z0-9_]+", ds), f"Dataset invalido: {ds}")
            exigir(ds not in ds_vistos, f"Dataset duplicado: {ds}")
            ds_vistos.add(ds)
            nombre = f"manifests_bronze_oficiales/{ds.replace('/', '__')}.json"
            bytes_manifest, manifest = leer(tar, nombre)
            digest = hashlib.sha256(bytes_manifest).hexdigest()
            exigir(digest == entry.get("bronze_manifest_sha256"), f"SHA-256 manifest no coincide: {ds}")
            pub = publ_por_dataset.get(ds)
            exigir(pub is not None, f"Falta reporte de publicacion: {ds}")
            exigir(pub.get("bronze_manifest_sha256") == digest, f"Publicacion SHA manifest no coincide: {ds}")
            gen_manifest = entry.get("bronze_manifest_generation")
            exigir(GEN.fullmatch(str(gen_manifest)) is not None, f"Generacion manifest ausente: {ds}")
            exigir(str(pub.get("bronze_manifest_generation")) == str(gen_manifest), f"Generacion manifest distinta: {ds}")
            uri_manifest = f"gs://bvl26-control-7f3a9c/manifests/bronze/{ds}/run_id={run_id}/manifest.json"
            exigir(entry.get("bronze_manifest_uri") == pub.get("bronze_manifest_uri") == uri_manifest, f"URI manifest distinta: {ds}")
            exigir(manifest.get("schema_version") == "BVL-BRONZE-MANIFEST-v1", f"Schema manifest invalido: {ds}")
            exigir(manifest.get("status") == "BRONZE_COMPLETADO" and manifest.get("quality_approved") is True, f"Bronze no aprobado: {ds}")
            exigir(manifest.get("silver_consumption_allowed") is False, f"Manifest habilita Silver inesperadamente: {ds}")
            exigir(manifest.get("dataset") == ds and manifest.get("run_id") == run_id and manifest.get("project_id") == proyecto, f"Identidad manifest invalida: {ds}")
            files = manifest.get("files")
            uris = manifest.get("bronze_input_uris")
            exigir(isinstance(files, list) and isinstance(uris, list), f"Archivos/URIs invalidos: {ds}")
            exigir(len(files) == len(uris) == manifest.get("files_count") == entry.get("files_count") == pub.get("files_count"), f"Conteo archivos distinto: {ds}")
            exigir(uris == entry.get("bronze_input_uris"), f"Listado input URIs distinto: {ds}")
            prefijo = f"gs://bvl26-bronze-7f3a9c/{ds}/run_id={run_id}/"
            suma = 0
            local_uris = set()
            for f in files:
                uri = f.get("uri")
                exigir(isinstance(uri, str) and uri.startswith(prefijo), f"Archivo fuera del prefijo oficial: {ds}")
                exigir(uri not in local_uris and uri not in objetos_vistos, f"Objeto repetido: {uri}")
                local_uris.add(uri)
                objetos_vistos.add(uri)
                exigir(GEN.fullmatch(str(f.get("generation"))) is not None, f"Generacion objeto invalida: {uri}")
                exigir(isinstance(f.get("sha256"), str) and SHA.fullmatch(f["sha256"]), f"SHA objeto invalido: {uri}")
                size = f.get("size_bytes")
                exigir(type(size) is int and size >= 0, f"Tamano objeto invalido: {uri}")
                suma += size
            exigir(set(uris) == local_uris and len(uris) == len(local_uris), f"Inventario input URI distinto: {ds}")
            exigir(suma == manifest.get("total_bytes") == entry.get("total_bytes") == pub.get("total_bytes"), f"Bytes distintos: {ds}")
            total_archivos += len(files)
            total_bytes += suma
            resumen.append((ds, len(files), suma))
        exigir(total_archivos == handoff.get("files_count") == publication.get("files_promoted") == 3599, "Conteo global archivos inconsistente")
        exigir(total_bytes == handoff.get("total_bytes") == publication.get("bytes_promoted") == 393623640, "Conteo global bytes inconsistente")
        print("PAQUETE_SHA256:", hashlib.sha256(ruta.read_bytes()).hexdigest())
        print("RUN_ID:", run_id)
        print("PROYECTO:", proyecto)
        for ds, files_count, bytes_count in sorted(resumen):
            print(f"DATASET_OK: {ds} archivos={files_count} bytes={bytes_count}")
        print("MANIFESTS_VERIFICADOS_OFFLINE:", len(resumen))
        print("REFERENCIAS_ARCHIVOS:", total_archivos)
        print("BYTES_DECLARADOS:", total_bytes)
        print("RESULTADO: BRONZE_HANDOFF_REFERENCIAS_CONSISTENTES_OFFLINE")
        print("GCS_EN_VIVO_VERIFICADO: NO")
        print("FIRMA_SILVER_VERIFICADA: NO")
        print("SILVER_AUTORIZADO: NO")
        print("OPERACIONES_GCS: 0")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("paquete", type=Path, help="Entrega original de Quality .tar.gz")
    args = p.parse_args()
    try:
        verificar(args.paquete)
    except (ValueError, OSError, tarfile.TarError) as exc:
        raise SystemExit(f"ALTO: {exc}") from exc


if __name__ == "__main__":
    main()
