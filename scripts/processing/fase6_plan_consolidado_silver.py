#!/usr/bin/env python3
"""BVL Fase 6: inventario y plan de dependencias para 13 productos restantes.

SOLO LECTURA de fuentes. SOLO ESCRITURA de informe JSON LOCAL en Descargas.
NO emite autorizaciones, NO importa procesadores legacy, NO hace GCS/Spark,
NO consume claims, NO publica versiones ni lanza Dataproc.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re

RUN = "bvl-bronze-inicial-20261125-001"
COMMIT_REFERENCIA = "387d55a9f8a8d6f2be9e74e991ef6da2d544a001"
CONTROL_BUCKET = "gs://bvl26-control-7f3a9c"
SILVER_BUCKET = "gs://bvl26-silver-7f3a9c"
DIRECT = (
    ("bcrp/embig", ("macroeconomia/embig",), 1, "scripts/processing/bronze_to_silver_dataproc_candidato.py", "normalizacion BCRP/ANSI, colisiones por fecha y writer create-only"),
    ("smv/cambios_patrimonio", ("finanzas_empresariales/cambios_patrimonio",), 1, "scripts/processing/bronze_to_silver_dataproc_candidato.py", "validar granularidad, schema/fechas y writer create-only"),
    ("smv/estado_resultados", ("finanzas_empresariales/estado_resultados",), 1, "scripts/processing/bronze_to_silver_dataproc_candidato.py", "validar granularidad, schema/fechas y writer create-only"),
    ("smv/flujo_efectivo", ("finanzas_empresariales/flujo_efectivo",), 1, "scripts/processing/bronze_to_silver_dataproc_candidato.py", "validar granularidad, schema/fechas y writer create-only"),
    ("smv/situacion_financiera", ("finanzas_empresariales/situacion_financiera",), 1, "scripts/processing/bronze_to_silver_dataproc_candidato.py", "validar granularidad, schema/fechas y writer create-only"),
    ("smv/principales_cuentas", ("finanzas_empresariales/principales_cuentas",), 20, "scripts/processing/principales_cuentas_to_silver.py", "retirar overwrite/rutas HDFS; preservar granularidad empresa-trimestre"),
    ("smv/cotizaciones", ("mercado_diario/cotizaciones",), 60, "scripts/processing/cotizaciones_to_silver.py", "retirar overwrite/rutas HDFS; preservar claves compuestas"),
    ("smv/montos_sab", ("mercado_diario/montos_sab",), 1221, "scripts/processing/montos_sab_to_silver.py", "retirar overwrite/rutas HDFS; procesar JSON con lectura por generacion"),
    ("smv/valores_inscritos", ("referencias_smv/valores_inscritos", "referencias_smv/puente_ruc_nemonico"), 290, "scripts/processing/valores_smv_to_silver.py", "UNA auth/claim y DOS salidas atomicas a evaluar; los 290 objetos no equivalen al JSON HDFS historico"),
    ("smv/hechos_valores_consulta", ("referencias_smv/hechos_valores_consulta",), 23, "scripts/integration/query_hechos_valores_smv.py", "crear TRANSFORMACION offline desde 22 XML + JSON; no ejecutar cliente SOAP de extraccion"),
    ("smv/hechos_importancia", ("hechos_importancia/eventos",), 1979, "scripts/processing/hechos_importancia_to_silver.py", "manifest oficial debe sustentar metadata de eventos; 88 TXT 0C y XML error no son eventos; no publicar documentos/texto fuera de contrato"),
)
DERIVED = "hechos_importancia/identidad_empresarial"
DEPENDS = (
    "hechos_importancia/eventos",
    "finanzas_empresariales/principales_cuentas",
    "referencias_smv/puente_ruc_nemonico",
    "mercado_diario/cotizaciones",
    "referencias_smv/hechos_valores_consulta",
)
DERIVED_CODE = "scripts/processing/hechos_identidad_gcp_candidato.py"
VALID_FILE = re.compile(r"^[A-Za-z0-9_./-]+$")


class PlanError(ValueError):
    pass


def check(cond, msg):
    if not cond:
        raise PlanError(msg)


def validate_mapping():
    names = [s[0] for s in DIRECT]
    products = [p for entry in DIRECT for p in entry[1]]
    check(len(names) == 11 and len(set(names)) == 11, "No son 11 procesos unicos")
    check(len(products) == 12 and len(set(products)) == 12, "No son 12 salidas directas")
    check(set(DEPENDS).issubset(products), "Dependencias no producidas por directos")
    check(sum(entry[2] for entry in DIRECT) == 3598, "El inventario esperado es 3598 archivos restantes")
    check(DERIVED not in products, "Derivado mezclado con fuentes directas")
    return True


def file_info(root: Path, rel: str):
    check(VALID_FILE.fullmatch(rel) is not None and ".." not in Path(rel).parts,
          "Ruta no permitida")
    f = root / rel
    check(f.is_file(), f"Procesador existente no localizado: {rel}")
    data = f.read_bytes()
    return {"path": rel, "bytes": len(data), "sha256": sha256(data).hexdigest(),
            "cloud_compatible": False, "quality_approved_for_cloud": False}


def compose_plan(root: Path):
    validate_mapping()
    root = root.resolve()
    by_file = {}
    items = []
    for index, (ds, products, files, code, blocker) in enumerate(DIRECT, start=1):
        if code not in by_file:
            by_file[code] = file_info(root, code)
        items.append({
            "order": index, "mode": "SINGLE_SOURCE", "bronze_dataset": ds,
            "bronze_file_count_reference": files, "products": list(products),
            "source_code": by_file[code],
            "proposed_bronze_manifest_uri": f"{CONTROL_BUCKET}/manifests/bronze/{ds}/run_id={RUN}/manifest.json",
            "authorization": "REQUIERE_DOCUMENTO_FIRMADO_POR_INTENTO", "claim": "REQUIERE_CLAIM_EXCLUSIVO",
            "preconditions": ["manifest original fijado: generacion y SHA", "codigo y ejecutable firmados por Quality", "runtime y writer compatible", "autorizacion vigente", "ausencia claim segun Quality"],
            "blockers": [blocker, "adaptador cloud especifico y pruebas independientes aun pendientes"],
            "ready_to_execute": False, "no_auto_retry": True, "no_publish_from_staging": True,
        })
    derived_file = file_info(root, DERIVED_CODE)
    derived = {
        "order": 12, "mode": "MULTI_SOURCE", "bronze_dataset": None,
        "products": [DERIVED], "silver_dependencies": list(DEPENDS), "source_code": derived_file,
        "authorization": "REQUIERE_FIRMA_KMS_Y_CINCO_VERSIONES_OFICIALES_FIJADAS",
        "blockers": ["el candidato historico esta bloqueado", "faltan cinco referencias Silver oficiales con generaciones y SHA", "adaptador de GCS/Quality no implementado para el derivado"],
        "ready_to_execute": False, "no_auto_retry": True, "no_publish_from_staging": True,
    }
    return {
        "schema_version": "BVL-SILVER-REMAINDER-PLANNING-v1",
        "status": "PLAN_LOCAL_NO_AUTORIZA_EJECUCION",
        "commit_base_reference": COMMIT_REFERENCIA,
        "run_reference": RUN, "project": "bvl-analytics-2026", "region": "southamerica-west1",
        "pilot": {"bronze": "bcrp/tipo_cambio", "silver": "macroeconomia/tipo_cambio",
                  "state_reported_by_quality": "SILVER_PILOT_INTERNAL_PUBLISHED",
                  "gold_consumption_allowed": False},
        "direct_process_count": 11, "direct_product_count": 12, "derived_product_count": 1,
        "pending_product_count": 13, "remaining_bronze_files_reference": 3598,
        "resource_policy": {"max_concurrent_dataproc_batches": 1,
                            "driver_vcpus": 4, "executors": 2, "executor_vcpus_each": 4,
                            "max_total_vcpus": 12, "automatic_retry": False},
        "steps": items + [derived],
        "release_required": ["adaptadores por fuente", "puerta F5 verificada sin cambios",
                             "firma KMS contexto de lanzamiento por batch", "manifests candidatos",
                             "pruebas negativas", "aprobacion Quality staging",
                             "publicacion versionada Quality", "registro current.json"],
        "execution_enabled": False,
        "writes_gcs": False,
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repo", type=Path, default=Path.cwd())
    ap.add_argument("--output", type=Path, default=Path.home() / "Descargas" / "BVL_FASE6_PLAN_13_NO_AUTORIZA.json")
    args = ap.parse_args(argv)
    result = compose_plan(args.repo)
    content = (json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.is_file():
        check(args.output.read_bytes() == content,
              "El destino existente difiere: conservar el previo y elegir otro --output")
    else:
        args.output.write_bytes(content)
    print("PLAN_GENERADO:", args.output)
    print("PROCESOS_BRONZE_RESTANTES: 11")
    print("PRODUCTOS_SILVER_RESTANTES: 13 (12 directos + 1 derivado)")
    print("FUENTES_CON_HASHES_VERIFICABLES:", len({x["source_code"]["path"] for x in result["steps"]}))
    print("PUBLICACION_GCS: NO | CLAIM: NO | DATAPROC: NO | GIT: SIN_CAMBIOS")
    print("EJECUCION_HABILITADA: NO")


if __name__ == "__main__":
    main()
