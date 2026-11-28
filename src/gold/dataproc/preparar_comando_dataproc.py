"""Genera un comando de referencia.

No invoca gcloud ni realiza operaciones cloud.
"""

import json
import shlex
from pathlib import Path

BASE = Path(__file__).resolve().parent


def preparar_comando():
    config = json.loads(
        (BASE / "config_dataproc.json").read_text()
    )

    for campo in (
        "cloud_execution_authorized",
        "silver_consumption_authorized",
        "production_authorized",
    ):
        if config[campo] is not False:
            raise PermissionError(
                "Configuracion no autorizada"
            )

    total = (
        config["driver_cores"]
        + config["executor_cores"]
        * config["executor_instances"]
    )

    if total != config["total_vcpu"] or total > 12:
        raise ValueError("Presupuesto CPU incompatible")

    if config["mode"] != "smoke-test":
        raise PermissionError("Modo no autorizado")

    propiedades = ",".join([
        "spark.dataproc.driver.compute.tier=standard",
        "spark.dataproc.executor.compute.tier=standard",
        f"spark.driver.cores={config['driver_cores']}",
        f"spark.executor.cores={config['executor_cores']}",
        f"spark.executor.instances={config['executor_instances']}",
        "spark.dynamicAllocation.enabled=false",
    ])

    comando = [
        "gcloud", "dataproc", "batches",
        "submit", "pyspark",
        "src/gold/dataproc/main_gold.py",
        f"--project={config['project']}",
        f"--region={config['region']}",
        f"--version={config['runtime_version']}",
        f"--service-account={config['service_account']}",
        "--batch=BATCH_ID_PENDIENTE",
        "--deps-bucket=gs://BUCKET_DEPENDENCIAS_AUTORIZADO",
        "--subnet=SUBRED_AUTORIZADA",
        "--py-files=outputs/dataproc/bvl_gold_modules.zip",
        f"--properties={propiedades}",
        f"--ttl={config['ttl']}",
        "--",
        "--modo", config["mode"],
    ]

    return comando


if __name__ == "__main__":
    print("BORRADOR. NO EJECUTAR SIN AUTORIZACION.")
    print()
    print(shlex.join(preparar_comando()))
    print()
    print("EJECUCION_CLOUD_AUTORIZADA: False")
    print("COMANDO_DATAPROC_PREPARADO_OK")
