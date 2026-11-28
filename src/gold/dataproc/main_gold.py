"""Entrada candidata para Dataproc Serverless.

Solo permite smoke test de infraestructura.
No lee Silver oficial ni publica Gold.
"""

import argparse
import json
import sys

from pyspark.sql import SparkSession

from ejecutor_portable import (
    DEPENDENCIAS,
    BLOQUEADOS,
    validar_producto,
    validar_fuentes,
)

from adaptador_gcs_quality import leer_objeto_gcs

from consumidor_gcs_seguro import consumir_objeto_verificado

from control_concurrencia_gcs import (
    verificar_generaciones_vigentes,
)

from control_versiones_gold import (
    preparar_actualizacion_gold,
)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--modo",
        choices=["smoke-test"],
        required=True,
    )

    argumentos = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("BVL-Gold-Dataproc-Candidato")
        .getOrCreate()
    )

    try:
        assert spark.range(10).count() == 10

        def comprobar_worker(valor):
            from ejecutor_portable import validar_producto

            validar_producto("mercado_valor_diario")

            return valor * 2

        resultados = (
            spark.sparkContext
            .parallelize(range(10), 2)
            .map(comprobar_worker)
            .collect()
        )

        assert resultados == [x * 2 for x in range(10)]

        print("MODULOS_ZIP_EN_WORKERS_OK")

        resultado = {
            "estado": "SMOKE_TEST_OK",
            "modo": argumentos.modo,
            "python": sys.version.split()[0],
            "spark": spark.version,
            "productos_candidatos": sorted(DEPENDENCIAS),
            "productos_bloqueados": sorted(BLOQUEADOS),
            "consumo_silver_oficial": False,
            "ejecucion_productiva_autorizada": False,
            "publicacion_autorizada": False,
        }

        print(json.dumps(resultado, indent=2))

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
