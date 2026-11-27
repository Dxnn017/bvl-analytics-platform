"""Recálculo incremental local de empresa_trimestre.

No escribe archivos ni publica datos oficiales.
"""

import sys
from pathlib import Path

from pyspark.sql import functions as F

DIRECTORIO_EMPRESAS = Path(__file__).resolve().parent.parent / "empresas"
sys.path.insert(0, str(DIRECTORIO_EMPRESAS))

from build_empresa_trimestre import construir_gold, validar_clave


GRUPO_FINANCIERO = ["rpj", "ejercicio", "trimestre_num"]


def recalcular_empresa_trimestre(
    silver_finanzas,
    gold_base,
    grupos_afectados,
):
    """Reconstruye grupos financieros completos desde Silver vigente."""

    grupos = grupos_afectados.select(
        *GRUPO_FINANCIERO
    ).distinct()

    # Evitar recálculos ambiguos.
    condicion_invalida = (
        F.col("rpj").isNull()
        | (F.trim(F.col("rpj").cast("string")) == "")
        | F.col("ejercicio").isNull()
        | F.col("trimestre_num").isNull()
    )

    if grupos.filter(condicion_invalida).limit(1).count():
        raise ValueError("Existen grupos financieros afectados inválidos.")

    # Recuperar todos los registros vigentes de los grupos.
    silver_afectado = silver_finanzas.join(
        grupos,
        GRUPO_FINANCIERO,
        "left_semi",
    )

    validar_clave(
        silver_afectado,
        ["id_registro"],
        "Silver financiero afectado",
    )

    # Recalcular colisiones e indicadores de cada grupo.
    gold_recalculado = construir_gold(silver_afectado)

    # Retirar las versiones anteriores de los grupos afectados.
    gold_conservado = gold_base.join(
        grupos,
        GRUPO_FINANCIERO,
        "left_anti",
    )

    # Una eliminación completa en Silver elimina el grupo de Gold.
    gold_candidato = gold_conservado.unionByName(
        gold_recalculado
    )

    validar_clave(
        gold_candidato,
        ["id_registro"],
        "Gold empresa_trimestre candidato",
    )

    return gold_candidato


def resolver_grupos_empresa_desde_tarea(spark, tarea):
    """Resuelve grupos financieros afectados desde una tarea Gold."""

    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType
    )

    if tarea.get("producto_gold") != "empresa_trimestre":
        raise ValueError("La tarea no corresponde a empresa_trimestre.")

    if tarea.get("requiere_resolver_detalle_externo", False):
        raise ValueError("Existen cambios externos pendientes de resolver.")

    cambios = tarea.get("cambios_directos_declarados")
    if not isinstance(cambios, list) or not cambios:
        raise ValueError("No existen cambios financieros declarados.")

    grupos = set()

    for cambio in cambios:
        if cambio.get("dataset") != "principales_cuentas":
            raise ValueError("Dataset incompatible con empresa_trimestre.")

        operaciones = cambio.get("operaciones_declaradas")
        if not isinstance(operaciones, list):
            raise ValueError("Falta el detalle de operaciones.")

        if len(operaciones) != cambio.get("total_cambios_declarados"):
            raise ValueError("Cantidad de operaciones incompatible.")

        for operacion in operaciones:
            if operacion.get("operacion") not in {
                "INSERT", "UPDATE", "DELETE"
            }:
                raise ValueError("Operacion financiera no soportada.")

            for campo in ("clave_anterior", "clave_nueva"):
                clave = operacion.get(campo)
                if clave is None:
                    continue

                if not isinstance(clave, dict):
                    raise ValueError("Clave financiera invalida.")

                requeridos = ("rpj", "ejercicio", "trimestre_num")
                if not all(campo in clave for campo in requeridos):
                    raise ValueError(
                        "Falta el grupo financiero anterior o nuevo. "
                        "Una clave id_registro aislada no es suficiente."
                    )

                rpj = str(clave["rpj"]).strip()
                if not rpj:
                    raise ValueError("RPJ financiero invalido.")

                ejercicio = int(clave["ejercicio"])
                trimestre = int(clave["trimestre_num"])

                if not 1900 <= ejercicio <= 9999 or not 1 <= trimestre <= 4:
                    raise ValueError("Periodo financiero invalido.")

                grupos.add((rpj, ejercicio, trimestre))

    if not grupos:
        raise ValueError("No se encontraron grupos financieros afectados.")

    esquema = StructType([
        StructField("rpj", StringType(), False),
        StructField("ejercicio", IntegerType(), False),
        StructField("trimestre_num", IntegerType(), False),
    ])

    return spark.createDataFrame(sorted(grupos), schema=esquema)
