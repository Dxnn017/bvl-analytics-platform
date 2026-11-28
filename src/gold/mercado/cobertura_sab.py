"""Contrato candidato BVL-SILVER-SAB-COVERAGE-v0.1."""

from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    LongType, BooleanType,
)

ESQUEMA = StructType([
    StructField("fecha", DateType(), False),
    StructField("registros_elegibles", LongType(), True),
    StructField("registros_cuarentena", LongType(), True),
    StructField("archivos_excluidos", LongType(), True),
    StructField("estado_cobertura", StringType(), False),
    StructField("version_silver", StringType(), True),
    StructField("scope_quality_id", StringType(), True),
    StructField("trazabilidad_verificada", BooleanType(), False),
])

CAMPOS = [c.name for c in ESQUEMA.fields if c.name != "fecha"]
COLUMNAS_COBERTURA = CAMPOS + ["cobertura_completa"]


def propagar_cobertura(gold, cobertura=None, version_silver=None):
    """Agrega evidencia candidata sin autorizar consumo productivo."""

    filas = []
    fechas = set()

    if cobertura is not None:
        if not isinstance(cobertura, list) or not version_silver:
            raise ValueError("Cobertura o version Silver invalida.")

        for r in cobertura:
            if r["version_silver"] != version_silver:
                raise ValueError("Version Silver incompatible.")

            fecha = date.fromisoformat(r["fecha_negociacion"])

            if fecha in fechas:
                raise ValueError("COBERTURA_SAB_FECHA_DUPLICADA")

            fechas.add(fecha)

            estado = r["estado_cobertura"]
            traza = r["trazabilidad_verificada"]
            scope = r["scope_quality_id"]

            if estado not in {"COMPLETA", "PARCIAL", "DESCONOCIDA"}:
                raise ValueError("Estado de cobertura invalido.")

            if type(traza) is not bool:
                raise ValueError("Trazabilidad invalida.")

            conteos = [
                r["registros_elegibles"],
                r["registros_cuarentena"],
                r["archivos_excluidos"],
            ]

            for n in conteos:
                if n is not None and (type(n) is not int or n < 0):
                    raise ValueError("Conteo de cobertura invalido.")

            if estado != "DESCONOCIDA":
                if not traza or not scope or None in conteos:
                    raise ValueError("Evidencia de cobertura insuficiente.")

                if estado == "COMPLETA" and any(conteos[1:]):
                    raise ValueError("Cobertura completa incompatible.")

                if estado == "PARCIAL" and not any(conteos[1:]):
                    raise ValueError("Cobertura parcial sin exclusiones.")

            filas.append((
                fecha, *conteos, estado,
                version_silver, scope, traza,
            ))

    metadatos = gold.sparkSession.createDataFrame(filas, ESQUEMA)

    gold = gold.drop(*COLUMNAS_COBERTURA)
    gold = gold.join(metadatos, "fecha", "left")

    incompatibles = gold.filter(
        F.col("registros_elegibles").isNotNull()
        & (F.col("registros_elegibles") != F.col("cantidad_sab_diaria"))
    )

    if incompatibles.limit(1).count():
        raise ValueError("COBERTURA_SAB_CONTEOS_INCOMPATIBLES")

    return (
        gold.withColumn(
            "estado_cobertura",
            F.coalesce("estado_cobertura", F.lit("DESCONOCIDA"))
        )
        .withColumn(
            "trazabilidad_verificada",
            F.coalesce("trazabilidad_verificada", F.lit(False))
        )
        .withColumn(
            "cobertura_completa",
            F.col("estado_cobertura") == "COMPLETA"
        )
    )
