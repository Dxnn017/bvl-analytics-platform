#!/usr/bin/env python3

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

p = argparse.ArgumentParser()
p.add_argument("--bronze", required=True)
p.add_argument("--current", required=True)
p.add_argument("--ingestion-date", required=True)
a = p.parse_args()

BRONZE_ROOT = "/datalake/bvl/bronze/smv/principales_cuentas/"
SILVER = "/datalake/bvl/silver/finanzas_empresariales/principales_cuentas"

if not a.bronze.startswith(BRONZE_ROOT):
    p.error("Ruta Bronze no autorizada")

if not (
    a.current == SILVER
    or a.current.startswith("/datalake/bvl/staging/")
):
    p.error("Ruta current no autorizada")

try:
    datetime.strptime(a.ingestion_date, "%Y-%m-%d")
except ValueError:
    p.error("Fecha de ingesta invalida")

spark = (
    SparkSession.builder
    .appName("BVL-Principales-Cuentas-Incremental")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

ORIGINALES = [
    "RPJ", "RUC", "NombreEmpresa", "TipoEmpresa",
    "TipoSector", "CIIU", "Ejercicio", "Trimestre",
    "TipoInformacion", "MetodoFlujoEfectivo", "Moneda",
    "ActivoTotal", "PatrimonioTotal", "TotalIngreso",
    "UtilidadNeta", "PasivoTotal"
]

CLAVE = [
    "ruc", "ejercicio", "trimestre",
    "tipo_informacion", "rpj",
    "metodo_flujo_efectivo", "moneda"
]

FILA = [
    "rpj", "ruc", "nombre_empresa", "tipo_empresa",
    "tipo_sector", "ciiu", "ejercicio", "trimestre",
    "tipo_informacion", "metodo_flujo_efectivo",
    "moneda", "activo_total", "patrimonio_total",
    "total_ingreso", "utilidad_neta", "pasivo_total"
]

def huella(columnas):
    return F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(
                    F.col(c).cast("string"),
                    F.lit("<NULL>")
                )
                for c in columnas
            ]
        ),
        256
    )

try:
    actual = (
        spark.read.parquet(a.current)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    entrada = (
        spark.read
        .option("multiLine", "true")
        .json(a.bronze)
    )

    if set(ORIGINALES).issubset(entrada.columns):
        raw = entrada
    else:
        candidatos = [
            c for c in ["Resultado", "resultado", "data"]
            if c in entrada.columns
        ]

        if len(candidatos) != 1:
            raise RuntimeError("Estructura Bronze no reconocida")

        raw = (
            entrada
            .select(F.explode(candidatos[0]).alias("r"))
            .select("r.*")
        )

    if not set(ORIGINALES).issubset(raw.columns):
        raise RuntimeError("Faltan columnas Bronze")

    raw_total = raw.count()

    texto = [
        ("RPJ", "rpj"),
        ("RUC", "ruc"),
        ("NombreEmpresa", "nombre_empresa"),
        ("TipoEmpresa", "tipo_empresa"),
        ("TipoSector", "tipo_sector"),
        ("CIIU", "ciiu"),
        ("Trimestre", "trimestre"),
        ("TipoInformacion", "tipo_informacion"),
        ("MetodoFlujoEfectivo", "metodo_flujo_efectivo"),
        ("Moneda", "moneda")
    ]

    numeros = [
        ("ActivoTotal", "activo_total"),
        ("PatrimonioTotal", "patrimonio_total"),
        ("TotalIngreso", "total_ingreso"),
        ("UtilidadNeta", "utilidad_neta"),
        ("PasivoTotal", "pasivo_total")
    ]

    lote = raw.select(
        *[
            F.trim(F.col(o).cast("string")).alias(n)
            for o, n in texto
        ],
        F.col("Ejercicio").cast("int").alias("ejercicio"),
        *[
            F.col(o).cast("double").alias(n)
            for o, n in numeros
        ]
    )

    lote = (
        lote
        .withColumn(
            "trimestre_num",
            F.regexp_extract(
                "trimestre", r"^([1-4])", 1
            ).cast("int")
        )
        .withColumn(
            "ruc_valido",
            F.col("ruc").rlike(r"^[0-9]{11}$")
            & (~F.col("ruc").isin("00000000000", "0"))
        )
        .withColumn("fuente_datos", F.lit("SMV"))
        .withColumn(
            "fecha_ingesta",
            F.to_date(F.lit(a.ingestion_date))
        )
        .withColumn("clave_negocio", huella(CLAVE))
        .withColumn("id_registro", huella(FILA))
        .dropDuplicates(["id_registro"])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    total_actual = actual.count()
    total_lote = lote.count()

    if total_actual != actual.select(
        "id_registro"
    ).distinct().count():
        raise RuntimeError("Silver tiene IDs duplicados")

    nuevos = (
        lote.join(
            actual.select("id_registro"),
            "id_registro",
            "left_anti"
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    cantidad_nuevos = nuevos.count()

    # Un ID nuevo con una clave existente puede ser
    # una correccion o un registro adicional legitimo.
    conflictos_existentes = nuevos.join(
        actual.select("clave_negocio").distinct(),
        "clave_negocio",
        "left_semi"
    )

    grupos_nuevos = (
        nuevos.groupBy("clave_negocio")
        .count()
        .filter(F.col("count") > 1)
        .select("clave_negocio")
    )

    conflictos_internos = nuevos.join(
        grupos_nuevos,
        "clave_negocio",
        "inner"
    )

    conflictos = (
        conflictos_existentes
        .unionByName(conflictos_internos)
        .dropDuplicates(["id_registro"])
    )

    cantidad_conflictos = conflictos.count()

    print(
        f"PRINCIPALES|silver_antes={total_actual}|"
        f"bronze={raw_total}|lote_unico={total_lote}|"
        f"nuevos={cantidad_nuevos}|"
        f"sin_cambio={total_lote-cantidad_nuevos}|"
        f"conflictos={cantidad_conflictos}",
        flush=True
    )

    if cantidad_nuevos == 0:
        print(
            "SIN_CAMBIOS|0 particiones modificadas; Silver intacto",
            flush=True
        )

    else:
        run = (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            + "_" + uuid4().hex[:8]
        )

        raiz = (
            "/datalake/bvl/staging/"
            f"principales_cuentas_incremental/run_{run}"
        )

        if cantidad_conflictos:
            ruta = f"{raiz}/conflictos"

            conflictos.write.mode(
                "errorifexists"
            ).parquet(ruta)

            print(
                f"REVISION_OBLIGATORIA|{cantidad_conflictos}|{ruta}",
                flush=True
            )
            print("SILVER_OFICIAL|SIN_CAMBIOS", flush=True)

            raise RuntimeError(
                "Posibles correcciones o colisiones: "
                "publicacion bloqueada"
            )

        # Reconstruir los indicadores de colisiones
        # conservando todas las filas historicas.
        combinado = (
            actual.drop(
                "cantidad_clave_negocio",
                "colision_clave_negocio"
            )
            .unionByName(
                nuevos.drop(
                    "cantidad_clave_negocio",
                    "colision_clave_negocio"
                )
            )
        )

        conteos = (
            combinado.groupBy("clave_negocio")
            .count()
            .withColumnRenamed(
                "count", "cantidad_clave_negocio"
            )
        )

        final = (
            combinado
            .join(conteos, "clave_negocio", "left")
            .withColumn(
                "colision_clave_negocio",
                F.col("cantidad_clave_negocio") > 1
            )
            .select(*actual.columns)
        )

        esperado = total_actual + cantidad_nuevos

        if (
            final.count() != esperado
            or final.select("id_registro").distinct().count()
            != esperado
        ):
            raise RuntimeError("Conteos o IDs invalidos")

        perdidos = (
            actual
            .select(*actual.columns)
            .exceptAll(final)
            .limit(1)
            .count()
        )

        if perdidos:
            raise RuntimeError(
                "El staging alteraria datos historicos"
            )

        destino = f"{raiz}/snapshot"

        (
            final.write
            .mode("errorifexists")
            .option("compression", "snappy")
            .partitionBy("ejercicio")
            .parquet(destino)
        )

        comprobado = spark.read.parquet(destino)

        if (
            comprobado.count() != esperado
            or comprobado.select("id_registro")
            .distinct().count() != esperado
        ):
            raise RuntimeError("Staging invalido")

        salida = Path("outputs/quality/incremental")
        salida.mkdir(parents=True, exist_ok=True)

        manifiesto = salida / f"principales_cuentas_{run}.json"

        manifiesto.write_text(
            json.dumps({
                "estado": "STAGING_VALIDADO_NO_PUBLICADO",
                "current": a.current,
                "bronze": a.bronze,
                "antes": total_actual,
                "nuevas": cantidad_nuevos,
                "esperado": esperado,
                "staging": destino
            }, indent=2),
            encoding="utf-8"
        )

        print(f"STAGING_VALIDADO|{destino}", flush=True)
        print(f"MANIFEST|{manifiesto}", flush=True)
        print("SILVER_OFICIAL|SIN_CAMBIOS", flush=True)

finally:
    spark.stop()
