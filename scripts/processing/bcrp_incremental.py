#!/usr/bin/env python3

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CONFIG = {
    "tipo_cambio": {
        "campo": "PD04638PD",
        "valor": "tipo_cambio_venta",
        "bronze_root": "/datalake/bvl/bronze/bcrp/tipo_cambio/",
        "silver": "/datalake/bvl/silver/macroeconomia/tipo_cambio",
    },
    "embig": {
        "campo": "PD04709XD",
        "valor": "embig_peru",
        "bronze_root": "/datalake/bvl/bronze/bcrp/embig/",
        "silver": "/datalake/bvl/silver/macroeconomia/embig",
    },
}

MESES = {
    "Ene": "01",
    "Feb": "02",
    "Mar": "03",
    "Abr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Ago": "08",
    "Set": "09",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dic": "12",
}

p = argparse.ArgumentParser()

p.add_argument(
    "--dataset",
    required=True,
    choices=CONFIG.keys()
)

p.add_argument("--current")
p.add_argument("--bronze", required=True)

a = p.parse_args()

cfg = CONFIG[a.dataset]

current = a.current or cfg["silver"]

if not a.bronze.startswith(cfg["bronze_root"]):
    p.error("Ruta Bronze no autorizada")

if not (
    current.startswith("/datalake/bvl/silver/")
    or current.startswith("/datalake/bvl/staging/")
):
    p.error("Ruta current no autorizada")

spark = (
    SparkSession.builder
    .appName(f"BVL-BCRP-Incremental-{a.dataset}")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def normalizar_fecha(columna):
    c = F.trim(columna)

    dia = F.substring(c, 1, 2)
    mes_txt = F.substring(c, 3, 3)
    anio_2 = F.substring(c, 6, 2).cast("int")

    mes = None

    for nombre, numero in MESES.items():
        condicion = mes_txt == nombre

        if mes is None:
            mes = F.when(condicion, F.lit(numero))
        else:
            mes = mes.when(condicion, F.lit(numero))

    mes = mes.otherwise(F.lit(None))

    anio = F.when(
        anio_2 >= 90,
        1900 + anio_2
    ).otherwise(
        2000 + anio_2
    )

    fecha_texto = F.concat(
        anio.cast("string"),
        F.lit("-"),
        mes,
        F.lit("-"),
        dia
    )

    return F.to_date(fecha_texto, "yyyy-MM-dd")


try:

    # =========================================
    # LEER SILVER ACTUAL
    # =========================================

    original = spark.read.parquet(current)

    # =========================================
    # LEER BRONZE CSV
    # =========================================

    raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(a.bronze)
    )

    bronze_total = raw.count()

    if "_c0" not in raw.columns:
        raise RuntimeError("No existe columna _c0")

    if cfg["campo"] not in raw.columns:
        raise RuntimeError(
            f"No existe campo {cfg['campo']}"
        )

    # =========================================
    # NORMALIZAR
    # =========================================

    nuevo = (
        raw
        .filter(F.col("_c0").isNotNull())
        .select(
            normalizar_fecha(
                F.col("_c0")
            ).alias("fecha"),

            F.when(
                F.trim(F.col(cfg["campo"])).isin(
                    "n.d.",
                    "N.D.",
                    ""
                ),
                None
            )
            .otherwise(
                F.regexp_replace(
                    F.col(cfg["campo"]),
                    ",",
                    ""
                )
            )
            .cast("double")
            .alias(cfg["valor"])
        )
        .filter(F.col("fecha").isNotNull())
        .dropDuplicates(["fecha"])
        .withColumn(
            "anio",
            F.year("fecha")
        )
        .withColumn(
            "mes",
            F.month("fecha")
        )
    )

    lote_total = nuevo.count()
    silver_total = original.count()

    # =========================================
    # VALIDAR CLAVES
    # =========================================

    silver_unicas = (
        original
        .select("fecha")
        .distinct()
        .count()
    )

    lote_unicas = (
        nuevo
        .select("fecha")
        .distinct()
        .count()
    )

    if silver_unicas != silver_total:
        raise RuntimeError(
            "Silver contiene fechas duplicadas"
        )

    if lote_unicas != lote_total:
        raise RuntimeError(
            "Lote contiene fechas duplicadas"
        )

    # =========================================
    # DETECTAR NUEVOS
    # =========================================

    nuevos_df = nuevo.join(
        original.select("fecha"),
        ["fecha"],
        "left_anti"
    )

    nuevos = nuevos_df.count()

    # =========================================
    # DETECTAR CORRECCIONES
    # =========================================

    comunes = (
        nuevo.alias("n")
        .join(
            original.alias("o"),
            ["fecha"],
            "inner"
        )
    )

    correcciones_df = comunes.filter(
        ~F.col(
            f"n.{cfg['valor']}"
        ).eqNullSafe(
            F.col(f"o.{cfg['valor']}")
        )
    )

    corregidas = correcciones_df.count()

    sin_cambio = lote_total - nuevos - corregidas

    # =========================================
    # REPORTE
    # =========================================

    print(
        f"===== AUDITORIA INCREMENTAL BCRP {a.dataset.upper()} =====",
        flush=True
    )

    print(
        f"bronze_filas: {bronze_total}",
        flush=True
    )

    print(
        f"silver_antes: {silver_total}",
        flush=True
    )

    print(
        f"filas_lote_validas: {lote_total}",
        flush=True
    )

    print(
        f"nuevas: {nuevos}",
        flush=True
    )

    print(
        f"sin_cambio: {sin_cambio}",
        flush=True
    )

    print(
        f"corregidas: {corregidas}",
        flush=True
    )

    # Las correcciones requieren revisión.
    if corregidas:
        print(
            "CORRECCIONES_DETECTADAS|NO_PUBLICAR",
            flush=True
        )

        correcciones_df.select(
            "fecha",
            F.col(
                f"o.{cfg['valor']}"
            ).alias("valor_silver"),
            F.col(
                f"n.{cfg['valor']}"
            ).alias("valor_bronze")
        ).show(20, truncate=False)

        raise RuntimeError(
            "Correcciones BCRP detectadas"
        )

    # =========================================
    # IDEMPOTENCIA
    # =========================================

    if nuevos == 0:

        print(
            "SIN_CAMBIOS|0 particiones modificadas; Silver intacto",
            flush=True
        )

    else:

        # =====================================
        # STAGING
        # =====================================

        run = (
            datetime.now(timezone.utc)
            .strftime("%Y%m%dT%H%M%S")
            + "_"
            + uuid4().hex[:6]
        )

        staging = (
            f"/datalake/bvl/staging/"
            f"bcrp_{a.dataset}_incremental/"
            f"run_{run}"
        )

        particiones = (
            nuevos_df
            .select("anio", "mes")
            .distinct()
            .collect()
        )

        manifest = {
            "dataset": a.dataset,
            "estado": "STAGING_VALIDADO_NO_PUBLICADO",
            "current": current,
            "bronze": a.bronze,
            "silver_antes": silver_total,
            "nuevas": nuevos,
            "corregidas": corregidas,
            "particiones": [],
        }

        for fila in particiones:

            anio = int(fila["anio"])
            mes = int(fila["mes"])

            existente = original.filter(
                (F.col("anio") == anio)
                & (F.col("mes") == mes)
            )

            agregado = nuevos_df.filter(
                (F.col("anio") == anio)
                & (F.col("mes") == mes)
            )

            combinado = (
                existente
                .unionByName(agregado)
                .dropDuplicates(["fecha"])
            )

            destino = (
                f"{staging}/"
                f"anio={anio}/mes={mes}"
            )

            esperado = combinado.count()

            (
                combinado
                .drop("anio", "mes")
                .write
                .mode("errorifexists")
                .option("compression", "snappy")
                .parquet(destino)
            )

            verificado = (
                spark.read
                .parquet(destino)
                .count()
            )

            if verificado != esperado:
                raise RuntimeError(
                    f"Particion invalida {anio}-{mes}"
                )

            manifest["particiones"].append({
                "anio": anio,
                "mes": mes,
                "filas": verificado,
                "ruta": destino,
            })

            print(
                f"PARTICION_STAGE|"
                f"{anio}|{mes}|"
                f"{verificado}|{destino}",
                flush=True
            )

        # =====================================
        # MANIFIESTO LOCAL
        # =====================================

        out = Path(
            "outputs/quality/incremental"
        )

        out.mkdir(
            parents=True,
            exist_ok=True
        )

        manifest_path = (
            out
            / f"bcrp_{a.dataset}_{run}.json"
        )

        manifest_path.write_text(
            json.dumps(
                manifest,
                ensure_ascii=False,
                indent=2
            ),
            encoding="utf-8"
        )

        print(
            f"STAGING_VALIDADO|{staging}",
            flush=True
        )

        print(
            f"MANIFEST|{manifest_path}",
            flush=True
        )

        print(
            "SILVER_OFICIAL|SIN_CAMBIOS",
            flush=True
        )

finally:
    spark.stop()
