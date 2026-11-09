#!/usr/bin/env python3

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CLAVES = ["fecha_negociacion", "codigo_sab"]

SILVER = "/datalake/bvl/silver/mercado_diario/montos_sab"

STAGING = "/datalake/bvl/staging/montos_sab_incremental"

# ==========================================
# 1. ARGUMENTOS
# ==========================================

p = argparse.ArgumentParser()

p.add_argument("--current", default=SILVER)
p.add_argument("--bronze", required=True)
p.add_argument("--ingestion-date", required=True)

a = p.parse_args()

datetime.strptime(a.ingestion_date, "%Y-%m-%d")

if not a.bronze.startswith(
    "/datalake/bvl/bronze/smv/montos_sab/"
):
    p.error("Fuente Bronze no autorizada")

if not (
    a.current.startswith("/datalake/bvl/silver/")
    or a.current.startswith("/datalake/bvl/staging/")
):
    p.error("Ruta Silver no autorizada")

# ==========================================
# 2. INICIALIZAR SPARK
# ==========================================

spark = (
    SparkSession.builder
    .appName("BVL-SAB-Incremental")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:

    # ======================================
    # 3. LEER BRONZE Y SILVER
    # ======================================

    original = spark.read.parquet(a.current)

    wrappers = (
        spark.read
        .option("multiLine", "true")
        .json(a.bronze)
    )

    if "Resultado" not in wrappers.columns:
        raise RuntimeError("Falta array Resultado")

    raw = (
        wrappers
        .select(F.explode("Resultado").alias("r"))
        .select("r.*")
    )

    for c in ["_FechaNegociacionConsulta", "CodigoSAB"]:
        if c not in raw.columns:
            raise RuntimeError(f"Falta columna: {c}")

    # ======================================
    # 4. NORMALIZACION
    # ======================================

    nuevo = (
        raw
        .withColumn(
            "fecha_negociacion",
            F.coalesce(
                F.to_date(
                    F.substring(
                        "_FechaNegociacionConsulta", 1, 10
                    ),
                    "dd/MM/yyyy"
                ),
                F.to_date(
                    F.substring(
                        "_FechaNegociacionConsulta", 1, 10
                    ),
                    "yyyy-MM-dd"
                )
            )
        )
        .withColumn(
            "codigo_sab",
            F.trim(F.col("CodigoSAB").cast("string"))
        )
        .withColumn(
            "anio", F.year("fecha_negociacion")
        )
        .withColumn(
            "mes", F.month("fecha_negociacion")
        )
        .withColumn(
            "fuente_datos", F.lit("SMV")
        )
        .withColumn(
            "fecha_ingesta",
            F.to_date(F.lit(a.ingestion_date))
        )
    )

    # ======================================
    # 5. VALIDAR ESQUEMAS
    # ======================================

    if set(nuevo.columns) != set(original.columns):
        raise RuntimeError(
            "Esquemas diferentes. Faltan: "
            + str(set(original.columns) - set(nuevo.columns))
            + " / Sobran: "
            + str(set(nuevo.columns) - set(original.columns))
        )

    pruebas = [
        F.max(
            (
                F.col(c).isNotNull()
                & F.col(c).cast(
                    original.schema[c].dataType
                ).isNull()
            ).cast("int")
        ).alias(c)
        for c in original.columns
        if nuevo.schema[c].dataType != original.schema[c].dataType
    ]

    if pruebas:
        errores = nuevo.agg(*pruebas).first().asDict()
        if any(v for v in errores.values()):
            raise RuntimeError(
                f"Conversiones invalidas: {errores}"
            )

    nuevo = nuevo.select(
        *[
            F.col(c)
            .cast(original.schema[c].dataType)
            .alias(c)
            for c in original.columns
        ]
    )

    # ======================================
    # 6. CALIDAD Y CLAVES
    # ======================================

    for nombre, df in [
        ("Silver", original),
        ("Lote", nuevo)
    ]:
        invalidos = df.filter(
            F.col("fecha_negociacion").isNull()
            | F.col("codigo_sab").isNull()
            | (F.length(F.trim("codigo_sab")) == 0)
        ).limit(1).count()

        if invalidos:
            raise RuntimeError(
                f"{nombre}: claves invalidas"
            )

    viejo_total = original.count()

    if original.select(*CLAVES).distinct().count() != viejo_total:
        raise RuntimeError(
            "Silver contiene claves duplicadas"
        )

    lote_total = nuevo.count()

    if lote_total == 0:
        raise RuntimeError("Lote vacio")

    # ======================================
    # 7. DETECTAR DUPLICADOS Y CAMBIOS
    # ======================================

    columnas = [
        c for c in original.columns
        if c not in CLAVES + ["fecha_ingesta"]
    ]

    nuevo_hash = nuevo.withColumn(
        "_h",
        F.sha2(
            F.to_json(
                F.struct(*columnas),
                {"ignoreNullFields": "false"}
            ),
            256
        )
    )

    ambiguos = (
        nuevo_hash
        .groupBy(*CLAVES)
        .agg(F.countDistinct("_h").alias("n"))
        .filter("n > 1")
        .limit(1)
        .count()
    )

    if ambiguos:
        raise RuntimeError(
            "Misma clave con valores diferentes"
        )

    nuevo = nuevo.dropDuplicates(CLAVES)

    lote_unico = nuevo.count()
    repetidas = lote_total - lote_unico

    agregados = nuevo.join(
        original.select(*CLAVES),
        CLAVES,
        "left_anti"
    )

    nuevas = agregados.count()

    ambos = (
        nuevo.alias("n")
        .join(original.alias("o"), CLAVES)
    )

    diferencias = None

    for c in columnas:
        condicion = ~F.col(
            f"n.`{c}`"
        ).eqNullSafe(F.col(f"o.`{c}`"))

        diferencias = (
            condicion if diferencias is None
            else diferencias | condicion
        )

    corregidas = (
        ambos.filter(diferencias)
        .select(*CLAVES)
        .distinct()
        .count()
    )

    # ======================================
    # 8. AUDITORIA
    # ======================================

    print("===== AUDITORIA INCREMENTAL MONTOS SAB =====", flush=True)
    print(f"silver_antes: {viejo_total}", flush=True)
    print(f"filas_lote: {lote_total}", flush=True)
    print(f"repetidas_lote: {repetidas}", flush=True)
    print(f"nuevas: {nuevas}", flush=True)
    print(
        f"sin_cambio: {lote_unico-nuevas-corregidas}",
        flush=True
    )
    print(f"corregidas: {corregidas}", flush=True)

    if corregidas:
        raise RuntimeError(
            "Correcciones detectadas: revisar antes de publicar"
        )

    # ======================================
    # 9. IDEMPOTENCIA
    # ======================================

    if nuevas == 0:
        print(
            "SIN_CAMBIOS|0 particiones modificadas; Silver intacto",
            flush=True
        )

    else:

        # ==================================
        # 10. STAGING INCREMENTAL
        # ==================================

        run = (
            datetime.now(timezone.utc)
            .strftime("%Y%m%dT%H%M%S")
            + "_"
            + uuid4().hex[:6]
        )

        ruta = f"{STAGING}/run_{run}"

        meses = (
            agregados
            .select("anio", "mes")
            .distinct()
            .collect()
        )

        manifest = {
            "estado": "STAGING_VALIDADO_NO_PUBLICADO",
            "current": a.current,
            "source": a.bronze,
            "nuevas": nuevas,
            "particiones": []
        }

        for fila in meses:

            y = int(fila.anio)
            m = int(fila.mes)

            anterior = original.filter(
                (F.col("anio") == y)
                & (F.col("mes") == m)
            )

            parcial = agregados.filter(
                (F.col("anio") == y)
                & (F.col("mes") == m)
            )

            combinado = anterior.unionByName(parcial)

            destino = f"{ruta}/anio={y}/mes={m}"

            esperado = combinado.count()

            (
                combinado
                .drop("anio", "mes")
                .write
                .mode("errorifexists")
                .option("compression", "snappy")
                .parquet(destino)
            )

            comprobado = spark.read.parquet(
                destino
            ).count()

            if esperado != comprobado:
                raise RuntimeError(
                    f"Staging invalido: {y}-{m}"
                )

            manifest["particiones"].append({
                "anio": y,
                "mes": m,
                "ruta": destino,
                "filas": comprobado
            })

            print(
                f"PARTICION_STAGE|{y}|{m}|{comprobado}|{destino}",
                flush=True
            )

        # ==================================
        # 11. GUARDAR MANIFIESTO
        # ==================================

        salida = Path("outputs/quality/incremental")

        salida.mkdir(
            parents=True,
            exist_ok=True
        )

        archivo = salida / f"montos_sab_{run}.json"

        archivo.write_text(
            json.dumps(
                manifest,
                indent=2,
                ensure_ascii=False
            ),
            encoding="utf-8"
        )

        print(f"STAGING_VALIDADO|{ruta}", flush=True)
        print(f"MANIFEST|{archivo}", flush=True)
        print("SILVER_OFICIAL|SIN_CAMBIOS", flush=True)

finally:
    spark.stop()
