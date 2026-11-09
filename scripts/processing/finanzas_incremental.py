#!/usr/bin/env python3

import argparse
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel


CONFIG = {
    "estado_resultados": {
        "renames": {}
    },
    "situacion_financiera": {
        "renames": {}
    },
    "flujo_efectivo": {
        "renames": {}
    },
    "cambios_patrimonio": {
        "renames": {
            "DescripcionColumna": "descripcion_columna",
            "OrdenColumna": "orden_columna",
        }
    },
}


BASE_KEY = [
    "ruc",
    "rpj",
    "ejercicio",
    "trimestre",
    "tipo_informacion",
    "moneda",
    "cuenta",
    "descripcion_cuenta",
    "metodo_flujo_efectivo",
    "fecha_registro",
]


parser = argparse.ArgumentParser()

parser.add_argument(
    "--dataset",
    required=True,
    choices=CONFIG.keys()
)

parser.add_argument("--current")

parser.add_argument(
    "--bronze",
    required=True
)

args = parser.parse_args()

dataset = args.dataset

silver_default = (
    f"/datalake/bvl/silver/"
    f"finanzas_empresariales/{dataset}"
)

current = args.current or silver_default

bronze_root = (
    f"/datalake/bvl/bronze/smv/{dataset}"
)

if not args.bronze.startswith(bronze_root):
    parser.error("Ruta Bronze no autorizada")

if not (
    current.startswith("/datalake/bvl/silver/")
    or current.startswith("/datalake/bvl/staging/")
):
    parser.error("Ruta current no autorizada")


# ============================================
# REUTILIZAR LIMPIEZA ORIGINAL
# ============================================

spec = importlib.util.spec_from_file_location(
    "bronze_mod",
    "scripts/processing/bronze_to_silver.py"
)

mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


spark = (
    SparkSession.builder
    .appName(
        f"BVL-Finanzas-Incremental-{dataset}"
    )
    .config(
        "spark.sql.shuffle.partitions",
        "4"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


try:

    # ========================================
    # 1. SILVER EXISTENTE
    # ========================================

    original = (
        spark.read.parquet(current)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # ========================================
    # 2. BRONZE
    # ========================================

    raw = mod.read_smv_json(
        spark,
        args.bronze,
        "data"
    )

    raw_count = raw.count()

    nuevo = mod.clean_financial_common(raw)

    for viejo, nuevo_nombre in (
        CONFIG[dataset]["renames"].items()
    ):
        if viejo in nuevo.columns:
            nuevo = nuevo.withColumnRenamed(
                viejo,
                nuevo_nombre
            )

    # Igual que pipeline histórico:
    # eliminar solo filas exactamente repetidas.
    nuevo = nuevo.dropDuplicates()

    # ========================================
    # 3. ESQUEMA
    # ========================================

    faltan = (
        set(original.columns)
        - set(nuevo.columns)
    )

    sobran = (
        set(nuevo.columns)
        - set(original.columns)
    )

    if faltan or sobran:
        raise RuntimeError(
            f"Esquema incompatible. "
            f"Faltan={sorted(faltan)} "
            f"Sobran={sorted(sobran)}"
        )

    nuevo = nuevo.select(
        *[
            F.col(c)
            .cast(original.schema[c].dataType)
            .alias(c)
            for c in original.columns
        ]
    )

    # ========================================
    # 4. CLAVE DE NEGOCIO
    # ========================================

    key = [
        c for c in BASE_KEY
        if c in original.columns
    ]

    if dataset == "cambios_patrimonio":
        for c in [
            "orden_columna",
            "descripcion_columna"
        ]:
            if c in original.columns:
                key.append(c)

    if not key:
        raise RuntimeError(
            "No fue posible determinar clave"
        )

    payload = [
        c for c in original.columns
        if c not in key
    ]

    # ========================================
    # 5. HASH DEL CONTENIDO
    # ========================================

    def agregar_hash(df):

        if payload:
            estructura = F.struct(
                *[
                    F.col(c)
                    for c in payload
                ]
            )
        else:
            estructura = F.struct(
                F.lit("SIN_PAYLOAD").alias("v")
            )

        return df.withColumn(
            "_payload_hash",
            F.sha2(
                F.to_json(
                    estructura,
                    {
                        "ignoreNullFields":
                        "false"
                    }
                ),
                256
            )
        )

    nh = agregar_hash(nuevo)
    oh = agregar_hash(original)

    # ========================================
    # 6. VALIDAR CLAVES DEL LOTE
    # ========================================

    grupos_lote = (
        nh.groupBy(*key)
        .agg(
            F.count("*").alias("filas"),
            F.countDistinct(
                "_payload_hash"
            ).alias("versiones")
        )
    )

    ambiguas_lote = (
        grupos_lote
        .filter(
            F.col("versiones") > 1
        )
        .count()
    )

    if ambiguas_lote:
        raise RuntimeError(
            f"{ambiguas_lote} claves "
            "con valores diferentes "
            "en el mismo lote"
        )

    # Duplicados idénticos por clave:
    # conservar uno.
    nuevo = (
        nh.dropDuplicates(key)
        .drop("_payload_hash")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    nh = agregar_hash(nuevo)

    # ========================================
    # 7. VALIDAR SILVER
    # ========================================

    silver_total = original.count()

    silver_claves = (
        original.select(*key)
        .distinct()
        .count()
    )

    if silver_claves != silver_total:
        raise RuntimeError(
            "Silver contiene claves duplicadas"
        )

    lote_total = nuevo.count()

    # ========================================
    # 8. NUEVOS
    # ========================================

    nuevos_df = (
        nuevo.join(
            original.select(*key),
            key,
            "left_anti"
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    nuevos_count = nuevos_df.count()

    # ========================================
    # 9. CORRECCIONES
    # ========================================

    comunes = (
        nh.alias("n")
        .join(
            oh.alias("o"),
            key,
            "inner"
        )
    )

    correcciones = comunes.filter(
        F.col("n._payload_hash")
        !=
        F.col("o._payload_hash")
    )

    corregidas = correcciones.count()

    sin_cambio = (
        lote_total
        - nuevos_count
        - corregidas
    )

    # ========================================
    # 10. REPORTE
    # ========================================

    print(
        "===== AUDITORIA INCREMENTAL "
        f"FINANZAS {dataset.upper()} =====",
        flush=True
    )

    print(
        f"bronze_raw: {raw_count}",
        flush=True
    )

    print(
        f"silver_antes: {silver_total}",
        flush=True
    )

    print(
        f"filas_lote: {lote_total}",
        flush=True
    )

    print(
        f"nuevas: {nuevos_count}",
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

    # ========================================
    # 11. BLOQUEAR CORRECCIONES
    # ========================================

    if corregidas:

        print(
            "CORRECCIONES_DETECTADAS|"
            "REVISAR_ANTES_DE_PUBLICAR",
            flush=True
        )

        raise RuntimeError(
            "Correcciones financieras "
            "detectadas"
        )

    # ========================================
    # 12. IDEMPOTENCIA
    # ========================================

    if nuevos_count == 0:

        print(
            "SIN_CAMBIOS|"
            "0 particiones modificadas; "
            "Silver intacto",
            flush=True
        )

    else:

        # ====================================
        # 13. STAGING POR EJERCICIO
        # ====================================

        run = (
            datetime.now(
                timezone.utc
            ).strftime(
                "%Y%m%dT%H%M%S"
            )
            + "_"
            + uuid4().hex[:6]
        )

        staging = (
            "/datalake/bvl/staging/"
            f"finanzas_{dataset}_incremental/"
            f"run_{run}"
        )

        ejercicios = (
            nuevos_df
            .select("ejercicio")
            .distinct()
            .collect()
        )

        manifest = {
            "dataset": dataset,
            "estado":
                "STAGING_VALIDADO_NO_PUBLICADO",
            "current": current,
            "bronze": args.bronze,
            "silver_antes": silver_total,
            "nuevas": nuevos_count,
            "corregidas": 0,
            "particiones": [],
        }

        for fila in ejercicios:

            ejercicio = fila["ejercicio"]

            existente = original.filter(
                F.col("ejercicio")
                == ejercicio
            )

            agregado = nuevos_df.filter(
                F.col("ejercicio")
                == ejercicio
            )

            # original ya fue validado como unico por clave,
            # nuevos_df proviene de LEFT ANTI y no comparte claves.
            # Por tanto la union conserva unicidad sin otro DISTINCT.
            filas_existentes = existente.count()
            filas_nuevas = agregado.count()

            total = filas_existentes + filas_nuevas

            combinado = (
                existente
                .unionByName(agregado)
            )

            print(
                f"STAGING_ESCRIBIENDO|"
                f"ejercicio={ejercicio}|"
                f"existentes={filas_existentes}|"
                f"nuevas={filas_nuevas}|"
                f"esperado={total}",
                flush=True
            )

            destino = (
                f"{staging}/"
                f"ejercicio={ejercicio}"
            )

            (
                combinado
                .drop("ejercicio")
                .write
                .mode("errorifexists")
                .option(
                    "compression",
                    "snappy"
                )
                .parquet(destino)
            )

            comprobado = (
                spark.read
                .parquet(destino)
                .count()
            )

            if comprobado != total:
                raise RuntimeError(
                    f"Staging inválido "
                    f"{ejercicio}"
                )

            manifest[
                "particiones"
            ].append({
                "ejercicio": ejercicio,
                "filas": comprobado,
                "ruta": destino,
            })

            print(
                f"PARTICION_STAGE|"
                f"{ejercicio}|"
                f"{comprobado}|"
                f"{destino}",
                flush=True
            )

        # ====================================
        # 14. MANIFIESTO
        # ====================================

        out = Path(
            "outputs/quality/incremental"
        )

        out.mkdir(
            parents=True,
            exist_ok=True
        )

        archivo = (
            out
            / (
                f"finanzas_{dataset}_"
                f"{run}.json"
            )
        )

        archivo.write_text(
            json.dumps(
                manifest,
                ensure_ascii=False,
                indent=2,
                default=str
            ),
            encoding="utf-8"
        )

        print(
            f"STAGING_VALIDADO|{staging}",
            flush=True
        )

        print(
            f"MANIFEST|{archivo}",
            flush=True
        )

        print(
            "SILVER_OFICIAL|SIN_CAMBIOS",
            flush=True
        )

finally:
    spark.stop()
