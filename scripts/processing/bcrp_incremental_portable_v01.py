#!/usr/bin/env python3

import argparse
from pyspark import StorageLevel
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2])
)
from scripts.pipeline.rutas_bvl import ruta
from scripts.pipeline.registro_silver_bvl import obtener_registro_vigente_gcs

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
p.add_argument(
    "--entorno",
    choices=("hdfs", "gcs"),
    default="hdfs"
)

a = p.parse_args()

cfg = CONFIG[a.dataset].copy()

rutas_dataset = {
    "tipo_cambio": (
        "bcrp/tipo_cambio",
        "macroeconomia/tipo_cambio"
    ),
    "embig": (
        "bcrp/embig",
        "macroeconomia/embig"
    )
}

rel_bronze, rel_silver = rutas_dataset[a.dataset]

cfg["bronze_root"] = ruta(
    "bronze", rel_bronze, entorno=a.entorno
)
cfg["silver"] = ruta(
    "silver", rel_silver, entorno=a.entorno
)

raiz_staging = ruta(
    "staging", entorno=a.entorno
)

if a.entorno == "gcs":
    if a.current:
        p.error("GCS no permite --current manual; se usa Quality")

    try:
        current, generacion_registro = obtener_registro_vigente_gcs(
            rel_silver
        )
    except Exception as error:
        p.error(f"No se pudo validar current.json: {error}")

    print(
        f"REGISTRO_SILVER_GENERACION|{generacion_registro}",
        flush=True
    )
else:
    current = a.current or cfg["silver"]


if not (
    a.bronze.rstrip("/") == cfg["bronze_root"].rstrip("/")
    or a.bronze.startswith(
        cfg["bronze_root"].rstrip("/") + "/"
    )
):
    p.error("Ruta Bronze no autorizada")

if not (
    current.rstrip("/") == cfg["silver"].rstrip("/")
    or current.startswith(cfg["silver"].rstrip("/") + "/")
    or current.rstrip("/") == raiz_staging.rstrip("/")
    or current.startswith(raiz_staging.rstrip("/") + "/")
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

    original = spark.read.parquet(current).persist(StorageLevel.DISK_ONLY)

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

    nuevos_df = nuevos_df.persist(StorageLevel.DISK_ONLY)
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

        if a.entorno == "gcs":
            staging = ruta(
                "staging",
                f"{rel_silver}/run_id={run}",
                entorno="gcs"
            )
        else:
            staging = (
                f"{raiz_staging.rstrip('/')}/"
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

        print(f"ETAPA|PARTICIONES|{len(particiones)}", flush=True)
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

            print(f"ETAPA|PARTICION_{anio}_{mes}|CONTANDO", flush=True)
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

        # Manifest candidato: no autoriza publicar Silver ni ejecutar Gold.
        claves_nuevas = [
            {
                "operacion": "INSERT",
                "clave_anterior": None,
                "clave_nueva": {"fecha": fila["fecha"].isoformat()}
            }
            for fila in nuevos_df.select("fecha").orderBy("fecha").collect()
        ]

        candidato = {
            "version_manifest": "0.1",
            "entorno": "PRUEBA",
            "run_id": run,
            "estado": "SILVER_CANDIDATO",
            "fecha_publicacion": None,
            "tipo_publicacion": "INCREMENTAL",
            "validacion_aprobada": False,
            "ruta_staging": staging,
            "datasets": [{
                "dataset": a.dataset,
                "ruta_silver": None,
                "version_esquema": None,
                "version_anterior": None,
                "version_publicada": None,
                "clave": ["fecha"],
                "particiones_afectadas": [
                    {"anio": p["anio"], "mes": p["mes"]}
                    for p in manifest["particiones"]
                ],
                "claves_afectadas": claves_nuevas,
                "ruta_detalle_cambios": None,
                "registros_nuevos": nuevos,
                "registros_corregidos": 0,
                "registros_eliminados": 0,
                "registros_antes": silver_total,
                "registros_despues": silver_total + nuevos,
                "autorizacion_eliminaciones": False
            }]
        }

        candidato_path = (
            out / f"candidato_silver_{a.dataset}_{run}.json"
        )

        candidato_path.write_text(
            json.dumps(candidato, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

        print(
            f"CANDIDATO_SILVER_LOCAL|{candidato_path}",
            flush=True
        )

        # Ruta de entrega futura. No realiza cargas a GCS.
        if a.entorno == "gcs":
            destino_candidato = ruta(
                "control",
                f"manifests/silver/candidates/{rel_silver}/run_id={run}.json",
                entorno="gcs"
            )

            print(
                f"CANDIDATO_GCS_PENDIENTE|{destino_candidato}",
                flush=True
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
