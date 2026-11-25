#!/usr/bin/env python3
"""Previsualiza o prepara en HDFS una fusion incremental sin modificar Silver activo.

Uso inicial, sin escrituras:
  spark-submit incremental_cotizaciones_bvl.py --incoming /datalake/bvl/silver/mercado_diario/cotizaciones

Uso con Parquet normalizado nuevo, solo staging:
  spark-submit incremental_cotizaciones_bvl.py --incoming /ruta/parquet/nuevo --stage

ATENCION: --incoming debe ser Parquet con el mismo esquema de la tabla Silver.
No lee JSON Bronze ni publica cambios en el Silver actual.
"""
import argparse
from functools import reduce
from operator import or_
from uuid import uuid4
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DEFAULT_CURRENT = "/datalake/bvl/silver/mercado_diario/cotizaciones"
KEYS = ["fecha_cotizacion", "valor"]
PARTS = ["anio", "mes"]


def q(alias, name):
    return F.col(f"{alias}.`{name.replace('`', '``')}`")


def main():
    p = argparse.ArgumentParser(description="Fusion segura de Cotizaciones BVL: auditoria y staging")
    p.add_argument("--incoming", required=True, help="Ruta Parquet NORMALIZADO de un nuevo lote; para prueba puede ser el Silver actual")
    p.add_argument("--current", default=DEFAULT_CURRENT, help="Ruta Silver vigente")
    p.add_argument("--stage", action="store_true", help="Construir salida en ruta HDFS nueva sin tocar Silver vigente")
    p.add_argument("--stage-root", default="/datalake/bvl/staging/incremental_cotizaciones")
    args = p.parse_args()

    if args.incoming.rstrip("/") == args.current.rstrip("/") and args.stage:
        print("AVISO: se esta usando el historico como lote de prueba; no demuestra incorporacion de datos nuevos.")
    if args.stage_root.rstrip("/") == args.current.rstrip("/"):
        raise SystemExit("ERROR: staging no puede apuntar a Silver vigente")

    spark = (SparkSession.builder.appName("BVL-Cotizaciones-Incremental-Preflight")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.parquet.compression.codec", "snappy")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    try:
        old = spark.read.parquet(args.current)
        raw_in = spark.read.parquet(args.incoming)
        for name, df in [("Silver vigente", old), ("Nuevo lote", raw_in)]:
            missing = [c for c in KEYS + PARTS if c not in df.columns]
            if missing:
                raise RuntimeError(f"{name}: faltan columnas {missing}")
        old_schema = {f.name: f.dataType.simpleString() for f in old.schema.fields}
        new_schema = {f.name: f.dataType.simpleString() for f in raw_in.schema.fields}
        if old_schema != new_schema:
            raise RuntimeError(f"Esquemas distintos. Silver={old_schema}; incoming={new_schema}")
        in_count = raw_in.count()
        incoming = raw_in.dropDuplicates()
        exact_duplicates = in_count - incoming.count()
        for name, df in [("Silver", old), ("Lote nuevo", incoming)]:
            invalid = df.filter(reduce(or_, [F.col(k).isNull() for k in KEYS])).limit(1).count()
            if invalid:
                raise RuntimeError(f"{name}: clave de negocio nula")
            duplicate_keys = (df.groupBy(*KEYS).count().filter(F.col("count") > 1).limit(1).count())
            if duplicate_keys:
                raise RuntimeError(f"{name}: mas de una fila distinta por clave; resolver conflictos antes de fusionar")

        new_rows = incoming.join(old.select(*KEYS), on=KEYS, how="left_anti")
        new_count = new_rows.count()
        common = incoming.alias("i").join(old.alias("o"), on=KEYS, how="inner")
        fields = [c for c in old.columns if c not in KEYS]
        different = reduce(or_, [~q("i", c).eqNullSafe(q("o", c)) for c in fields]) if fields else F.lit(False)
        changed_keys = common.filter(different).select(*KEYS).distinct()
        changed_count = changed_keys.count()
        same_count = incoming.count() - new_count - changed_count
        current_count = old.count()
        expected = current_count + new_count
        print("\n========== PREFLIGHT INCREMENTAL BVL ==========")
        print(f"Silver vigente           : {current_count}")
        print(f"Filas del lote           : {in_count}")
        print(f"Duplicados exactos lote  : {exact_duplicates}")
        print(f"Nuevos registros         : {new_count}")
        print(f"Claves existentes iguales: {same_count}")
        print(f"Claves modificadas       : {changed_count}")
        print(f"Total esperado post-merge: {expected}")

        # No aceptar sustituciones sin una regla documentada de vigencia/correccion.
        if changed_count:
            print("BLOQUEADO: hay correcciones/conflictos. Auditar antes de publicarlas.")
            changed_keys.show(15, truncate=False)
            if args.stage:
                raise RuntimeError("No se genera staging si hay claves modificadas")
            return
        if not args.stage:
            print("MODO LECTURA: Silver no fue modificado")
            print("==============================================\n")
            return
        if not set(PARTS).issubset(old.columns):
            raise RuntimeError("Faltan columnas de particion")
        stage_path = f"{args.stage_root.rstrip('/')}/run_{uuid4().hex}"
        # Ruta nueva; nunca sobreescribe el dataset vigente.
        merged = old.unionByName(new_rows)
        merged.write.mode("errorifexists").option("compression", "snappy").partitionBy(*PARTS).parquet(stage_path)
        staged = spark.read.parquet(stage_path)
        staged_count = staged.count()
        distinct_keys = staged.select(*KEYS).distinct().count()
        if staged_count != expected or distinct_keys != expected:
            raise RuntimeError(f"VALIDACION FALLIDA staging={staged_count} claves={distinct_keys} esperado={expected}")
        print(f"STAGING VALIDADO       : {stage_path}")
        print(f"Registros en staging   : {staged_count}")
        print("Silver vigente NO se modifico. NO publicar staging sin revisar evidencia.")
        print("==============================================\n")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
