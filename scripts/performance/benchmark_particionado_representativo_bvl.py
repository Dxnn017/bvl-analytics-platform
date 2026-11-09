#!/usr/bin/env python3
"""BVL: compara consulta temporal sobre Parquet particionado y sin particionar.

Replica real SOLO en experimento de rendimiento; nunca alimentar Gold ni modelos.
La preparacion de la copia sin particiones se cronometra aparte.
Requiere --input apuntando a salida en HDFS staging del benchmark 10x o 100x.
"""
import argparse
import csv
import json
import math
import re
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input', required=True, help='Ruta HDFS de un resultado auditado del benchmark 10x/100x')
    p.add_argument('--year', type=int, default=2025)
    p.add_argument('--month', type=int, default=3)
    p.add_argument('--reps', type=int, default=7)
    p.add_argument('--warmups', type=int, default=2)
    p.add_argument('--out', default='outputs/performance')
    a = p.parse_args()
    if not a.input.startswith('/datalake/bvl/staging/benchmark_escalabilidad_auditado/factor_'):
        p.error('--input debe ser una salida auditada del benchmark ubicada en HDFS staging')
    if a.reps < 3 or a.warmups < 1 or not (1 <= a.month <= 12):
        p.error('Se requieren >=3 repeticiones, >=1 calentamiento y mes válido')
    return a


def result_for(df, year, month):
    return (df.filter((F.col('anio') == year) & (F.col('mes') == month))
            .agg(F.count('*').alias('filas'),
                 F.sum('monto_negociado').alias('monto'),
                 F.avg('cierre_actual').alias('cierre_medio'))
            .collect()[0].asDict())


def same(a, b):
    if a['filas'] != b['filas']:
        return False
    for k in ('monto', 'cierre_medio'):
        x, y = a[k], b[k]
        if x is None or y is None:
            if x != y:
                return False
        elif not math.isclose(float(x), float(y), rel_tol=1e-8, abs_tol=1e-4):
            return False
    return True


def timed(df, year, month):
    start = time.perf_counter()
    result = result_for(df, year, month)
    return time.perf_counter() - start, result


def main():
    a = parse_args()
    run = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '_' + uuid4().hex[:8]
    copy_path = f'/datalake/bvl/staging/benchmark_optimizacion/consulta_sin_particionar_{run}'
    local = Path(a.out)
    local.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.appName('BVL-Benchmark-Particionado-Representativo')
             .config('spark.sql.shuffle.partitions', '8')
             .config('spark.sql.parquet.compression.codec', 'snappy')
             .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        optimizado = spark.read.parquet(a.input)
        required = {'anio', 'mes', 'monto_negociado', 'cierre_actual'}
        if not required.issubset(optimizado.columns):
            raise RuntimeError('Columnas faltantes: ' + str(sorted(required-set(optimizado.columns))))
        input_rows = optimizado.count()
        print(f'APPLICATION_ID|{spark.sparkContext.applicationId}', flush=True)
        print(f'ORIGEN_PARTICIONADO|{a.input}', flush=True)
        print(f'REGISTROS_INPUT|{input_rows}', flush=True)
        start = time.perf_counter()
        (optimizado.repartition(4).write.mode('errorifexists')
         .option('compression','snappy').parquet(copy_path))
        prep_seconds = time.perf_counter() - start
        normal = spark.read.parquet(copy_path)
        copied = normal.count()
        if copied != input_rows:
            raise RuntimeError(f'Copia inconsistente: original {input_rows}, copia {copied}')
        print(f'SIN_PARTICIONAR_RUTA|{copy_path}', flush=True)
        print(f'TIEMPO_PREPARACION_S|{prep_seconds:.3f}', flush=True)
        print(f'REGISTROS_COPIA|{copied}', flush=True)
        preview_a = result_for(normal, a.year, a.month)
        preview_b = result_for(optimizado, a.year, a.month)
        if not same(preview_a, preview_b):
            raise RuntimeError(f'Consultas diferentes: SIN={preview_a}, CON={preview_b}')
        if preview_a['filas'] == 0:
            raise RuntimeError('El filtro no produjo filas; seleccionar año/mes que exista')
        print(f'CONSULTA_VALIDADA|{json.dumps(preview_a, default=str)}', flush=True)
        # Mostrar evidencia de pruning, no usar el tiempo de EXPLAIN para el benchmark.
        fil = (F.col('anio') == a.year) & (F.col('mes') == a.month)
        for label, df in [('SIN_PARTICIONAR', normal), ('PARTICIONADO', optimizado)]:
            plan = (df.filter(fil).agg(F.count('*'))._jdf.queryExecution().executedPlan().toString())
            plan_file = local / f'plan_{label.lower()}_{run}.txt'
            plan_file.write_text(plan, encoding='utf-8')
            print(f'PLAN_{label}|{plan_file}', flush=True)
            filtros = re.findall(r'PartitionFilters:\s*\[([^]]*)\]', plan)
            pruning = any(f.strip() for f in filtros)
            print(f'PRUNING_{label}|{pruning}', flush=True)
        spark.catalog.clearCache()
        for _ in range(a.warmups):
            result_for(normal, a.year, a.month)
            result_for(optimizado, a.year, a.month)
        values = {'SIN_PARTICIONAR': [], 'PARTICIONADO': []}
        detail = []
        for idx in range(a.reps):
            order = [('SIN_PARTICIONAR', normal), ('PARTICIONADO', optimizado)]
            if idx % 2:
                order.reverse()
            for label, df in order:
                elapsed, result = timed(df, a.year, a.month)
                if not same(result, preview_a):
                    raise RuntimeError(f'Resultado cambió durante benchmark: {label}, {result}')
                values[label].append(elapsed)
                detail.append({'repeticion': idx+1, 'escenario': label,
                               'tiempo_s': round(elapsed, 5), 'filas_consulta': result['filas']})
                print(f'REPETICION|{idx+1}|{label}|{elapsed:.5f}', flush=True)
        t0 = statistics.median(values['SIN_PARTICIONAR'])
        t1 = statistics.median(values['PARTICIONADO'])
        improvement = 100 * (t0-t1)/t0 if t0 else 0
        summary = {'run': run, 'application_id': spark.sparkContext.applicationId,
                   'input_hdfs': a.input, 'origen_filas': input_rows,
                   'copia_sin_particionar_hdfs': copy_path, 'preparacion_s': prep_seconds,
                   'year': a.year, 'month': a.month, 'filas_consulta': preview_a['filas'],
                   'reps': a.reps, 'warmups': a.warmups,
                   'mediana_sin_particionar_s': t0, 'mediana_particionado_s': t1,
                   'mejora_pct': improvement, 'resultados_equivalentes': True,
                   'nota': 'Benchmark de CONSULTA temporal sobre datos replicados, no mejora del pipeline completo.'}
        csv_file = local / f'particionado_detalle_{run}.csv'
        with csv_file.open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(detail[0].keys()))
            writer.writeheader()
            writer.writerows(detail)
        json_file = local / f'particionado_resumen_{run}.json'
        json_file.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
        print(f'MEDIANA_SIN_PART_S|{t0:.5f}', flush=True)
        print(f'MEDIANA_PART_S|{t1:.5f}', flush=True)
        print(f'MEJORA_CONSULTA_PCT|{improvement:.2f}', flush=True)
        print('MISMA_CONSULTA_Y_RESULTADOS|SI', flush=True)
        print(f'DETALLE_CSV|{csv_file}', flush=True)
        print(f'RESUMEN_JSON|{json_file}', flush=True)
        print('NOTA|Comparar tiempos solo dentro de este experimento; no equivale a optimización de extremo a extremo', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
