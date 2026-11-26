#!/usr/bin/env python3
"""Transformaciones PURAS candidatas de BVL Silver restante.

No importa scripts historicos que ejecutan Spark al importarse.
No hace lecturas ni escrituras GCS, claims, ni crea SparkSession.
Los resultados deben validarse contra el Bronze real antes del lanzamiento.
"""
from __future__ import annotations
import csv
import io
import json
import math
import re

from fase6_restantes_core import CANDIDATE_TRANSFORMS, ScopeError, require

MAX_ITEM_BYTES = 32 * 1024 * 1024
MAX_ROWS = 200_000


def load_json_records(raw_documents: list[bytes], *, required_wrappers=('Resultado', 'data')):
    """Recibe exclusivamente bytes previamente fijados y verificados.

    Prohibe JSONs con errores de respuesta y claves duplicadas. No SOAP.
    """
    require(isinstance(raw_documents, list) and bool(raw_documents),
            'No hay documentos JSON')
    rows = []
    def unique_pairs(pairs):
        result = {}
        for k, v in pairs:
            require(k not in result, 'Claves JSON duplicadas')
            result[k] = v
        return result
    for raw in raw_documents:
        require(isinstance(raw, bytes) and 0 < len(raw) <= MAX_ITEM_BYTES,
                'Tamano de objeto JSON fuera de limite')
        try:
            obj = json.loads(raw.decode('utf-8-sig'), object_pairs_hook=unique_pairs,
                             parse_constant=lambda v: (_ for _ in ()).throw(ValueError(v)))
        except (ValueError, UnicodeError) as exc:
            raise ScopeError('JSON Bronze no valido') from exc
        if isinstance(obj, dict):
            # Algunas respuestas SMV contienen Code/Message aun cuando hay array.
            for code_key in ('ResponseCode', 'CodigoError'):
                code = obj.get(code_key)
                if code is not None and str(code).strip() not in ('', '0', '00', '200'):
                    raise ScopeError(f'Respuesta de servicio invalida ({code_key})')
            found = [key for key in required_wrappers if key in obj]
            require(len(found) == 1 and isinstance(obj[found[0]], list),
                    'Wrapper JSON no reconocido/ambiguo')
            records = obj[found[0]]
        elif isinstance(obj, list):
            records = obj
        else:
            raise ScopeError('JSON sin arreglo de registros')
        for record in records:
            require(isinstance(record, dict), 'Registro JSON no es objeto')
            rows.append(record)
            require(len(rows) <= MAX_ROWS, 'Exceso de registros para el candidato')
    require(bool(rows), 'No hay registros SMV validos')
    return rows


def load_bcrp_csv(raw: bytes, required_value: str):
    require(isinstance(raw, bytes) and 0 < len(raw) <= MAX_ITEM_BYTES,
            'CSV BCRP invalido')
    # Diagnostico Quality 2026-11-26: EMBIG oficial BCRP PD04709XD
    # contiene acentos CP1252. No autodetectar ni hacer fallback silencioso.
    # Los bytes de entrada ya deben estar fijados por generacion/SHA256 Bronze.
    require(required_value == 'PD04709XD',
            'Codificacion BCRP no ratificada para esta serie')
    try:
        text = raw.decode('cp1252', errors='strict')
    except UnicodeDecodeError as exc:
        raise ScopeError('CSV EMBIG no valido para CP1252 explicito') from exc
    reader = csv.DictReader(io.StringIO(text, newline=''), restkey='_EXTRA')
    names = reader.fieldnames or []
    if names and (names[0] is None or names[0].strip() == ''):
        names[0] = '_c0'
    require(len(set(names)) == len(names) and set(('_c0', required_value)) <= set(names),
            'Cabeceras BCRP requeridas ausentes')
    rows = []
    for row in reader:
        require(row.get('_EXTRA') is None and None not in row,
                'Fila BCRP con cantidad de columnas incorrecta')
        rows.append(row)
        require(len(rows) <= MAX_ROWS, 'BCRP sobre limite de filas')
    require(bool(rows), 'BCRP sin filas')
    return rows


def spark_data_frame(spark, rows):
    """DataFrame de una fuente validada. Spark nunca lee rutas GCS no fijadas."""
    require(bool(rows) and len(rows) <= MAX_ROWS, 'Sin filas validas')
    # Spark puede inferir nulos, pero transforma solo columnas con esquema esperado.
    return spark.createDataFrame(rows)


def _pyspark():
    from pyspark.sql import functions as F
    return F


def _date_bcrp(column):
    F = _pyspark()
    # Reutiliza exacta regla de siglos/meses del procesador del piloto.
    from bronze_to_silver_dataproc_candidato import normalize_bcrp_date
    return normalize_bcrp_date(column)


def _safe_date(column, fmt):
    F = _pyspark()
    return F.try_to_timestamp(F.trim(column.cast('string')), F.lit(fmt)).cast('date')


def _safe_int(col):
    F = _pyspark()
    return F.expr(f'try_cast(`{col}` as int)')


def _safe_double(col):
    F = _pyspark()
    return F.expr(f'try_cast(`{col}` as double)')


def _metrics(name, before, after, invalid=0, duplicates=0, **extra):
    require(type(before) is int and type(after) is int and before >= after >= 0,
            'Conteos invalidos')
    require(before == after + invalid + duplicates, 'Conciliacion de conteos invalida')
    return {'dataset': name, 'bronze_rows': before, 'silver_rows': after,
            'invalid_or_filtered_rows': invalid, 'duplicates_removed': duplicates,
            'quality_status': 'CANDIDATO_PENDIENTE_QUALITY', **extra}


def transform_embig(df):
    F = _pyspark()
    require({'_c0', 'PD04709XD'} <= set(df.columns), 'Esquema EMBIG incompleto')
    before = df.count()
    d = df.filter(F.col('_c0').isNotNull() & (F.trim('_c0') != ''))
    descriptions = before - d.count()
    raw = d.select(_date_bcrp(F.col('_c0')).alias('fecha'),
                   F.trim(F.col('PD04709XD')).alias('txt'))
    valid = raw.filter(F.col('fecha').isNotNull())
    bad_dates = d.count() - valid.count()
    missing = F.col('txt').isNull() | F.lower('txt').isin('', 'n.d.', 'n.d', 'nd')
    number = F.regexp_replace(F.col('txt'), ',', '.')
    valid_number = number.rlike(r'^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)$')
    bad_number = valid.filter(~missing & ~valid_number).count()
    require(bad_number == 0, f'EMBIG contiene {bad_number} numeros invalidos')
    clean = valid.select('fecha', F.when(missing, F.lit(None).cast('double'))
                         .otherwise(number.cast('double')).alias('embig_peru'))
    conflict = clean.groupBy('fecha').agg(
        F.countDistinct(F.coalesce(F.col('embig_peru').cast('string'),
                                   F.lit('__NULL__'))).alias('versions'))
    require(conflict.filter('versions > 1').count() == 0,
            'EMBIG: una fecha tiene dos valores distintos')
    output = clean.dropDuplicates(['fecha'])
    after = output.count()
    metrics = _metrics('bcrp/embig', before, after, descriptions + bad_dates,
                       valid.count() - after,
                       descriptive_rows=descriptions, invalid_date_rows=bad_dates)
    return output.withColumn('anio', F.year('fecha')).withColumn('mes', F.month('fecha')), metrics


FINANCIAL = frozenset({
    'smv/cambios_patrimonio', 'smv/estado_resultados',
    'smv/flujo_efectivo', 'smv/situacion_financiera'
})
FINANCIAL_RENAME = {
    'CIIU': 'ciiu', 'Cuenta': 'cuenta', 'DescripcionCuenta': 'descripcion_cuenta',
    'Ejercicio': 'ejercicio', 'FechaRegistro': 'fecha_registro',
    'MetodoFlujoEfectivo': 'metodo_flujo_efectivo', 'Moneda': 'moneda',
    'NombreEmpresa': 'nombre_empresa', 'RPJ': 'rpj', 'RUC': 'ruc',
    'TipoEmpresa': 'tipo_empresa', 'TipoInformacion': 'tipo_informacion',
    'TipoSector': 'tipo_sector', 'Trimestre': 'trimestre',
    'Monto1': 'monto1', 'Monto2': 'monto2', 'Monto3': 'monto3', 'Monto4': 'monto4'
}


def transform_financial(df, dataset):
    F = _pyspark()
    require(dataset in FINANCIAL, 'Familia financiera no ratificada')
    require({'RUC', 'Ejercicio'} <= set(df.columns), 'Fuente financiera sin RUC/Ejercicio')
    before = df.count()
    for original, normalized in FINANCIAL_RENAME.items():
        if original in df.columns:
            df = df.withColumnRenamed(original, normalized)
    if dataset == 'smv/cambios_patrimonio':
        for old, new in (('DescripcionColumna', 'descripcion_columna'),
                         ('OrdenColumna', 'orden_columna')):
            if old in df.columns:
                df = df.withColumnRenamed(old, new)
    if 'fecha_registro' in df.columns:
        df = df.withColumn('fecha_registro', _safe_date(F.col('fecha_registro'), 'dd/MM/yyyy'))
    df = df.withColumn('ejercicio', _safe_int('ejercicio'))
    for key in ('ruc', 'rpj'):
        if key in df.columns:
            df = df.withColumn(key, F.trim(F.col(key).cast('string')))
    # No descartar conflictos en granularidad sin contrato explicito.
    require(df.filter(F.col('ejercicio').isNull()).count() == 0,
            'Ejercicio financiero invalido: conciliacion requerida')
    if 'ruc' in df.columns:
        require(df.filter(F.col('ruc').isNull() | (F.col('ruc') == '')).count() == 0,
                'RUC financiero nulo: requiere Quality')
    output = df.dropDuplicates()
    after = output.count()
    return output, _metrics(dataset, before, after, 0, before - after)


def transform_cotizaciones(df):
    F = _pyspark()
    require({'FechaCotizacion', 'Valor'} <= set(df.columns),
            'Cotizaciones sin fecha/valor')
    before = df.count()
    rename_map = {
        'Valor': 'valor', 'Descripcion': 'descripcion',
        'FechaCotizacion': 'fecha_cotizacion', 'FechaAnterior': 'fecha_anterior',
        'FechaRegistro': 'fecha_registro', 'Apertura_Actual': 'apertura_actual',
        'Cierre_Actual': 'cierre_actual', 'Cierre_Anterior': 'cierre_anterior',
        'Maxima_Actual': 'maxima_actual', 'Minima_Actual': 'minima_actual',
        'Monto_Negociado': 'monto_negociado', 'Promedio_Actual': 'promedio_actual',
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    for col in ('fecha_cotizacion', 'fecha_anterior', 'fecha_registro'):
        if col in df.columns:
            df = df.withColumn(col, _safe_date(F.substring(F.col(col), 1, 10), 'dd/MM/yyyy'))
    require(df.filter(F.col('fecha_cotizacion').isNull() | F.col('valor').isNull()).count() == 0,
            'Cotizaciones con clave fecha/valor invalida')
    # Contrato local historico: clave [fecha_cotizacion, valor].
    # Si hay filas discrepantes para una misma clave, no eliminar arbitrariamente.
    clean = df.dropDuplicates()
    total_clean = clean.count()
    duplicates_by_key = clean.groupBy('fecha_cotizacion', 'valor').count().filter('count > 1').count()
    require(duplicates_by_key == 0,
            'Cotizaciones con claves [fecha,valor] contradictorias')
    out = clean.withColumn('anio', F.year('fecha_cotizacion')).withColumn('mes', F.month('fecha_cotizacion'))
    return out, _metrics('smv/cotizaciones', before, total_clean, 0,
                         before - total_clean, conflicting_keys=0)


def transform_principales(df, *, fecha_ingesta='2026-10-04'):
    F = _pyspark()
    fields = [
        'RPJ', 'RUC', 'NombreEmpresa', 'TipoEmpresa', 'TipoSector',
        'CIIU', 'Ejercicio', 'Trimestre', 'TipoInformacion',
        'MetodoFlujoEfectivo', 'Moneda', 'ActivoTotal', 'PatrimonioTotal',
        'TotalIngreso', 'UtilidadNeta', 'PasivoTotal',
    ]
    require(set(fields) <= set(df.columns), 'Principales Cuentas: faltan campos historicos')
    before = df.count()
    texts = ['RPJ', 'RUC', 'NombreEmpresa', 'TipoEmpresa', 'TipoSector',
             'CIIU', 'Trimestre', 'TipoInformacion', 'MetodoFlujoEfectivo', 'Moneda']
    numerical = ['ActivoTotal', 'PatrimonioTotal', 'TotalIngreso',
                 'UtilidadNeta', 'PasivoTotal']
    snake = {
        'RPJ': 'rpj', 'RUC': 'ruc', 'NombreEmpresa': 'nombre_empresa',
        'TipoEmpresa': 'tipo_empresa', 'TipoSector': 'tipo_sector',
        'CIIU': 'ciiu', 'Ejercicio': 'ejercicio', 'Trimestre': 'trimestre',
        'TipoInformacion': 'tipo_informacion', 'MetodoFlujoEfectivo': 'metodo_flujo_efectivo',
        'Moneda': 'moneda', 'ActivoTotal': 'activo_total',
        'PatrimonioTotal': 'patrimonio_total', 'TotalIngreso': 'total_ingreso',
        'UtilidadNeta': 'utilidad_neta', 'PasivoTotal': 'pasivo_total',
    }
    selects = ([F.trim(F.col(x).cast('string')).alias(snake[x]) for x in texts]
               + [F.expr('try_cast(`Ejercicio` as int)').alias('ejercicio')]
               + [F.expr(f'try_cast(`{x}` as double)').alias(snake[x]) for x in numerical])
    output = df.select(*selects)
    output = (output.withColumn('trimestre_num',
                               F.expr("try_cast(regexp_extract(trimestre, '^([1-4])', 1) as int)"))
                     .withColumn('ruc_valido', F.col('ruc').rlike('^[0-9]{11}$') &
                                 (F.col('ruc') != '00000000000'))
                     .withColumn('fuente_datos', F.lit('SMV'))
                     .withColumn('fecha_ingesta', F.lit(fecha_ingesta).cast('date')))
    require(output.filter(F.col('ejercicio').isNull() | F.col('trimestre_num').isNull()).count() == 0,
            'Principales Cuentas: ejercicio/trimestre invalido')
    key = ['ruc', 'ejercicio', 'trimestre', 'tipo_informacion',
           'rpj', 'metodo_flujo_efectivo', 'moneda']
    full = [snake[x] for x in fields]
    def signature(cols):
        return F.sha2(F.concat_ws('||', *[F.coalesce(F.col(x).cast('string'), F.lit('<NULL>')) for x in cols]), 256)
    output = output.withColumn('clave_negocio', signature(key)).withColumn('id_registro', signature(full))
    counts = output.groupBy('clave_negocio').count().withColumnRenamed('count', 'cantidad_clave_negocio')
    output = output.join(counts, on='clave_negocio', how='left').withColumn(
        'colision_clave_negocio', F.col('cantidad_clave_negocio') > 1)
    unique_ids = output.select('id_registro').distinct().count()
    collisions = counts.filter('cantidad_clave_negocio > 1').count()
    # Registros diferentes con igual clave se CONSERVAN, no se borran.
    return output, _metrics('smv/principales_cuentas', before, before, 0, 0,
                            exact_duplicates=before - unique_ids,
                            business_key_collisions=collisions)


def transform_montos_sab(df, *, fecha_ingesta='2026-10-04'):
    """Misma clave del procesador HDFS: fecha_negociacion + CodigoSAB."""
    F = _pyspark()
    require({'_FechaNegociacionConsulta', 'CodigoSAB'} <= set(df.columns),
            'Montos SAB: campos de clave ausentes')
    before = df.count()
    raw = F.substring(F.col('_FechaNegociacionConsulta').cast('string'), 1, 10)
    result = (
        df.withColumn('fecha_negociacion',
            F.coalesce(_safe_date(raw, 'dd/MM/yyyy'), _safe_date(raw, 'yyyy-MM-dd')))
          .withColumn('codigo_sab', F.trim(F.col('CodigoSAB').cast('string')))
          .withColumn('anio', F.year('fecha_negociacion'))
          .withColumn('mes', F.month('fecha_negociacion'))
          .withColumn('fuente_datos', F.lit('SMV'))
          .withColumn('fecha_ingesta', F.lit(fecha_ingesta).cast('date'))
    )
    require(result.filter(F.col('fecha_negociacion').isNull() |
                          F.col('codigo_sab').isNull() |
                          (F.col('codigo_sab') == '')).count() == 0,
            'Montos SAB: clave negocio nula/invalida')
    distinct = result.select('fecha_negociacion', 'codigo_sab').distinct().count()
    require(distinct == before,
            'Montos SAB: claves fecha+SAB duplicadas, sin sobrescritura arbitraria')
    return result, _metrics('smv/montos_sab', before, before, 0, 0,
                            business_key_unique=distinct)


TRANSFORMS = {
    'bcrp/embig': transform_embig,
    'smv/cambios_patrimonio': lambda df: transform_financial(df, 'smv/cambios_patrimonio'),
    'smv/estado_resultados': lambda df: transform_financial(df, 'smv/estado_resultados'),
    'smv/flujo_efectivo': lambda df: transform_financial(df, 'smv/flujo_efectivo'),
    'smv/situacion_financiera': lambda df: transform_financial(df, 'smv/situacion_financiera'),
    'smv/principales_cuentas': transform_principales,
    'smv/cotizaciones': transform_cotizaciones,
    'smv/montos_sab': transform_montos_sab,
}


def dispatch_transform(dataset, df):
    require(dataset in CANDIDATE_TRANSFORMS and dataset in TRANSFORMS,
            'Procesador no validado: no se permite una transformacion generica')
    result = TRANSFORMS[dataset](df)
    require(isinstance(result, tuple) and len(result) == 2,
            'Transformador no entrego dataframe + metricas')
    return result
