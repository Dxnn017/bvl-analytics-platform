#!/usr/bin/env python3
"""BVL F6: transformacion candidata, PURA, Valores Inscritos -> DOS productos.

Se invoca sobre DataFrame Spark construido SOLO desde objetos Bronze oficiales
previamente validados. No lee GCS, no consume claims y nunca publica Silver.
Las dos salidas deben tratarse como una sola unidad para Quality.
"""
from __future__ import annotations

BRONZE_DATASET = 'smv/valores_inscritos'
OUTPUTS = ('referencias_smv/valores_inscritos', 'referencias_smv/puente_ruc_nemonico')
REQUIRED = (
    'RUC', 'RazonSocial', 'DenominacionValor', 'NemonicoValor',
    'CodigoISIN', 'TipoValor', 'FechaInscripcion', 'ResolucionInscripcion',
    'Moneda', 'MontoInscrito', 'Cotizacion', 'FechaUltCot',
    '_empresa_origen', '_fuente', '_fecha_extraccion',
)
BRIDGE_ATTRIBUTES = (
    'razon_social', 'codigo_isin', 'denominacion_valor', 'tipo_valor',
    'moneda', 'fecha_inscripcion', 'resolucion_inscripcion',
)

class ValoresQualityError(ValueError):
    pass


def ensure(ok, msg):
    if not ok:
        raise ValoresQualityError(msg)


def transform_valores_inscritos(df):
    """Return (valores, puente, quality). Fail-closed on ambiguous nemonicos/keys.

    No destructive deduplication of raw `valores`. Spark ANSI compatible.
    """
    from pyspark.sql import functions as F
    missing = sorted(set(REQUIRED) - set(df.columns))
    ensure(not missing, 'Columnas oficiales faltantes: ' + ','.join(missing))
    before = df.count()
    ensure(before > 0, 'Fuente vacia')

    def txt(name):
        c = F.trim(F.col(name).cast('string'))
        return F.when(c == '', F.lit(None).cast('string')).otherwise(c)

    def safe_double(name):
        return F.expr(f"try_cast(regexp_replace(trim(`{name}`), ',', '') as double)")

    def safe_date(name):
        # try_to_timestamp returns null on malformed dates even under ANSI.
        return F.to_date(F.expr(f"try_to_timestamp(`{name}`, 'dd/MM/yyyy')"))

    valores = df.select(
        txt('RUC').alias('ruc'),
        txt('RazonSocial').alias('razon_social'),
        txt('DenominacionValor').alias('denominacion_valor'),
        F.upper(txt('NemonicoValor')).alias('nemonico_valor'),
        F.upper(txt('CodigoISIN')).alias('codigo_isin'),
        txt('TipoValor').alias('tipo_valor'),
        safe_date('FechaInscripcion').alias('fecha_inscripcion'),
        txt('ResolucionInscripcion').alias('resolucion_inscripcion'),
        txt('Moneda').alias('moneda'),
        safe_double('MontoInscrito').alias('monto_inscrito'),
        safe_double('Cotizacion').alias('cotizacion'),
        safe_date('FechaUltCot').alias('fecha_ultima_cotizacion'),
        txt('_empresa_origen').alias('empresa_origen'),
        txt('_fuente').alias('fuente'),
        F.expr('try_to_timestamp(`_fecha_extraccion`)').alias('fecha_extraccion'),
    )

    ensure(valores.filter(F.col('ruc').isNull() | F.col('nemonico_valor').isNull()).count() == 0,
           'RUC/nemonico nulo o vacio')
    # Valores no numericos no nulos no se aceptan como perdida silenciosa.
    for source, target in (('MontoInscrito', 'monto_inscrito'), ('Cotizacion', 'cotizacion')):
        bad = df.select(txt(source).alias('src'), safe_double(source).alias('v')).filter(
            F.col('src').isNotNull() & (F.col('v').isNull() | F.isnan('v') | (F.abs('v') == float('inf'))))
        ensure(bad.limit(1).count() == 0, f'Valor numerico invalido: {source}')
    # Aceptar fecha ausente; NO aceptar fecha expresamente invalida.
    for source, target in (('FechaInscripcion', 'fecha_inscripcion'), ('FechaUltCot', 'fecha_ultima_cotizacion')):
        bad = df.select(txt(source).alias('src'), safe_date(source).alias('v'))
        ensure(bad.filter(F.col('src').isNotNull() & F.col('v').isNull()).limit(1).count() == 0,
               f'Fecha invalida: {source}')
    # Una misma nemonico NO puede apuntar a varias empresas.
    ambig = valores.groupBy('nemonico_valor').agg(F.countDistinct('ruc').alias('n')).filter('n > 1').count()
    ensure(ambig == 0, 'Nemonico asociado a multiples RUC')

    grouped = valores.groupBy('ruc', 'nemonico_valor')
    stats = grouped.agg(*[F.countDistinct(F.col(a)).alias('_n_' + a) for a in BRIDGE_ATTRIBUTES])
    conflicts = stats.filter(' OR '.join(f'`_n_{a}` > 1' for a in BRIDGE_ATTRIBUTES)).count()
    ensure(conflicts == 0, 'Atributos contradictorios en puente RUC-nemonico')

    # MIN es determinista cuando solo hay un valor no nulo (o solo nulos).
    puente = grouped.agg(*[F.min(a).alias(a) for a in BRIDGE_ATTRIBUTES])
    n_bridge = puente.count()
    n_distinct_keys = valores.select('ruc', 'nemonico_valor').distinct().count()
    ensure(n_bridge == n_distinct_keys, 'Conservacion de claves del puente')
    return valores, puente, {
        'dataset': BRONZE_DATASET,
        'products': list(OUTPUTS),
        'bronze_rows': before,
        'silver_valores_rows': valores.count(),
        'silver_puente_rows': n_bridge,
        'unique_ruc_nemonico': n_distinct_keys,
        'ambiguous_nemonicos': ambig,
        'conflicting_bridge_attributes': conflicts,
        'all_outputs_required_for_quality': True,
    }
