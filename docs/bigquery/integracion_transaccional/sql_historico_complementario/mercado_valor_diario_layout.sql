-- CANDIDATO LOCAL NO AUTORIZADO.
-- Consulta de validacion exclusivamente.
-- No ejecutar sin acreditacion operativa.

-- Valida columna de particion y orden clustering.
-- No acredita granularidad ni todas las opciones.
-- Esos controles requieren metadata de tabla.

WITH esperado AS (
  SELECT *
  FROM UNNEST([
    STRUCT('nemonico' AS columna, 'NO' AS particion, 1 AS clustering_orden),
    STRUCT('fecha' AS columna, 'YES' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('descripcion' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('fecha_anterior' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('precio_anterior' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('precio_cierre' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('precio_apertura' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('precio_maximo' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('precio_minimo' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('precio_promedio' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('monto_negociado' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('fuente' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('fecha_ingesta' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('tipo_cambio_venta' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('embig_peru' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('ruc' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('razon_social' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('codigo_isin' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('tipo_valor' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('moneda_referencia' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('fecha_inscripcion' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('precio_anterior_valido' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('precio_cierre_valido' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('rango_intradia_valido' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('tipo_cambio_disponible' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('embig_disponible' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('empresa_vinculada' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('inscripcion_compatible' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('monto_negociado_valido' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('monto_negociado_positivo' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('retorno_calculable' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('variacion_precio' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('retorno_diario_pct' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('rango_intradia' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('rango_intradia_pct' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('anio' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('mes' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('trimestre' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('dias_desde_cierre_anterior' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('macro_disponible' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('nemonico_original' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden)
  ])
),
observado AS (
  SELECT
    column_name AS columna,
    is_partitioning_column AS particion,
    clustering_ordinal_position AS clustering_orden
  FROM `bvl-analytics-2026.gold_analytics.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'mercado_valor_diario'
),
comparacion AS (
  SELECT
    COALESCE(E.columna, O.columna) AS columna,
    E.particion AS particion_esperada,
    O.particion AS particion_observada,
    E.clustering_orden AS clustering_esperado,
    O.clustering_orden AS clustering_observado,
    CASE
      WHEN E.columna IS NULL THEN 'ADICIONAL'
      WHEN O.columna IS NULL THEN 'FALTANTE'
      WHEN E.particion IS DISTINCT FROM O.particion
        THEN 'PARTICION_INCORRECTA'
      WHEN E.clustering_orden
           IS DISTINCT FROM O.clustering_orden
        THEN 'CLUSTERING_INCORRECTO'
      ELSE 'OK'
    END AS estado
  FROM esperado E
  FULL OUTER JOIN observado O
    ON E.columna = O.columna
)
SELECT
  'mercado_valor_diario' AS producto,
  *
FROM comparacion
ORDER BY columna;
