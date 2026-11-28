-- CANDIDATO LOCAL NO AUTORIZADO.
-- Consulta de validacion exclusivamente.
-- No ejecutar sin acreditacion operativa.

-- Valida columna de particion y orden clustering.
-- No acredita granularidad ni todas las opciones.
-- Esos controles requieren metadata de tabla.

WITH esperado AS (
  SELECT *
  FROM UNNEST([
    STRUCT('fecha' AS columna, 'YES' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('codigo_sab' AS columna, 'NO' AS particion, 1 AS clustering_orden),
    STRUCT('sociedad_agente_bolsa' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('monto_compras' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('monto_ventas' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('monto_total' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('porcentaje_compras_original' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('porcentaje_ventas_original' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('porcentaje_total_original' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('fuente_datos' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('fecha_ingesta' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('montos_validos' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('diferencia_monetaria' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('total_consistente' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('saldo_neto' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('total_compras_diario' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('total_ventas_diario' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('total_actividad_diaria' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('cantidad_sab_diaria' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('registros_invalidos_diarios' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('actividad_diaria_valida' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('participacion_compras_pct' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('participacion_ventas_pct' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('participacion_total_pct' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('diferencia_participacion_compras' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('diferencia_participacion_ventas' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('diferencia_participacion_total' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('ranking_diario' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('aporte_hhi' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('hhi_diario' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('anio' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('mes' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('trimestre' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('registros_elegibles' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('registros_cuarentena' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('archivos_excluidos' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('estado_cobertura' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('version_silver' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('scope_quality_id' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('trazabilidad_verificada' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('cobertura_completa' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden)
  ])
),
observado AS (
  SELECT
    column_name AS columna,
    is_partitioning_column AS particion,
    clustering_ordinal_position AS clustering_orden
  FROM `bvl-analytics-2026.gold_analytics.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'actividad_sab_diaria'
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
  'actividad_sab_diaria' AS producto,
  *
FROM comparacion
ORDER BY columna;
