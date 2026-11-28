-- CANDIDATO LOCAL NO AUTORIZADO.
-- Consulta de validacion exclusivamente.
-- No ejecutar sin acreditacion operativa.

-- Valida columna de particion y orden clustering.
-- No acredita granularidad ni todas las opciones.
-- Esos controles requieren metadata de tabla.

WITH esperado AS (
  SELECT *
  FROM UNNEST([
    STRUCT('rpj' AS columna, 'NO' AS particion, 2 AS clustering_orden),
    STRUCT('ejercicio' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('trimestre_num' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('id_registro' AS columna, 'NO' AS particion, 1 AS clustering_orden),
    STRUCT('clave_negocio' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('ruc' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('ruc_valido' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('nombre_empresa' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('tipo_empresa' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('tipo_sector' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('ciiu' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('trimestre_original' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('tipo_informacion' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('metodo_flujo_efectivo' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('moneda_original' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('activo_total' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('pasivo_total' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('patrimonio_total' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('total_ingreso' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('utilidad_neta' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('cantidad_clave_negocio' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('colision_clave_negocio' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('fuente_datos' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('fecha_ingesta' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('moneda' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('anio' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('trimestre' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('periodo_valido' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('periodo' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('fecha_fin_trimestre' AS columna, 'YES' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('activo_valido' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('pasivo_valido' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('patrimonio_valido' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('patrimonio_positivo' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('ingreso_valido' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('ingreso_positivo' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('utilidad_valida' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('moneda_valida' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('identidad_contable_evaluable' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('diferencia_contable' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('identidad_contable_valida' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('cantidad_registros_empresa_periodo' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('colision_financiera' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('sin_incidencias_estructurales' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('apto_kpi_general' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('roa_calculable' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('roe_calculable' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('endeudamiento_calculable' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('margen_neto_calculable' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('autonomia_calculable' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('roa_aproximado_pct' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('roe_aproximado_pct' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('endeudamiento_pct' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('margen_neto_pct' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('autonomia_financiera_pct' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('resultado_financiero' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('patrimonio_negativo' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden),
    STRUCT('motivos_calidad' AS columna, 'NO' AS particion, CAST(NULL AS INT64) AS clustering_orden)
  ])
),
observado AS (
  SELECT
    column_name AS columna,
    is_partitioning_column AS particion,
    clustering_ordinal_position AS clustering_orden
  FROM `bvl-analytics-2026.gold_analytics.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'empresa_trimestre'
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
  'empresa_trimestre' AS producto,
  *
FROM comparacion
ORDER BY columna;
