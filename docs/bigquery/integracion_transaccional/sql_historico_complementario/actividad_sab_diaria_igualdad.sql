-- CANDIDATO LOCAL NO AUTORIZADO.
-- Consulta de validacion exclusivamente.
-- No ejecutar sin acreditacion operativa.

-- Comparacion de una version historica completa.
-- No usar despues de aplicar deltas posteriores.
-- El staging debe corresponder a la misma version.
-- Los duplicados impiden aprobar la igualdad.

WITH origen AS (
  SELECT `fecha`, `codigo_sab`, `sociedad_agente_bolsa`, `monto_compras`, `monto_ventas`, `monto_total`, `porcentaje_compras_original`, `porcentaje_ventas_original`, `porcentaje_total_original`, `fuente_datos`, `fecha_ingesta`, `montos_validos`, `diferencia_monetaria`, `total_consistente`, `saldo_neto`, `total_compras_diario`, `total_ventas_diario`, `total_actividad_diaria`, `cantidad_sab_diaria`, `registros_invalidos_diarios`, `actividad_diaria_valida`, `participacion_compras_pct`, `participacion_ventas_pct`, `participacion_total_pct`, `diferencia_participacion_compras`, `diferencia_participacion_ventas`, `diferencia_participacion_total`, `ranking_diario`, `aporte_hhi`, `hhi_diario`, `anio`, `mes`, `trimestre`, `registros_elegibles`, `registros_cuarentena`, `archivos_excluidos`, `estado_cobertura`, `version_silver`, `scope_quality_id`, `trazabilidad_verificada`, `cobertura_completa`,
         TRUE AS presente_origen
  FROM `bvl-analytics-2026.gold_analytics_staging.actividad_sab_diaria_hist_BATCH_CANDIDATO`
),
destino AS (
  SELECT `fecha`, `codigo_sab`, `sociedad_agente_bolsa`, `monto_compras`, `monto_ventas`, `monto_total`, `porcentaje_compras_original`, `porcentaje_ventas_original`, `porcentaje_total_original`, `fuente_datos`, `fecha_ingesta`, `montos_validos`, `diferencia_monetaria`, `total_consistente`, `saldo_neto`, `total_compras_diario`, `total_ventas_diario`, `total_actividad_diaria`, `cantidad_sab_diaria`, `registros_invalidos_diarios`, `actividad_diaria_valida`, `participacion_compras_pct`, `participacion_ventas_pct`, `participacion_total_pct`, `diferencia_participacion_compras`, `diferencia_participacion_ventas`, `diferencia_participacion_total`, `ranking_diario`, `aporte_hhi`, `hhi_diario`, `anio`, `mes`, `trimestre`, `registros_elegibles`, `registros_cuarentena`, `archivos_excluidos`, `estado_cobertura`, `version_silver`, `scope_quality_id`, `trazabilidad_verificada`, `cobertura_completa`,
         TRUE AS presente_destino
  FROM `bvl-analytics-2026.gold_analytics.actividad_sab_diaria`
),
calidad_origen AS (
  SELECT
    COUNT(*) AS filas_origen,
    COUNTIF(`fecha` IS NULL OR `codigo_sab` IS NULL OR TRIM(`codigo_sab`) = '') AS claves_invalidas_origen
  FROM origen
),
calidad_destino AS (
  SELECT
    COUNT(*) AS filas_destino,
    COUNTIF(`fecha` IS NULL OR `codigo_sab` IS NULL OR TRIM(`codigo_sab`) = '') AS claves_invalidas_destino
  FROM destino
),
duplicados_origen AS (
  SELECT COUNT(*) AS claves_duplicadas_origen
  FROM (
    SELECT `fecha`, `codigo_sab`
    FROM origen
    GROUP BY `fecha`, `codigo_sab`
    HAVING COUNT(*) > 1
  )
),
duplicados_destino AS (
  SELECT COUNT(*) AS claves_duplicadas_destino
  FROM (
    SELECT `fecha`, `codigo_sab`
    FROM destino
    GROUP BY `fecha`, `codigo_sab`
    HAVING COUNT(*) > 1
  )
),
comparacion AS (
  SELECT
    CASE
      WHEN S.presente_origen IS NULL
        THEN 'ADICIONAL_DESTINO'
      WHEN T.presente_destino IS NULL
        THEN 'FALTANTE_DESTINO'
      WHEN (
          S.`fecha` IS DISTINCT FROM T.`fecha`
          OR S.`codigo_sab` IS DISTINCT FROM T.`codigo_sab`
          OR S.`sociedad_agente_bolsa` IS DISTINCT FROM T.`sociedad_agente_bolsa`
          OR S.`monto_compras` IS DISTINCT FROM T.`monto_compras`
          OR S.`monto_ventas` IS DISTINCT FROM T.`monto_ventas`
          OR S.`monto_total` IS DISTINCT FROM T.`monto_total`
          OR S.`porcentaje_compras_original` IS DISTINCT FROM T.`porcentaje_compras_original`
          OR S.`porcentaje_ventas_original` IS DISTINCT FROM T.`porcentaje_ventas_original`
          OR S.`porcentaje_total_original` IS DISTINCT FROM T.`porcentaje_total_original`
          OR S.`fuente_datos` IS DISTINCT FROM T.`fuente_datos`
          OR S.`fecha_ingesta` IS DISTINCT FROM T.`fecha_ingesta`
          OR S.`montos_validos` IS DISTINCT FROM T.`montos_validos`
          OR S.`diferencia_monetaria` IS DISTINCT FROM T.`diferencia_monetaria`
          OR S.`total_consistente` IS DISTINCT FROM T.`total_consistente`
          OR S.`saldo_neto` IS DISTINCT FROM T.`saldo_neto`
          OR S.`total_compras_diario` IS DISTINCT FROM T.`total_compras_diario`
          OR S.`total_ventas_diario` IS DISTINCT FROM T.`total_ventas_diario`
          OR S.`total_actividad_diaria` IS DISTINCT FROM T.`total_actividad_diaria`
          OR S.`cantidad_sab_diaria` IS DISTINCT FROM T.`cantidad_sab_diaria`
          OR S.`registros_invalidos_diarios` IS DISTINCT FROM T.`registros_invalidos_diarios`
          OR S.`actividad_diaria_valida` IS DISTINCT FROM T.`actividad_diaria_valida`
          OR S.`participacion_compras_pct` IS DISTINCT FROM T.`participacion_compras_pct`
          OR S.`participacion_ventas_pct` IS DISTINCT FROM T.`participacion_ventas_pct`
          OR S.`participacion_total_pct` IS DISTINCT FROM T.`participacion_total_pct`
          OR S.`diferencia_participacion_compras` IS DISTINCT FROM T.`diferencia_participacion_compras`
          OR S.`diferencia_participacion_ventas` IS DISTINCT FROM T.`diferencia_participacion_ventas`
          OR S.`diferencia_participacion_total` IS DISTINCT FROM T.`diferencia_participacion_total`
          OR S.`ranking_diario` IS DISTINCT FROM T.`ranking_diario`
          OR S.`aporte_hhi` IS DISTINCT FROM T.`aporte_hhi`
          OR S.`hhi_diario` IS DISTINCT FROM T.`hhi_diario`
          OR S.`anio` IS DISTINCT FROM T.`anio`
          OR S.`mes` IS DISTINCT FROM T.`mes`
          OR S.`trimestre` IS DISTINCT FROM T.`trimestre`
          OR S.`registros_elegibles` IS DISTINCT FROM T.`registros_elegibles`
          OR S.`registros_cuarentena` IS DISTINCT FROM T.`registros_cuarentena`
          OR S.`archivos_excluidos` IS DISTINCT FROM T.`archivos_excluidos`
          OR S.`estado_cobertura` IS DISTINCT FROM T.`estado_cobertura`
          OR S.`version_silver` IS DISTINCT FROM T.`version_silver`
          OR S.`scope_quality_id` IS DISTINCT FROM T.`scope_quality_id`
          OR S.`trazabilidad_verificada` IS DISTINCT FROM T.`trazabilidad_verificada`
          OR S.`cobertura_completa` IS DISTINCT FROM T.`cobertura_completa`
      ) THEN 'MODIFICADA'
      ELSE 'IGUAL'
    END AS estado
  FROM origen S
  FULL OUTER JOIN destino T
    ON S.`fecha` = T.`fecha` AND S.`codigo_sab` = T.`codigo_sab`
),
metricas AS (
  SELECT
    COUNTIF(estado = 'ADICIONAL_DESTINO')
      AS filas_adicionales,
    COUNTIF(estado = 'FALTANTE_DESTINO')
      AS filas_faltantes,
    COUNTIF(estado = 'MODIFICADA')
      AS filas_modificadas,
    COUNTIF(estado = 'IGUAL')
      AS filas_iguales
  FROM comparacion
),
resultado AS (
  SELECT
    'actividad_sab_diaria' AS producto,
    @expected_rows AS filas_esperadas,
    41 AS columnas_comparadas,
    O.*,
    T.*,
    D1.*,
    D2.*,
    M.*
  FROM calidad_origen O
  CROSS JOIN calidad_destino T
  CROSS JOIN duplicados_origen D1
  CROSS JOIN duplicados_destino D2
  CROSS JOIN metricas M
)
SELECT *,
  COALESCE(
    filas_esperadas > 0
    AND filas_origen = filas_esperadas
    AND filas_destino = filas_esperadas
    AND filas_iguales = filas_esperadas
    AND claves_invalidas_origen = 0
    AND claves_invalidas_destino = 0
    AND claves_duplicadas_origen = 0
    AND claves_duplicadas_destino = 0
    AND filas_adicionales = 0
    AND filas_faltantes = 0
    AND filas_modificadas = 0,
    FALSE
  ) AS igualdad_aprobada
FROM resultado;
