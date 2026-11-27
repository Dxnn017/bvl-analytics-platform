-- SIMULACIÓN LOCAL: NO EJECUTAR EN GCP SIN QUALITY.
-- El staging debe incluir:
-- operation: UPSERT | DELETE
-- delete_authorized: BOOL
-- run_id: STRING
-- source_version: STRING

ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics_staging.actividad_sab_diaria_delta`
  WHERE operation NOT IN ('UPSERT', 'DELETE')
     OR operation IS NULL
     OR run_id IS NULL
     OR source_version IS NULL
     OR (operation = 'DELETE'
         AND delete_authorized IS NOT TRUE)
) AS 'Operación inválida o eliminación no autorizada';

ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT `fecha`, `codigo_sab`
    FROM `gold_analytics_staging.actividad_sab_diaria_delta`
    GROUP BY `fecha`, `codigo_sab`
    HAVING COUNT(*) > 1
  )
) AS 'Claves duplicadas en staging';

-- Controles obligatorios antes del MERGE.
ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics_staging.actividad_sab_diaria_delta`
  WHERE `fecha` IS NULL OR TRIM(CAST(`fecha` AS STRING)) = '' OR `codigo_sab` IS NULL OR TRIM(CAST(`codigo_sab` AS STRING)) = ''
) AS 'Claves nulas o vacías en staging';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics.actividad_sab_diaria`
  WHERE `fecha` IS NULL OR TRIM(CAST(`fecha` AS STRING)) = '' OR `codigo_sab` IS NULL OR TRIM(CAST(`codigo_sab` AS STRING)) = ''
) AS 'Claves nulas o vacías en destino';

ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT `fecha`, `codigo_sab`
    FROM `gold_analytics.actividad_sab_diaria`
    GROUP BY `fecha`, `codigo_sab`
    HAVING COUNT(*) > 1
  )
) AS 'Claves duplicadas en destino';

-- Bloqueo temporal: DELETE requiere autorización verificable.
ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics_staging.actividad_sab_diaria_delta`
  WHERE operation = 'DELETE'
) AS 'DELETE bloqueado hasta autorización oficial de Quality';

MERGE `gold_analytics.actividad_sab_diaria` T
USING `gold_analytics_staging.actividad_sab_diaria_delta` S
ON T.`fecha` = S.`fecha` AND T.`codigo_sab` = S.`codigo_sab`

WHEN MATCHED AND S.operation = 'DELETE'
  THEN DELETE

WHEN MATCHED AND S.operation = 'UPSERT'
  THEN UPDATE SET
    `sociedad_agente_bolsa` = S.`sociedad_agente_bolsa`,
    `monto_compras` = S.`monto_compras`,
    `monto_ventas` = S.`monto_ventas`,
    `monto_total` = S.`monto_total`,
    `porcentaje_compras_original` = S.`porcentaje_compras_original`,
    `porcentaje_ventas_original` = S.`porcentaje_ventas_original`,
    `porcentaje_total_original` = S.`porcentaje_total_original`,
    `fuente_datos` = S.`fuente_datos`,
    `fecha_ingesta` = S.`fecha_ingesta`,
    `montos_validos` = S.`montos_validos`,
    `diferencia_monetaria` = S.`diferencia_monetaria`,
    `total_consistente` = S.`total_consistente`,
    `saldo_neto` = S.`saldo_neto`,
    `total_compras_diario` = S.`total_compras_diario`,
    `total_ventas_diario` = S.`total_ventas_diario`,
    `total_actividad_diaria` = S.`total_actividad_diaria`,
    `cantidad_sab_diaria` = S.`cantidad_sab_diaria`,
    `registros_invalidos_diarios` = S.`registros_invalidos_diarios`,
    `actividad_diaria_valida` = S.`actividad_diaria_valida`,
    `participacion_compras_pct` = S.`participacion_compras_pct`,
    `participacion_ventas_pct` = S.`participacion_ventas_pct`,
    `participacion_total_pct` = S.`participacion_total_pct`,
    `diferencia_participacion_compras` = S.`diferencia_participacion_compras`,
    `diferencia_participacion_ventas` = S.`diferencia_participacion_ventas`,
    `diferencia_participacion_total` = S.`diferencia_participacion_total`,
    `ranking_diario` = S.`ranking_diario`,
    `aporte_hhi` = S.`aporte_hhi`,
    `hhi_diario` = S.`hhi_diario`,
    `anio` = S.`anio`,
    `mes` = S.`mes`,
    `trimestre` = S.`trimestre`

WHEN NOT MATCHED AND S.operation = 'UPSERT'
  THEN INSERT (`fecha`, `codigo_sab`, `sociedad_agente_bolsa`, `monto_compras`, `monto_ventas`, `monto_total`, `porcentaje_compras_original`, `porcentaje_ventas_original`, `porcentaje_total_original`, `fuente_datos`, `fecha_ingesta`, `montos_validos`, `diferencia_monetaria`, `total_consistente`, `saldo_neto`, `total_compras_diario`, `total_ventas_diario`, `total_actividad_diaria`, `cantidad_sab_diaria`, `registros_invalidos_diarios`, `actividad_diaria_valida`, `participacion_compras_pct`, `participacion_ventas_pct`, `participacion_total_pct`, `diferencia_participacion_compras`, `diferencia_participacion_ventas`, `diferencia_participacion_total`, `ranking_diario`, `aporte_hhi`, `hhi_diario`, `anio`, `mes`, `trimestre`)
  VALUES (S.`fecha`, S.`codigo_sab`, S.`sociedad_agente_bolsa`, S.`monto_compras`, S.`monto_ventas`, S.`monto_total`, S.`porcentaje_compras_original`, S.`porcentaje_ventas_original`, S.`porcentaje_total_original`, S.`fuente_datos`, S.`fecha_ingesta`, S.`montos_validos`, S.`diferencia_monetaria`, S.`total_consistente`, S.`saldo_neto`, S.`total_compras_diario`, S.`total_ventas_diario`, S.`total_actividad_diaria`, S.`cantidad_sab_diaria`, S.`registros_invalidos_diarios`, S.`actividad_diaria_valida`, S.`participacion_compras_pct`, S.`participacion_ventas_pct`, S.`participacion_total_pct`, S.`diferencia_participacion_compras`, S.`diferencia_participacion_ventas`, S.`diferencia_participacion_total`, S.`ranking_diario`, S.`aporte_hhi`, S.`hhi_diario`, S.`anio`, S.`mes`, S.`trimestre`);
