-- BLOQUEO DE SEGURIDAD: SQL INDEPENDIENTE NO AUTORIZADO.
-- No retirar sin integración del preflight firmado y Quality.
ASSERT FALSE AS
  'EJECUCION DIRECTA PROHIBIDA: usar orquestacion autorizada por Quality';

-- SIMULACIÓN LOCAL: NO EJECUTAR EN GCP SIN QUALITY.
-- El staging debe incluir:
-- operation: UPSERT | DELETE
-- delete_authorized: BOOL
-- run_id: STRING
-- source_version: STRING

ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT *
    FROM `gold_analytics_staging.actividad_sab_diaria_delta`
    WHERE batch_id = @batch_id
      AND run_id = @run_id
      AND source_version = @source_version
      AND payload_sha256 = @payload_sha256
  )
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
    FROM (
    SELECT *
    FROM `gold_analytics_staging.actividad_sab_diaria_delta`
    WHERE batch_id = @batch_id
      AND run_id = @run_id
      AND source_version = @source_version
      AND payload_sha256 = @payload_sha256
  )
    GROUP BY `fecha`, `codigo_sab`
    HAVING COUNT(*) > 1
  )
) AS 'Claves duplicadas en staging';

-- Controles obligatorios antes del MERGE.
ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT *
    FROM `gold_analytics_staging.actividad_sab_diaria_delta`
    WHERE batch_id = @batch_id
      AND run_id = @run_id
      AND source_version = @source_version
      AND payload_sha256 = @payload_sha256
  )
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
  FROM (
    SELECT *
    FROM `gold_analytics_staging.actividad_sab_diaria_delta`
    WHERE batch_id = @batch_id
      AND run_id = @run_id
      AND source_version = @source_version
      AND payload_sha256 = @payload_sha256
  )
  WHERE operation = 'DELETE'
) AS 'DELETE bloqueado hasta autorización oficial de Quality';

-- Validación de campos obligatorios para UPSERT.
ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT *
    FROM `gold_analytics_staging.actividad_sab_diaria_delta`
    WHERE batch_id = @batch_id
      AND run_id = @run_id
      AND source_version = @source_version
      AND payload_sha256 = @payload_sha256
  )
  WHERE operation = 'UPSERT'
    AND (
      `montos_validos` IS NULL OR
      `total_consistente` IS NULL OR
      `cantidad_sab_diaria` IS NULL
    )
) AS 'UPSERT rechazado: campos obligatorios nulos';

-- Integridad de identidad del batch.
ASSERT (
  @batch_id IS NOT NULL
  AND TRIM(@batch_id) != ''
  AND @run_id IS NOT NULL
  AND TRIM(@run_id) != ''
  AND @source_version IS NOT NULL
  AND TRIM(@source_version) != ''
  AND @payload_sha256 IS NOT NULL
  AND REGEXP_CONTAINS(@payload_sha256, r'^[0-9a-fA-F]{64}$')
) AS 'ERROR: parametros del batch invalidos';

ASSERT (
  SELECT COUNT(*) > 0
  FROM `gold_analytics_staging.actividad_sab_diaria_delta`
  WHERE batch_id = @batch_id
    AND run_id = @run_id
    AND source_version = @source_version
    AND payload_sha256 = @payload_sha256
) AS 'ERROR: batch vacio';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics_staging.actividad_sab_diaria_delta`
  WHERE batch_id = @batch_id
    AND (
      run_id IS DISTINCT FROM @run_id
      OR source_version IS DISTINCT FROM @source_version
      OR payload_sha256 IS DISTINCT FROM @payload_sha256
    )
) AS 'ERROR: batch_id con metadatos inconsistentes';

MERGE `gold_analytics.actividad_sab_diaria` T
USING (
  SELECT *
  FROM `gold_analytics_staging.actividad_sab_diaria_delta`
  WHERE batch_id = @batch_id
    AND run_id = @run_id
    AND source_version = @source_version
    AND payload_sha256 = @payload_sha256
) S
ON T.`fecha` = S.`fecha` AND T.`codigo_sab` = S.`codigo_sab`

WHEN MATCHED AND S.operation = 'DELETE'
  THEN DELETE

WHEN MATCHED AND S.operation = 'UPSERT'
  AND (
    T.`fecha` IS DISTINCT FROM S.`fecha`
    OR T.`codigo_sab` IS DISTINCT FROM S.`codigo_sab`
    OR T.`sociedad_agente_bolsa` IS DISTINCT FROM S.`sociedad_agente_bolsa`
    OR T.`monto_compras` IS DISTINCT FROM S.`monto_compras`
    OR T.`monto_ventas` IS DISTINCT FROM S.`monto_ventas`
    OR T.`monto_total` IS DISTINCT FROM S.`monto_total`
    OR T.`porcentaje_compras_original` IS DISTINCT FROM S.`porcentaje_compras_original`
    OR T.`porcentaje_ventas_original` IS DISTINCT FROM S.`porcentaje_ventas_original`
    OR T.`porcentaje_total_original` IS DISTINCT FROM S.`porcentaje_total_original`
    OR T.`fuente_datos` IS DISTINCT FROM S.`fuente_datos`
    OR T.`fecha_ingesta` IS DISTINCT FROM S.`fecha_ingesta`
    OR T.`montos_validos` IS DISTINCT FROM S.`montos_validos`
    OR T.`diferencia_monetaria` IS DISTINCT FROM S.`diferencia_monetaria`
    OR T.`total_consistente` IS DISTINCT FROM S.`total_consistente`
    OR T.`saldo_neto` IS DISTINCT FROM S.`saldo_neto`
    OR T.`total_compras_diario` IS DISTINCT FROM S.`total_compras_diario`
    OR T.`total_ventas_diario` IS DISTINCT FROM S.`total_ventas_diario`
    OR T.`total_actividad_diaria` IS DISTINCT FROM S.`total_actividad_diaria`
    OR T.`cantidad_sab_diaria` IS DISTINCT FROM S.`cantidad_sab_diaria`
    OR T.`registros_invalidos_diarios` IS DISTINCT FROM S.`registros_invalidos_diarios`
    OR T.`actividad_diaria_valida` IS DISTINCT FROM S.`actividad_diaria_valida`
    OR T.`participacion_compras_pct` IS DISTINCT FROM S.`participacion_compras_pct`
    OR T.`participacion_ventas_pct` IS DISTINCT FROM S.`participacion_ventas_pct`
    OR T.`participacion_total_pct` IS DISTINCT FROM S.`participacion_total_pct`
    OR T.`diferencia_participacion_compras` IS DISTINCT FROM S.`diferencia_participacion_compras`
    OR T.`diferencia_participacion_ventas` IS DISTINCT FROM S.`diferencia_participacion_ventas`
    OR T.`diferencia_participacion_total` IS DISTINCT FROM S.`diferencia_participacion_total`
    OR T.`ranking_diario` IS DISTINCT FROM S.`ranking_diario`
    OR T.`aporte_hhi` IS DISTINCT FROM S.`aporte_hhi`
    OR T.`hhi_diario` IS DISTINCT FROM S.`hhi_diario`
    OR T.`anio` IS DISTINCT FROM S.`anio`
    OR T.`mes` IS DISTINCT FROM S.`mes`
    OR T.`trimestre` IS DISTINCT FROM S.`trimestre`
  )
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
