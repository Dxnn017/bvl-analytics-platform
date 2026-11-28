-- CANDIDATO LOCAL NO AUTORIZADO.
-- No retirar el bloqueo sin acreditacion Quality.
-- No ejecutar directamente en GCP.

ASSERT FALSE AS
  'EJECUCION OPERATIVA NO AUTORIZADA';

-- Producto: actividad_sab_diaria
-- Requiere reserva exclusiva externa por producto.
-- La identidad criptografica debe acreditarse antes
-- de cargar staging. Comparar parametros no verifica
-- por si solo el contenido de los objetos Parquet.
-- Carga historica oficial previa obligatoria.
-- Ejecutar como un script unico, no por fragmentos.

ASSERT (
  @expected_rows IS NOT NULL
  AND @expected_rows > 0
  AND @batch_id IS NOT NULL
  AND TRIM(@batch_id) != ''
  AND @run_id IS NOT NULL
  AND TRIM(@run_id) != ''
  AND @source_version IS NOT NULL
  AND TRIM(@source_version) != ''
  AND @previous_version IS NOT NULL
  AND TRIM(@previous_version) != ''
  AND @new_version IS NOT NULL
  AND TRIM(@new_version) != ''
  AND @previous_version != @new_version
  AND COALESCE(REGEXP_CONTAINS(@source_manifest_sha256, r'^[0-9a-f]{64}$'), FALSE)
  AND COALESCE(REGEXP_CONTAINS(@gold_contract_sha256, r'^[0-9a-f]{64}$'), FALSE)
  AND COALESCE(REGEXP_CONTAINS(@payload_sha256, r'^[0-9a-f]{64}$'), FALSE)
  AND COALESCE(REGEXP_CONTAINS(@staging_content_sha256, r'^[0-9a-f]{64}$'), FALSE)
  AND COALESCE(REGEXP_CONTAINS(@evidence_sha256, r'^[0-9a-f]{64}$'), FALSE)
  AND COALESCE(REGEXP_CONTAINS(@transfer_sha256, r'^[0-9a-f]{64}$'), FALSE)
  AND @gold_contract_sha256 =
      'f6c520defe557a979306dfcd8da05b97be2ac30aeba8271be8adaa53edad0bbe'
) AS 'Identidad candidata incompatible';

BEGIN
  BEGIN TRANSACTION;

  ASSERT (
    SELECT COUNT(*) <= 1
    FROM `bvl-analytics-2026.gold_analytics_control.incremental_batches_v2`
    WHERE product = 'actividad_sab_diaria'
      AND batch_id = @batch_id
  ) AS 'Batch duplicado';

  ASSERT (
    SELECT COUNT(*) = 0
    FROM `bvl-analytics-2026.gold_analytics_control.incremental_batches_v2`
    WHERE product = 'actividad_sab_diaria'
      AND batch_id = @batch_id
      AND (
        run_id IS DISTINCT FROM @run_id
      OR source_version IS DISTINCT FROM @source_version
      OR previous_version IS DISTINCT FROM @previous_version
      OR new_version IS DISTINCT FROM @new_version
      OR source_manifest_sha256 IS DISTINCT FROM @source_manifest_sha256
      OR gold_contract_sha256 IS DISTINCT FROM @gold_contract_sha256
      OR payload_sha256 IS DISTINCT FROM @payload_sha256
      OR staging_content_sha256 IS DISTINCT FROM @staging_content_sha256
      OR evidence_sha256 IS DISTINCT FROM @evidence_sha256
      OR transfer_sha256 IS DISTINCT FROM @transfer_sha256
        OR status IS DISTINCT FROM 'COMPLETED'
        OR received_rows != @expected_rows
        OR updated_rows !=
           business_updates + technical_updates
        OR received_rows !=
           inserted_rows + updated_rows + unchanged_rows
        OR deleted_rows != 0
        OR inserted_rows < 0
        OR business_updates < 0
        OR technical_updates < 0
        OR unchanged_rows < 0
      )
  ) AS 'Identidad o auditoria de replay incompatible';

  IF EXISTS (
    SELECT 1 FROM `bvl-analytics-2026.gold_analytics_control.incremental_batches_v2`
    WHERE product = 'actividad_sab_diaria'
      AND batch_id = @batch_id
  ) THEN

    ASSERT (
      SELECT COUNT(*) = 1
      FROM `bvl-analytics-2026.gold_analytics_control.version_history_v2`
      WHERE product = 'actividad_sab_diaria'
        AND version = @new_version
    ) AS 'Replay sin historial de version';

    SELECT 'REPLAY_NOOP' AS resultado_batch,
           0 AS applied_rows;

  ELSE

    ASSERT (
      SELECT COUNT(*) = 1
      FROM `bvl-analytics-2026.gold_analytics_control.product_versions_v2`
      WHERE product = 'actividad_sab_diaria'
    ) AS 'Registro de version ausente o duplicado';

    ASSERT (
      SELECT COUNT(*) = 1
      FROM `bvl-analytics-2026.gold_analytics_control.product_versions_v2`
      WHERE product = 'actividad_sab_diaria'
        AND current_version = @previous_version
    ) AS 'Continuidad incompatible';

    ASSERT (
      SELECT COUNT(*) = 1
      FROM `bvl-analytics-2026.gold_analytics_control.version_history_v2`
      WHERE product = 'actividad_sab_diaria'
        AND version = @previous_version
    ) AS 'Version anterior sin historial';

    ASSERT (
      SELECT COUNT(*) = 0
      FROM `bvl-analytics-2026.gold_analytics_control.version_history_v2`
      WHERE product = 'actividad_sab_diaria'
        AND version = @new_version
    ) AS 'Version previamente aplicada';

    -- No omitir filas del batch con metadatos distintos.
    ASSERT (
      SELECT COUNT(*) = 0
      FROM `bvl-analytics-2026.gold_analytics_staging.actividad_sab_diaria_delta`
      WHERE batch_id = @batch_id
        AND (
          run_id IS DISTINCT FROM @run_id
          OR source_version IS DISTINCT FROM @source_version
          OR payload_sha256 IS DISTINCT FROM @payload_sha256
          OR operation IS DISTINCT FROM 'UPSERT'
          OR delete_authorized IS DISTINCT FROM FALSE
        )
    ) AS 'Batch incompatible o DELETE bloqueado';

    CREATE TEMP TABLE delta_batch AS
    SELECT *
    FROM `bvl-analytics-2026.gold_analytics_staging.actividad_sab_diaria_delta`
    WHERE batch_id = @batch_id;

    ASSERT (
      SELECT COUNT(*) = @expected_rows
      FROM delta_batch
    ) AS 'Conteo staging incompatible';

    -- Controles originales del producto:
    -- claves, nulos, cobertura y operaciones.

ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT *
    FROM `bvl-analytics-2026.gold_analytics_staging.actividad_sab_diaria_delta`
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
    FROM `bvl-analytics-2026.gold_analytics_staging.actividad_sab_diaria_delta`
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
    FROM `bvl-analytics-2026.gold_analytics_staging.actividad_sab_diaria_delta`
    WHERE batch_id = @batch_id
      AND run_id = @run_id
      AND source_version = @source_version
      AND payload_sha256 = @payload_sha256
  )
  WHERE `fecha` IS NULL OR TRIM(CAST(`fecha` AS STRING)) = '' OR `codigo_sab` IS NULL OR TRIM(CAST(`codigo_sab` AS STRING)) = ''
) AS 'Claves nulas o vacías en staging';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `bvl-analytics-2026.gold_analytics.actividad_sab_diaria`
  WHERE `fecha` IS NULL OR TRIM(CAST(`fecha` AS STRING)) = '' OR `codigo_sab` IS NULL OR TRIM(CAST(`codigo_sab` AS STRING)) = ''
) AS 'Claves nulas o vacías en destino';

ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT `fecha`, `codigo_sab`
    FROM `bvl-analytics-2026.gold_analytics.actividad_sab_diaria`
    GROUP BY `fecha`, `codigo_sab`
    HAVING COUNT(*) > 1
  )
) AS 'Claves duplicadas en destino';

-- Bloqueo temporal: DELETE requiere autorización verificable.
ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT *
    FROM `bvl-analytics-2026.gold_analytics_staging.actividad_sab_diaria_delta`
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
    FROM `bvl-analytics-2026.gold_analytics_staging.actividad_sab_diaria_delta`
    WHERE batch_id = @batch_id
      AND run_id = @run_id
      AND source_version = @source_version
      AND payload_sha256 = @payload_sha256
  )
  WHERE operation = 'UPSERT'
    AND (
      `montos_validos` IS NULL OR
      `total_consistente` IS NULL OR
      `cantidad_sab_diaria` IS NULL OR
       `estado_cobertura` IS NULL OR
       `estado_cobertura` NOT IN
         ('COMPLETA', 'PARCIAL', 'DESCONOCIDA') OR
       `trazabilidad_verificada` IS NULL OR
       `cobertura_completa` IS NULL OR
       `cobertura_completa` IS DISTINCT FROM
         (`estado_cobertura` = 'COMPLETA') OR
       (`registros_elegibles` IS NOT NULL AND
         (`registros_elegibles` < 0 OR
          `registros_elegibles` != `cantidad_sab_diaria`)) OR
       `registros_cuarentena` < 0 OR
       `archivos_excluidos` < 0 OR
       (`estado_cobertura` IN ('COMPLETA', 'PARCIAL')
         AND (
           `trazabilidad_verificada` IS NOT TRUE OR
           NULLIF(TRIM(`version_silver`), '') IS NULL OR
           NULLIF(TRIM(`scope_quality_id`), '') IS NULL OR
           `registros_elegibles` IS NULL OR
           `registros_cuarentena` IS NULL OR
           `archivos_excluidos` IS NULL
         )) OR
       (`estado_cobertura` = 'COMPLETA'
         AND (`registros_cuarentena` != 0 OR
              `archivos_excluidos` != 0)) OR
       (`estado_cobertura` = 'PARCIAL'
         AND `registros_cuarentena` = 0
         AND `archivos_excluidos` = 0)
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
  FROM `bvl-analytics-2026.gold_analytics_staging.actividad_sab_diaria_delta`
  WHERE batch_id = @batch_id
    AND run_id = @run_id
    AND source_version = @source_version
    AND payload_sha256 = @payload_sha256
) AS 'ERROR: batch vacio';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `bvl-analytics-2026.gold_analytics_staging.actividad_sab_diaria_delta`
  WHERE batch_id = @batch_id
    AND (
      run_id IS DISTINCT FROM @run_id
      OR source_version IS DISTINCT FROM @source_version
      OR payload_sha256 IS DISTINCT FROM @payload_sha256
    )
) AS 'ERROR: batch_id con metadatos inconsistentes';




ASSERT (
  SELECT COUNT(*) = 0
  FROM delta_batch
  WHERE `fecha` IS NULL
     OR anio IS DISTINCT FROM
        EXTRACT(YEAR FROM `fecha`)
     OR mes IS DISTINCT FROM
        EXTRACT(MONTH FROM `fecha`)
) AS 'Particiones incompatibles';


    ASSERT (
      SELECT COUNT(*) = 0
      FROM delta_batch
      WHERE `monto_compras` IS DISTINCT FROM TRUNC(`monto_compras`, 2)
     OR `monto_ventas` IS DISTINCT FROM TRUNC(`monto_ventas`, 2)
     OR `monto_total` IS DISTINCT FROM TRUNC(`monto_total`, 2)
     OR IS_NAN(`porcentaje_compras_original`) OR IS_INF(`porcentaje_compras_original`)
     OR IS_NAN(`porcentaje_ventas_original`) OR IS_INF(`porcentaje_ventas_original`)
     OR IS_NAN(`porcentaje_total_original`) OR IS_INF(`porcentaje_total_original`)
     OR `diferencia_monetaria` IS DISTINCT FROM TRUNC(`diferencia_monetaria`, 2)
     OR `saldo_neto` IS DISTINCT FROM TRUNC(`saldo_neto`, 2)
     OR `total_compras_diario` IS DISTINCT FROM TRUNC(`total_compras_diario`, 2)
     OR `total_ventas_diario` IS DISTINCT FROM TRUNC(`total_ventas_diario`, 2)
     OR `total_actividad_diaria` IS DISTINCT FROM TRUNC(`total_actividad_diaria`, 2)
     OR IS_NAN(`participacion_compras_pct`) OR IS_INF(`participacion_compras_pct`)
     OR IS_NAN(`participacion_ventas_pct`) OR IS_INF(`participacion_ventas_pct`)
     OR IS_NAN(`participacion_total_pct`) OR IS_INF(`participacion_total_pct`)
     OR IS_NAN(`diferencia_participacion_compras`) OR IS_INF(`diferencia_participacion_compras`)
     OR IS_NAN(`diferencia_participacion_ventas`) OR IS_INF(`diferencia_participacion_ventas`)
     OR IS_NAN(`diferencia_participacion_total`) OR IS_INF(`diferencia_participacion_total`)
     OR IS_NAN(`aporte_hhi`) OR IS_INF(`aporte_hhi`)
     OR IS_NAN(`hhi_diario`) OR IS_INF(`hhi_diario`)
    ) AS 'Numericos incompatibles';

    -- Clasificacion previa al MERGE.
    CREATE TEMP TABLE metricas_batch AS
    WITH clasificacion AS (
      SELECT CASE
        WHEN T.`fecha` IS NULL
          THEN 'INSERT'
        WHEN (T.`fecha` IS DISTINCT FROM S.`fecha` OR T.`codigo_sab` IS DISTINCT FROM S.`codigo_sab` OR T.`sociedad_agente_bolsa` IS DISTINCT FROM S.`sociedad_agente_bolsa` OR T.`monto_compras` IS DISTINCT FROM S.`monto_compras` OR T.`monto_ventas` IS DISTINCT FROM S.`monto_ventas` OR T.`monto_total` IS DISTINCT FROM S.`monto_total` OR T.`porcentaje_compras_original` IS DISTINCT FROM S.`porcentaje_compras_original` OR T.`porcentaje_ventas_original` IS DISTINCT FROM S.`porcentaje_ventas_original` OR T.`porcentaje_total_original` IS DISTINCT FROM S.`porcentaje_total_original` OR T.`fuente_datos` IS DISTINCT FROM S.`fuente_datos` OR T.`montos_validos` IS DISTINCT FROM S.`montos_validos` OR T.`diferencia_monetaria` IS DISTINCT FROM S.`diferencia_monetaria` OR T.`total_consistente` IS DISTINCT FROM S.`total_consistente` OR T.`saldo_neto` IS DISTINCT FROM S.`saldo_neto` OR T.`total_compras_diario` IS DISTINCT FROM S.`total_compras_diario` OR T.`total_ventas_diario` IS DISTINCT FROM S.`total_ventas_diario` OR T.`total_actividad_diaria` IS DISTINCT FROM S.`total_actividad_diaria` OR T.`cantidad_sab_diaria` IS DISTINCT FROM S.`cantidad_sab_diaria` OR T.`registros_invalidos_diarios` IS DISTINCT FROM S.`registros_invalidos_diarios` OR T.`actividad_diaria_valida` IS DISTINCT FROM S.`actividad_diaria_valida` OR T.`participacion_compras_pct` IS DISTINCT FROM S.`participacion_compras_pct` OR T.`participacion_ventas_pct` IS DISTINCT FROM S.`participacion_ventas_pct` OR T.`participacion_total_pct` IS DISTINCT FROM S.`participacion_total_pct` OR T.`diferencia_participacion_compras` IS DISTINCT FROM S.`diferencia_participacion_compras` OR T.`diferencia_participacion_ventas` IS DISTINCT FROM S.`diferencia_participacion_ventas` OR T.`diferencia_participacion_total` IS DISTINCT FROM S.`diferencia_participacion_total` OR T.`ranking_diario` IS DISTINCT FROM S.`ranking_diario` OR T.`aporte_hhi` IS DISTINCT FROM S.`aporte_hhi` OR T.`hhi_diario` IS DISTINCT FROM S.`hhi_diario` OR T.`anio` IS DISTINCT FROM S.`anio` OR T.`mes` IS DISTINCT FROM S.`mes` OR T.`trimestre` IS DISTINCT FROM S.`trimestre` OR T.`registros_elegibles` IS DISTINCT FROM S.`registros_elegibles` OR T.`registros_cuarentena` IS DISTINCT FROM S.`registros_cuarentena` OR T.`archivos_excluidos` IS DISTINCT FROM S.`archivos_excluidos` OR T.`estado_cobertura` IS DISTINCT FROM S.`estado_cobertura` OR T.`version_silver` IS DISTINCT FROM S.`version_silver` OR T.`scope_quality_id` IS DISTINCT FROM S.`scope_quality_id` OR T.`trazabilidad_verificada` IS DISTINCT FROM S.`trazabilidad_verificada` OR T.`cobertura_completa` IS DISTINCT FROM S.`cobertura_completa`)
          THEN 'UPDATE_BUSINESS'
        WHEN (T.`fecha_ingesta` IS DISTINCT FROM S.`fecha_ingesta`)
          THEN 'UPDATE_TECHNICAL'
        ELSE 'NOOP'
      END AS accion
      FROM delta_batch S
      LEFT JOIN `bvl-analytics-2026.gold_analytics.actividad_sab_diaria` T
        ON T.`fecha` = S.`fecha` AND T.`codigo_sab` = S.`codigo_sab`
    )
    SELECT
      COUNT(*) AS received_rows,
      COUNTIF(accion = 'INSERT') AS inserted_rows,
      COUNTIF(accion IN (
        'UPDATE_BUSINESS',
        'UPDATE_TECHNICAL'
      )) AS updated_rows,
      COUNTIF(accion = 'UPDATE_BUSINESS')
        AS business_updates,
      COUNTIF(accion = 'UPDATE_TECHNICAL')
        AS technical_updates,
      COUNTIF(accion = 'NOOP') AS unchanged_rows,
      CAST(0 AS INT64) AS deleted_rows
    FROM clasificacion;

    ASSERT (
      SELECT
        received_rows = @expected_rows
        AND updated_rows =
            business_updates + technical_updates
        AND received_rows =
            inserted_rows + updated_rows + unchanged_rows
        AND deleted_rows = 0
      FROM metricas_batch
    ) AS 'Metricas incompatibles';

    MERGE `bvl-analytics-2026.gold_analytics.actividad_sab_diaria` T
    USING delta_batch S
      ON T.`fecha` = S.`fecha` AND T.`codigo_sab` = S.`codigo_sab`

    WHEN MATCHED
      AND (T.`fecha` IS DISTINCT FROM S.`fecha` OR T.`codigo_sab` IS DISTINCT FROM S.`codigo_sab` OR T.`sociedad_agente_bolsa` IS DISTINCT FROM S.`sociedad_agente_bolsa` OR T.`monto_compras` IS DISTINCT FROM S.`monto_compras` OR T.`monto_ventas` IS DISTINCT FROM S.`monto_ventas` OR T.`monto_total` IS DISTINCT FROM S.`monto_total` OR T.`porcentaje_compras_original` IS DISTINCT FROM S.`porcentaje_compras_original` OR T.`porcentaje_ventas_original` IS DISTINCT FROM S.`porcentaje_ventas_original` OR T.`porcentaje_total_original` IS DISTINCT FROM S.`porcentaje_total_original` OR T.`fuente_datos` IS DISTINCT FROM S.`fuente_datos` OR T.`fecha_ingesta` IS DISTINCT FROM S.`fecha_ingesta` OR T.`montos_validos` IS DISTINCT FROM S.`montos_validos` OR T.`diferencia_monetaria` IS DISTINCT FROM S.`diferencia_monetaria` OR T.`total_consistente` IS DISTINCT FROM S.`total_consistente` OR T.`saldo_neto` IS DISTINCT FROM S.`saldo_neto` OR T.`total_compras_diario` IS DISTINCT FROM S.`total_compras_diario` OR T.`total_ventas_diario` IS DISTINCT FROM S.`total_ventas_diario` OR T.`total_actividad_diaria` IS DISTINCT FROM S.`total_actividad_diaria` OR T.`cantidad_sab_diaria` IS DISTINCT FROM S.`cantidad_sab_diaria` OR T.`registros_invalidos_diarios` IS DISTINCT FROM S.`registros_invalidos_diarios` OR T.`actividad_diaria_valida` IS DISTINCT FROM S.`actividad_diaria_valida` OR T.`participacion_compras_pct` IS DISTINCT FROM S.`participacion_compras_pct` OR T.`participacion_ventas_pct` IS DISTINCT FROM S.`participacion_ventas_pct` OR T.`participacion_total_pct` IS DISTINCT FROM S.`participacion_total_pct` OR T.`diferencia_participacion_compras` IS DISTINCT FROM S.`diferencia_participacion_compras` OR T.`diferencia_participacion_ventas` IS DISTINCT FROM S.`diferencia_participacion_ventas` OR T.`diferencia_participacion_total` IS DISTINCT FROM S.`diferencia_participacion_total` OR T.`ranking_diario` IS DISTINCT FROM S.`ranking_diario` OR T.`aporte_hhi` IS DISTINCT FROM S.`aporte_hhi` OR T.`hhi_diario` IS DISTINCT FROM S.`hhi_diario` OR T.`anio` IS DISTINCT FROM S.`anio` OR T.`mes` IS DISTINCT FROM S.`mes` OR T.`trimestre` IS DISTINCT FROM S.`trimestre` OR T.`registros_elegibles` IS DISTINCT FROM S.`registros_elegibles` OR T.`registros_cuarentena` IS DISTINCT FROM S.`registros_cuarentena` OR T.`archivos_excluidos` IS DISTINCT FROM S.`archivos_excluidos` OR T.`estado_cobertura` IS DISTINCT FROM S.`estado_cobertura` OR T.`version_silver` IS DISTINCT FROM S.`version_silver` OR T.`scope_quality_id` IS DISTINCT FROM S.`scope_quality_id` OR T.`trazabilidad_verificada` IS DISTINCT FROM S.`trazabilidad_verificada` OR T.`cobertura_completa` IS DISTINCT FROM S.`cobertura_completa`)
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
      `trimestre` = S.`trimestre`,
      `registros_elegibles` = S.`registros_elegibles`,
      `registros_cuarentena` = S.`registros_cuarentena`,
      `archivos_excluidos` = S.`archivos_excluidos`,
      `estado_cobertura` = S.`estado_cobertura`,
      `version_silver` = S.`version_silver`,
      `scope_quality_id` = S.`scope_quality_id`,
      `trazabilidad_verificada` = S.`trazabilidad_verificada`,
      `cobertura_completa` = S.`cobertura_completa`

    WHEN NOT MATCHED
    THEN INSERT (`fecha`, `codigo_sab`, `sociedad_agente_bolsa`, `monto_compras`, `monto_ventas`, `monto_total`, `porcentaje_compras_original`, `porcentaje_ventas_original`, `porcentaje_total_original`, `fuente_datos`, `fecha_ingesta`, `montos_validos`, `diferencia_monetaria`, `total_consistente`, `saldo_neto`, `total_compras_diario`, `total_ventas_diario`, `total_actividad_diaria`, `cantidad_sab_diaria`, `registros_invalidos_diarios`, `actividad_diaria_valida`, `participacion_compras_pct`, `participacion_ventas_pct`, `participacion_total_pct`, `diferencia_participacion_compras`, `diferencia_participacion_ventas`, `diferencia_participacion_total`, `ranking_diario`, `aporte_hhi`, `hhi_diario`, `anio`, `mes`, `trimestre`, `registros_elegibles`, `registros_cuarentena`, `archivos_excluidos`, `estado_cobertura`, `version_silver`, `scope_quality_id`, `trazabilidad_verificada`, `cobertura_completa`)
      VALUES (S.`fecha`, S.`codigo_sab`, S.`sociedad_agente_bolsa`, S.`monto_compras`, S.`monto_ventas`, S.`monto_total`, S.`porcentaje_compras_original`, S.`porcentaje_ventas_original`, S.`porcentaje_total_original`, S.`fuente_datos`, S.`fecha_ingesta`, S.`montos_validos`, S.`diferencia_monetaria`, S.`total_consistente`, S.`saldo_neto`, S.`total_compras_diario`, S.`total_ventas_diario`, S.`total_actividad_diaria`, S.`cantidad_sab_diaria`, S.`registros_invalidos_diarios`, S.`actividad_diaria_valida`, S.`participacion_compras_pct`, S.`participacion_ventas_pct`, S.`participacion_total_pct`, S.`diferencia_participacion_compras`, S.`diferencia_participacion_ventas`, S.`diferencia_participacion_total`, S.`ranking_diario`, S.`aporte_hhi`, S.`hhi_diario`, S.`anio`, S.`mes`, S.`trimestre`, S.`registros_elegibles`, S.`registros_cuarentena`, S.`archivos_excluidos`, S.`estado_cobertura`, S.`version_silver`, S.`scope_quality_id`, S.`trazabilidad_verificada`, S.`cobertura_completa`);

    -- Conciliacion exacta de claves afectadas.
    ASSERT (
      SELECT COUNT(*) = 0
      FROM delta_batch S
      LEFT JOIN `bvl-analytics-2026.gold_analytics.actividad_sab_diaria` T
        ON T.`fecha` = S.`fecha` AND T.`codigo_sab` = S.`codigo_sab`
      WHERE T.`fecha` IS NULL
         OR (T.`fecha` IS DISTINCT FROM S.`fecha` OR T.`codigo_sab` IS DISTINCT FROM S.`codigo_sab` OR T.`sociedad_agente_bolsa` IS DISTINCT FROM S.`sociedad_agente_bolsa` OR T.`monto_compras` IS DISTINCT FROM S.`monto_compras` OR T.`monto_ventas` IS DISTINCT FROM S.`monto_ventas` OR T.`monto_total` IS DISTINCT FROM S.`monto_total` OR T.`porcentaje_compras_original` IS DISTINCT FROM S.`porcentaje_compras_original` OR T.`porcentaje_ventas_original` IS DISTINCT FROM S.`porcentaje_ventas_original` OR T.`porcentaje_total_original` IS DISTINCT FROM S.`porcentaje_total_original` OR T.`fuente_datos` IS DISTINCT FROM S.`fuente_datos` OR T.`fecha_ingesta` IS DISTINCT FROM S.`fecha_ingesta` OR T.`montos_validos` IS DISTINCT FROM S.`montos_validos` OR T.`diferencia_monetaria` IS DISTINCT FROM S.`diferencia_monetaria` OR T.`total_consistente` IS DISTINCT FROM S.`total_consistente` OR T.`saldo_neto` IS DISTINCT FROM S.`saldo_neto` OR T.`total_compras_diario` IS DISTINCT FROM S.`total_compras_diario` OR T.`total_ventas_diario` IS DISTINCT FROM S.`total_ventas_diario` OR T.`total_actividad_diaria` IS DISTINCT FROM S.`total_actividad_diaria` OR T.`cantidad_sab_diaria` IS DISTINCT FROM S.`cantidad_sab_diaria` OR T.`registros_invalidos_diarios` IS DISTINCT FROM S.`registros_invalidos_diarios` OR T.`actividad_diaria_valida` IS DISTINCT FROM S.`actividad_diaria_valida` OR T.`participacion_compras_pct` IS DISTINCT FROM S.`participacion_compras_pct` OR T.`participacion_ventas_pct` IS DISTINCT FROM S.`participacion_ventas_pct` OR T.`participacion_total_pct` IS DISTINCT FROM S.`participacion_total_pct` OR T.`diferencia_participacion_compras` IS DISTINCT FROM S.`diferencia_participacion_compras` OR T.`diferencia_participacion_ventas` IS DISTINCT FROM S.`diferencia_participacion_ventas` OR T.`diferencia_participacion_total` IS DISTINCT FROM S.`diferencia_participacion_total` OR T.`ranking_diario` IS DISTINCT FROM S.`ranking_diario` OR T.`aporte_hhi` IS DISTINCT FROM S.`aporte_hhi` OR T.`hhi_diario` IS DISTINCT FROM S.`hhi_diario` OR T.`anio` IS DISTINCT FROM S.`anio` OR T.`mes` IS DISTINCT FROM S.`mes` OR T.`trimestre` IS DISTINCT FROM S.`trimestre` OR T.`registros_elegibles` IS DISTINCT FROM S.`registros_elegibles` OR T.`registros_cuarentena` IS DISTINCT FROM S.`registros_cuarentena` OR T.`archivos_excluidos` IS DISTINCT FROM S.`archivos_excluidos` OR T.`estado_cobertura` IS DISTINCT FROM S.`estado_cobertura` OR T.`version_silver` IS DISTINCT FROM S.`version_silver` OR T.`scope_quality_id` IS DISTINCT FROM S.`scope_quality_id` OR T.`trazabilidad_verificada` IS DISTINCT FROM S.`trazabilidad_verificada` OR T.`cobertura_completa` IS DISTINCT FROM S.`cobertura_completa`)
    ) AS 'Conciliacion posterior incompatible';

    ASSERT (
      SELECT COUNT(*) = @expected_rows
      FROM delta_batch S
      JOIN `bvl-analytics-2026.gold_analytics.actividad_sab_diaria` T
        ON T.`fecha` = S.`fecha` AND T.`codigo_sab` = S.`codigo_sab`
    ) AS 'Cardinalidad posterior incompatible';

    INSERT INTO `bvl-analytics-2026.gold_analytics_control.incremental_batches_v2` (
      product, batch_id, run_id, source_version,
      previous_version, new_version,
      source_manifest_sha256,
      gold_contract_sha256,
      payload_sha256, staging_content_sha256,
      evidence_sha256, transfer_sha256,
      status, received_rows, inserted_rows,
      updated_rows, business_updates,
      technical_updates, unchanged_rows,
      deleted_rows, finished_at
    )
    SELECT
      'actividad_sab_diaria', @batch_id, @run_id,
      @source_version, @previous_version,
      @new_version, @source_manifest_sha256,
      @gold_contract_sha256,
      @payload_sha256, @staging_content_sha256,
      @evidence_sha256, @transfer_sha256,
      'COMPLETED', received_rows, inserted_rows,
      updated_rows, business_updates,
      technical_updates, unchanged_rows,
      deleted_rows, CURRENT_TIMESTAMP()
    FROM metricas_batch;

    UPDATE `bvl-analytics-2026.gold_analytics_control.product_versions_v2`
    SET current_version = @new_version,
        updated_at = CURRENT_TIMESTAMP()
    WHERE product = 'actividad_sab_diaria'
      AND current_version = @previous_version;

    ASSERT @@row_count = 1
      AS 'Avance de version incompatible';

    INSERT INTO `bvl-analytics-2026.gold_analytics_control.version_history_v2` (
      product, version, applied_at
    )
    VALUES (
      'actividad_sab_diaria', @new_version, CURRENT_TIMESTAMP()
    );

  END IF;

  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
