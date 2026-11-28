-- PROPUESTA LOCAL. NO EJECUTAR EN GCP.
-- Producto: mercado_valor_diario
-- Integración candidata: MERGE + auditoría en una transacción.
--
-- PENDIENTES OBLIGATORIOS:
-- 1. Acreditar versión Silver oficial.
-- 2. Verificar autorización Quality.
-- 3. Verificar SHA-256 del contenido real.
-- 4. Implementar reserva exclusiva del batch.
-- 5. Resolver REPLAY sin reprocesamiento.
-- 6. Obtener métricas reales de INSERT/UPDATE/DELETE/NOOP.
-- 7. Validar comportamiento y concurrencia en BigQuery.

ASSERT FALSE AS
  'EJECUCION BLOQUEADA: faltan autorizaciones y controles oficiales';

BEGIN
  BEGIN TRANSACTION;

  -- Comprobación provisional contra el registro persistente.
  -- NO sustituye una reserva exclusiva del batch.
  ASSERT (
    SELECT COUNT(*) = 0
    FROM `gold_analytics_control.incremental_batches`
    WHERE product = 'mercado_valor_diario'
      AND batch_id = @batch_id
  ) AS 'Batch existente: requiere resolución de replay';

  -- MERGE y validaciones del producto.
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
    FROM `gold_analytics_staging.mercado_valor_diario_delta`
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
    SELECT `nemonico`, `fecha`
    FROM (
    SELECT *
    FROM `gold_analytics_staging.mercado_valor_diario_delta`
    WHERE batch_id = @batch_id
      AND run_id = @run_id
      AND source_version = @source_version
      AND payload_sha256 = @payload_sha256
  )
    GROUP BY `nemonico`, `fecha`
    HAVING COUNT(*) > 1
  )
) AS 'Claves duplicadas en staging';

-- Controles obligatorios antes del MERGE.
ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT *
    FROM `gold_analytics_staging.mercado_valor_diario_delta`
    WHERE batch_id = @batch_id
      AND run_id = @run_id
      AND source_version = @source_version
      AND payload_sha256 = @payload_sha256
  )
  WHERE `nemonico` IS NULL OR TRIM(CAST(`nemonico` AS STRING)) = '' OR `fecha` IS NULL OR TRIM(CAST(`fecha` AS STRING)) = ''
) AS 'Claves nulas o vacías en staging';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics.mercado_valor_diario`
  WHERE `nemonico` IS NULL OR TRIM(CAST(`nemonico` AS STRING)) = '' OR `fecha` IS NULL OR TRIM(CAST(`fecha` AS STRING)) = ''
) AS 'Claves nulas o vacías en destino';

ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT `nemonico`, `fecha`
    FROM `gold_analytics.mercado_valor_diario`
    GROUP BY `nemonico`, `fecha`
    HAVING COUNT(*) > 1
  )
) AS 'Claves duplicadas en destino';

-- Bloqueo temporal: DELETE requiere autorización verificable.
ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT *
    FROM `gold_analytics_staging.mercado_valor_diario_delta`
    WHERE batch_id = @batch_id
      AND run_id = @run_id
      AND source_version = @source_version
      AND payload_sha256 = @payload_sha256
  )
  WHERE operation = 'DELETE'
) AS 'DELETE bloqueado hasta autorización oficial de Quality';


-- Claves canonicas: no migrar destinos antiguos automaticamente.
ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics.mercado_valor_diario`
  WHERE `nemonico` IS DISTINCT FROM UPPER(TRIM(`nemonico`))
) AS 'Destino no canonico: requiere migracion autorizada';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics_staging.mercado_valor_diario_delta`
  WHERE batch_id = @batch_id
    AND run_id = @run_id
    AND source_version = @source_version
    AND payload_sha256 = @payload_sha256
    AND operation = 'UPSERT'
    AND (
      `nemonico_original` IS NULL OR
      `nemonico` IS DISTINCT FROM
        UPPER(TRIM(`nemonico_original`))
    )
) AS 'Nemonico canonico o trazabilidad incompatible';

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
  FROM `gold_analytics_staging.mercado_valor_diario_delta`
  WHERE batch_id = @batch_id
    AND run_id = @run_id
    AND source_version = @source_version
    AND payload_sha256 = @payload_sha256
) AS 'ERROR: batch vacio';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics_staging.mercado_valor_diario_delta`
  WHERE batch_id = @batch_id
    AND (
      run_id IS DISTINCT FROM @run_id
      OR source_version IS DISTINCT FROM @source_version
      OR payload_sha256 IS DISTINCT FROM @payload_sha256
    )
) AS 'ERROR: batch_id con metadatos inconsistentes';

MERGE `gold_analytics.mercado_valor_diario` T
USING (
  SELECT *
  FROM `gold_analytics_staging.mercado_valor_diario_delta`
  WHERE batch_id = @batch_id
    AND run_id = @run_id
    AND source_version = @source_version
    AND payload_sha256 = @payload_sha256
) S
ON T.`nemonico` = S.`nemonico` AND T.`fecha` = S.`fecha`

WHEN MATCHED AND S.operation = 'DELETE'
  THEN DELETE

WHEN MATCHED AND S.operation = 'UPSERT'
  AND (
    T.`nemonico` IS DISTINCT FROM S.`nemonico`
    OR T.`fecha` IS DISTINCT FROM S.`fecha`
    OR T.`descripcion` IS DISTINCT FROM S.`descripcion`
    OR T.`fecha_anterior` IS DISTINCT FROM S.`fecha_anterior`
    OR T.`precio_anterior` IS DISTINCT FROM S.`precio_anterior`
    OR T.`precio_cierre` IS DISTINCT FROM S.`precio_cierre`
    OR T.`precio_apertura` IS DISTINCT FROM S.`precio_apertura`
    OR T.`precio_maximo` IS DISTINCT FROM S.`precio_maximo`
    OR T.`precio_minimo` IS DISTINCT FROM S.`precio_minimo`
    OR T.`precio_promedio` IS DISTINCT FROM S.`precio_promedio`
    OR T.`monto_negociado` IS DISTINCT FROM S.`monto_negociado`
    OR T.`fuente` IS DISTINCT FROM S.`fuente`
    OR T.`fecha_ingesta` IS DISTINCT FROM S.`fecha_ingesta`
    OR T.`tipo_cambio_venta` IS DISTINCT FROM S.`tipo_cambio_venta`
    OR T.`embig_peru` IS DISTINCT FROM S.`embig_peru`
    OR T.`ruc` IS DISTINCT FROM S.`ruc`
    OR T.`razon_social` IS DISTINCT FROM S.`razon_social`
    OR T.`codigo_isin` IS DISTINCT FROM S.`codigo_isin`
    OR T.`tipo_valor` IS DISTINCT FROM S.`tipo_valor`
    OR T.`moneda_referencia` IS DISTINCT FROM S.`moneda_referencia`
    OR T.`fecha_inscripcion` IS DISTINCT FROM S.`fecha_inscripcion`
    OR T.`precio_anterior_valido` IS DISTINCT FROM S.`precio_anterior_valido`
    OR T.`precio_cierre_valido` IS DISTINCT FROM S.`precio_cierre_valido`
    OR T.`rango_intradia_valido` IS DISTINCT FROM S.`rango_intradia_valido`
    OR T.`tipo_cambio_disponible` IS DISTINCT FROM S.`tipo_cambio_disponible`
    OR T.`embig_disponible` IS DISTINCT FROM S.`embig_disponible`
    OR T.`empresa_vinculada` IS DISTINCT FROM S.`empresa_vinculada`
    OR T.`inscripcion_compatible` IS DISTINCT FROM S.`inscripcion_compatible`
    OR T.`monto_negociado_valido` IS DISTINCT FROM S.`monto_negociado_valido`
    OR T.`monto_negociado_positivo` IS DISTINCT FROM S.`monto_negociado_positivo`
    OR T.`retorno_calculable` IS DISTINCT FROM S.`retorno_calculable`
    OR T.`variacion_precio` IS DISTINCT FROM S.`variacion_precio`
    OR T.`retorno_diario_pct` IS DISTINCT FROM S.`retorno_diario_pct`
    OR T.`rango_intradia` IS DISTINCT FROM S.`rango_intradia`
    OR T.`rango_intradia_pct` IS DISTINCT FROM S.`rango_intradia_pct`
    OR T.`anio` IS DISTINCT FROM S.`anio`
    OR T.`mes` IS DISTINCT FROM S.`mes`
    OR T.`trimestre` IS DISTINCT FROM S.`trimestre`
    OR T.`dias_desde_cierre_anterior` IS DISTINCT FROM S.`dias_desde_cierre_anterior`
    OR T.`macro_disponible` IS DISTINCT FROM S.`macro_disponible`
    OR T.`nemonico_original` IS DISTINCT FROM S.`nemonico_original`
  )
THEN UPDATE SET
    `descripcion` = S.`descripcion`,
    `fecha_anterior` = S.`fecha_anterior`,
    `precio_anterior` = S.`precio_anterior`,
    `precio_cierre` = S.`precio_cierre`,
    `precio_apertura` = S.`precio_apertura`,
    `precio_maximo` = S.`precio_maximo`,
    `precio_minimo` = S.`precio_minimo`,
    `precio_promedio` = S.`precio_promedio`,
    `monto_negociado` = S.`monto_negociado`,
    `fuente` = S.`fuente`,
    `fecha_ingesta` = S.`fecha_ingesta`,
    `tipo_cambio_venta` = S.`tipo_cambio_venta`,
    `embig_peru` = S.`embig_peru`,
    `ruc` = S.`ruc`,
    `razon_social` = S.`razon_social`,
    `codigo_isin` = S.`codigo_isin`,
    `tipo_valor` = S.`tipo_valor`,
    `moneda_referencia` = S.`moneda_referencia`,
    `fecha_inscripcion` = S.`fecha_inscripcion`,
    `precio_anterior_valido` = S.`precio_anterior_valido`,
    `precio_cierre_valido` = S.`precio_cierre_valido`,
    `rango_intradia_valido` = S.`rango_intradia_valido`,
    `tipo_cambio_disponible` = S.`tipo_cambio_disponible`,
    `embig_disponible` = S.`embig_disponible`,
    `empresa_vinculada` = S.`empresa_vinculada`,
    `inscripcion_compatible` = S.`inscripcion_compatible`,
    `monto_negociado_valido` = S.`monto_negociado_valido`,
    `monto_negociado_positivo` = S.`monto_negociado_positivo`,
    `retorno_calculable` = S.`retorno_calculable`,
    `variacion_precio` = S.`variacion_precio`,
    `retorno_diario_pct` = S.`retorno_diario_pct`,
    `rango_intradia` = S.`rango_intradia`,
    `rango_intradia_pct` = S.`rango_intradia_pct`,
    `anio` = S.`anio`,
    `mes` = S.`mes`,
    `trimestre` = S.`trimestre`,
    `dias_desde_cierre_anterior` = S.`dias_desde_cierre_anterior`,
    `macro_disponible` = S.`macro_disponible`,
    `nemonico_original` = S.`nemonico_original`

WHEN NOT MATCHED AND S.operation = 'UPSERT'
  THEN INSERT (`nemonico`, `fecha`, `descripcion`, `fecha_anterior`, `precio_anterior`, `precio_cierre`, `precio_apertura`, `precio_maximo`, `precio_minimo`, `precio_promedio`, `monto_negociado`, `fuente`, `fecha_ingesta`, `tipo_cambio_venta`, `embig_peru`, `ruc`, `razon_social`, `codigo_isin`, `tipo_valor`, `moneda_referencia`, `fecha_inscripcion`, `precio_anterior_valido`, `precio_cierre_valido`, `rango_intradia_valido`, `tipo_cambio_disponible`, `embig_disponible`, `empresa_vinculada`, `inscripcion_compatible`, `monto_negociado_valido`, `monto_negociado_positivo`, `retorno_calculable`, `variacion_precio`, `retorno_diario_pct`, `rango_intradia`, `rango_intradia_pct`, `anio`, `mes`, `trimestre`, `dias_desde_cierre_anterior`, `macro_disponible`, `nemonico_original`)
  VALUES (S.`nemonico`, S.`fecha`, S.`descripcion`, S.`fecha_anterior`, S.`precio_anterior`, S.`precio_cierre`, S.`precio_apertura`, S.`precio_maximo`, S.`precio_minimo`, S.`precio_promedio`, S.`monto_negociado`, S.`fuente`, S.`fecha_ingesta`, S.`tipo_cambio_venta`, S.`embig_peru`, S.`ruc`, S.`razon_social`, S.`codigo_isin`, S.`tipo_valor`, S.`moneda_referencia`, S.`fecha_inscripcion`, S.`precio_anterior_valido`, S.`precio_cierre_valido`, S.`rango_intradia_valido`, S.`tipo_cambio_disponible`, S.`embig_disponible`, S.`empresa_vinculada`, S.`inscripcion_compatible`, S.`monto_negociado_valido`, S.`monto_negociado_positivo`, S.`retorno_calculable`, S.`variacion_precio`, S.`retorno_diario_pct`, S.`rango_intradia`, S.`rango_intradia_pct`, S.`anio`, S.`mes`, S.`trimestre`, S.`dias_desde_cierre_anterior`, S.`macro_disponible`, S.`nemonico_original`);

  -- Auditoría del batch en la misma transacción.
  -- Métricas NULL hasta incorporar conteos verificables.
  INSERT INTO `gold_analytics_control.incremental_batches` (
    batch_id,
    product,
    run_id,
    source_version,
    payload_sha256,
    status,
    started_at,
    finished_at,
    inserted_rows,
    updated_rows,
    deleted_rows,
    unchanged_rows,
    error_message
  )
  VALUES (
    @batch_id,
    'mercado_valor_diario',
    @run_id,
    @source_version,
    @payload_sha256,
    'COMPLETED',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
  );

  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
