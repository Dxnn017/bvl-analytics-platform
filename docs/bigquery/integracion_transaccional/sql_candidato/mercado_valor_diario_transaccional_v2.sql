-- CANDIDATO LOCAL NO AUTORIZADO.
-- No retirar el bloqueo sin acreditacion Quality.
-- No ejecutar directamente en GCP.

ASSERT FALSE AS
  'EJECUCION OPERATIVA NO AUTORIZADA';

-- Producto: mercado_valor_diario
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
      '3e984da7684e08e3d2c825933cb4d7e02e8f6a1dbadcf367c5749093c380d340'
) AS 'Identidad candidata incompatible';

BEGIN
  BEGIN TRANSACTION;

  ASSERT (
    SELECT COUNT(*) <= 1
    FROM `bvl-analytics-2026.gold_analytics_control.incremental_batches_v2`
    WHERE product = 'mercado_valor_diario'
      AND batch_id = @batch_id
  ) AS 'Batch duplicado';

  ASSERT (
    SELECT COUNT(*) = 0
    FROM `bvl-analytics-2026.gold_analytics_control.incremental_batches_v2`
    WHERE product = 'mercado_valor_diario'
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
    WHERE product = 'mercado_valor_diario'
      AND batch_id = @batch_id
  ) THEN

    ASSERT (
      SELECT COUNT(*) = 1
      FROM `bvl-analytics-2026.gold_analytics_control.version_history_v2`
      WHERE product = 'mercado_valor_diario'
        AND version = @new_version
    ) AS 'Replay sin historial de version';

    SELECT 'REPLAY_NOOP' AS resultado_batch,
           0 AS applied_rows;

  ELSE

    ASSERT (
      SELECT COUNT(*) = 1
      FROM `bvl-analytics-2026.gold_analytics_control.product_versions_v2`
      WHERE product = 'mercado_valor_diario'
    ) AS 'Registro de version ausente o duplicado';

    ASSERT (
      SELECT COUNT(*) = 1
      FROM `bvl-analytics-2026.gold_analytics_control.product_versions_v2`
      WHERE product = 'mercado_valor_diario'
        AND current_version = @previous_version
    ) AS 'Continuidad incompatible';

    ASSERT (
      SELECT COUNT(*) = 1
      FROM `bvl-analytics-2026.gold_analytics_control.version_history_v2`
      WHERE product = 'mercado_valor_diario'
        AND version = @previous_version
    ) AS 'Version anterior sin historial';

    ASSERT (
      SELECT COUNT(*) = 0
      FROM `bvl-analytics-2026.gold_analytics_control.version_history_v2`
      WHERE product = 'mercado_valor_diario'
        AND version = @new_version
    ) AS 'Version previamente aplicada';

    -- No omitir filas del batch con metadatos distintos.
    ASSERT (
      SELECT COUNT(*) = 0
      FROM `bvl-analytics-2026.gold_analytics_staging.mercado_valor_diario_delta`
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
    FROM `bvl-analytics-2026.gold_analytics_staging.mercado_valor_diario_delta`
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
    FROM `bvl-analytics-2026.gold_analytics_staging.mercado_valor_diario_delta`
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
    FROM `bvl-analytics-2026.gold_analytics_staging.mercado_valor_diario_delta`
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
    FROM `bvl-analytics-2026.gold_analytics_staging.mercado_valor_diario_delta`
    WHERE batch_id = @batch_id
      AND run_id = @run_id
      AND source_version = @source_version
      AND payload_sha256 = @payload_sha256
  )
  WHERE `nemonico` IS NULL OR TRIM(CAST(`nemonico` AS STRING)) = '' OR `fecha` IS NULL OR TRIM(CAST(`fecha` AS STRING)) = ''
) AS 'Claves nulas o vacías en staging';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `bvl-analytics-2026.gold_analytics.mercado_valor_diario`
  WHERE `nemonico` IS NULL OR TRIM(CAST(`nemonico` AS STRING)) = '' OR `fecha` IS NULL OR TRIM(CAST(`fecha` AS STRING)) = ''
) AS 'Claves nulas o vacías en destino';

ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT `nemonico`, `fecha`
    FROM `bvl-analytics-2026.gold_analytics.mercado_valor_diario`
    GROUP BY `nemonico`, `fecha`
    HAVING COUNT(*) > 1
  )
) AS 'Claves duplicadas en destino';

-- Bloqueo temporal: DELETE requiere autorización verificable.
ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT *
    FROM `bvl-analytics-2026.gold_analytics_staging.mercado_valor_diario_delta`
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
  FROM `bvl-analytics-2026.gold_analytics.mercado_valor_diario`
  WHERE `nemonico` IS DISTINCT FROM UPPER(TRIM(`nemonico`))
) AS 'Destino no canonico: requiere migracion autorizada';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `bvl-analytics-2026.gold_analytics_staging.mercado_valor_diario_delta`
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
  FROM `bvl-analytics-2026.gold_analytics_staging.mercado_valor_diario_delta`
  WHERE batch_id = @batch_id
    AND run_id = @run_id
    AND source_version = @source_version
    AND payload_sha256 = @payload_sha256
) AS 'ERROR: batch vacio';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `bvl-analytics-2026.gold_analytics_staging.mercado_valor_diario_delta`
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
      WHERE IS_NAN(`precio_anterior`) OR IS_INF(`precio_anterior`)
     OR IS_NAN(`precio_cierre`) OR IS_INF(`precio_cierre`)
     OR IS_NAN(`precio_apertura`) OR IS_INF(`precio_apertura`)
     OR IS_NAN(`precio_maximo`) OR IS_INF(`precio_maximo`)
     OR IS_NAN(`precio_minimo`) OR IS_INF(`precio_minimo`)
     OR IS_NAN(`precio_promedio`) OR IS_INF(`precio_promedio`)
     OR IS_NAN(`monto_negociado`) OR IS_INF(`monto_negociado`)
     OR IS_NAN(`tipo_cambio_venta`) OR IS_INF(`tipo_cambio_venta`)
     OR IS_NAN(`embig_peru`) OR IS_INF(`embig_peru`)
     OR IS_NAN(`variacion_precio`) OR IS_INF(`variacion_precio`)
     OR IS_NAN(`retorno_diario_pct`) OR IS_INF(`retorno_diario_pct`)
     OR IS_NAN(`rango_intradia`) OR IS_INF(`rango_intradia`)
     OR IS_NAN(`rango_intradia_pct`) OR IS_INF(`rango_intradia_pct`)
    ) AS 'Numericos incompatibles';

    -- Clasificacion previa al MERGE.
    CREATE TEMP TABLE metricas_batch AS
    WITH clasificacion AS (
      SELECT CASE
        WHEN T.`nemonico` IS NULL
          THEN 'INSERT'
        WHEN (T.`nemonico` IS DISTINCT FROM S.`nemonico` OR T.`fecha` IS DISTINCT FROM S.`fecha` OR T.`descripcion` IS DISTINCT FROM S.`descripcion` OR T.`fecha_anterior` IS DISTINCT FROM S.`fecha_anterior` OR T.`precio_anterior` IS DISTINCT FROM S.`precio_anterior` OR T.`precio_cierre` IS DISTINCT FROM S.`precio_cierre` OR T.`precio_apertura` IS DISTINCT FROM S.`precio_apertura` OR T.`precio_maximo` IS DISTINCT FROM S.`precio_maximo` OR T.`precio_minimo` IS DISTINCT FROM S.`precio_minimo` OR T.`precio_promedio` IS DISTINCT FROM S.`precio_promedio` OR T.`monto_negociado` IS DISTINCT FROM S.`monto_negociado` OR T.`fuente` IS DISTINCT FROM S.`fuente` OR T.`tipo_cambio_venta` IS DISTINCT FROM S.`tipo_cambio_venta` OR T.`embig_peru` IS DISTINCT FROM S.`embig_peru` OR T.`ruc` IS DISTINCT FROM S.`ruc` OR T.`razon_social` IS DISTINCT FROM S.`razon_social` OR T.`codigo_isin` IS DISTINCT FROM S.`codigo_isin` OR T.`tipo_valor` IS DISTINCT FROM S.`tipo_valor` OR T.`moneda_referencia` IS DISTINCT FROM S.`moneda_referencia` OR T.`fecha_inscripcion` IS DISTINCT FROM S.`fecha_inscripcion` OR T.`precio_anterior_valido` IS DISTINCT FROM S.`precio_anterior_valido` OR T.`precio_cierre_valido` IS DISTINCT FROM S.`precio_cierre_valido` OR T.`rango_intradia_valido` IS DISTINCT FROM S.`rango_intradia_valido` OR T.`tipo_cambio_disponible` IS DISTINCT FROM S.`tipo_cambio_disponible` OR T.`embig_disponible` IS DISTINCT FROM S.`embig_disponible` OR T.`empresa_vinculada` IS DISTINCT FROM S.`empresa_vinculada` OR T.`inscripcion_compatible` IS DISTINCT FROM S.`inscripcion_compatible` OR T.`monto_negociado_valido` IS DISTINCT FROM S.`monto_negociado_valido` OR T.`monto_negociado_positivo` IS DISTINCT FROM S.`monto_negociado_positivo` OR T.`retorno_calculable` IS DISTINCT FROM S.`retorno_calculable` OR T.`variacion_precio` IS DISTINCT FROM S.`variacion_precio` OR T.`retorno_diario_pct` IS DISTINCT FROM S.`retorno_diario_pct` OR T.`rango_intradia` IS DISTINCT FROM S.`rango_intradia` OR T.`rango_intradia_pct` IS DISTINCT FROM S.`rango_intradia_pct` OR T.`anio` IS DISTINCT FROM S.`anio` OR T.`mes` IS DISTINCT FROM S.`mes` OR T.`trimestre` IS DISTINCT FROM S.`trimestre` OR T.`dias_desde_cierre_anterior` IS DISTINCT FROM S.`dias_desde_cierre_anterior` OR T.`macro_disponible` IS DISTINCT FROM S.`macro_disponible` OR T.`nemonico_original` IS DISTINCT FROM S.`nemonico_original`)
          THEN 'UPDATE_BUSINESS'
        WHEN (T.`fecha_ingesta` IS DISTINCT FROM S.`fecha_ingesta`)
          THEN 'UPDATE_TECHNICAL'
        ELSE 'NOOP'
      END AS accion
      FROM delta_batch S
      LEFT JOIN `bvl-analytics-2026.gold_analytics.mercado_valor_diario` T
        ON T.`nemonico` = S.`nemonico` AND T.`fecha` = S.`fecha`
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

    MERGE `bvl-analytics-2026.gold_analytics.mercado_valor_diario` T
    USING delta_batch S
      ON T.`nemonico` = S.`nemonico` AND T.`fecha` = S.`fecha`

    WHEN MATCHED
      AND (T.`nemonico` IS DISTINCT FROM S.`nemonico` OR T.`fecha` IS DISTINCT FROM S.`fecha` OR T.`descripcion` IS DISTINCT FROM S.`descripcion` OR T.`fecha_anterior` IS DISTINCT FROM S.`fecha_anterior` OR T.`precio_anterior` IS DISTINCT FROM S.`precio_anterior` OR T.`precio_cierre` IS DISTINCT FROM S.`precio_cierre` OR T.`precio_apertura` IS DISTINCT FROM S.`precio_apertura` OR T.`precio_maximo` IS DISTINCT FROM S.`precio_maximo` OR T.`precio_minimo` IS DISTINCT FROM S.`precio_minimo` OR T.`precio_promedio` IS DISTINCT FROM S.`precio_promedio` OR T.`monto_negociado` IS DISTINCT FROM S.`monto_negociado` OR T.`fuente` IS DISTINCT FROM S.`fuente` OR T.`fecha_ingesta` IS DISTINCT FROM S.`fecha_ingesta` OR T.`tipo_cambio_venta` IS DISTINCT FROM S.`tipo_cambio_venta` OR T.`embig_peru` IS DISTINCT FROM S.`embig_peru` OR T.`ruc` IS DISTINCT FROM S.`ruc` OR T.`razon_social` IS DISTINCT FROM S.`razon_social` OR T.`codigo_isin` IS DISTINCT FROM S.`codigo_isin` OR T.`tipo_valor` IS DISTINCT FROM S.`tipo_valor` OR T.`moneda_referencia` IS DISTINCT FROM S.`moneda_referencia` OR T.`fecha_inscripcion` IS DISTINCT FROM S.`fecha_inscripcion` OR T.`precio_anterior_valido` IS DISTINCT FROM S.`precio_anterior_valido` OR T.`precio_cierre_valido` IS DISTINCT FROM S.`precio_cierre_valido` OR T.`rango_intradia_valido` IS DISTINCT FROM S.`rango_intradia_valido` OR T.`tipo_cambio_disponible` IS DISTINCT FROM S.`tipo_cambio_disponible` OR T.`embig_disponible` IS DISTINCT FROM S.`embig_disponible` OR T.`empresa_vinculada` IS DISTINCT FROM S.`empresa_vinculada` OR T.`inscripcion_compatible` IS DISTINCT FROM S.`inscripcion_compatible` OR T.`monto_negociado_valido` IS DISTINCT FROM S.`monto_negociado_valido` OR T.`monto_negociado_positivo` IS DISTINCT FROM S.`monto_negociado_positivo` OR T.`retorno_calculable` IS DISTINCT FROM S.`retorno_calculable` OR T.`variacion_precio` IS DISTINCT FROM S.`variacion_precio` OR T.`retorno_diario_pct` IS DISTINCT FROM S.`retorno_diario_pct` OR T.`rango_intradia` IS DISTINCT FROM S.`rango_intradia` OR T.`rango_intradia_pct` IS DISTINCT FROM S.`rango_intradia_pct` OR T.`anio` IS DISTINCT FROM S.`anio` OR T.`mes` IS DISTINCT FROM S.`mes` OR T.`trimestre` IS DISTINCT FROM S.`trimestre` OR T.`dias_desde_cierre_anterior` IS DISTINCT FROM S.`dias_desde_cierre_anterior` OR T.`macro_disponible` IS DISTINCT FROM S.`macro_disponible` OR T.`nemonico_original` IS DISTINCT FROM S.`nemonico_original`)
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

    WHEN NOT MATCHED
    THEN INSERT (`nemonico`, `fecha`, `descripcion`, `fecha_anterior`, `precio_anterior`, `precio_cierre`, `precio_apertura`, `precio_maximo`, `precio_minimo`, `precio_promedio`, `monto_negociado`, `fuente`, `fecha_ingesta`, `tipo_cambio_venta`, `embig_peru`, `ruc`, `razon_social`, `codigo_isin`, `tipo_valor`, `moneda_referencia`, `fecha_inscripcion`, `precio_anterior_valido`, `precio_cierre_valido`, `rango_intradia_valido`, `tipo_cambio_disponible`, `embig_disponible`, `empresa_vinculada`, `inscripcion_compatible`, `monto_negociado_valido`, `monto_negociado_positivo`, `retorno_calculable`, `variacion_precio`, `retorno_diario_pct`, `rango_intradia`, `rango_intradia_pct`, `anio`, `mes`, `trimestre`, `dias_desde_cierre_anterior`, `macro_disponible`, `nemonico_original`)
      VALUES (S.`nemonico`, S.`fecha`, S.`descripcion`, S.`fecha_anterior`, S.`precio_anterior`, S.`precio_cierre`, S.`precio_apertura`, S.`precio_maximo`, S.`precio_minimo`, S.`precio_promedio`, S.`monto_negociado`, S.`fuente`, S.`fecha_ingesta`, S.`tipo_cambio_venta`, S.`embig_peru`, S.`ruc`, S.`razon_social`, S.`codigo_isin`, S.`tipo_valor`, S.`moneda_referencia`, S.`fecha_inscripcion`, S.`precio_anterior_valido`, S.`precio_cierre_valido`, S.`rango_intradia_valido`, S.`tipo_cambio_disponible`, S.`embig_disponible`, S.`empresa_vinculada`, S.`inscripcion_compatible`, S.`monto_negociado_valido`, S.`monto_negociado_positivo`, S.`retorno_calculable`, S.`variacion_precio`, S.`retorno_diario_pct`, S.`rango_intradia`, S.`rango_intradia_pct`, S.`anio`, S.`mes`, S.`trimestre`, S.`dias_desde_cierre_anterior`, S.`macro_disponible`, S.`nemonico_original`);

    -- Conciliacion exacta de claves afectadas.
    ASSERT (
      SELECT COUNT(*) = 0
      FROM delta_batch S
      LEFT JOIN `bvl-analytics-2026.gold_analytics.mercado_valor_diario` T
        ON T.`nemonico` = S.`nemonico` AND T.`fecha` = S.`fecha`
      WHERE T.`nemonico` IS NULL
         OR (T.`nemonico` IS DISTINCT FROM S.`nemonico` OR T.`fecha` IS DISTINCT FROM S.`fecha` OR T.`descripcion` IS DISTINCT FROM S.`descripcion` OR T.`fecha_anterior` IS DISTINCT FROM S.`fecha_anterior` OR T.`precio_anterior` IS DISTINCT FROM S.`precio_anterior` OR T.`precio_cierre` IS DISTINCT FROM S.`precio_cierre` OR T.`precio_apertura` IS DISTINCT FROM S.`precio_apertura` OR T.`precio_maximo` IS DISTINCT FROM S.`precio_maximo` OR T.`precio_minimo` IS DISTINCT FROM S.`precio_minimo` OR T.`precio_promedio` IS DISTINCT FROM S.`precio_promedio` OR T.`monto_negociado` IS DISTINCT FROM S.`monto_negociado` OR T.`fuente` IS DISTINCT FROM S.`fuente` OR T.`fecha_ingesta` IS DISTINCT FROM S.`fecha_ingesta` OR T.`tipo_cambio_venta` IS DISTINCT FROM S.`tipo_cambio_venta` OR T.`embig_peru` IS DISTINCT FROM S.`embig_peru` OR T.`ruc` IS DISTINCT FROM S.`ruc` OR T.`razon_social` IS DISTINCT FROM S.`razon_social` OR T.`codigo_isin` IS DISTINCT FROM S.`codigo_isin` OR T.`tipo_valor` IS DISTINCT FROM S.`tipo_valor` OR T.`moneda_referencia` IS DISTINCT FROM S.`moneda_referencia` OR T.`fecha_inscripcion` IS DISTINCT FROM S.`fecha_inscripcion` OR T.`precio_anterior_valido` IS DISTINCT FROM S.`precio_anterior_valido` OR T.`precio_cierre_valido` IS DISTINCT FROM S.`precio_cierre_valido` OR T.`rango_intradia_valido` IS DISTINCT FROM S.`rango_intradia_valido` OR T.`tipo_cambio_disponible` IS DISTINCT FROM S.`tipo_cambio_disponible` OR T.`embig_disponible` IS DISTINCT FROM S.`embig_disponible` OR T.`empresa_vinculada` IS DISTINCT FROM S.`empresa_vinculada` OR T.`inscripcion_compatible` IS DISTINCT FROM S.`inscripcion_compatible` OR T.`monto_negociado_valido` IS DISTINCT FROM S.`monto_negociado_valido` OR T.`monto_negociado_positivo` IS DISTINCT FROM S.`monto_negociado_positivo` OR T.`retorno_calculable` IS DISTINCT FROM S.`retorno_calculable` OR T.`variacion_precio` IS DISTINCT FROM S.`variacion_precio` OR T.`retorno_diario_pct` IS DISTINCT FROM S.`retorno_diario_pct` OR T.`rango_intradia` IS DISTINCT FROM S.`rango_intradia` OR T.`rango_intradia_pct` IS DISTINCT FROM S.`rango_intradia_pct` OR T.`anio` IS DISTINCT FROM S.`anio` OR T.`mes` IS DISTINCT FROM S.`mes` OR T.`trimestre` IS DISTINCT FROM S.`trimestre` OR T.`dias_desde_cierre_anterior` IS DISTINCT FROM S.`dias_desde_cierre_anterior` OR T.`macro_disponible` IS DISTINCT FROM S.`macro_disponible` OR T.`nemonico_original` IS DISTINCT FROM S.`nemonico_original`)
    ) AS 'Conciliacion posterior incompatible';

    ASSERT (
      SELECT COUNT(*) = @expected_rows
      FROM delta_batch S
      JOIN `bvl-analytics-2026.gold_analytics.mercado_valor_diario` T
        ON T.`nemonico` = S.`nemonico` AND T.`fecha` = S.`fecha`
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
      'mercado_valor_diario', @batch_id, @run_id,
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
    WHERE product = 'mercado_valor_diario'
      AND current_version = @previous_version;

    ASSERT @@row_count = 1
      AS 'Avance de version incompatible';

    INSERT INTO `bvl-analytics-2026.gold_analytics_control.version_history_v2` (
      product, version, applied_at
    )
    VALUES (
      'mercado_valor_diario', @new_version, CURRENT_TIMESTAMP()
    );

  END IF;

  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
