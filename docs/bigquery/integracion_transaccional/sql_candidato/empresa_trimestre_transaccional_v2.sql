-- CANDIDATO LOCAL NO AUTORIZADO.
-- No retirar el bloqueo sin acreditacion Quality.
-- No ejecutar directamente en GCP.

ASSERT FALSE AS
  'EJECUCION OPERATIVA NO AUTORIZADA';

-- Producto: empresa_trimestre
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
      '6cfa9e3cf6ddb78d4ac8a7b97b4d35cb5b869185ab2ba7ccccb0ea6d5171e705'
) AS 'Identidad candidata incompatible';

BEGIN
  BEGIN TRANSACTION;

  ASSERT (
    SELECT COUNT(*) <= 1
    FROM `bvl-analytics-2026.gold_analytics_control.incremental_batches_v2`
    WHERE product = 'empresa_trimestre'
      AND batch_id = @batch_id
  ) AS 'Batch duplicado';

  ASSERT (
    SELECT COUNT(*) = 0
    FROM `bvl-analytics-2026.gold_analytics_control.incremental_batches_v2`
    WHERE product = 'empresa_trimestre'
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
    WHERE product = 'empresa_trimestre'
      AND batch_id = @batch_id
  ) THEN

    ASSERT (
      SELECT COUNT(*) = 1
      FROM `bvl-analytics-2026.gold_analytics_control.version_history_v2`
      WHERE product = 'empresa_trimestre'
        AND version = @new_version
    ) AS 'Replay sin historial de version';

    SELECT 'REPLAY_NOOP' AS resultado_batch,
           0 AS applied_rows;

  ELSE

    ASSERT (
      SELECT COUNT(*) = 1
      FROM `bvl-analytics-2026.gold_analytics_control.product_versions_v2`
      WHERE product = 'empresa_trimestre'
    ) AS 'Registro de version ausente o duplicado';

    ASSERT (
      SELECT COUNT(*) = 1
      FROM `bvl-analytics-2026.gold_analytics_control.product_versions_v2`
      WHERE product = 'empresa_trimestre'
        AND current_version = @previous_version
    ) AS 'Continuidad incompatible';

    ASSERT (
      SELECT COUNT(*) = 1
      FROM `bvl-analytics-2026.gold_analytics_control.version_history_v2`
      WHERE product = 'empresa_trimestre'
        AND version = @previous_version
    ) AS 'Version anterior sin historial';

    ASSERT (
      SELECT COUNT(*) = 0
      FROM `bvl-analytics-2026.gold_analytics_control.version_history_v2`
      WHERE product = 'empresa_trimestre'
        AND version = @new_version
    ) AS 'Version previamente aplicada';

    -- No omitir filas del batch con metadatos distintos.
    ASSERT (
      SELECT COUNT(*) = 0
      FROM `bvl-analytics-2026.gold_analytics_staging.empresa_trimestre_delta`
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
    FROM `bvl-analytics-2026.gold_analytics_staging.empresa_trimestre_delta`
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
    FROM `bvl-analytics-2026.gold_analytics_staging.empresa_trimestre_delta`
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
    SELECT `id_registro`
    FROM (
    SELECT *
    FROM `bvl-analytics-2026.gold_analytics_staging.empresa_trimestre_delta`
    WHERE batch_id = @batch_id
      AND run_id = @run_id
      AND source_version = @source_version
      AND payload_sha256 = @payload_sha256
  )
    GROUP BY `id_registro`
    HAVING COUNT(*) > 1
  )
) AS 'Claves duplicadas en staging';

-- Controles obligatorios antes del MERGE.
ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT *
    FROM `bvl-analytics-2026.gold_analytics_staging.empresa_trimestre_delta`
    WHERE batch_id = @batch_id
      AND run_id = @run_id
      AND source_version = @source_version
      AND payload_sha256 = @payload_sha256
  )
  WHERE `id_registro` IS NULL OR TRIM(CAST(`id_registro` AS STRING)) = ''
) AS 'Claves nulas o vacías en staging';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `bvl-analytics-2026.gold_analytics.empresa_trimestre`
  WHERE `id_registro` IS NULL OR TRIM(CAST(`id_registro` AS STRING)) = ''
) AS 'Claves nulas o vacías en destino';

ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT `id_registro`
    FROM `bvl-analytics-2026.gold_analytics.empresa_trimestre`
    GROUP BY `id_registro`
    HAVING COUNT(*) > 1
  )
) AS 'Claves duplicadas en destino';

-- Bloqueo temporal: DELETE requiere autorización verificable.
ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT *
    FROM `bvl-analytics-2026.gold_analytics_staging.empresa_trimestre_delta`
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
    FROM `bvl-analytics-2026.gold_analytics_staging.empresa_trimestre_delta`
    WHERE batch_id = @batch_id
      AND run_id = @run_id
      AND source_version = @source_version
      AND payload_sha256 = @payload_sha256
  )
  WHERE operation = 'UPSERT'
    AND (
      `periodo_valido` IS NULL OR
      `patrimonio_valido` IS NULL OR
      `ingreso_valido` IS NULL OR
      `utilidad_valida` IS NULL OR
      `moneda_valida` IS NULL OR
      `identidad_contable_evaluable` IS NULL OR
      `identidad_contable_valida` IS NULL OR
      `colision_financiera` IS NULL OR
      `sin_incidencias_estructurales` IS NULL OR
      `apto_kpi_general` IS NULL OR
      `resultado_financiero` IS NULL OR
      `patrimonio_negativo` IS NULL OR
      `motivos_calidad` IS NULL
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
  FROM `bvl-analytics-2026.gold_analytics_staging.empresa_trimestre_delta`
  WHERE batch_id = @batch_id
    AND run_id = @run_id
    AND source_version = @source_version
    AND payload_sha256 = @payload_sha256
) AS 'ERROR: batch vacio';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `bvl-analytics-2026.gold_analytics_staging.empresa_trimestre_delta`
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
  WHERE `fecha_fin_trimestre` IS NULL
     OR anio IS DISTINCT FROM
        EXTRACT(YEAR FROM `fecha_fin_trimestre`)
     OR trimestre IS DISTINCT FROM
        EXTRACT(QUARTER FROM `fecha_fin_trimestre`)
) AS 'Particiones incompatibles';


    ASSERT (
      SELECT COUNT(*) = 0
      FROM delta_batch
      WHERE IS_NAN(`activo_total`) OR IS_INF(`activo_total`)
     OR IS_NAN(`pasivo_total`) OR IS_INF(`pasivo_total`)
     OR IS_NAN(`patrimonio_total`) OR IS_INF(`patrimonio_total`)
     OR IS_NAN(`total_ingreso`) OR IS_INF(`total_ingreso`)
     OR IS_NAN(`utilidad_neta`) OR IS_INF(`utilidad_neta`)
     OR IS_NAN(`diferencia_contable`) OR IS_INF(`diferencia_contable`)
     OR IS_NAN(`roa_aproximado_pct`) OR IS_INF(`roa_aproximado_pct`)
     OR IS_NAN(`roe_aproximado_pct`) OR IS_INF(`roe_aproximado_pct`)
     OR IS_NAN(`endeudamiento_pct`) OR IS_INF(`endeudamiento_pct`)
     OR IS_NAN(`margen_neto_pct`) OR IS_INF(`margen_neto_pct`)
     OR IS_NAN(`autonomia_financiera_pct`) OR IS_INF(`autonomia_financiera_pct`)
    ) AS 'Numericos incompatibles';

    -- Clasificacion previa al MERGE.
    CREATE TEMP TABLE metricas_batch AS
    WITH clasificacion AS (
      SELECT CASE
        WHEN T.`id_registro` IS NULL
          THEN 'INSERT'
        WHEN (T.`rpj` IS DISTINCT FROM S.`rpj` OR T.`ejercicio` IS DISTINCT FROM S.`ejercicio` OR T.`trimestre_num` IS DISTINCT FROM S.`trimestre_num` OR T.`id_registro` IS DISTINCT FROM S.`id_registro` OR T.`clave_negocio` IS DISTINCT FROM S.`clave_negocio` OR T.`ruc` IS DISTINCT FROM S.`ruc` OR T.`ruc_valido` IS DISTINCT FROM S.`ruc_valido` OR T.`nombre_empresa` IS DISTINCT FROM S.`nombre_empresa` OR T.`tipo_empresa` IS DISTINCT FROM S.`tipo_empresa` OR T.`tipo_sector` IS DISTINCT FROM S.`tipo_sector` OR T.`ciiu` IS DISTINCT FROM S.`ciiu` OR T.`trimestre_original` IS DISTINCT FROM S.`trimestre_original` OR T.`tipo_informacion` IS DISTINCT FROM S.`tipo_informacion` OR T.`metodo_flujo_efectivo` IS DISTINCT FROM S.`metodo_flujo_efectivo` OR T.`moneda_original` IS DISTINCT FROM S.`moneda_original` OR T.`activo_total` IS DISTINCT FROM S.`activo_total` OR T.`pasivo_total` IS DISTINCT FROM S.`pasivo_total` OR T.`patrimonio_total` IS DISTINCT FROM S.`patrimonio_total` OR T.`total_ingreso` IS DISTINCT FROM S.`total_ingreso` OR T.`utilidad_neta` IS DISTINCT FROM S.`utilidad_neta` OR T.`cantidad_clave_negocio` IS DISTINCT FROM S.`cantidad_clave_negocio` OR T.`colision_clave_negocio` IS DISTINCT FROM S.`colision_clave_negocio` OR T.`fuente_datos` IS DISTINCT FROM S.`fuente_datos` OR T.`moneda` IS DISTINCT FROM S.`moneda` OR T.`anio` IS DISTINCT FROM S.`anio` OR T.`trimestre` IS DISTINCT FROM S.`trimestre` OR T.`periodo_valido` IS DISTINCT FROM S.`periodo_valido` OR T.`periodo` IS DISTINCT FROM S.`periodo` OR T.`fecha_fin_trimestre` IS DISTINCT FROM S.`fecha_fin_trimestre` OR T.`activo_valido` IS DISTINCT FROM S.`activo_valido` OR T.`pasivo_valido` IS DISTINCT FROM S.`pasivo_valido` OR T.`patrimonio_valido` IS DISTINCT FROM S.`patrimonio_valido` OR T.`patrimonio_positivo` IS DISTINCT FROM S.`patrimonio_positivo` OR T.`ingreso_valido` IS DISTINCT FROM S.`ingreso_valido` OR T.`ingreso_positivo` IS DISTINCT FROM S.`ingreso_positivo` OR T.`utilidad_valida` IS DISTINCT FROM S.`utilidad_valida` OR T.`moneda_valida` IS DISTINCT FROM S.`moneda_valida` OR T.`identidad_contable_evaluable` IS DISTINCT FROM S.`identidad_contable_evaluable` OR T.`diferencia_contable` IS DISTINCT FROM S.`diferencia_contable` OR T.`identidad_contable_valida` IS DISTINCT FROM S.`identidad_contable_valida` OR T.`cantidad_registros_empresa_periodo` IS DISTINCT FROM S.`cantidad_registros_empresa_periodo` OR T.`colision_financiera` IS DISTINCT FROM S.`colision_financiera` OR T.`sin_incidencias_estructurales` IS DISTINCT FROM S.`sin_incidencias_estructurales` OR T.`apto_kpi_general` IS DISTINCT FROM S.`apto_kpi_general` OR T.`roa_calculable` IS DISTINCT FROM S.`roa_calculable` OR T.`roe_calculable` IS DISTINCT FROM S.`roe_calculable` OR T.`endeudamiento_calculable` IS DISTINCT FROM S.`endeudamiento_calculable` OR T.`margen_neto_calculable` IS DISTINCT FROM S.`margen_neto_calculable` OR T.`autonomia_calculable` IS DISTINCT FROM S.`autonomia_calculable` OR T.`roa_aproximado_pct` IS DISTINCT FROM S.`roa_aproximado_pct` OR T.`roe_aproximado_pct` IS DISTINCT FROM S.`roe_aproximado_pct` OR T.`endeudamiento_pct` IS DISTINCT FROM S.`endeudamiento_pct` OR T.`margen_neto_pct` IS DISTINCT FROM S.`margen_neto_pct` OR T.`autonomia_financiera_pct` IS DISTINCT FROM S.`autonomia_financiera_pct` OR T.`resultado_financiero` IS DISTINCT FROM S.`resultado_financiero` OR T.`patrimonio_negativo` IS DISTINCT FROM S.`patrimonio_negativo` OR T.`motivos_calidad` IS DISTINCT FROM S.`motivos_calidad`)
          THEN 'UPDATE_BUSINESS'
        WHEN (T.`fecha_ingesta` IS DISTINCT FROM S.`fecha_ingesta`)
          THEN 'UPDATE_TECHNICAL'
        ELSE 'NOOP'
      END AS accion
      FROM delta_batch S
      LEFT JOIN `bvl-analytics-2026.gold_analytics.empresa_trimestre` T
        ON T.`id_registro` = S.`id_registro`
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

    MERGE `bvl-analytics-2026.gold_analytics.empresa_trimestre` T
    USING delta_batch S
      ON T.`id_registro` = S.`id_registro`

    WHEN MATCHED
      AND (T.`rpj` IS DISTINCT FROM S.`rpj` OR T.`ejercicio` IS DISTINCT FROM S.`ejercicio` OR T.`trimestre_num` IS DISTINCT FROM S.`trimestre_num` OR T.`id_registro` IS DISTINCT FROM S.`id_registro` OR T.`clave_negocio` IS DISTINCT FROM S.`clave_negocio` OR T.`ruc` IS DISTINCT FROM S.`ruc` OR T.`ruc_valido` IS DISTINCT FROM S.`ruc_valido` OR T.`nombre_empresa` IS DISTINCT FROM S.`nombre_empresa` OR T.`tipo_empresa` IS DISTINCT FROM S.`tipo_empresa` OR T.`tipo_sector` IS DISTINCT FROM S.`tipo_sector` OR T.`ciiu` IS DISTINCT FROM S.`ciiu` OR T.`trimestre_original` IS DISTINCT FROM S.`trimestre_original` OR T.`tipo_informacion` IS DISTINCT FROM S.`tipo_informacion` OR T.`metodo_flujo_efectivo` IS DISTINCT FROM S.`metodo_flujo_efectivo` OR T.`moneda_original` IS DISTINCT FROM S.`moneda_original` OR T.`activo_total` IS DISTINCT FROM S.`activo_total` OR T.`pasivo_total` IS DISTINCT FROM S.`pasivo_total` OR T.`patrimonio_total` IS DISTINCT FROM S.`patrimonio_total` OR T.`total_ingreso` IS DISTINCT FROM S.`total_ingreso` OR T.`utilidad_neta` IS DISTINCT FROM S.`utilidad_neta` OR T.`cantidad_clave_negocio` IS DISTINCT FROM S.`cantidad_clave_negocio` OR T.`colision_clave_negocio` IS DISTINCT FROM S.`colision_clave_negocio` OR T.`fuente_datos` IS DISTINCT FROM S.`fuente_datos` OR T.`fecha_ingesta` IS DISTINCT FROM S.`fecha_ingesta` OR T.`moneda` IS DISTINCT FROM S.`moneda` OR T.`anio` IS DISTINCT FROM S.`anio` OR T.`trimestre` IS DISTINCT FROM S.`trimestre` OR T.`periodo_valido` IS DISTINCT FROM S.`periodo_valido` OR T.`periodo` IS DISTINCT FROM S.`periodo` OR T.`fecha_fin_trimestre` IS DISTINCT FROM S.`fecha_fin_trimestre` OR T.`activo_valido` IS DISTINCT FROM S.`activo_valido` OR T.`pasivo_valido` IS DISTINCT FROM S.`pasivo_valido` OR T.`patrimonio_valido` IS DISTINCT FROM S.`patrimonio_valido` OR T.`patrimonio_positivo` IS DISTINCT FROM S.`patrimonio_positivo` OR T.`ingreso_valido` IS DISTINCT FROM S.`ingreso_valido` OR T.`ingreso_positivo` IS DISTINCT FROM S.`ingreso_positivo` OR T.`utilidad_valida` IS DISTINCT FROM S.`utilidad_valida` OR T.`moneda_valida` IS DISTINCT FROM S.`moneda_valida` OR T.`identidad_contable_evaluable` IS DISTINCT FROM S.`identidad_contable_evaluable` OR T.`diferencia_contable` IS DISTINCT FROM S.`diferencia_contable` OR T.`identidad_contable_valida` IS DISTINCT FROM S.`identidad_contable_valida` OR T.`cantidad_registros_empresa_periodo` IS DISTINCT FROM S.`cantidad_registros_empresa_periodo` OR T.`colision_financiera` IS DISTINCT FROM S.`colision_financiera` OR T.`sin_incidencias_estructurales` IS DISTINCT FROM S.`sin_incidencias_estructurales` OR T.`apto_kpi_general` IS DISTINCT FROM S.`apto_kpi_general` OR T.`roa_calculable` IS DISTINCT FROM S.`roa_calculable` OR T.`roe_calculable` IS DISTINCT FROM S.`roe_calculable` OR T.`endeudamiento_calculable` IS DISTINCT FROM S.`endeudamiento_calculable` OR T.`margen_neto_calculable` IS DISTINCT FROM S.`margen_neto_calculable` OR T.`autonomia_calculable` IS DISTINCT FROM S.`autonomia_calculable` OR T.`roa_aproximado_pct` IS DISTINCT FROM S.`roa_aproximado_pct` OR T.`roe_aproximado_pct` IS DISTINCT FROM S.`roe_aproximado_pct` OR T.`endeudamiento_pct` IS DISTINCT FROM S.`endeudamiento_pct` OR T.`margen_neto_pct` IS DISTINCT FROM S.`margen_neto_pct` OR T.`autonomia_financiera_pct` IS DISTINCT FROM S.`autonomia_financiera_pct` OR T.`resultado_financiero` IS DISTINCT FROM S.`resultado_financiero` OR T.`patrimonio_negativo` IS DISTINCT FROM S.`patrimonio_negativo` OR T.`motivos_calidad` IS DISTINCT FROM S.`motivos_calidad`)
    THEN UPDATE SET
      `rpj` = S.`rpj`,
      `ejercicio` = S.`ejercicio`,
      `trimestre_num` = S.`trimestre_num`,
      `clave_negocio` = S.`clave_negocio`,
      `ruc` = S.`ruc`,
      `ruc_valido` = S.`ruc_valido`,
      `nombre_empresa` = S.`nombre_empresa`,
      `tipo_empresa` = S.`tipo_empresa`,
      `tipo_sector` = S.`tipo_sector`,
      `ciiu` = S.`ciiu`,
      `trimestre_original` = S.`trimestre_original`,
      `tipo_informacion` = S.`tipo_informacion`,
      `metodo_flujo_efectivo` = S.`metodo_flujo_efectivo`,
      `moneda_original` = S.`moneda_original`,
      `activo_total` = S.`activo_total`,
      `pasivo_total` = S.`pasivo_total`,
      `patrimonio_total` = S.`patrimonio_total`,
      `total_ingreso` = S.`total_ingreso`,
      `utilidad_neta` = S.`utilidad_neta`,
      `cantidad_clave_negocio` = S.`cantidad_clave_negocio`,
      `colision_clave_negocio` = S.`colision_clave_negocio`,
      `fuente_datos` = S.`fuente_datos`,
      `fecha_ingesta` = S.`fecha_ingesta`,
      `moneda` = S.`moneda`,
      `anio` = S.`anio`,
      `trimestre` = S.`trimestre`,
      `periodo_valido` = S.`periodo_valido`,
      `periodo` = S.`periodo`,
      `fecha_fin_trimestre` = S.`fecha_fin_trimestre`,
      `activo_valido` = S.`activo_valido`,
      `pasivo_valido` = S.`pasivo_valido`,
      `patrimonio_valido` = S.`patrimonio_valido`,
      `patrimonio_positivo` = S.`patrimonio_positivo`,
      `ingreso_valido` = S.`ingreso_valido`,
      `ingreso_positivo` = S.`ingreso_positivo`,
      `utilidad_valida` = S.`utilidad_valida`,
      `moneda_valida` = S.`moneda_valida`,
      `identidad_contable_evaluable` = S.`identidad_contable_evaluable`,
      `diferencia_contable` = S.`diferencia_contable`,
      `identidad_contable_valida` = S.`identidad_contable_valida`,
      `cantidad_registros_empresa_periodo` = S.`cantidad_registros_empresa_periodo`,
      `colision_financiera` = S.`colision_financiera`,
      `sin_incidencias_estructurales` = S.`sin_incidencias_estructurales`,
      `apto_kpi_general` = S.`apto_kpi_general`,
      `roa_calculable` = S.`roa_calculable`,
      `roe_calculable` = S.`roe_calculable`,
      `endeudamiento_calculable` = S.`endeudamiento_calculable`,
      `margen_neto_calculable` = S.`margen_neto_calculable`,
      `autonomia_calculable` = S.`autonomia_calculable`,
      `roa_aproximado_pct` = S.`roa_aproximado_pct`,
      `roe_aproximado_pct` = S.`roe_aproximado_pct`,
      `endeudamiento_pct` = S.`endeudamiento_pct`,
      `margen_neto_pct` = S.`margen_neto_pct`,
      `autonomia_financiera_pct` = S.`autonomia_financiera_pct`,
      `resultado_financiero` = S.`resultado_financiero`,
      `patrimonio_negativo` = S.`patrimonio_negativo`,
      `motivos_calidad` = S.`motivos_calidad`

    WHEN NOT MATCHED
    THEN INSERT (`rpj`, `ejercicio`, `trimestre_num`, `id_registro`, `clave_negocio`, `ruc`, `ruc_valido`, `nombre_empresa`, `tipo_empresa`, `tipo_sector`, `ciiu`, `trimestre_original`, `tipo_informacion`, `metodo_flujo_efectivo`, `moneda_original`, `activo_total`, `pasivo_total`, `patrimonio_total`, `total_ingreso`, `utilidad_neta`, `cantidad_clave_negocio`, `colision_clave_negocio`, `fuente_datos`, `fecha_ingesta`, `moneda`, `anio`, `trimestre`, `periodo_valido`, `periodo`, `fecha_fin_trimestre`, `activo_valido`, `pasivo_valido`, `patrimonio_valido`, `patrimonio_positivo`, `ingreso_valido`, `ingreso_positivo`, `utilidad_valida`, `moneda_valida`, `identidad_contable_evaluable`, `diferencia_contable`, `identidad_contable_valida`, `cantidad_registros_empresa_periodo`, `colision_financiera`, `sin_incidencias_estructurales`, `apto_kpi_general`, `roa_calculable`, `roe_calculable`, `endeudamiento_calculable`, `margen_neto_calculable`, `autonomia_calculable`, `roa_aproximado_pct`, `roe_aproximado_pct`, `endeudamiento_pct`, `margen_neto_pct`, `autonomia_financiera_pct`, `resultado_financiero`, `patrimonio_negativo`, `motivos_calidad`)
      VALUES (S.`rpj`, S.`ejercicio`, S.`trimestre_num`, S.`id_registro`, S.`clave_negocio`, S.`ruc`, S.`ruc_valido`, S.`nombre_empresa`, S.`tipo_empresa`, S.`tipo_sector`, S.`ciiu`, S.`trimestre_original`, S.`tipo_informacion`, S.`metodo_flujo_efectivo`, S.`moneda_original`, S.`activo_total`, S.`pasivo_total`, S.`patrimonio_total`, S.`total_ingreso`, S.`utilidad_neta`, S.`cantidad_clave_negocio`, S.`colision_clave_negocio`, S.`fuente_datos`, S.`fecha_ingesta`, S.`moneda`, S.`anio`, S.`trimestre`, S.`periodo_valido`, S.`periodo`, S.`fecha_fin_trimestre`, S.`activo_valido`, S.`pasivo_valido`, S.`patrimonio_valido`, S.`patrimonio_positivo`, S.`ingreso_valido`, S.`ingreso_positivo`, S.`utilidad_valida`, S.`moneda_valida`, S.`identidad_contable_evaluable`, S.`diferencia_contable`, S.`identidad_contable_valida`, S.`cantidad_registros_empresa_periodo`, S.`colision_financiera`, S.`sin_incidencias_estructurales`, S.`apto_kpi_general`, S.`roa_calculable`, S.`roe_calculable`, S.`endeudamiento_calculable`, S.`margen_neto_calculable`, S.`autonomia_calculable`, S.`roa_aproximado_pct`, S.`roe_aproximado_pct`, S.`endeudamiento_pct`, S.`margen_neto_pct`, S.`autonomia_financiera_pct`, S.`resultado_financiero`, S.`patrimonio_negativo`, S.`motivos_calidad`);

    -- Conciliacion exacta de claves afectadas.
    ASSERT (
      SELECT COUNT(*) = 0
      FROM delta_batch S
      LEFT JOIN `bvl-analytics-2026.gold_analytics.empresa_trimestre` T
        ON T.`id_registro` = S.`id_registro`
      WHERE T.`id_registro` IS NULL
         OR (T.`rpj` IS DISTINCT FROM S.`rpj` OR T.`ejercicio` IS DISTINCT FROM S.`ejercicio` OR T.`trimestre_num` IS DISTINCT FROM S.`trimestre_num` OR T.`id_registro` IS DISTINCT FROM S.`id_registro` OR T.`clave_negocio` IS DISTINCT FROM S.`clave_negocio` OR T.`ruc` IS DISTINCT FROM S.`ruc` OR T.`ruc_valido` IS DISTINCT FROM S.`ruc_valido` OR T.`nombre_empresa` IS DISTINCT FROM S.`nombre_empresa` OR T.`tipo_empresa` IS DISTINCT FROM S.`tipo_empresa` OR T.`tipo_sector` IS DISTINCT FROM S.`tipo_sector` OR T.`ciiu` IS DISTINCT FROM S.`ciiu` OR T.`trimestre_original` IS DISTINCT FROM S.`trimestre_original` OR T.`tipo_informacion` IS DISTINCT FROM S.`tipo_informacion` OR T.`metodo_flujo_efectivo` IS DISTINCT FROM S.`metodo_flujo_efectivo` OR T.`moneda_original` IS DISTINCT FROM S.`moneda_original` OR T.`activo_total` IS DISTINCT FROM S.`activo_total` OR T.`pasivo_total` IS DISTINCT FROM S.`pasivo_total` OR T.`patrimonio_total` IS DISTINCT FROM S.`patrimonio_total` OR T.`total_ingreso` IS DISTINCT FROM S.`total_ingreso` OR T.`utilidad_neta` IS DISTINCT FROM S.`utilidad_neta` OR T.`cantidad_clave_negocio` IS DISTINCT FROM S.`cantidad_clave_negocio` OR T.`colision_clave_negocio` IS DISTINCT FROM S.`colision_clave_negocio` OR T.`fuente_datos` IS DISTINCT FROM S.`fuente_datos` OR T.`fecha_ingesta` IS DISTINCT FROM S.`fecha_ingesta` OR T.`moneda` IS DISTINCT FROM S.`moneda` OR T.`anio` IS DISTINCT FROM S.`anio` OR T.`trimestre` IS DISTINCT FROM S.`trimestre` OR T.`periodo_valido` IS DISTINCT FROM S.`periodo_valido` OR T.`periodo` IS DISTINCT FROM S.`periodo` OR T.`fecha_fin_trimestre` IS DISTINCT FROM S.`fecha_fin_trimestre` OR T.`activo_valido` IS DISTINCT FROM S.`activo_valido` OR T.`pasivo_valido` IS DISTINCT FROM S.`pasivo_valido` OR T.`patrimonio_valido` IS DISTINCT FROM S.`patrimonio_valido` OR T.`patrimonio_positivo` IS DISTINCT FROM S.`patrimonio_positivo` OR T.`ingreso_valido` IS DISTINCT FROM S.`ingreso_valido` OR T.`ingreso_positivo` IS DISTINCT FROM S.`ingreso_positivo` OR T.`utilidad_valida` IS DISTINCT FROM S.`utilidad_valida` OR T.`moneda_valida` IS DISTINCT FROM S.`moneda_valida` OR T.`identidad_contable_evaluable` IS DISTINCT FROM S.`identidad_contable_evaluable` OR T.`diferencia_contable` IS DISTINCT FROM S.`diferencia_contable` OR T.`identidad_contable_valida` IS DISTINCT FROM S.`identidad_contable_valida` OR T.`cantidad_registros_empresa_periodo` IS DISTINCT FROM S.`cantidad_registros_empresa_periodo` OR T.`colision_financiera` IS DISTINCT FROM S.`colision_financiera` OR T.`sin_incidencias_estructurales` IS DISTINCT FROM S.`sin_incidencias_estructurales` OR T.`apto_kpi_general` IS DISTINCT FROM S.`apto_kpi_general` OR T.`roa_calculable` IS DISTINCT FROM S.`roa_calculable` OR T.`roe_calculable` IS DISTINCT FROM S.`roe_calculable` OR T.`endeudamiento_calculable` IS DISTINCT FROM S.`endeudamiento_calculable` OR T.`margen_neto_calculable` IS DISTINCT FROM S.`margen_neto_calculable` OR T.`autonomia_calculable` IS DISTINCT FROM S.`autonomia_calculable` OR T.`roa_aproximado_pct` IS DISTINCT FROM S.`roa_aproximado_pct` OR T.`roe_aproximado_pct` IS DISTINCT FROM S.`roe_aproximado_pct` OR T.`endeudamiento_pct` IS DISTINCT FROM S.`endeudamiento_pct` OR T.`margen_neto_pct` IS DISTINCT FROM S.`margen_neto_pct` OR T.`autonomia_financiera_pct` IS DISTINCT FROM S.`autonomia_financiera_pct` OR T.`resultado_financiero` IS DISTINCT FROM S.`resultado_financiero` OR T.`patrimonio_negativo` IS DISTINCT FROM S.`patrimonio_negativo` OR T.`motivos_calidad` IS DISTINCT FROM S.`motivos_calidad`)
    ) AS 'Conciliacion posterior incompatible';

    ASSERT (
      SELECT COUNT(*) = @expected_rows
      FROM delta_batch S
      JOIN `bvl-analytics-2026.gold_analytics.empresa_trimestre` T
        ON T.`id_registro` = S.`id_registro`
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
      'empresa_trimestre', @batch_id, @run_id,
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
    WHERE product = 'empresa_trimestre'
      AND current_version = @previous_version;

    ASSERT @@row_count = 1
      AS 'Avance de version incompatible';

    INSERT INTO `bvl-analytics-2026.gold_analytics_control.version_history_v2` (
      product, version, applied_at
    )
    VALUES (
      'empresa_trimestre', @new_version, CURRENT_TIMESTAMP()
    );

  END IF;

  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
