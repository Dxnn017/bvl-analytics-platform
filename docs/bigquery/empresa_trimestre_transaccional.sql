-- PROPUESTA LOCAL. NO EJECUTAR EN GCP.
-- Producto: empresa_trimestre
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
    WHERE product = 'empresa_trimestre'
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
    FROM `gold_analytics_staging.empresa_trimestre_delta`
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
    FROM `gold_analytics_staging.empresa_trimestre_delta`
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
    FROM `gold_analytics_staging.empresa_trimestre_delta`
    WHERE batch_id = @batch_id
      AND run_id = @run_id
      AND source_version = @source_version
      AND payload_sha256 = @payload_sha256
  )
  WHERE `id_registro` IS NULL OR TRIM(CAST(`id_registro` AS STRING)) = ''
) AS 'Claves nulas o vacías en staging';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics.empresa_trimestre`
  WHERE `id_registro` IS NULL OR TRIM(CAST(`id_registro` AS STRING)) = ''
) AS 'Claves nulas o vacías en destino';

ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT `id_registro`
    FROM `gold_analytics.empresa_trimestre`
    GROUP BY `id_registro`
    HAVING COUNT(*) > 1
  )
) AS 'Claves duplicadas en destino';

-- Bloqueo temporal: DELETE requiere autorización verificable.
ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT *
    FROM `gold_analytics_staging.empresa_trimestre_delta`
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
    FROM `gold_analytics_staging.empresa_trimestre_delta`
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
  FROM `gold_analytics_staging.empresa_trimestre_delta`
  WHERE batch_id = @batch_id
    AND run_id = @run_id
    AND source_version = @source_version
    AND payload_sha256 = @payload_sha256
) AS 'ERROR: batch vacio';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics_staging.empresa_trimestre_delta`
  WHERE batch_id = @batch_id
    AND (
      run_id IS DISTINCT FROM @run_id
      OR source_version IS DISTINCT FROM @source_version
      OR payload_sha256 IS DISTINCT FROM @payload_sha256
    )
) AS 'ERROR: batch_id con metadatos inconsistentes';

MERGE `gold_analytics.empresa_trimestre` T
USING (
  SELECT *
  FROM `gold_analytics_staging.empresa_trimestre_delta`
  WHERE batch_id = @batch_id
    AND run_id = @run_id
    AND source_version = @source_version
    AND payload_sha256 = @payload_sha256
) S
ON T.`id_registro` = S.`id_registro`

WHEN MATCHED AND S.operation = 'DELETE'
  THEN DELETE

WHEN MATCHED AND S.operation = 'UPSERT'
  AND (
    T.`rpj` IS DISTINCT FROM S.`rpj`
    OR T.`ejercicio` IS DISTINCT FROM S.`ejercicio`
    OR T.`trimestre_num` IS DISTINCT FROM S.`trimestre_num`
    OR T.`id_registro` IS DISTINCT FROM S.`id_registro`
    OR T.`clave_negocio` IS DISTINCT FROM S.`clave_negocio`
    OR T.`ruc` IS DISTINCT FROM S.`ruc`
    OR T.`ruc_valido` IS DISTINCT FROM S.`ruc_valido`
    OR T.`nombre_empresa` IS DISTINCT FROM S.`nombre_empresa`
    OR T.`tipo_empresa` IS DISTINCT FROM S.`tipo_empresa`
    OR T.`tipo_sector` IS DISTINCT FROM S.`tipo_sector`
    OR T.`ciiu` IS DISTINCT FROM S.`ciiu`
    OR T.`trimestre_original` IS DISTINCT FROM S.`trimestre_original`
    OR T.`tipo_informacion` IS DISTINCT FROM S.`tipo_informacion`
    OR T.`metodo_flujo_efectivo` IS DISTINCT FROM S.`metodo_flujo_efectivo`
    OR T.`moneda_original` IS DISTINCT FROM S.`moneda_original`
    OR T.`activo_total` IS DISTINCT FROM S.`activo_total`
    OR T.`pasivo_total` IS DISTINCT FROM S.`pasivo_total`
    OR T.`patrimonio_total` IS DISTINCT FROM S.`patrimonio_total`
    OR T.`total_ingreso` IS DISTINCT FROM S.`total_ingreso`
    OR T.`utilidad_neta` IS DISTINCT FROM S.`utilidad_neta`
    OR T.`cantidad_clave_negocio` IS DISTINCT FROM S.`cantidad_clave_negocio`
    OR T.`colision_clave_negocio` IS DISTINCT FROM S.`colision_clave_negocio`
    OR T.`fuente_datos` IS DISTINCT FROM S.`fuente_datos`
    OR T.`fecha_ingesta` IS DISTINCT FROM S.`fecha_ingesta`
    OR T.`moneda` IS DISTINCT FROM S.`moneda`
    OR T.`anio` IS DISTINCT FROM S.`anio`
    OR T.`trimestre` IS DISTINCT FROM S.`trimestre`
    OR T.`periodo_valido` IS DISTINCT FROM S.`periodo_valido`
    OR T.`periodo` IS DISTINCT FROM S.`periodo`
    OR T.`fecha_fin_trimestre` IS DISTINCT FROM S.`fecha_fin_trimestre`
    OR T.`activo_valido` IS DISTINCT FROM S.`activo_valido`
    OR T.`pasivo_valido` IS DISTINCT FROM S.`pasivo_valido`
    OR T.`patrimonio_valido` IS DISTINCT FROM S.`patrimonio_valido`
    OR T.`patrimonio_positivo` IS DISTINCT FROM S.`patrimonio_positivo`
    OR T.`ingreso_valido` IS DISTINCT FROM S.`ingreso_valido`
    OR T.`ingreso_positivo` IS DISTINCT FROM S.`ingreso_positivo`
    OR T.`utilidad_valida` IS DISTINCT FROM S.`utilidad_valida`
    OR T.`moneda_valida` IS DISTINCT FROM S.`moneda_valida`
    OR T.`identidad_contable_evaluable` IS DISTINCT FROM S.`identidad_contable_evaluable`
    OR T.`diferencia_contable` IS DISTINCT FROM S.`diferencia_contable`
    OR T.`identidad_contable_valida` IS DISTINCT FROM S.`identidad_contable_valida`
    OR T.`cantidad_registros_empresa_periodo` IS DISTINCT FROM S.`cantidad_registros_empresa_periodo`
    OR T.`colision_financiera` IS DISTINCT FROM S.`colision_financiera`
    OR T.`sin_incidencias_estructurales` IS DISTINCT FROM S.`sin_incidencias_estructurales`
    OR T.`apto_kpi_general` IS DISTINCT FROM S.`apto_kpi_general`
    OR T.`roa_calculable` IS DISTINCT FROM S.`roa_calculable`
    OR T.`roe_calculable` IS DISTINCT FROM S.`roe_calculable`
    OR T.`endeudamiento_calculable` IS DISTINCT FROM S.`endeudamiento_calculable`
    OR T.`margen_neto_calculable` IS DISTINCT FROM S.`margen_neto_calculable`
    OR T.`autonomia_calculable` IS DISTINCT FROM S.`autonomia_calculable`
    OR T.`roa_aproximado_pct` IS DISTINCT FROM S.`roa_aproximado_pct`
    OR T.`roe_aproximado_pct` IS DISTINCT FROM S.`roe_aproximado_pct`
    OR T.`endeudamiento_pct` IS DISTINCT FROM S.`endeudamiento_pct`
    OR T.`margen_neto_pct` IS DISTINCT FROM S.`margen_neto_pct`
    OR T.`autonomia_financiera_pct` IS DISTINCT FROM S.`autonomia_financiera_pct`
    OR T.`resultado_financiero` IS DISTINCT FROM S.`resultado_financiero`
    OR T.`patrimonio_negativo` IS DISTINCT FROM S.`patrimonio_negativo`
    OR T.`motivos_calidad` IS DISTINCT FROM S.`motivos_calidad`
  )
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

WHEN NOT MATCHED AND S.operation = 'UPSERT'
  THEN INSERT (`rpj`, `ejercicio`, `trimestre_num`, `id_registro`, `clave_negocio`, `ruc`, `ruc_valido`, `nombre_empresa`, `tipo_empresa`, `tipo_sector`, `ciiu`, `trimestre_original`, `tipo_informacion`, `metodo_flujo_efectivo`, `moneda_original`, `activo_total`, `pasivo_total`, `patrimonio_total`, `total_ingreso`, `utilidad_neta`, `cantidad_clave_negocio`, `colision_clave_negocio`, `fuente_datos`, `fecha_ingesta`, `moneda`, `anio`, `trimestre`, `periodo_valido`, `periodo`, `fecha_fin_trimestre`, `activo_valido`, `pasivo_valido`, `patrimonio_valido`, `patrimonio_positivo`, `ingreso_valido`, `ingreso_positivo`, `utilidad_valida`, `moneda_valida`, `identidad_contable_evaluable`, `diferencia_contable`, `identidad_contable_valida`, `cantidad_registros_empresa_periodo`, `colision_financiera`, `sin_incidencias_estructurales`, `apto_kpi_general`, `roa_calculable`, `roe_calculable`, `endeudamiento_calculable`, `margen_neto_calculable`, `autonomia_calculable`, `roa_aproximado_pct`, `roe_aproximado_pct`, `endeudamiento_pct`, `margen_neto_pct`, `autonomia_financiera_pct`, `resultado_financiero`, `patrimonio_negativo`, `motivos_calidad`)
  VALUES (S.`rpj`, S.`ejercicio`, S.`trimestre_num`, S.`id_registro`, S.`clave_negocio`, S.`ruc`, S.`ruc_valido`, S.`nombre_empresa`, S.`tipo_empresa`, S.`tipo_sector`, S.`ciiu`, S.`trimestre_original`, S.`tipo_informacion`, S.`metodo_flujo_efectivo`, S.`moneda_original`, S.`activo_total`, S.`pasivo_total`, S.`patrimonio_total`, S.`total_ingreso`, S.`utilidad_neta`, S.`cantidad_clave_negocio`, S.`colision_clave_negocio`, S.`fuente_datos`, S.`fecha_ingesta`, S.`moneda`, S.`anio`, S.`trimestre`, S.`periodo_valido`, S.`periodo`, S.`fecha_fin_trimestre`, S.`activo_valido`, S.`pasivo_valido`, S.`patrimonio_valido`, S.`patrimonio_positivo`, S.`ingreso_valido`, S.`ingreso_positivo`, S.`utilidad_valida`, S.`moneda_valida`, S.`identidad_contable_evaluable`, S.`diferencia_contable`, S.`identidad_contable_valida`, S.`cantidad_registros_empresa_periodo`, S.`colision_financiera`, S.`sin_incidencias_estructurales`, S.`apto_kpi_general`, S.`roa_calculable`, S.`roe_calculable`, S.`endeudamiento_calculable`, S.`margen_neto_calculable`, S.`autonomia_calculable`, S.`roa_aproximado_pct`, S.`roe_aproximado_pct`, S.`endeudamiento_pct`, S.`margen_neto_pct`, S.`autonomia_financiera_pct`, S.`resultado_financiero`, S.`patrimonio_negativo`, S.`motivos_calidad`);

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
    'empresa_trimestre',
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
