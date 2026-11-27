-- SIMULACIÓN LOCAL: NO EJECUTAR EN GCP SIN QUALITY.
-- El staging debe incluir:
-- operation: UPSERT | DELETE
-- delete_authorized: BOOL
-- run_id: STRING
-- source_version: STRING

ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics_staging.empresa_trimestre_delta`
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
    FROM `gold_analytics_staging.empresa_trimestre_delta`
    GROUP BY `id_registro`
    HAVING COUNT(*) > 1
  )
) AS 'Claves duplicadas en staging';

-- Controles obligatorios antes del MERGE.
ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics_staging.empresa_trimestre_delta`
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
  FROM `gold_analytics_staging.empresa_trimestre_delta`
  WHERE operation = 'DELETE'
) AS 'DELETE bloqueado hasta autorización oficial de Quality';

MERGE `gold_analytics.empresa_trimestre` T
USING `gold_analytics_staging.empresa_trimestre_delta` S
ON T.`id_registro` = S.`id_registro`

WHEN MATCHED AND S.operation = 'DELETE'
  THEN DELETE

WHEN MATCHED AND S.operation = 'UPSERT'
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
