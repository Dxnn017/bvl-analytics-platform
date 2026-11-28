-- BVL Analytics Platform
-- ESTADO: CANDIDATO LOCAL NO AUTORIZADO
-- Producto: empresa_trimestre
-- Finalidad: Calidad historica
-- No ejecutar sin autorizacion expresa.

-- Parametro obligatorio:
-- expected_rows INT64, obtenido del
-- manifest Gold oficialmente aprobado.
-- Referencia candidata: 5158 filas.

WITH metricas AS (
  SELECT
    COUNT(*) AS registros_observados,
    COUNTIF(`id_registro` IS NULL OR TRIM(`id_registro`) = '') AS `clave_invalida__id_registro`,
    COUNTIF(IS_NAN(`activo_total`) OR IS_INF(`activo_total`)) AS `no_finito__activo_total`,
    COUNTIF(IS_NAN(`pasivo_total`) OR IS_INF(`pasivo_total`)) AS `no_finito__pasivo_total`,
    COUNTIF(IS_NAN(`patrimonio_total`) OR IS_INF(`patrimonio_total`)) AS `no_finito__patrimonio_total`,
    COUNTIF(IS_NAN(`total_ingreso`) OR IS_INF(`total_ingreso`)) AS `no_finito__total_ingreso`,
    COUNTIF(IS_NAN(`utilidad_neta`) OR IS_INF(`utilidad_neta`)) AS `no_finito__utilidad_neta`,
    COUNTIF(`periodo_valido` IS NULL) AS `nulo_obligatorio__periodo_valido`,
    COUNTIF(`patrimonio_valido` IS NULL) AS `nulo_obligatorio__patrimonio_valido`,
    COUNTIF(`ingreso_valido` IS NULL) AS `nulo_obligatorio__ingreso_valido`,
    COUNTIF(`utilidad_valida` IS NULL) AS `nulo_obligatorio__utilidad_valida`,
    COUNTIF(`moneda_valida` IS NULL) AS `nulo_obligatorio__moneda_valida`,
    COUNTIF(`identidad_contable_evaluable` IS NULL) AS `nulo_obligatorio__identidad_contable_evaluable`,
    COUNTIF(IS_NAN(`diferencia_contable`) OR IS_INF(`diferencia_contable`)) AS `no_finito__diferencia_contable`,
    COUNTIF(`identidad_contable_valida` IS NULL) AS `nulo_obligatorio__identidad_contable_valida`,
    COUNTIF(`colision_financiera` IS NULL) AS `nulo_obligatorio__colision_financiera`,
    COUNTIF(`sin_incidencias_estructurales` IS NULL) AS `nulo_obligatorio__sin_incidencias_estructurales`,
    COUNTIF(`apto_kpi_general` IS NULL) AS `nulo_obligatorio__apto_kpi_general`,
    COUNTIF(IS_NAN(`roa_aproximado_pct`) OR IS_INF(`roa_aproximado_pct`)) AS `no_finito__roa_aproximado_pct`,
    COUNTIF(IS_NAN(`roe_aproximado_pct`) OR IS_INF(`roe_aproximado_pct`)) AS `no_finito__roe_aproximado_pct`,
    COUNTIF(IS_NAN(`endeudamiento_pct`) OR IS_INF(`endeudamiento_pct`)) AS `no_finito__endeudamiento_pct`,
    COUNTIF(IS_NAN(`margen_neto_pct`) OR IS_INF(`margen_neto_pct`)) AS `no_finito__margen_neto_pct`,
    COUNTIF(IS_NAN(`autonomia_financiera_pct`) OR IS_INF(`autonomia_financiera_pct`)) AS `no_finito__autonomia_financiera_pct`,
    COUNTIF(`resultado_financiero` IS NULL) AS `nulo_obligatorio__resultado_financiero`,
    COUNTIF(`patrimonio_negativo` IS NULL) AS `nulo_obligatorio__patrimonio_negativo`,
    COUNTIF(`motivos_calidad` IS NULL) AS `nulo_obligatorio__motivos_calidad`,
    COUNTIF(`fecha_fin_trimestre` IS NULL OR `anio` IS DISTINCT FROM EXTRACT(YEAR FROM `fecha_fin_trimestre`) OR `trimestre` IS DISTINCT FROM EXTRACT(QUARTER FROM `fecha_fin_trimestre`)) AS `particiones_invalidas`
  FROM `bvl-analytics-2026.gold_analytics.empresa_trimestre`
), duplicados AS (
  SELECT COUNT(*) AS claves_duplicadas
  FROM (
    SELECT `id_registro`
    FROM `bvl-analytics-2026.gold_analytics.empresa_trimestre`
    GROUP BY `id_registro`
    HAVING COUNT(*) > 1
  )
)
SELECT
  'empresa_trimestre' AS producto,
  @expected_rows AS registros_esperados,
  m.*,
  d.claves_duplicadas,
  (@expected_rows > 0
    AND m.registros_observados = @expected_rows
    AND d.claves_duplicadas = 0
    AND m.`clave_invalida__id_registro` = 0
    AND m.`no_finito__activo_total` = 0
    AND m.`no_finito__pasivo_total` = 0
    AND m.`no_finito__patrimonio_total` = 0
    AND m.`no_finito__total_ingreso` = 0
    AND m.`no_finito__utilidad_neta` = 0
    AND m.`nulo_obligatorio__periodo_valido` = 0
    AND m.`nulo_obligatorio__patrimonio_valido` = 0
    AND m.`nulo_obligatorio__ingreso_valido` = 0
    AND m.`nulo_obligatorio__utilidad_valida` = 0
    AND m.`nulo_obligatorio__moneda_valida` = 0
    AND m.`nulo_obligatorio__identidad_contable_evaluable` = 0
    AND m.`no_finito__diferencia_contable` = 0
    AND m.`nulo_obligatorio__identidad_contable_valida` = 0
    AND m.`nulo_obligatorio__colision_financiera` = 0
    AND m.`nulo_obligatorio__sin_incidencias_estructurales` = 0
    AND m.`nulo_obligatorio__apto_kpi_general` = 0
    AND m.`no_finito__roa_aproximado_pct` = 0
    AND m.`no_finito__roe_aproximado_pct` = 0
    AND m.`no_finito__endeudamiento_pct` = 0
    AND m.`no_finito__margen_neto_pct` = 0
    AND m.`no_finito__autonomia_financiera_pct` = 0
    AND m.`nulo_obligatorio__resultado_financiero` = 0
    AND m.`nulo_obligatorio__patrimonio_negativo` = 0
    AND m.`nulo_obligatorio__motivos_calidad` = 0
    AND m.`particiones_invalidas` = 0) AS controles_aprobados
FROM metricas AS m
CROSS JOIN duplicados AS d;
