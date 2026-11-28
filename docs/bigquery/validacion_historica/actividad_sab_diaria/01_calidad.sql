-- BVL Analytics Platform
-- ESTADO: CANDIDATO LOCAL NO AUTORIZADO
-- Producto: actividad_sab_diaria
-- Finalidad: Calidad historica
-- No ejecutar sin autorizacion expresa.

-- Parametro obligatorio:
-- expected_rows INT64, obtenido del
-- manifest Gold oficialmente aprobado.
-- Referencia candidata: 21911 filas.

WITH metricas AS (
  SELECT
    COUNT(*) AS registros_observados,
    COUNTIF(`fecha` IS NULL) AS `clave_invalida__fecha`,
    COUNTIF(`codigo_sab` IS NULL OR TRIM(`codigo_sab`) = '') AS `clave_invalida__codigo_sab`,
    COUNTIF(`monto_compras` != TRUNC(`monto_compras`, 2)) AS `escala_invalida__monto_compras`,
    COUNTIF(`monto_ventas` != TRUNC(`monto_ventas`, 2)) AS `escala_invalida__monto_ventas`,
    COUNTIF(`monto_total` != TRUNC(`monto_total`, 2)) AS `escala_invalida__monto_total`,
    COUNTIF(IS_NAN(`porcentaje_compras_original`) OR IS_INF(`porcentaje_compras_original`)) AS `no_finito__porcentaje_compras_original`,
    COUNTIF(IS_NAN(`porcentaje_ventas_original`) OR IS_INF(`porcentaje_ventas_original`)) AS `no_finito__porcentaje_ventas_original`,
    COUNTIF(IS_NAN(`porcentaje_total_original`) OR IS_INF(`porcentaje_total_original`)) AS `no_finito__porcentaje_total_original`,
    COUNTIF(`montos_validos` IS NULL) AS `nulo_obligatorio__montos_validos`,
    COUNTIF(`diferencia_monetaria` != TRUNC(`diferencia_monetaria`, 2)) AS `escala_invalida__diferencia_monetaria`,
    COUNTIF(`total_consistente` IS NULL) AS `nulo_obligatorio__total_consistente`,
    COUNTIF(`saldo_neto` != TRUNC(`saldo_neto`, 2)) AS `escala_invalida__saldo_neto`,
    COUNTIF(`total_compras_diario` != TRUNC(`total_compras_diario`, 2)) AS `escala_invalida__total_compras_diario`,
    COUNTIF(`total_ventas_diario` != TRUNC(`total_ventas_diario`, 2)) AS `escala_invalida__total_ventas_diario`,
    COUNTIF(`total_actividad_diaria` != TRUNC(`total_actividad_diaria`, 2)) AS `escala_invalida__total_actividad_diaria`,
    COUNTIF(`cantidad_sab_diaria` IS NULL) AS `nulo_obligatorio__cantidad_sab_diaria`,
    COUNTIF(IS_NAN(`participacion_compras_pct`) OR IS_INF(`participacion_compras_pct`)) AS `no_finito__participacion_compras_pct`,
    COUNTIF(IS_NAN(`participacion_ventas_pct`) OR IS_INF(`participacion_ventas_pct`)) AS `no_finito__participacion_ventas_pct`,
    COUNTIF(IS_NAN(`participacion_total_pct`) OR IS_INF(`participacion_total_pct`)) AS `no_finito__participacion_total_pct`,
    COUNTIF(IS_NAN(`diferencia_participacion_compras`) OR IS_INF(`diferencia_participacion_compras`)) AS `no_finito__diferencia_participacion_compras`,
    COUNTIF(IS_NAN(`diferencia_participacion_ventas`) OR IS_INF(`diferencia_participacion_ventas`)) AS `no_finito__diferencia_participacion_ventas`,
    COUNTIF(IS_NAN(`diferencia_participacion_total`) OR IS_INF(`diferencia_participacion_total`)) AS `no_finito__diferencia_participacion_total`,
    COUNTIF(IS_NAN(`aporte_hhi`) OR IS_INF(`aporte_hhi`)) AS `no_finito__aporte_hhi`,
    COUNTIF(IS_NAN(`hhi_diario`) OR IS_INF(`hhi_diario`)) AS `no_finito__hhi_diario`,
    COUNTIF(`estado_cobertura` IS NULL) AS `nulo_obligatorio__estado_cobertura`,
    COUNTIF(`trazabilidad_verificada` IS NULL) AS `nulo_obligatorio__trazabilidad_verificada`,
    COUNTIF(`cobertura_completa` IS NULL) AS `nulo_obligatorio__cobertura_completa`,
    COUNTIF(`fecha` IS NULL OR `anio` IS DISTINCT FROM EXTRACT(YEAR FROM `fecha`) OR `mes` IS DISTINCT FROM EXTRACT(MONTH FROM `fecha`)) AS `particiones_invalidas`
  FROM `bvl-analytics-2026.gold_analytics.actividad_sab_diaria`
), duplicados AS (
  SELECT COUNT(*) AS claves_duplicadas
  FROM (
    SELECT `fecha`, `codigo_sab`
    FROM `bvl-analytics-2026.gold_analytics.actividad_sab_diaria`
    GROUP BY `fecha`, `codigo_sab`
    HAVING COUNT(*) > 1
  )
)
SELECT
  'actividad_sab_diaria' AS producto,
  @expected_rows AS registros_esperados,
  m.*,
  d.claves_duplicadas,
  (@expected_rows > 0
    AND m.registros_observados = @expected_rows
    AND d.claves_duplicadas = 0
    AND m.`clave_invalida__fecha` = 0
    AND m.`clave_invalida__codigo_sab` = 0
    AND m.`escala_invalida__monto_compras` = 0
    AND m.`escala_invalida__monto_ventas` = 0
    AND m.`escala_invalida__monto_total` = 0
    AND m.`no_finito__porcentaje_compras_original` = 0
    AND m.`no_finito__porcentaje_ventas_original` = 0
    AND m.`no_finito__porcentaje_total_original` = 0
    AND m.`nulo_obligatorio__montos_validos` = 0
    AND m.`escala_invalida__diferencia_monetaria` = 0
    AND m.`nulo_obligatorio__total_consistente` = 0
    AND m.`escala_invalida__saldo_neto` = 0
    AND m.`escala_invalida__total_compras_diario` = 0
    AND m.`escala_invalida__total_ventas_diario` = 0
    AND m.`escala_invalida__total_actividad_diaria` = 0
    AND m.`nulo_obligatorio__cantidad_sab_diaria` = 0
    AND m.`no_finito__participacion_compras_pct` = 0
    AND m.`no_finito__participacion_ventas_pct` = 0
    AND m.`no_finito__participacion_total_pct` = 0
    AND m.`no_finito__diferencia_participacion_compras` = 0
    AND m.`no_finito__diferencia_participacion_ventas` = 0
    AND m.`no_finito__diferencia_participacion_total` = 0
    AND m.`no_finito__aporte_hhi` = 0
    AND m.`no_finito__hhi_diario` = 0
    AND m.`nulo_obligatorio__estado_cobertura` = 0
    AND m.`nulo_obligatorio__trazabilidad_verificada` = 0
    AND m.`nulo_obligatorio__cobertura_completa` = 0
    AND m.`particiones_invalidas` = 0) AS controles_aprobados
FROM metricas AS m
CROSS JOIN duplicados AS d;
