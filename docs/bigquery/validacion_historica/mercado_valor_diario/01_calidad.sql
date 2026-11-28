-- BVL Analytics Platform
-- ESTADO: CANDIDATO LOCAL NO AUTORIZADO
-- Producto: mercado_valor_diario
-- Finalidad: Calidad historica
-- No ejecutar sin autorizacion expresa.

-- Parametro obligatorio:
-- expected_rows INT64, obtenido del
-- manifest Gold oficialmente aprobado.
-- Referencia candidata: 83130 filas.

WITH metricas AS (
  SELECT
    COUNT(*) AS registros_observados,
    COUNTIF(`nemonico` IS NULL OR TRIM(`nemonico`) = '') AS `clave_invalida__nemonico`,
    COUNTIF(`fecha` IS NULL) AS `clave_invalida__fecha`,
    COUNTIF(IS_NAN(`precio_anterior`) OR IS_INF(`precio_anterior`)) AS `no_finito__precio_anterior`,
    COUNTIF(IS_NAN(`precio_cierre`) OR IS_INF(`precio_cierre`)) AS `no_finito__precio_cierre`,
    COUNTIF(IS_NAN(`precio_apertura`) OR IS_INF(`precio_apertura`)) AS `no_finito__precio_apertura`,
    COUNTIF(IS_NAN(`precio_maximo`) OR IS_INF(`precio_maximo`)) AS `no_finito__precio_maximo`,
    COUNTIF(IS_NAN(`precio_minimo`) OR IS_INF(`precio_minimo`)) AS `no_finito__precio_minimo`,
    COUNTIF(IS_NAN(`precio_promedio`) OR IS_INF(`precio_promedio`)) AS `no_finito__precio_promedio`,
    COUNTIF(IS_NAN(`monto_negociado`) OR IS_INF(`monto_negociado`)) AS `no_finito__monto_negociado`,
    COUNTIF(IS_NAN(`tipo_cambio_venta`) OR IS_INF(`tipo_cambio_venta`)) AS `no_finito__tipo_cambio_venta`,
    COUNTIF(IS_NAN(`embig_peru`) OR IS_INF(`embig_peru`)) AS `no_finito__embig_peru`,
    COUNTIF(IS_NAN(`variacion_precio`) OR IS_INF(`variacion_precio`)) AS `no_finito__variacion_precio`,
    COUNTIF(IS_NAN(`retorno_diario_pct`) OR IS_INF(`retorno_diario_pct`)) AS `no_finito__retorno_diario_pct`,
    COUNTIF(IS_NAN(`rango_intradia`) OR IS_INF(`rango_intradia`)) AS `no_finito__rango_intradia`,
    COUNTIF(IS_NAN(`rango_intradia_pct`) OR IS_INF(`rango_intradia_pct`)) AS `no_finito__rango_intradia_pct`,
    COUNTIF(`fecha` IS NULL OR `anio` IS DISTINCT FROM EXTRACT(YEAR FROM `fecha`) OR `mes` IS DISTINCT FROM EXTRACT(MONTH FROM `fecha`)) AS `particiones_invalidas`
  FROM `bvl-analytics-2026.gold_analytics.mercado_valor_diario`
), duplicados AS (
  SELECT COUNT(*) AS claves_duplicadas
  FROM (
    SELECT `nemonico`, `fecha`
    FROM `bvl-analytics-2026.gold_analytics.mercado_valor_diario`
    GROUP BY `nemonico`, `fecha`
    HAVING COUNT(*) > 1
  )
)
SELECT
  'mercado_valor_diario' AS producto,
  @expected_rows AS registros_esperados,
  m.*,
  d.claves_duplicadas,
  (@expected_rows > 0
    AND m.registros_observados = @expected_rows
    AND d.claves_duplicadas = 0
    AND m.`clave_invalida__nemonico` = 0
    AND m.`clave_invalida__fecha` = 0
    AND m.`no_finito__precio_anterior` = 0
    AND m.`no_finito__precio_cierre` = 0
    AND m.`no_finito__precio_apertura` = 0
    AND m.`no_finito__precio_maximo` = 0
    AND m.`no_finito__precio_minimo` = 0
    AND m.`no_finito__precio_promedio` = 0
    AND m.`no_finito__monto_negociado` = 0
    AND m.`no_finito__tipo_cambio_venta` = 0
    AND m.`no_finito__embig_peru` = 0
    AND m.`no_finito__variacion_precio` = 0
    AND m.`no_finito__retorno_diario_pct` = 0
    AND m.`no_finito__rango_intradia` = 0
    AND m.`no_finito__rango_intradia_pct` = 0
    AND m.`particiones_invalidas` = 0) AS controles_aprobados
FROM metricas AS m
CROSS JOIN duplicados AS d;
