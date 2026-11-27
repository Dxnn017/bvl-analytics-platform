-- SIMULACIÓN LOCAL: NO EJECUTAR EN GCP SIN QUALITY.
-- El staging debe incluir:
-- operation: UPSERT | DELETE
-- delete_authorized: BOOL
-- run_id: STRING
-- source_version: STRING

ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics_staging.mercado_valor_diario_delta`
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
    FROM `gold_analytics_staging.mercado_valor_diario_delta`
    GROUP BY `nemonico`, `fecha`
    HAVING COUNT(*) > 1
  )
) AS 'Claves duplicadas en staging';

-- Controles obligatorios antes del MERGE.
ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics_staging.mercado_valor_diario_delta`
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
  FROM `gold_analytics_staging.mercado_valor_diario_delta`
  WHERE operation = 'DELETE'
) AS 'DELETE bloqueado hasta autorización oficial de Quality';

MERGE `gold_analytics.mercado_valor_diario` T
USING `gold_analytics_staging.mercado_valor_diario_delta` S
ON T.`nemonico` = S.`nemonico` AND T.`fecha` = S.`fecha`

WHEN MATCHED AND S.operation = 'DELETE'
  THEN DELETE

WHEN MATCHED AND S.operation = 'UPSERT'
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
    `macro_disponible` = S.`macro_disponible`

WHEN NOT MATCHED AND S.operation = 'UPSERT'
  THEN INSERT (`nemonico`, `fecha`, `descripcion`, `fecha_anterior`, `precio_anterior`, `precio_cierre`, `precio_apertura`, `precio_maximo`, `precio_minimo`, `precio_promedio`, `monto_negociado`, `fuente`, `fecha_ingesta`, `tipo_cambio_venta`, `embig_peru`, `ruc`, `razon_social`, `codigo_isin`, `tipo_valor`, `moneda_referencia`, `fecha_inscripcion`, `precio_anterior_valido`, `precio_cierre_valido`, `rango_intradia_valido`, `tipo_cambio_disponible`, `embig_disponible`, `empresa_vinculada`, `inscripcion_compatible`, `monto_negociado_valido`, `monto_negociado_positivo`, `retorno_calculable`, `variacion_precio`, `retorno_diario_pct`, `rango_intradia`, `rango_intradia_pct`, `anio`, `mes`, `trimestre`, `dias_desde_cierre_anterior`, `macro_disponible`)
  VALUES (S.`nemonico`, S.`fecha`, S.`descripcion`, S.`fecha_anterior`, S.`precio_anterior`, S.`precio_cierre`, S.`precio_apertura`, S.`precio_maximo`, S.`precio_minimo`, S.`precio_promedio`, S.`monto_negociado`, S.`fuente`, S.`fecha_ingesta`, S.`tipo_cambio_venta`, S.`embig_peru`, S.`ruc`, S.`razon_social`, S.`codigo_isin`, S.`tipo_valor`, S.`moneda_referencia`, S.`fecha_inscripcion`, S.`precio_anterior_valido`, S.`precio_cierre_valido`, S.`rango_intradia_valido`, S.`tipo_cambio_disponible`, S.`embig_disponible`, S.`empresa_vinculada`, S.`inscripcion_compatible`, S.`monto_negociado_valido`, S.`monto_negociado_positivo`, S.`retorno_calculable`, S.`variacion_precio`, S.`retorno_diario_pct`, S.`rango_intradia`, S.`rango_intradia_pct`, S.`anio`, S.`mes`, S.`trimestre`, S.`dias_desde_cierre_anterior`, S.`macro_disponible`);
