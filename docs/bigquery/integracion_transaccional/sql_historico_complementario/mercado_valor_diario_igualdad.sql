-- CANDIDATO LOCAL NO AUTORIZADO.
-- Consulta de validacion exclusivamente.
-- No ejecutar sin acreditacion operativa.

-- Comparacion de una version historica completa.
-- No usar despues de aplicar deltas posteriores.
-- El staging debe corresponder a la misma version.
-- Los duplicados impiden aprobar la igualdad.

WITH origen AS (
  SELECT `nemonico`, `fecha`, `descripcion`, `fecha_anterior`, `precio_anterior`, `precio_cierre`, `precio_apertura`, `precio_maximo`, `precio_minimo`, `precio_promedio`, `monto_negociado`, `fuente`, `fecha_ingesta`, `tipo_cambio_venta`, `embig_peru`, `ruc`, `razon_social`, `codigo_isin`, `tipo_valor`, `moneda_referencia`, `fecha_inscripcion`, `precio_anterior_valido`, `precio_cierre_valido`, `rango_intradia_valido`, `tipo_cambio_disponible`, `embig_disponible`, `empresa_vinculada`, `inscripcion_compatible`, `monto_negociado_valido`, `monto_negociado_positivo`, `retorno_calculable`, `variacion_precio`, `retorno_diario_pct`, `rango_intradia`, `rango_intradia_pct`, `anio`, `mes`, `trimestre`, `dias_desde_cierre_anterior`, `macro_disponible`, `nemonico_original`,
         TRUE AS presente_origen
  FROM `bvl-analytics-2026.gold_analytics_staging.mercado_valor_diario_hist_BATCH_CANDIDATO`
),
destino AS (
  SELECT `nemonico`, `fecha`, `descripcion`, `fecha_anterior`, `precio_anterior`, `precio_cierre`, `precio_apertura`, `precio_maximo`, `precio_minimo`, `precio_promedio`, `monto_negociado`, `fuente`, `fecha_ingesta`, `tipo_cambio_venta`, `embig_peru`, `ruc`, `razon_social`, `codigo_isin`, `tipo_valor`, `moneda_referencia`, `fecha_inscripcion`, `precio_anterior_valido`, `precio_cierre_valido`, `rango_intradia_valido`, `tipo_cambio_disponible`, `embig_disponible`, `empresa_vinculada`, `inscripcion_compatible`, `monto_negociado_valido`, `monto_negociado_positivo`, `retorno_calculable`, `variacion_precio`, `retorno_diario_pct`, `rango_intradia`, `rango_intradia_pct`, `anio`, `mes`, `trimestre`, `dias_desde_cierre_anterior`, `macro_disponible`, `nemonico_original`,
         TRUE AS presente_destino
  FROM `bvl-analytics-2026.gold_analytics.mercado_valor_diario`
),
calidad_origen AS (
  SELECT
    COUNT(*) AS filas_origen,
    COUNTIF(`nemonico` IS NULL OR TRIM(`nemonico`) = '' OR `fecha` IS NULL) AS claves_invalidas_origen
  FROM origen
),
calidad_destino AS (
  SELECT
    COUNT(*) AS filas_destino,
    COUNTIF(`nemonico` IS NULL OR TRIM(`nemonico`) = '' OR `fecha` IS NULL) AS claves_invalidas_destino
  FROM destino
),
duplicados_origen AS (
  SELECT COUNT(*) AS claves_duplicadas_origen
  FROM (
    SELECT `nemonico`, `fecha`
    FROM origen
    GROUP BY `nemonico`, `fecha`
    HAVING COUNT(*) > 1
  )
),
duplicados_destino AS (
  SELECT COUNT(*) AS claves_duplicadas_destino
  FROM (
    SELECT `nemonico`, `fecha`
    FROM destino
    GROUP BY `nemonico`, `fecha`
    HAVING COUNT(*) > 1
  )
),
comparacion AS (
  SELECT
    CASE
      WHEN S.presente_origen IS NULL
        THEN 'ADICIONAL_DESTINO'
      WHEN T.presente_destino IS NULL
        THEN 'FALTANTE_DESTINO'
      WHEN (
          S.`nemonico` IS DISTINCT FROM T.`nemonico`
          OR S.`fecha` IS DISTINCT FROM T.`fecha`
          OR S.`descripcion` IS DISTINCT FROM T.`descripcion`
          OR S.`fecha_anterior` IS DISTINCT FROM T.`fecha_anterior`
          OR S.`precio_anterior` IS DISTINCT FROM T.`precio_anterior`
          OR S.`precio_cierre` IS DISTINCT FROM T.`precio_cierre`
          OR S.`precio_apertura` IS DISTINCT FROM T.`precio_apertura`
          OR S.`precio_maximo` IS DISTINCT FROM T.`precio_maximo`
          OR S.`precio_minimo` IS DISTINCT FROM T.`precio_minimo`
          OR S.`precio_promedio` IS DISTINCT FROM T.`precio_promedio`
          OR S.`monto_negociado` IS DISTINCT FROM T.`monto_negociado`
          OR S.`fuente` IS DISTINCT FROM T.`fuente`
          OR S.`fecha_ingesta` IS DISTINCT FROM T.`fecha_ingesta`
          OR S.`tipo_cambio_venta` IS DISTINCT FROM T.`tipo_cambio_venta`
          OR S.`embig_peru` IS DISTINCT FROM T.`embig_peru`
          OR S.`ruc` IS DISTINCT FROM T.`ruc`
          OR S.`razon_social` IS DISTINCT FROM T.`razon_social`
          OR S.`codigo_isin` IS DISTINCT FROM T.`codigo_isin`
          OR S.`tipo_valor` IS DISTINCT FROM T.`tipo_valor`
          OR S.`moneda_referencia` IS DISTINCT FROM T.`moneda_referencia`
          OR S.`fecha_inscripcion` IS DISTINCT FROM T.`fecha_inscripcion`
          OR S.`precio_anterior_valido` IS DISTINCT FROM T.`precio_anterior_valido`
          OR S.`precio_cierre_valido` IS DISTINCT FROM T.`precio_cierre_valido`
          OR S.`rango_intradia_valido` IS DISTINCT FROM T.`rango_intradia_valido`
          OR S.`tipo_cambio_disponible` IS DISTINCT FROM T.`tipo_cambio_disponible`
          OR S.`embig_disponible` IS DISTINCT FROM T.`embig_disponible`
          OR S.`empresa_vinculada` IS DISTINCT FROM T.`empresa_vinculada`
          OR S.`inscripcion_compatible` IS DISTINCT FROM T.`inscripcion_compatible`
          OR S.`monto_negociado_valido` IS DISTINCT FROM T.`monto_negociado_valido`
          OR S.`monto_negociado_positivo` IS DISTINCT FROM T.`monto_negociado_positivo`
          OR S.`retorno_calculable` IS DISTINCT FROM T.`retorno_calculable`
          OR S.`variacion_precio` IS DISTINCT FROM T.`variacion_precio`
          OR S.`retorno_diario_pct` IS DISTINCT FROM T.`retorno_diario_pct`
          OR S.`rango_intradia` IS DISTINCT FROM T.`rango_intradia`
          OR S.`rango_intradia_pct` IS DISTINCT FROM T.`rango_intradia_pct`
          OR S.`anio` IS DISTINCT FROM T.`anio`
          OR S.`mes` IS DISTINCT FROM T.`mes`
          OR S.`trimestre` IS DISTINCT FROM T.`trimestre`
          OR S.`dias_desde_cierre_anterior` IS DISTINCT FROM T.`dias_desde_cierre_anterior`
          OR S.`macro_disponible` IS DISTINCT FROM T.`macro_disponible`
          OR S.`nemonico_original` IS DISTINCT FROM T.`nemonico_original`
      ) THEN 'MODIFICADA'
      ELSE 'IGUAL'
    END AS estado
  FROM origen S
  FULL OUTER JOIN destino T
    ON S.`nemonico` = T.`nemonico` AND S.`fecha` = T.`fecha`
),
metricas AS (
  SELECT
    COUNTIF(estado = 'ADICIONAL_DESTINO')
      AS filas_adicionales,
    COUNTIF(estado = 'FALTANTE_DESTINO')
      AS filas_faltantes,
    COUNTIF(estado = 'MODIFICADA')
      AS filas_modificadas,
    COUNTIF(estado = 'IGUAL')
      AS filas_iguales
  FROM comparacion
),
resultado AS (
  SELECT
    'mercado_valor_diario' AS producto,
    @expected_rows AS filas_esperadas,
    41 AS columnas_comparadas,
    O.*,
    T.*,
    D1.*,
    D2.*,
    M.*
  FROM calidad_origen O
  CROSS JOIN calidad_destino T
  CROSS JOIN duplicados_origen D1
  CROSS JOIN duplicados_destino D2
  CROSS JOIN metricas M
)
SELECT *,
  COALESCE(
    filas_esperadas > 0
    AND filas_origen = filas_esperadas
    AND filas_destino = filas_esperadas
    AND filas_iguales = filas_esperadas
    AND claves_invalidas_origen = 0
    AND claves_invalidas_destino = 0
    AND claves_duplicadas_origen = 0
    AND claves_duplicadas_destino = 0
    AND filas_adicionales = 0
    AND filas_faltantes = 0
    AND filas_modificadas = 0,
    FALSE
  ) AS igualdad_aprobada
FROM resultado;
