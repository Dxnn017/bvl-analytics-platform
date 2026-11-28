-- CANDIDATO LOCAL NO AUTORIZADO.
-- Consulta de referencia, no fuente operativa.
-- No ejecutar sin acreditacion correspondiente.

ASSERT FALSE AS
  'CONSUMO OPERATIVO NO AUTORIZADO';

ASSERT (
  @fecha_inicio IS NOT NULL
  AND @fecha_fin IS NOT NULL
  AND @fecha_inicio <= @fecha_fin
) AS 'Intervalo temporal incompatible';

-- Detalle sin agregacion automatica.
-- Conserva filas y controles de calidad.
-- Interpretar indicadores junto con sus flags.

SELECT
  `nemonico`,
  `fecha`,
  `nemonico_original`,
  `moneda_referencia`,
  `precio_anterior`,
  `precio_cierre`,
  `precio_maximo`,
  `precio_minimo`,
  `monto_negociado`,
  `retorno_diario_pct`,
  `rango_intradia_pct`,
  `precio_cierre_valido`,
  `precio_anterior_valido`,
  `retorno_calculable`,
  `rango_intradia_valido`,
  `monto_negociado_valido`,
  `fecha_ingesta`
FROM `bvl-analytics-2026.gold_analytics.mercado_valor_diario`
WHERE `fecha` BETWEEN
  @fecha_inicio AND @fecha_fin
ORDER BY `nemonico`, `fecha`;
