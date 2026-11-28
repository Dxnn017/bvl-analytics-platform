-- BVL Analytics Platform
-- ESTADO: CANDIDATO LOCAL NO AUTORIZADO
-- Producto: mercado_valor_diario
-- Finalidad: Distribucion por particiones
-- No ejecutar sin autorizacion expresa.

SELECT
  `anio`, `mes`,
  COUNT(*) AS registros,
  MIN(`fecha`) AS fecha_minima,
  MAX(`fecha`) AS fecha_maxima
FROM `bvl-analytics-2026.gold_analytics.mercado_valor_diario`
GROUP BY `anio`, `mes`
ORDER BY `anio`, `mes`;
