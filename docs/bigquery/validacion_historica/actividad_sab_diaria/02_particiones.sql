-- BVL Analytics Platform
-- ESTADO: CANDIDATO LOCAL NO AUTORIZADO
-- Producto: actividad_sab_diaria
-- Finalidad: Distribucion por particiones
-- No ejecutar sin autorizacion expresa.

SELECT
  `anio`, `mes`,
  COUNT(*) AS registros,
  MIN(`fecha`) AS fecha_minima,
  MAX(`fecha`) AS fecha_maxima
FROM `bvl-analytics-2026.gold_analytics.actividad_sab_diaria`
GROUP BY `anio`, `mes`
ORDER BY `anio`, `mes`;
