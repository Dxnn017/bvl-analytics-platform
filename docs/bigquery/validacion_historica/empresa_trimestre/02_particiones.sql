-- BVL Analytics Platform
-- ESTADO: CANDIDATO LOCAL NO AUTORIZADO
-- Producto: empresa_trimestre
-- Finalidad: Distribucion por particiones
-- No ejecutar sin autorizacion expresa.

SELECT
  `anio`, `trimestre`,
  COUNT(*) AS registros,
  MIN(`fecha_fin_trimestre`) AS fecha_minima,
  MAX(`fecha_fin_trimestre`) AS fecha_maxima
FROM `bvl-analytics-2026.gold_analytics.empresa_trimestre`
GROUP BY `anio`, `trimestre`
ORDER BY `anio`, `trimestre`;
