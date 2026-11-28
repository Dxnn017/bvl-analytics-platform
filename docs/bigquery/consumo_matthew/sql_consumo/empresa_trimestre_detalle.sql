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
  `id_registro`,
  `rpj`,
  `ruc`,
  `nombre_empresa`,
  `fecha_fin_trimestre`,
  `moneda`,
  `tipo_informacion`,
  `activo_total`,
  `pasivo_total`,
  `patrimonio_total`,
  `total_ingreso`,
  `utilidad_neta`,
  `roa_aproximado_pct`,
  `roe_aproximado_pct`,
  `endeudamiento_pct`,
  `margen_neto_pct`,
  `autonomia_financiera_pct`,
  `apto_kpi_general`,
  `roa_calculable`,
  `roe_calculable`,
  `endeudamiento_calculable`,
  `margen_neto_calculable`,
  `autonomia_calculable`,
  `colision_financiera`,
  `identidad_contable_valida`,
  `motivos_calidad`,
  `fecha_ingesta`
FROM `bvl-analytics-2026.gold_analytics.empresa_trimestre`
WHERE `fecha_fin_trimestre` BETWEEN
  @fecha_inicio AND @fecha_fin
ORDER BY `id_registro`;
