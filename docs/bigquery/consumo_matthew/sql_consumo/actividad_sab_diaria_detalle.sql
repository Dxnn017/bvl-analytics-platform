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
  `fecha`,
  `codigo_sab`,
  `sociedad_agente_bolsa`,
  `monto_compras`,
  `monto_ventas`,
  `monto_total`,
  `saldo_neto`,
  `participacion_total_pct`,
  `ranking_diario`,
  `hhi_diario`,
  `total_actividad_diaria`,
  `cantidad_sab_diaria`,
  `registros_invalidos_diarios`,
  `montos_validos`,
  `total_consistente`,
  `actividad_diaria_valida`,
  `estado_cobertura`,
  `cobertura_completa`,
  `trazabilidad_verificada`,
  `registros_elegibles`,
  `registros_cuarentena`,
  `archivos_excluidos`,
  `scope_quality_id`,
  `version_silver`,
  `fecha_ingesta`
FROM `bvl-analytics-2026.gold_analytics.actividad_sab_diaria`
WHERE `fecha` BETWEEN
  @fecha_inicio AND @fecha_fin
ORDER BY `fecha`, `codigo_sab`;
