-- BVL Analytics Platform
-- ESTADO: CANDIDATO LOCAL NO AUTORIZADO
-- Producto: actividad_sab_diaria
-- Finalidad: Comparacion de esquema
-- No ejecutar sin autorizacion expresa.

WITH esperado AS (
  SELECT * FROM UNNEST([
    STRUCT(1 AS posicion, 'fecha' AS nombre, 'DATE' AS tipo, 'YES' AS nullable),
    STRUCT(2 AS posicion, 'codigo_sab' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(3 AS posicion, 'sociedad_agente_bolsa' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(4 AS posicion, 'monto_compras' AS nombre, 'NUMERIC' AS tipo, 'YES' AS nullable),
    STRUCT(5 AS posicion, 'monto_ventas' AS nombre, 'NUMERIC' AS tipo, 'YES' AS nullable),
    STRUCT(6 AS posicion, 'monto_total' AS nombre, 'NUMERIC' AS tipo, 'YES' AS nullable),
    STRUCT(7 AS posicion, 'porcentaje_compras_original' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(8 AS posicion, 'porcentaje_ventas_original' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(9 AS posicion, 'porcentaje_total_original' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(10 AS posicion, 'fuente_datos' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(11 AS posicion, 'fecha_ingesta' AS nombre, 'DATE' AS tipo, 'YES' AS nullable),
    STRUCT(12 AS posicion, 'montos_validos' AS nombre, 'BOOL' AS tipo, 'NO' AS nullable),
    STRUCT(13 AS posicion, 'diferencia_monetaria' AS nombre, 'NUMERIC' AS tipo, 'YES' AS nullable),
    STRUCT(14 AS posicion, 'total_consistente' AS nombre, 'BOOL' AS tipo, 'NO' AS nullable),
    STRUCT(15 AS posicion, 'saldo_neto' AS nombre, 'NUMERIC' AS tipo, 'YES' AS nullable),
    STRUCT(16 AS posicion, 'total_compras_diario' AS nombre, 'NUMERIC' AS tipo, 'YES' AS nullable),
    STRUCT(17 AS posicion, 'total_ventas_diario' AS nombre, 'NUMERIC' AS tipo, 'YES' AS nullable),
    STRUCT(18 AS posicion, 'total_actividad_diaria' AS nombre, 'NUMERIC' AS tipo, 'YES' AS nullable),
    STRUCT(19 AS posicion, 'cantidad_sab_diaria' AS nombre, 'INT64' AS tipo, 'NO' AS nullable),
    STRUCT(20 AS posicion, 'registros_invalidos_diarios' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(21 AS posicion, 'actividad_diaria_valida' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(22 AS posicion, 'participacion_compras_pct' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(23 AS posicion, 'participacion_ventas_pct' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(24 AS posicion, 'participacion_total_pct' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(25 AS posicion, 'diferencia_participacion_compras' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(26 AS posicion, 'diferencia_participacion_ventas' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(27 AS posicion, 'diferencia_participacion_total' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(28 AS posicion, 'ranking_diario' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(29 AS posicion, 'aporte_hhi' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(30 AS posicion, 'hhi_diario' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(31 AS posicion, 'anio' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(32 AS posicion, 'mes' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(33 AS posicion, 'trimestre' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(34 AS posicion, 'registros_elegibles' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(35 AS posicion, 'registros_cuarentena' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(36 AS posicion, 'archivos_excluidos' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(37 AS posicion, 'estado_cobertura' AS nombre, 'STRING' AS tipo, 'NO' AS nullable),
    STRUCT(38 AS posicion, 'version_silver' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(39 AS posicion, 'scope_quality_id' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(40 AS posicion, 'trazabilidad_verificada' AS nombre, 'BOOL' AS tipo, 'NO' AS nullable),
    STRUCT(41 AS posicion, 'cobertura_completa' AS nombre, 'BOOL' AS tipo, 'NO' AS nullable)
  ])
), observado AS (
  SELECT
    ordinal_position AS posicion,
    column_name AS nombre,
    data_type AS tipo,
    is_nullable AS nullable
  FROM `bvl-analytics-2026.gold_analytics.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'actividad_sab_diaria'
    AND is_system_defined = 'NO'
)
SELECT
  COALESCE(e.nombre, o.nombre) AS columna,
  e.posicion AS posicion_esperada,
  o.posicion AS posicion_observada,
  e.tipo AS tipo_esperado,
  o.tipo AS tipo_observado,
  e.nullable AS nullable_esperado,
  o.nullable AS nullable_observado,
  CASE
    WHEN e.nombre IS NULL THEN 'ADICIONAL'
    WHEN o.nombre IS NULL THEN 'FALTANTE'
    WHEN e.posicion != o.posicion THEN 'POSICION_INCORRECTA'
    WHEN e.tipo != o.tipo THEN 'TIPO_INCORRECTO'
    WHEN e.nullable != o.nullable THEN 'NULABILIDAD_INCORRECTA'
    ELSE 'OK'
  END AS estado
FROM esperado AS e
FULL OUTER JOIN observado AS o
ON e.nombre = o.nombre
ORDER BY COALESCE(e.posicion, o.posicion);
