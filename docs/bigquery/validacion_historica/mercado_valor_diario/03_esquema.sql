-- BVL Analytics Platform
-- ESTADO: CANDIDATO LOCAL NO AUTORIZADO
-- Producto: mercado_valor_diario
-- Finalidad: Comparacion de esquema
-- No ejecutar sin autorizacion expresa.

WITH esperado AS (
  SELECT * FROM UNNEST([
    STRUCT(1 AS posicion, 'nemonico' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(2 AS posicion, 'fecha' AS nombre, 'DATE' AS tipo, 'YES' AS nullable),
    STRUCT(3 AS posicion, 'descripcion' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(4 AS posicion, 'fecha_anterior' AS nombre, 'DATE' AS tipo, 'YES' AS nullable),
    STRUCT(5 AS posicion, 'precio_anterior' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(6 AS posicion, 'precio_cierre' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(7 AS posicion, 'precio_apertura' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(8 AS posicion, 'precio_maximo' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(9 AS posicion, 'precio_minimo' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(10 AS posicion, 'precio_promedio' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(11 AS posicion, 'monto_negociado' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(12 AS posicion, 'fuente' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(13 AS posicion, 'fecha_ingesta' AS nombre, 'DATE' AS tipo, 'YES' AS nullable),
    STRUCT(14 AS posicion, 'tipo_cambio_venta' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(15 AS posicion, 'embig_peru' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(16 AS posicion, 'ruc' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(17 AS posicion, 'razon_social' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(18 AS posicion, 'codigo_isin' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(19 AS posicion, 'tipo_valor' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(20 AS posicion, 'moneda_referencia' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(21 AS posicion, 'fecha_inscripcion' AS nombre, 'DATE' AS tipo, 'YES' AS nullable),
    STRUCT(22 AS posicion, 'precio_anterior_valido' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(23 AS posicion, 'precio_cierre_valido' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(24 AS posicion, 'rango_intradia_valido' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(25 AS posicion, 'tipo_cambio_disponible' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(26 AS posicion, 'embig_disponible' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(27 AS posicion, 'empresa_vinculada' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(28 AS posicion, 'inscripcion_compatible' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(29 AS posicion, 'monto_negociado_valido' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(30 AS posicion, 'monto_negociado_positivo' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(31 AS posicion, 'retorno_calculable' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(32 AS posicion, 'variacion_precio' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(33 AS posicion, 'retorno_diario_pct' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(34 AS posicion, 'rango_intradia' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(35 AS posicion, 'rango_intradia_pct' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(36 AS posicion, 'anio' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(37 AS posicion, 'mes' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(38 AS posicion, 'trimestre' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(39 AS posicion, 'dias_desde_cierre_anterior' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(40 AS posicion, 'macro_disponible' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(41 AS posicion, 'nemonico_original' AS nombre, 'STRING' AS tipo, 'YES' AS nullable)
  ])
), observado AS (
  SELECT
    ordinal_position AS posicion,
    column_name AS nombre,
    data_type AS tipo,
    is_nullable AS nullable
  FROM `bvl-analytics-2026.gold_analytics.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'mercado_valor_diario'
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
