-- BVL Analytics Platform
-- ESTADO: CANDIDATO LOCAL NO AUTORIZADO
-- Producto: empresa_trimestre
-- Finalidad: Comparacion de esquema
-- No ejecutar sin autorizacion expresa.

WITH esperado AS (
  SELECT * FROM UNNEST([
    STRUCT(1 AS posicion, 'rpj' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(2 AS posicion, 'ejercicio' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(3 AS posicion, 'trimestre_num' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(4 AS posicion, 'id_registro' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(5 AS posicion, 'clave_negocio' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(6 AS posicion, 'ruc' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(7 AS posicion, 'ruc_valido' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(8 AS posicion, 'nombre_empresa' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(9 AS posicion, 'tipo_empresa' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(10 AS posicion, 'tipo_sector' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(11 AS posicion, 'ciiu' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(12 AS posicion, 'trimestre_original' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(13 AS posicion, 'tipo_informacion' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(14 AS posicion, 'metodo_flujo_efectivo' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(15 AS posicion, 'moneda_original' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(16 AS posicion, 'activo_total' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(17 AS posicion, 'pasivo_total' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(18 AS posicion, 'patrimonio_total' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(19 AS posicion, 'total_ingreso' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(20 AS posicion, 'utilidad_neta' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(21 AS posicion, 'cantidad_clave_negocio' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(22 AS posicion, 'colision_clave_negocio' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(23 AS posicion, 'fuente_datos' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(24 AS posicion, 'fecha_ingesta' AS nombre, 'DATE' AS tipo, 'YES' AS nullable),
    STRUCT(25 AS posicion, 'moneda' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(26 AS posicion, 'anio' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(27 AS posicion, 'trimestre' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(28 AS posicion, 'periodo_valido' AS nombre, 'BOOL' AS tipo, 'NO' AS nullable),
    STRUCT(29 AS posicion, 'periodo' AS nombre, 'STRING' AS tipo, 'YES' AS nullable),
    STRUCT(30 AS posicion, 'fecha_fin_trimestre' AS nombre, 'DATE' AS tipo, 'YES' AS nullable),
    STRUCT(31 AS posicion, 'activo_valido' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(32 AS posicion, 'pasivo_valido' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(33 AS posicion, 'patrimonio_valido' AS nombre, 'BOOL' AS tipo, 'NO' AS nullable),
    STRUCT(34 AS posicion, 'patrimonio_positivo' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(35 AS posicion, 'ingreso_valido' AS nombre, 'BOOL' AS tipo, 'NO' AS nullable),
    STRUCT(36 AS posicion, 'ingreso_positivo' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(37 AS posicion, 'utilidad_valida' AS nombre, 'BOOL' AS tipo, 'NO' AS nullable),
    STRUCT(38 AS posicion, 'moneda_valida' AS nombre, 'BOOL' AS tipo, 'NO' AS nullable),
    STRUCT(39 AS posicion, 'identidad_contable_evaluable' AS nombre, 'BOOL' AS tipo, 'NO' AS nullable),
    STRUCT(40 AS posicion, 'diferencia_contable' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(41 AS posicion, 'identidad_contable_valida' AS nombre, 'BOOL' AS tipo, 'NO' AS nullable),
    STRUCT(42 AS posicion, 'cantidad_registros_empresa_periodo' AS nombre, 'INT64' AS tipo, 'YES' AS nullable),
    STRUCT(43 AS posicion, 'colision_financiera' AS nombre, 'BOOL' AS tipo, 'NO' AS nullable),
    STRUCT(44 AS posicion, 'sin_incidencias_estructurales' AS nombre, 'BOOL' AS tipo, 'NO' AS nullable),
    STRUCT(45 AS posicion, 'apto_kpi_general' AS nombre, 'BOOL' AS tipo, 'NO' AS nullable),
    STRUCT(46 AS posicion, 'roa_calculable' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(47 AS posicion, 'roe_calculable' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(48 AS posicion, 'endeudamiento_calculable' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(49 AS posicion, 'margen_neto_calculable' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(50 AS posicion, 'autonomia_calculable' AS nombre, 'BOOL' AS tipo, 'YES' AS nullable),
    STRUCT(51 AS posicion, 'roa_aproximado_pct' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(52 AS posicion, 'roe_aproximado_pct' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(53 AS posicion, 'endeudamiento_pct' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(54 AS posicion, 'margen_neto_pct' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(55 AS posicion, 'autonomia_financiera_pct' AS nombre, 'FLOAT64' AS tipo, 'YES' AS nullable),
    STRUCT(56 AS posicion, 'resultado_financiero' AS nombre, 'STRING' AS tipo, 'NO' AS nullable),
    STRUCT(57 AS posicion, 'patrimonio_negativo' AS nombre, 'BOOL' AS tipo, 'NO' AS nullable),
    STRUCT(58 AS posicion, 'motivos_calidad' AS nombre, 'STRING' AS tipo, 'NO' AS nullable)
  ])
), observado AS (
  SELECT
    ordinal_position AS posicion,
    column_name AS nombre,
    data_type AS tipo,
    is_nullable AS nullable
  FROM `bvl-analytics-2026.gold_analytics.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'empresa_trimestre'
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
