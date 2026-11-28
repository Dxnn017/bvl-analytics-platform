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

-- Una fila por fecha.
-- No sumar los totales repetidos por SAB.
-- Los dias incompletos permanecen visibles.
-- No filtrar intermediarios antes de esta agrupacion.

WITH diario AS (
  SELECT
    fecha,
    COUNT(*) AS filas_sab,
    COUNT(DISTINCT codigo_sab) AS sab_distintos,

    COUNTIF(
      codigo_sab IS NULL
      OR TRIM(codigo_sab) = ''
    ) AS claves_invalidas,

    COUNT(DISTINCT total_compras_diario)
      AS variantes_compras,

    COUNT(DISTINCT total_ventas_diario)
      AS variantes_ventas,

    COUNT(DISTINCT total_actividad_diaria)
      AS variantes_actividad,

    COUNT(DISTINCT hhi_diario)
      AS variantes_hhi,

    COUNT(total_compras_diario)
      AS compras_presentes,

    COUNT(total_ventas_diario)
      AS ventas_presentes,

    COUNT(total_actividad_diaria)
      AS actividad_presente,

    COUNT(hhi_diario)
      AS hhi_presente,

    COUNT(DISTINCT cantidad_sab_diaria)
      AS variantes_cantidad,

    COUNT(cantidad_sab_diaria)
      AS cantidad_presente,

    MAX(cantidad_sab_diaria)
      AS cantidad_sab_gold,

    MAX(total_compras_diario)
      AS compras_candidatas,

    MAX(total_ventas_diario)
      AS ventas_candidatas,

    MAX(total_actividad_diaria)
      AS actividad_candidata,

    MAX(hhi_diario)
      AS hhi_candidato,

    COUNTIF(
      estado_cobertura = 'COMPLETA'
      AND cobertura_completa IS TRUE
      AND trazabilidad_verificada IS TRUE
    ) AS filas_cobertura_completa,

    COUNTIF(
      actividad_diaria_valida IS TRUE
    ) AS filas_actividad_valida,

    COUNTIF(
      estado_cobertura = 'PARCIAL'
    ) AS filas_cobertura_parcial,

    COUNTIF(
      estado_cobertura = 'DESCONOCIDA'
    ) AS filas_cobertura_desconocida

  FROM `bvl-analytics-2026.gold_analytics.actividad_sab_diaria`
  WHERE fecha BETWEEN
    @fecha_inicio AND @fecha_fin
  GROUP BY fecha
),
evaluado AS (
  SELECT *,
    COALESCE(
      filas_sab = sab_distintos
      AND claves_invalidas = 0
      AND variantes_cantidad = 1
      AND cantidad_presente = filas_sab
      AND cantidad_sab_gold = filas_sab
      AND variantes_compras = 1
      AND variantes_ventas = 1
      AND variantes_actividad = 1
      AND compras_presentes = filas_sab
      AND ventas_presentes = filas_sab
      AND actividad_presente = filas_sab
      AND filas_actividad_valida = filas_sab,
      FALSE
    ) AS totales_consistentes
  FROM diario
)
SELECT
  fecha,
  filas_sab,
  sab_distintos,
  filas_cobertura_completa,
  filas_cobertura_parcial,
  filas_cobertura_desconocida,
  totales_consistentes,

  CASE
    WHEN filas_cobertura_completa = filas_sab
      THEN 'COMPLETA'
    WHEN filas_cobertura_parcial = filas_sab
      THEN 'PARCIAL'
    WHEN filas_cobertura_desconocida = filas_sab
      THEN 'DESCONOCIDA'
    ELSE 'MIXTA_O_INCONSISTENTE'
  END AS cobertura_observada,

  CASE WHEN totales_consistentes
    THEN compras_candidatas
  END AS compras_observadas,

  CASE WHEN totales_consistentes
    THEN ventas_candidatas
  END AS ventas_observadas,

  CASE WHEN totales_consistentes
    THEN actividad_candidata
  END AS actividad_observada,

  CASE
    WHEN totales_consistentes
      AND variantes_hhi = 1
      AND hhi_presente = filas_sab
      AND actividad_candidata > 0
      THEN hhi_candidato
  END AS hhi_observado,

  CASE
    WHEN totales_consistentes
      AND filas_cobertura_completa = filas_sab
      THEN actividad_candidata
  END AS actividad_con_cobertura_completa

FROM evaluado
ORDER BY fecha;
