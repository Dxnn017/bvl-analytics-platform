-- CANDIDATO LOCAL NO AUTORIZADO.
-- Consulta de validacion exclusivamente.
-- No ejecutar sin acreditacion operativa.

-- Comparacion de una version historica completa.
-- No usar despues de aplicar deltas posteriores.
-- El staging debe corresponder a la misma version.
-- Los duplicados impiden aprobar la igualdad.

WITH origen AS (
  SELECT `rpj`, `ejercicio`, `trimestre_num`, `id_registro`, `clave_negocio`, `ruc`, `ruc_valido`, `nombre_empresa`, `tipo_empresa`, `tipo_sector`, `ciiu`, `trimestre_original`, `tipo_informacion`, `metodo_flujo_efectivo`, `moneda_original`, `activo_total`, `pasivo_total`, `patrimonio_total`, `total_ingreso`, `utilidad_neta`, `cantidad_clave_negocio`, `colision_clave_negocio`, `fuente_datos`, `fecha_ingesta`, `moneda`, `anio`, `trimestre`, `periodo_valido`, `periodo`, `fecha_fin_trimestre`, `activo_valido`, `pasivo_valido`, `patrimonio_valido`, `patrimonio_positivo`, `ingreso_valido`, `ingreso_positivo`, `utilidad_valida`, `moneda_valida`, `identidad_contable_evaluable`, `diferencia_contable`, `identidad_contable_valida`, `cantidad_registros_empresa_periodo`, `colision_financiera`, `sin_incidencias_estructurales`, `apto_kpi_general`, `roa_calculable`, `roe_calculable`, `endeudamiento_calculable`, `margen_neto_calculable`, `autonomia_calculable`, `roa_aproximado_pct`, `roe_aproximado_pct`, `endeudamiento_pct`, `margen_neto_pct`, `autonomia_financiera_pct`, `resultado_financiero`, `patrimonio_negativo`, `motivos_calidad`,
         TRUE AS presente_origen
  FROM `bvl-analytics-2026.gold_analytics_staging.empresa_trimestre_hist_BATCH_CANDIDATO`
),
destino AS (
  SELECT `rpj`, `ejercicio`, `trimestre_num`, `id_registro`, `clave_negocio`, `ruc`, `ruc_valido`, `nombre_empresa`, `tipo_empresa`, `tipo_sector`, `ciiu`, `trimestre_original`, `tipo_informacion`, `metodo_flujo_efectivo`, `moneda_original`, `activo_total`, `pasivo_total`, `patrimonio_total`, `total_ingreso`, `utilidad_neta`, `cantidad_clave_negocio`, `colision_clave_negocio`, `fuente_datos`, `fecha_ingesta`, `moneda`, `anio`, `trimestre`, `periodo_valido`, `periodo`, `fecha_fin_trimestre`, `activo_valido`, `pasivo_valido`, `patrimonio_valido`, `patrimonio_positivo`, `ingreso_valido`, `ingreso_positivo`, `utilidad_valida`, `moneda_valida`, `identidad_contable_evaluable`, `diferencia_contable`, `identidad_contable_valida`, `cantidad_registros_empresa_periodo`, `colision_financiera`, `sin_incidencias_estructurales`, `apto_kpi_general`, `roa_calculable`, `roe_calculable`, `endeudamiento_calculable`, `margen_neto_calculable`, `autonomia_calculable`, `roa_aproximado_pct`, `roe_aproximado_pct`, `endeudamiento_pct`, `margen_neto_pct`, `autonomia_financiera_pct`, `resultado_financiero`, `patrimonio_negativo`, `motivos_calidad`,
         TRUE AS presente_destino
  FROM `bvl-analytics-2026.gold_analytics.empresa_trimestre`
),
calidad_origen AS (
  SELECT
    COUNT(*) AS filas_origen,
    COUNTIF(`id_registro` IS NULL OR TRIM(`id_registro`) = '') AS claves_invalidas_origen
  FROM origen
),
calidad_destino AS (
  SELECT
    COUNT(*) AS filas_destino,
    COUNTIF(`id_registro` IS NULL OR TRIM(`id_registro`) = '') AS claves_invalidas_destino
  FROM destino
),
duplicados_origen AS (
  SELECT COUNT(*) AS claves_duplicadas_origen
  FROM (
    SELECT `id_registro`
    FROM origen
    GROUP BY `id_registro`
    HAVING COUNT(*) > 1
  )
),
duplicados_destino AS (
  SELECT COUNT(*) AS claves_duplicadas_destino
  FROM (
    SELECT `id_registro`
    FROM destino
    GROUP BY `id_registro`
    HAVING COUNT(*) > 1
  )
),
comparacion AS (
  SELECT
    CASE
      WHEN S.presente_origen IS NULL
        THEN 'ADICIONAL_DESTINO'
      WHEN T.presente_destino IS NULL
        THEN 'FALTANTE_DESTINO'
      WHEN (
          S.`rpj` IS DISTINCT FROM T.`rpj`
          OR S.`ejercicio` IS DISTINCT FROM T.`ejercicio`
          OR S.`trimestre_num` IS DISTINCT FROM T.`trimestre_num`
          OR S.`id_registro` IS DISTINCT FROM T.`id_registro`
          OR S.`clave_negocio` IS DISTINCT FROM T.`clave_negocio`
          OR S.`ruc` IS DISTINCT FROM T.`ruc`
          OR S.`ruc_valido` IS DISTINCT FROM T.`ruc_valido`
          OR S.`nombre_empresa` IS DISTINCT FROM T.`nombre_empresa`
          OR S.`tipo_empresa` IS DISTINCT FROM T.`tipo_empresa`
          OR S.`tipo_sector` IS DISTINCT FROM T.`tipo_sector`
          OR S.`ciiu` IS DISTINCT FROM T.`ciiu`
          OR S.`trimestre_original` IS DISTINCT FROM T.`trimestre_original`
          OR S.`tipo_informacion` IS DISTINCT FROM T.`tipo_informacion`
          OR S.`metodo_flujo_efectivo` IS DISTINCT FROM T.`metodo_flujo_efectivo`
          OR S.`moneda_original` IS DISTINCT FROM T.`moneda_original`
          OR S.`activo_total` IS DISTINCT FROM T.`activo_total`
          OR S.`pasivo_total` IS DISTINCT FROM T.`pasivo_total`
          OR S.`patrimonio_total` IS DISTINCT FROM T.`patrimonio_total`
          OR S.`total_ingreso` IS DISTINCT FROM T.`total_ingreso`
          OR S.`utilidad_neta` IS DISTINCT FROM T.`utilidad_neta`
          OR S.`cantidad_clave_negocio` IS DISTINCT FROM T.`cantidad_clave_negocio`
          OR S.`colision_clave_negocio` IS DISTINCT FROM T.`colision_clave_negocio`
          OR S.`fuente_datos` IS DISTINCT FROM T.`fuente_datos`
          OR S.`fecha_ingesta` IS DISTINCT FROM T.`fecha_ingesta`
          OR S.`moneda` IS DISTINCT FROM T.`moneda`
          OR S.`anio` IS DISTINCT FROM T.`anio`
          OR S.`trimestre` IS DISTINCT FROM T.`trimestre`
          OR S.`periodo_valido` IS DISTINCT FROM T.`periodo_valido`
          OR S.`periodo` IS DISTINCT FROM T.`periodo`
          OR S.`fecha_fin_trimestre` IS DISTINCT FROM T.`fecha_fin_trimestre`
          OR S.`activo_valido` IS DISTINCT FROM T.`activo_valido`
          OR S.`pasivo_valido` IS DISTINCT FROM T.`pasivo_valido`
          OR S.`patrimonio_valido` IS DISTINCT FROM T.`patrimonio_valido`
          OR S.`patrimonio_positivo` IS DISTINCT FROM T.`patrimonio_positivo`
          OR S.`ingreso_valido` IS DISTINCT FROM T.`ingreso_valido`
          OR S.`ingreso_positivo` IS DISTINCT FROM T.`ingreso_positivo`
          OR S.`utilidad_valida` IS DISTINCT FROM T.`utilidad_valida`
          OR S.`moneda_valida` IS DISTINCT FROM T.`moneda_valida`
          OR S.`identidad_contable_evaluable` IS DISTINCT FROM T.`identidad_contable_evaluable`
          OR S.`diferencia_contable` IS DISTINCT FROM T.`diferencia_contable`
          OR S.`identidad_contable_valida` IS DISTINCT FROM T.`identidad_contable_valida`
          OR S.`cantidad_registros_empresa_periodo` IS DISTINCT FROM T.`cantidad_registros_empresa_periodo`
          OR S.`colision_financiera` IS DISTINCT FROM T.`colision_financiera`
          OR S.`sin_incidencias_estructurales` IS DISTINCT FROM T.`sin_incidencias_estructurales`
          OR S.`apto_kpi_general` IS DISTINCT FROM T.`apto_kpi_general`
          OR S.`roa_calculable` IS DISTINCT FROM T.`roa_calculable`
          OR S.`roe_calculable` IS DISTINCT FROM T.`roe_calculable`
          OR S.`endeudamiento_calculable` IS DISTINCT FROM T.`endeudamiento_calculable`
          OR S.`margen_neto_calculable` IS DISTINCT FROM T.`margen_neto_calculable`
          OR S.`autonomia_calculable` IS DISTINCT FROM T.`autonomia_calculable`
          OR S.`roa_aproximado_pct` IS DISTINCT FROM T.`roa_aproximado_pct`
          OR S.`roe_aproximado_pct` IS DISTINCT FROM T.`roe_aproximado_pct`
          OR S.`endeudamiento_pct` IS DISTINCT FROM T.`endeudamiento_pct`
          OR S.`margen_neto_pct` IS DISTINCT FROM T.`margen_neto_pct`
          OR S.`autonomia_financiera_pct` IS DISTINCT FROM T.`autonomia_financiera_pct`
          OR S.`resultado_financiero` IS DISTINCT FROM T.`resultado_financiero`
          OR S.`patrimonio_negativo` IS DISTINCT FROM T.`patrimonio_negativo`
          OR S.`motivos_calidad` IS DISTINCT FROM T.`motivos_calidad`
      ) THEN 'MODIFICADA'
      ELSE 'IGUAL'
    END AS estado
  FROM origen S
  FULL OUTER JOIN destino T
    ON S.`id_registro` = T.`id_registro`
),
metricas AS (
  SELECT
    COUNTIF(estado = 'ADICIONAL_DESTINO')
      AS filas_adicionales,
    COUNTIF(estado = 'FALTANTE_DESTINO')
      AS filas_faltantes,
    COUNTIF(estado = 'MODIFICADA')
      AS filas_modificadas,
    COUNTIF(estado = 'IGUAL')
      AS filas_iguales
  FROM comparacion
),
resultado AS (
  SELECT
    'empresa_trimestre' AS producto,
    @expected_rows AS filas_esperadas,
    58 AS columnas_comparadas,
    O.*,
    T.*,
    D1.*,
    D2.*,
    M.*
  FROM calidad_origen O
  CROSS JOIN calidad_destino T
  CROSS JOIN duplicados_origen D1
  CROSS JOIN duplicados_destino D2
  CROSS JOIN metricas M
)
SELECT *,
  COALESCE(
    filas_esperadas > 0
    AND filas_origen = filas_esperadas
    AND filas_destino = filas_esperadas
    AND filas_iguales = filas_esperadas
    AND claves_invalidas_origen = 0
    AND claves_invalidas_destino = 0
    AND claves_duplicadas_origen = 0
    AND claves_duplicadas_destino = 0
    AND filas_adicionales = 0
    AND filas_faltantes = 0
    AND filas_modificadas = 0,
    FALSE
  ) AS igualdad_aprobada
FROM resultado;
