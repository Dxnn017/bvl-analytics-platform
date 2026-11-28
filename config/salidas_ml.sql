-- Preparación del destino para despliegue futuro. No se ejecuta en modo local.
CREATE TABLE IF NOT EXISTS `bvl-analytics-2026.gold_analytics.ml_predicciones_riesgo` (
  nemonico STRING NOT NULL, ruc STRING, fecha_corte DATE NOT NULL, horizonte_dias INT64 NOT NULL,
  clase_predicha INT64, etiqueta_riesgo STRING, probabilidad_clase_0 FLOAT64,
  probabilidad_clase_1 FLOAT64, probabilidad_clase_2 FLOAT64, score_riesgo FLOAT64,
  frecuencia_cotizacion_20d FLOAT64, volatilidad_20d_pct FLOAT64, hhi_mercado_fecha FLOAT64,
  modelo_nombre STRING, modelo_version STRING NOT NULL, run_id STRING,
  version_gold_mercado STRING, version_gold_sab STRING, version_gold_empresa STRING,
  hash_features_entrada STRING, fecha_prediccion_utc TIMESTAMP
) PARTITION BY fecha_corte CLUSTER BY nemonico, modelo_version;

CREATE TABLE IF NOT EXISTS `bvl-analytics-2026.gold_analytics.ml_corridas_registro` (
  run_id STRING NOT NULL, tipo_ejecucion STRING, fecha_corte_min DATE, fecha_corte_max DATE,
  registros_evaluados INT64, registros_insertados INT64, registros_actualizados INT64,
  cambios_clase_detectados INT64, modelo_version STRING, f1_clase_minoritaria FLOAT64,
  f1_baseline_minoritaria FLOAT64, versiones_gold_json STRING, estado STRING,
  fecha_ejecucion_utc TIMESTAMP
) PARTITION BY DATE(fecha_ejecucion_utc) CLUSTER BY run_id;
