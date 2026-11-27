-- PROPUESTA LOCAL. NO EJECUTAR EN GCP SIN QUALITY.
-- Registro persistente de batches Gold -> BigQuery.

CREATE TABLE IF NOT EXISTS
  `gold_analytics_control.incremental_batches` (
    batch_id STRING NOT NULL,
    product STRING NOT NULL,
    run_id STRING NOT NULL,
    source_version STRING NOT NULL,
    payload_sha256 STRING NOT NULL,
    status STRING NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    inserted_rows INT64,
    updated_rows INT64,
    deleted_rows INT64,
    unchanged_rows INT64,
    error_message STRING
)
PARTITION BY DATE(started_at)
CLUSTER BY product, batch_id;

-- Reglas que deberá implementar el adaptador:
-- 1. Validar autorización Quality antes de procesar.
-- 2. Rechazar claves duplicadas o nulas.
-- 3. Identificar batch por producto, batch_id y hash.
-- 4. Rechazar reutilización de batch_id con otro hash.
-- 5. Evitar reaplicar batches completados.
-- 6. Registrar cambios y estado final de cada ejecución.
-- 7. Aplicar MERGE y auditoría de manera transaccional.
-- 8. Impedir concurrencia sobre el mismo batch.
-- 9. Mantener DELETE bloqueado hasta autorización oficial.
-- 10. Conservar cobertura parcial explícita de Montos SAB.
