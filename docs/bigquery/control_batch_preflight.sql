-- PROPUESTA LOCAL: control previo de idempotencia.
-- NO ejecutar en GCP sin validación y autorización de Quality.
--
-- Parámetros requeridos:
-- @product, @batch_id, @run_id, @source_version, @payload_sha256
--
-- IMPORTANTE:
-- Este preflight no reserva el batch ni evita carreras concurrentes.
-- Debe integrarse en un mecanismo transaccional autorizado.

ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics_control.incremental_batches`
  WHERE product = @product
    AND batch_id = @batch_id
    AND (
      run_id != @run_id
      OR source_version != @source_version
      OR payload_sha256 != @payload_sha256
    )
) AS 'ERROR: batch_id reutilizado con trazabilidad diferente';

ASSERT (
  SELECT COUNT(*) <= 1
  FROM `gold_analytics_control.incremental_batches`
  WHERE product = @product
    AND batch_id = @batch_id
) AS 'ERROR: registros de batch duplicados';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `gold_analytics_control.incremental_batches`
  WHERE product = @product
    AND batch_id = @batch_id
    AND status NOT IN ('COMPLETED')
) AS 'ERROR: batch existente sin finalizacion confirmada';

-- Un batch COMPLETED con la misma identidad debe tratarse como REPLAY.
-- El orquestador debe omitir el MERGE para ese caso.
-- No basta con ejecutar este preflight para garantizarlo.
-- La atomicidad, exclusión concurrente y auditoría siguen pendientes.
