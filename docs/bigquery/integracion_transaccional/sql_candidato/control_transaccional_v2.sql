-- CANDIDATO LOCAL NO AUTORIZADO.
-- No retirar el bloqueo sin acreditacion Quality.
-- No ejecutar directamente en GCP.

ASSERT FALSE AS
  'EJECUCION OPERATIVA NO AUTORIZADA';

CREATE TABLE IF NOT EXISTS `bvl-analytics-2026.gold_analytics_control.product_versions_v2` (
  product STRING NOT NULL,
  current_version STRING NOT NULL,
  updated_at TIMESTAMP NOT NULL
)
CLUSTER BY product;

CREATE TABLE IF NOT EXISTS `bvl-analytics-2026.gold_analytics_control.version_history_v2` (
  product STRING NOT NULL,
  version STRING NOT NULL,
  applied_at TIMESTAMP NOT NULL
)
CLUSTER BY product, version;

CREATE TABLE IF NOT EXISTS `bvl-analytics-2026.gold_analytics_control.incremental_batches_v2` (
  product STRING NOT NULL,
  batch_id STRING NOT NULL,
  run_id STRING NOT NULL,
  source_version STRING NOT NULL,
  previous_version STRING NOT NULL,
  new_version STRING NOT NULL,
  source_manifest_sha256 STRING NOT NULL,
  gold_contract_sha256 STRING NOT NULL,
  payload_sha256 STRING NOT NULL,
  staging_content_sha256 STRING NOT NULL,
  evidence_sha256 STRING NOT NULL,
  transfer_sha256 STRING NOT NULL,
  status STRING NOT NULL,
  received_rows INT64 NOT NULL,
  inserted_rows INT64 NOT NULL,
  updated_rows INT64 NOT NULL,
  business_updates INT64 NOT NULL,
  technical_updates INT64 NOT NULL,
  unchanged_rows INT64 NOT NULL,
  deleted_rows INT64 NOT NULL,
  finished_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(finished_at)
CLUSTER BY product, batch_id;
