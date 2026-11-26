# BVL Analytics — Fase 6. Única entrega técnica a Quality

**Alcance:** `bcrp/tipo_cambio` → `macroeconomia/tipo_cambio`, staging exclusivamente.
**Base aprobada:** puerta Fase 5 en `41266f0`; commit de procesador anterior `0f68dce`. Este paquete es un **nuevo candidato** y requiere commit y aprobación independiente de Quality. **No es una autorización de lanzamiento.**

## Hallazgos atendidos

1. **Contexto firmado:** `fase6_contexto_lanzamiento.py` verifica la firma ECDSA P-256 DER, JSON canónico y SPKI pin de Quality *reutilizando* la puerta aprobada. La firma del contexto `BVL-SILVER-LAUNCH-CONTEXT-v1` incluye commit completo del procesador, SHA-256 del entrypoint, SHA-256 del ZIP de dependencias, generaciones y URIs GCS de ambos, run, attempt, fingerprint de autorización, identidad, batch ID, subred, runtime 2.2, configuración 4+2×4 vCPU, TTL y prohibiciones de sobrescritura, retry y publicación. Las referencias GCS de contexto y firma vienen de Quality y son independientes de los bytes recibidos. El runtime compara bytes locales con los SHA firmados y vuelve a contrastarlos con generaciones GCS exactas antes de ejecutar el preflight/claim Fase 5. `--processor-commit` dejó de servir como única prueba.
2. **ANSI + CSV:** se usa `F.try_to_timestamp(..., lit('yyyy-MM-dd')).cast('date')` para devolver NULL frente a 31Feb o años no válidos incluso con ANSI activo; el año solo se convierte a entero si contiene dos dígitos. Los campos fecha vacíos/espacios y `NULL` se consideran descriptivos; los valores numéricos `""`, `n.d.` y `NULL` se convierten a NULL. Los errores numéricos siguen siendo bloqueantes y los conflictos por fecha no se deduplican arbitrariamente.
3. **Dataproc 2.2:** Python 3.12, Spark 3.5.x, PyArrow 15.x, `google-cloud-storage` 2.16.x y codec Snappy se verifican antes del claim. ZIP plano de `--py-files` producido de forma reproducible a partir de los archivos **confirmados en HEAD** (`fase6_preparar_release.py`), sin instalar librerías externas. Validar en Cloud subred/permiso de lectura del release y credenciales efectivas.

## Pruebas

- Fuera de Debian: 20 pruebas offline aprobadas (12 existentes + 8 nuevas); las pruebas Spark requieren instalación real y **no están acreditadas aún para este ajuste**.
- Debian: ejecutar `tests/test_fase6_tipo_cambio_spark_local.py` con `spark-submit --master 'local[1]' ...`: 5 pruebas esperadas, incluyendo casos ANSI y CSV vacíos; registrar resultados. No ejecutar batch cloud.
- Git: `git diff --check`; asegurar que `scripts/pipeline/*` no se alteran; dejar nuevos hashes verificables.

## Contrato propuesto para firma Quality

`BVL-SILVER-LAUNCH-CONTEXT-v1` con exactamente estos campos:

```
schema_version,status,quality_approved,project_id,region,dataset,product,
operation,run_id,attempt_id,authorization_fingerprint,processor_commit,
entrypoint_sha256,dependencies_sha256,entrypoint_uri,entrypoint_generation,
dependencies_uri,dependencies_generation,runtime_version,service_account,
batch_id,subnet,driver_cores,executor_cores,executor_instances,ttl_seconds,
not_before_utc,expires_at_utc,overwrite_allowed,automatic_retry_allowed,
official_publication_allowed
```

Valores obligatorios de calidad: status `QUALITY_APPROVED_SUPERVISED_LAUNCH`, `quality_approved:true`; runtime `2.2`, identidad `sa-bvl-silver@bvl-analytics-2026.iam.gserviceaccount.com`, CPU 4+4×2, TTL 60–3600 s; flags de publicación/overwrite/retry `false`. Fechas de contexto máximas 4 horas y **no se amplía** la vigencia de la autorización original. Quality ratifica commit y SHA del release y firma con su KMS propio. Las generaciones se fijan después de crear los artefactos en GCS sin overwrite.

- Objeto firmado propuesto: `gs://bvl26-control-7f3a9c/authorizations/silver_launch/run_id=<run>/attempt_id=<attempt>/launch_context.json`.
- Firma `.sig` junto al JSON; referencias externas Quality a ambas generaciones y SHA-256.
- Se preserva el contrato Bronze–Silver Fase 5 y la puerta Fase 5, sin reenviar o consumir claims.

## Propuesta **no ejecutable** hasta rellenar y aprobar todos los valores

Esta forma la utiliza exclusivamente Quality/orquestación en Cloud Shell, una vez firmado el contexto, conciliado claim y confirmado el batch. Los objetos del directorio de dependencias se publican create-only en el bucket de *Dataproc staging*, no en Silver. Los archivos `trusted_refs.json`, `quality_public_kms.pem`, `contrato_bronze_silver_v1.json`, `launch_refs.json` deben distribuirse como archivos autenticados para el driver. Sustituir `<...>` por valores de las referencias independientes de Quality. **No copiar/ejecutar el comando antes de la autorización expresa.**

```bash
# PROPUESTA PARA REVISIÓN QUALITY — NO EJECUTAR POR DANIELA
# Todos los campos y generaciones deben coincidir con launch_context firmado.
gcloud dataproc batches submit pyspark \
  "gs://bvl26-dataproc-staging-7f3a9c/bvl-phase6/releases/<COMMIT40>/fase6_tipo_cambio_batch.py" \
  --project=bvl-analytics-2026 \
  --region=southamerica-west1 \
  --version=2.2 \
  --batch="<BATCH_ID_FIRMADO>" \
  --service-account=sa-bvl-silver@bvl-analytics-2026.iam.gserviceaccount.com \
  --staging-bucket=bvl26-dataproc-staging-7f3a9c \
  --subnet="<SUBNET_URI_RATIFICADO>" \
  --py-files="gs://bvl26-dataproc-staging-7f3a9c/bvl-phase6/releases/<COMMIT40>/fase6_dependencias.zip" \
  --files="<GCS_CONTRATO>,<GCS_PEM_PUBLICO>,<GCS_REFS_F5>,<GCS_LAUNCH_REFS>" \
  --ttl=30m \
  --properties=spark.driver.cores=4,spark.executor.cores=4,spark.executor.instances=2,spark.dynamicAllocation.enabled=false,spark.sql.ansi.enabled=true \
  -- \
  --execute --ack-supervised \
  --contract=contrato_bronze_silver_v1.json \
  --quality-public-pem=quality_public_kms.pem \
  --trusted-refs=trusted_refs.json \
  --launch-refs=launch_refs.json \
  --processor-commit="<COMMIT40>" \
  --attempt-id="<ATTEMPT_ID_FIRMADO>" \
  --batch-id="<BATCH_ID_FIRMADO>"
```

Comprobación indispensable antes de lanzar: compatibilidad de los nombres efectivos de los cuatro archivos entregados por `--files` y permiso READ de `sa-bvl-silver` sobre artefactos de release. Runtime 2.2 puede variar en parches; validar su imagen real, cuota 12 vCPU, subred y `credentials.service_account_email` (la puerta F5 exige identidad verificable; no relajarla). No se puede garantizar la capacidad disponible solo con la cuota.

**Al fallar tras el claim:** no reintentar, no borrar ni modificar staging/claim; pedir conciliación Quality y nueva autorización si procede. Solo Quality puede publicar la versión oficial posterior.

## Artefactos a entregar

Tras aplicar el parche y publicar el nuevo commit, ejecutar **localmente**:

```bash
python3 scripts/processing/fase6_preparar_release.py
```

Genera en `~/Descargas/BVL_FASE6_RELEASE_<COMMIT12>/`:
- `fase6_tipo_cambio_batch.py` (exacto Git HEAD)
- `fase6_dependencias.zip` (módulos en raíz, reproducible)
- `solicitud_quality_NO_FIRMADA.json` (solicitud, NO autorización)

Enviar a Quality hash completo del commit, dos hashes y resultados de todas las pruebas; Quality verifica artefactos, concilia auth/claim, publica create-only y firma el contexto. La puerta Fase 5 no se modifica.
