# Quality 329C — Auditoría local de `TRANSFER_ALLOWED` V2

**Objeto auditado:** `TRANSFER_ALLOWED_V2_BORRADOR_NO_FIRMAR.json` (original, bytes conservados). SHA-256 `f3173e22d1e88f5518b5ef476d000a8a2a168df9d1a43d0a63ba285cd925b65e`.

## Alcance de la verificación

- Compara los 12 registros completos contra el piloto ratificado y cada operación contra el plan de 3,599 registros, con huellas SHA-256 *exactas* de los tres JSON originales.
- Valida `run_id`, bucket, rutas, 12 datasets, 12 destinos únicos, 23,052,390 bytes, precondición `ifGenerationMatch=0`, banderas `false` y prohibición de cambios a la política o archivos.
- Calcula SPKI DER/SHA-256 de las claves PEM públicas oficiales Quality y Orchestrator y contrasta contra las anclas locales de confianza. Valida la referencia KMS de Orchestrator.
- Nunca firma documentos, no genera aprobaciones, no consulta GCP ni habilita la subida.

## Incompatibilidades pendientes con el contrato operativo real

1. El nuevo `schema_version=329C-TRANSFER-DRAFT-2` es **distinto** del validador de `QUALITY_APPROVED` (`329C-PROPUESTA-PAYLOAD-1`). Se puede auditar como borrador local, **pero no aceptar como release real**.
2. `quality.official_signed_payload_sha256` es `null`: falta el objeto Quality oficialmente emitido, su firma DER, generación GCS y verificación de procedencia.
3. No hay firma DER del documento Orchestrator, generación GCS ni autorización humana/operativa emitida y autenticada. La presencia de `issuer_candidate` no demuestra emisor.
4. `pending` declara sin cerrar el identificador único, caducidad, consumo/antirreplay y protección del registro de confianza. Requiere un contrato de liberación distinto del borrador.
5. La autorización de 12 archivos NO habilita la migración de los 3,599, que necesitará una autorización independiente.
6. La verificación offline no comprueba permisos IAM en runtime ni auditoría de GCS; esas operaciones permanecen bloqueadas hasta autorización de Quality.

## Ejecutar en Debian (solo lectura local)

```bash
PY="$HOME/.venvs/bvl-quality329c/bin/python"
"$PY" scripts/ingestion/auditar_transfer_allowed_v2_offline.py
"$PY" -m unittest discover -s tests -p "test_auditar_transfer_allowed_v2_offline.py" -q
```

**Importante:** ejecutar desde `~/bvl-analytics-platform` con el plan de 3,599, piloto y claves PEM oficiales ya presentes. Las pruebas negativas son locales y nunca pasan el gate de Cloud.
