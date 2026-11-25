# Auditoría local de compatibilidad — QUALITY_APPROVED 329C

**Estado:** PROPUESTA, solo offline; no firma oficial, no publicación GCS, no transferencia.
**Referencia exacta:** `QUALITY_APPROVED_PAYLOAD_PROPUESTO_NO_FIRMAR.json`, esquema `329C-PROPUESTA-PAYLOAD-1`, SHA-256 `7a8c98a32e141b6e759755f137948bd33ffe192f696d5fc886334bf61aa6df6f`.

## Compatible con ajuste de código

La interfaz anterior de Quality esperaba `reservation_ref`, `scope: PILOT_12`, `draft_only` y/o atestaciones Ed25519. El nuevo payload usa objetos anidados `reservation`, `scope`, `authorization_policy`, `preflight_evidence`, `signature` y `semantic_exceptions`. **No es compatible directamente sin este adaptador**. Se añadió una auditoría offline para comprobar exactamente los bytes del payload, JSON sin claves duplicadas, esquema, tipos, reserva, 12 archivos, 23 052 390 bytes, identidad de lote y 12 destinos exactos comparados con las referencias locales de plan y piloto previamente fijadas por SHA-256. El digest de la lista de destinos se reproduce como SHA-256 de JSON compacto UTF-8 con lista de URIs ordenadas lexicográficamente.

La firma Quality será **DER ECDSA P-256/SHA-256 sobre bytes exactos**. Se comprueba con la clave pública Quality oficial anclada por huella DER/SPKI SHA-256. La prueba positiva de firma utiliza claves **efímeras de laboratorio**, NO KMS. La clave Orchestrator es diferente y no se debe aceptar para firmar Quality.

## Incompatibilidad aún no resuelta con Quality

La fuente proporcionada declara expresamente que el mecanismo de autorización humana externa **todavía no tiene autoridad de atestación ni servicio aprobados**. No se puede autenticar una plantilla JSON editable sin una cadena independiente de confianza. Faltan: (1) definir y anclar la clave pública de atestación humana/rol/identidad, (2) registro de aprobación y revocación, (3) anti-replay/vigencia y acción exacta, (4) firma oficial Quality sobre payload exacto, (5) documento TRANSFER_ALLOWED firmado independientemente por Orchestrator, y (6) autorización adicional de ejecución del Paso 101.7. También se exige verificar procedencia de GCS y generaciones. El `status` que aparece en el payload **no es una aprobación emitida**.

`verificar_humana_pendiente()` **rechaza siempre**, incluso un supuesto `decision=APPROVED`, hasta acordar la atestación. `puerta_transferencia()` también **rechaza siempre**. Ningún comando realiza llamadas a GCS, HDFS o KMS ni modifica IAM.

## Uso en el repositorio (sin Cloud)

```bash
PY="$HOME/.venvs/bvl-quality329c/bin/python"
"$PY" scripts/ingestion/verificar_quality_payload_329c_v1.py \
  --payload fixtures/QUALITY_APPROVED_PAYLOAD_PROPUESTO_NO_FIRMAR.json \
  --plan fixtures/plan_3599_CANDIDATO_LOCAL.json \
  --piloto fixtures/piloto_12_CANDIDATO_LOCAL.json
"$PY" -m unittest discover -s tests -p 'test_*.py' -q
```

**Interpretación:** `PAYLOAD_329C_COMPATIBLE_LOCAL_SIN_CLOUD` valida solamente estructura y referencias del candidato. `transfer_allowed` debe permanecer `false`, incluso cuando el resultado local sea `PASS`.

## Pendiente a acordar antes de Cloud

- Quality debe definir un canal oficial autenticado del registro humano externo, su clave verificadora, política de autorizadores, validez, revocación y consumo idempotente; **no puede depender solo del `payload_sha256`**.
- Fijar la semántica de `batch_identity_sha256` (se verifica contra referencia documental ratificada, no se recalcula aquí por falta de su algoritmo formal).
- Quality deberá aportar firma DER auténtica y prueba de publicación GCS (objeto, generación y cadena de confianza); después `TRANSFER_ALLOWED` independiente y autorización de ejecución. No se implementan accesos operativos hasta ratificar todo ello.
