# BVL F6 — Atestación IAM Quality, **propuesta técnica no autorizada**

**Base de parche:** `6a38924f99cd00156ac8595d2c227decf1edabdb` (IAM condicionado del launcher documental).
**Producto:** `hechos_importancia/documental_restringido`.
**NO es autorización:** ni esta documentación, ni el diagnóstico `probe-7dc...`, ni el JSON de ejemplo habilitan la ejecución.

## Hallazgo y límite de la prueba real

La prueba de Quality acredita `storage.objects.create` para **un solo objeto diagnóstico**:

`gs://bvl26-silver-7f3a9c/staging/hechos_importancia/documental_restringido/_iam_diagnostic_only/probe-7dc9827c171d4fe28424e91343f6bbe4.json`

- creación HTTP 200, generación `1795841107238750`;
- SHA de contenido comprobado, lectura PASS y limpieza por generación HTTP 204;
- no cambió IAM, no consumió claims ni ejecutó Dataproc;
- SHA-256 del **informe diagnóstico** `f4ff57f304b5ec381b156220279238aad0fa6e55e6da9ed6e596a762278151d8`.

**Este único objeto NO acredita por sí mismo los diez destinos del intento.** Las consultas `testIamPermissions` de objetos futuros devuelven HTTP 400. No convertir 400/UNKNOWN ni un booleano en `ALLOW`.

## Atestación Quality requerida (fuera de este paquete)

La puerta exige un documento firmado Quality denominado
`BVL-F6-DOCUMENTAL-IAM-SCOPE-ATTESTATION-v1` y tres evidencias pinneadas en Control.
Las referencias (`--iam-attestation-refs`) deben señalar URIs, generación y SHA-256 **exactos** de `iam_scope_attestation.json` y `iam_scope_attestation.sig`, dentro de:

```
gs://bvl26-control-7f3a9c/authorizations/silver_documental/
  run_id=<RUN>/attempt_id=<ATTEMPT>/
```

En `.../iam_evidence/` deben existir **sin sobreescritura**:

1. `bucket_policy_snapshot.json` **o** `managed_folder_policy_snapshot.json`: política IAM real leída por Quality (versión 3, `etag`, `bindings`) y tipo/nombre del recurso bajo evaluación. Solo acepta condiciones CEL exactamente de la forma `resource.name.startsWith('projects/_/buckets/.../objects/.../')` o equivalente con comillas dobles, comprobada localmente para los diez nombres.
2. `custom_role_definition.json`: definición real del rol personalizado aplicado al principal; debe contener `storage.objects.create`. El `name` debe coincidir con el binding de la política.
3. `deny_review.json`: evaluación independiente de Quality de IAM Deny, herencia y bloqueos aplicables al principal y a los diez objetos exactos. Debe registrar `principal`, `permission`, `objects_sha256`, `outcome=NO_APPLICABLE_DENY_VERIFIED` y `evaluated_at_utc`. **No usar un `false` sin evidencias**: Quality debe conservar en el expediente las fuentes reales de su decisión.

Todos los elementos se fijan por URI, generación y SHA-256 en la atestación firmada, con un `run_id`, `attempt_id`, `batch_id`, commit, fingerprint, SHA del contexto firmado y prefijo staging idénticos a los de la ejecución. Calidad debe verificar también que el principal real de Dataproc será `sa-bvl-silver` y que no hay cambios IAM posteriores a la evidencia.

## Cobertura de destinos y caducidad

La puerta calcula **sin escribir** 10 nombres exactos bajo el intento firmado: ocho `parquet/part-00000...part-00007.snappy.parquet`, un `_quality/manifest_candidate_documental.json` y un `_quality/failure_candidate.json`. Se rechazan listas incompletas, nombres externos, colisiones, intentos distintos y prefijos que la política no cubra. El probe diagnóstico está explícitamente fuera del alcance concedido.

La atestación y las capturas de política/rol no pueden superar 10 minutos de antigüedad; tampoco pueden exceder la vigencia del contexto documental. Si Quality no puede obtener una política/rol/denegación suficientemente acreditados, se bloquea antes del claim.

## Controles conservados

- Conexión de fuentes F5, verificación KMS/P-256, pins de generación/SHA, y bloqueo de `hechos_importancia/eventos` sin modificaciones.
- Verificación IAM de lectura por objeto y `storage.objects.list` de Silver continúan independientes.
- Staging vacío, `validate_part_capacity`, inventario documental, vencimiento del F5 plan y claim exclusivo continúan ANTES de la escritura real.
- `if_generation_match=0`, `retry=None`, no overwrite/delete ni reintentos automáticos.
- `--execute` + `--ack-supervised` **siguen exigiendo autorización real de Quality**; se agrega `--iam-attestation-refs` a las referencias obligatorias.

## Limitaciones explícitas

Una atestación firmada acredita quién revisó y qué snapshot del IAM se evaluó; **no congela IAM ni puede garantizar que no se revoque un permiso entre el preflight y una escritura futura**. Por eso se mantiene control supervisado, validez breve, condición exacta, permisos IAM del runtime y manejo de fallo tras claim sin reintento. El primer lanzamiento exige revisión independiente de Quality de la firma, snapshot y lista completa de destinos.

**Este paquete de código solo contiene validadores y pruebas unitarias.** No contiene firmas Quality, roles IAM efectivos, políticas GCP oficiales ni permisos de ejecución. No realiza consultas, cambios ni escrituras GCP, ni crea un claim.
