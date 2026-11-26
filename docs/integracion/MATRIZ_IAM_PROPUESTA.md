# Matriz de identidad/permisos — para validación Lizeth y Matthew

**No son cambios IAM ya realizados.** Propuesta de permisos mínimos exactos; la concesión concreta debe realizarla infraestructura y verificar condiciones por recurso/prefijo. No usar `roles/editor`, `roles/owner` ni `storage.objectAdmin` para el ejecutor Silver.

| Identidad | Recurso | Permisos necesarios / controles |
|---|---|---|
| **Quality firmante** (identidad definida por Lizeth) | Clave KMS `quality-approved-signer`, versión autorizada | `cloudkms.cryptoKeyVersions.useToSign`. El rol `roles/cloudkms.signer` o rol personalizado restringido a la clave; la clave **privada nunca** se exporta. |
| **Quality publicadora Control** | Prefijos de Control: anexos, actas, autorizaciones y firmas | `storage.objects.create` para objetos inmutables; `storage.objects.get` para leer. No permitir sobrescribir ni eliminar artefactos, incluso con distinto principal; gestionar controles de inmutabilidad/retención según política. |
| **`sa-bvl-silver@bvl-analytics-2026.iam.gserviceaccount.com`** | Bronze publicado | `storage.objects.get`; `storage.objects.list` solamente si el lector/servicio lo requiere. **Sin create, delete, update**. |
| **Misma SA Silver** | Control manifests Bronze/Silver, anexo, acta, autorizaciones y firmas | `storage.objects.get`; acceso únicamente a objetos autorizados. `storage.objects.list` opcional si hay operaciones que realmente lo necesitan. |
| **Misma SA Silver** | Control `authorizations/consumed/silver/` | `storage.objects.create` **exclusivamente en ese prefijo**; precondición `if_generation_match=0`; **sin delete/update**. Proteger el registro contra borrado por otras identidades no autorizadas. |
| **Misma SA Silver** | Bucket Silver `/staging/<producto>/run_id=.../attempt_id=.../` | `storage.objects.create` para salidas autorizadas; `storage.objects.get`/`list` para verificar staging. **Sin overwrite/delete ni escritura a Silver oficial**. |
| **Orquestador autorizado (Workflows)** | Dataproc Serverless | `dataproc.batches.create`, `dataproc.batches.get` (y list si se usa); `iam.serviceAccounts.actAs` solo sobre la SA de Silver. La facultad de enviar batch no sustituye validación de autorización. |
| **Cuenta de servicio runtime Dataproc 2.2** | Proyecto/servicio de procesamiento | `roles/dataproc.worker` según la configuración/runtime 2.2 y los permisos de datos anteriores; verificar APIs, subred, conectividad, cuotas, logs. |
| **Quality publicadora Silver** | Silver oficial, evidencias de publicación | Roles separados de la SA de procesamiento para aprobar y publicar. La puerta de `BUILD_SILVER_STAGING` **no** concede estos permisos. |
| **Piero Analytics** | Silver oficial/Gold/BigQuery | Acceso de solo lectura a productos Silver aprobados y permisos de ejecución Gold/BigQuery asignados por infraestructura; nunca escribir Bronze ni emitir autorizaciones Quality. |
| **Matthew portal/MLlib** | Portal, batch submission y datos Gold autorizados | Permisos mínimos propios de Workflows/API y consulta; sin acceso directo a claves KMS Quality ni publicación Silver. |

**Condición adicional:** Cloud IAM no es un sistema de ACL por archivo arbitrario. Validar la viabilidad de restringir prefijos mediante políticas/condiciones y pruebas IAM efectivas. Si las condiciones IAM no ofrecen suficiente aislamiento, separar buckets/identidades o usar un servicio de control privilegiado que escriba solamente los claims. La precondición GCS sola **no** reemplaza IAM.

**Fuentes oficiales para verificar roles:**
- https://docs.cloud.google.com/storage/docs/access-control/iam-roles
- https://docs.cloud.google.com/storage/docs/access-control/iam-permissions
- https://docs.cloud.google.com/managed-spark/docs/concepts/iam-serverless
- https://docs.cloud.google.com/kms/docs/reference/permissions-and-roles
