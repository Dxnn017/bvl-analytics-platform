# BVL F6 — Preclaim IAM condicionado, candidato v1

Esta correccion **no autoriza la ejecucion** y no altera Fase 5, los procesadores
ni el alcance documental ratificado. Solo cambia el preclaim IAM del launcher.

- Bronze: `storage.objects.get` sobre PDF y TXT representantes con nombre real.
- Control: `get` para manifest Bronze, contexto documental y firma concretos.
- Dataproc staging: `get` sobre entrypoint y dependencias fijados.
- Silver: `storage.objects.list` en bucket y `create` para los 8 Parquet y
  dos objetos `_quality/` previstos dentro de `run_id`/`attempt_id`.
- Las consultas sobre objetos se hacen con `Blob.test_iam_permissions` (solo lectura).
- El listado efectivo del staging exacto sigue antes del claim; no se escriben
  probes, no se amplian permisos y no se crea/consume ningun claim.
- Si el API no permite consultar un objeto inexistente, devuelve error o permiso
  ambiguo, se bloquea antes del claim: Quality debe aportar un mecanismo
  verificado de evaluacion de ese objeto, sin sustituir el resultado por un
  booleano local.
- Los 1979 objetos Bronze se leen por generacion+SHA durante la unica ejecucion
  autorizada; muestras IAM no garantizan permiso sobre todos los objetos.
- Se conservan `if_generation_match=0`, `retry=None`, el bloqueo `eventos`,
  firmas y F5, sin cambios. Esta comprobacion no garantiza que IAM permanezca
  invariable entre preflight y uso.
