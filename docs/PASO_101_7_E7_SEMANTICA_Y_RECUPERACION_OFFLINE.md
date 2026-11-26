# Paso 101.7-E.7 — Prueba LOCAL de huella semántica y recuperación

**Naturaleza:** simulador complementario. No conecta GCS ni KMS, no cambia IAM ni ejecuta HDFS. No es un mecanismo de autorización operativa, no modifica `TRANSFERENCIAS_HABILITADAS` y no se conecta al motor de transferencia real.

## Diseño confirmado a nivel documental

- `sa-bvl-ingestion@bvl-analytics-2026.iam.gserviceaccount.com`: lector de Control, creador append-only en Bronze (`landing/`), mediante impersonación sin llaves JSON. Host con acceso HDFS: Debian BVL existente (ubicación de la futura ejecución sujeta a validación de credenciales/red). No puede reclamar autorizaciones en Control.
- `sa-bvl-auth-consumer@bvl-analytics-2026.iam.gserviceaccount.com`: cuenta propuesta, NO creada. Con `storage.objects.create` y `storage.objects.get` únicamente en `authorizations/consumed/bronze/`, sin `list`, `update`, `delete`, permiso KMS ni escritura en Bronze. La creación y bindings IAM requieren autorización de Quality. El ejecutor entrega a esta identidad el documento verificado; ella intenta registrar el marcador antes de iniciar transferencia.
- `sa-bvl-quality` firma la aprobación técnica con KMS; `sa-bvl-orchestrator` firma la habilitación independiente con KMS. Las identidades operativas no deben poder firmar como ellas.

## Huella semántica estable

Se forma exclusivamente con campos tipados del acto de ejecución: proyecto, capa, acción `INGEST_PILOT_12`, `run_id`, URI canónica de reserva, su generación y SHA-256, SHA-256 del piloto y SHA-256 de la lista canónica de los 12 destinos. Se fija una serialización JSON ordenada. **No** incluye UUID, hora de emisión, firma ni ID de un nuevo documento, por lo que reemitir una autorización para la misma operación colisiona en el mismo marcador `sem-<hash>.json`. Se debe ratificar esta estructura exacta con Quality y vincularla a la cadena verificada de `QUALITY_APPROVED`, `TRANSFER_ALLOWED` y los tres consentimientos humanos.

El marcador simulado sigue `authorizations/consumed/bronze/run_id=<RUN_ID>/sem-<SHA256>.json`. En Cloud real: `upload_from_string(..., if_generation_match=0)`; respuesta 412 implica `BLOCKED`; toda otra excepción también `BLOCKED`. **No** reconciliar 412 como una autorización ya válida para otro ejecutor. El consumidor debe registrar evidencia `generation`, hash, momento y procedencia del marcador.

## Recuperación

Si el reclamo queda consumido y ocurre un fallo, permanece consumido. No reintentar automáticamente ni crear otra autorización equivalente. Quality reconcilia por destino, generación, SHA-256 y tamaños; prohíbe sobrescribir objetos ya correctos. Un nuevo acto `RECOVERY` necesita nueva autorización humana, nueva cadena de firma, enlace a la huella original y conjunto **estrictamente menor o igual a los archivos pendientes**. El simulador exige declaraciones de parada del escritor anterior, conciliación y nueva autorización humana; en producción deben sustituirse por evidencias verificables, no booleanos entregados por el operador.

## BLOQUEADOR: fencing real del escritor anterior

El reclamo `ifGenerationMatch=0` solo garantiza exclusividad sobre **su objeto en Control**; NO detiene de forma atómica a una identidad que ya tiene permisos de escritura en Bronze. Se requiere un protocolo operacional ratificado con Matthew y Lizeth: proceso aislado por intento, terminación certificada, restricción/revocación de su capacidad de escritura y validación de que ya no tiene acceso efectivo antes de permitir recuperación. El lector del registro por sí solo no garantiza fencing porque existe carrera entre comprobar el estado y subir el siguiente objeto. La política IAM, la propagación de revocaciones y cualquier sesión activa requieren ensayo autorizado separado. No marcar E.7 operativo hasta probar este candado.

## Registro de confianza humana

Claves públicas PEM + registro con firma de **custodio independiente**, huella SHA-256 anclada fuera de banda, versión monotonamente creciente, almacenamiento inmutable append-only, registro de revocaciones autenticado e inmutable, y procedimiento de rotación separado. La identificación del custodio, clave pública raíz de confianza, almacenamiento y permisos son decisiones pendientes. Ni editar el JSON ni añadir una huella no autenticada cambia la confianza. No publicar evidencias personales sensibles ni claves privadas en GitHub.

## Limitaciones

Esta prueba verifica únicamente semántica de claves, contención local y lógica de rechazo. No verifica identidad real, permisos IAM aplicados, disponibilidad de servicios, firmas KMS oficiales, revocación efectiva de tokens ni bloqueo real de procesos antiguos. No constituye autorización para transferir 12 o 3599 objetos.
