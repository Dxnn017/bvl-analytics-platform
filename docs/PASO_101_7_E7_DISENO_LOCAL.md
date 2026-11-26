# BVL Analytics — Paso 101.7-E.7 (solo diseño y pruebas locales)

**Resultado de este paquete:** simulador y auditor de vinculación/registro de confianza, NO un módulo habilitado para producir transferencias. Sin llamadas GCS/HDFS/IAM/KMS. `transfer_allowed=false` siempre. **No reemplaza el auditor del Paso E ya aprobado con 41 pruebas.**

## 1. Vinculación criptográfica y alcance

El ejecutor final debe verificar en cadena, usando **bytes originales**, SHA-256 y firma DER individual de: (a) HUMAN_QUALITY_AUTHORIZATION, (b) HUMAN_TRANSFER_AUTHORIZATION, (c) HUMAN_EXECUTION_AUTHORIZATION; además de los bytes+firma KMS de QUALITY_APPROVED y TRANSFER_ALLOWED. Todos los objetos firmados deben estar anclados a referencias de **fuente autenticada independiente**, no a hashes declarados dentro del mismo JSON o recibidos junto con él por un canal no autenticado.

Anclas previas invariables: `run_id=bvl-bronze-inicial-20261125-001`; reserva URI canónica `gs://bvl26-control-7f3a9c/reservations/bronze/bvl-bronze-inicial-20261125-001.json`, generación `1795640338029794` y SHA-256 `80cb92939b975659d32514fff3a64a30e02148434222f1391b1afd675913c232`; 12 archivos, 23,052,390 bytes, SHA plan piloto `82f384ab19231d89b4f4487ab7424eb24250943b4bdd40c85e88e8e08b5d16b7`, lista de destinos `acf864e31fc403faec3bf67ba13555474e4fa185c3475763781ca88fb0b1d923`. El hash del plan completo `1397f92911b159e8df257fc82a7dd01593fce0d2e898e796a82efc77ae35d90e` **no** autoriza los otros 3,587 archivos.

Rechazar por defecto `null`, referencias cruzadas (p. ej. `TRANSFER` enlazado a documento Quality incorrecto), firmas ausentes, cambios de bytes, `run_id` distinto, destinos agregados, `full_batch_authorized=true`, sobrescrituras y cualquier documento DRAFT. **Las pruebas del paquete ejercitan enlaces de bytes/hash de documentos y firmas sintéticas y reutilizan las comprobaciones ECDSA del auditor previo; no verifican firmas oficiales ni sustituyen la verificación ECDSA P-256 que ya tiene el auditor anterior.**

## 2. Consumo atómico compartido (diseño propuesto)

Objeto de reclamo `gs://bvl26-control-7f3a9c/authorizations/consumed/bronze/run_id=<RUN_ID>/<EXECUTION_AUTHORIZATION_UUID>.json`.

- Solo después de verificar las tres firmas humanas, las dos firmas KMS, las generaciones y hashes de Control, la reserva y el alcance exacto, la identidad autorizada intenta **crear** el marcador con `if_generation_match=0` (REST `ifGenerationMatch=0`), nunca mediante lectura seguida de escritura.
- `201`/creación exclusiva confirmada: un solo ejecutor queda dueño **del reclamo**, aún sujeto a los bloqueos restantes y a autorización de ejecución efectiva. **No es prueba de que haya subido ningún archivo**.
- `412` (conflicto): **no transferir**; no tratar la colisión como aprobación. `403`, timeout, error ambiguo o escritura parcial: **bloquear** y reconciliar con Quality mediante lectura autorizada sin intentar recrear/borrar el marcador.
- Registro inmutable con UUID de HUMAN_EXECUTION_AUTHORIZATION, hashes exactos de las tres autorizaciones y firmas, `run_id`, alcance, identidad ejecutora comprobada, marca temporal GCS, generación y referencia de release. Quality audita objetos existentes por URI exacta. Prohibir `update`, `delete` y `list` al ejecutor.
- **Limitación crítica:** una creación exclusiva por `<EXECUTION_UUID>` impide repetir ese UUID, pero no impide por sí sola emitir otro UUID usando HUMAN_QUALITY_AUTHORIZATION o HUMAN_TRANSFER_AUTHORIZATION anteriores. El contrato final debe prohibir y comprobar ese reuso (p. ej. vínculo 1:1 de las tres autorizaciones a `execution_uuid` más registro de emisión/consumo custodiado y atómico). Si se exige atomicidad simultánea de tres marcadores GCS, GCS no ofrece transacciones multiobjeto: hace falta diseñar una única unidad autorizativa o usar un ledger transaccional. **Pendiente de aprobación Quality.**
- Reanudación tras caída: no “liberar” o borrar el marcador. Conciliar marcador, evidencia por archivo y generaciones existentes; continuar únicamente con una política firmada de reanudación o autorización nueva, sin sobrescribir; nunca tratar HTTP 412 como permiso.

**Simulación:** `os.open(..., O_CREAT|O_EXCL)` en almacenamiento local emula el “solo uno gana” de `ifGenerationMatch=0`; con 2 procesos, un éxito simulado y un `412` simulado. Es una prueba de algoritmo, **no prueba IAM ni semántica real de GCS**.

## 3. Registro de confianza humana

El registro `registro_confianza_local.json` emitido en la prueba de posesión es evidencia **no operativa**. Su `trust_record_candidate_sha256` dentro de un borrador no autentica el registro. Propuesta:

1. Generar una versión canónica del registro; mantener documento y clave PEM bajo custodia de Quality, con firma separada de **un custodio autorizador** y huellas del PEM y DER/SPKI **ancladas fuera de banda**. Capturar canal autenticado de confirmación, momento de consumo del desafío y evidencia asociada.
2. Publicar una versión **inmutable y auditable** con `if_generation_match=0` en una ruta Control definida por Quality (a autorizar por separado); la identidad ejecutora solo dispone de `get` sobre la versión exacta y lee hash/generación desde una liberación autenticada.
3. Rotación y revocación: registro/snapshot **nuevo**, firmado por custodio, con `not_before`, estado de revocación y ancla de última versión; el ejecutor rechaza versión vencida o revocada y debe fail-closed si no puede consultar un estado de revocación suficientemente fresco. La vieja huella `2c96...` permanece revocada.
4. Copia local solo lectura, respaldo, bitácora y control de cambios fuera del repositorio público. No subir el ZIP con firma del desafío, registro de confianza, referencias de videollamada ni identificadores personales al repo público.

El módulo offline solo puede comprobar el hash del registro si ese hash se entrega mediante un **canal confiable ajeno a los borradores**; no autentica por sí solo al custodio ni implementa publicación Cloud.

## 4. Identidades y permisos propuestos, pendientes de decisión

- **Ejecutor de migración:** `sa-bvl-ingestion@bvl-analytics-2026.iam.gserviceaccount.com` (identidad técnica existente), desde la **VM Debian Big Data con acceso HDFS** (`danielanieve@bigdata`, según terminal), ejecutando Python mediante impersonación efímera de una identidad base autorizada, sin llaves JSON. La identidad humana que opera el equipo y su derecho a impersonar **deben verificarse** y documentarse; el hostname no prueba autorización.
- **Consumidor atómico propuesto (nuevo, NO creado):** `sa-bvl-auth-consumer@bvl-analytics-2026.iam.gserviceaccount.com`. De recibir aprobación de Quality, solo permiso `storage.objects.create` en el prefijo `authorizations/consumed/bronze/` del bucket Control, sin `delete`, `update` ni `list`; Quality tendrá `get` para auditoría. El derecho del orquestador a impersonar este actor debe ser mínimo, revisado y ratificado. **No otorgar escritura en Control a `sa-bvl-ingestion` sin decisión explícita, pues contradice el principio de solo lectura vigente.**
- **Ingestion existente:** `storage.objects.get` limitado a reserva y aprobación en Control; `storage.objects.create/get` limitado a `landing/` Bronze; protección contra sobrescritura `if_generation_match=0`, sin publicación antes de permiso oficial.
- Si Quality rechaza una identidad nueva, deberá escoger y documentar **otra identidad separada** para consumir y el acoplamiento de autorización; no asumir privilegios de `sa-bvl-quality` o `sa-bvl-orchestrator`.

## 5. Test y punto de detención

`python3 -m unittest discover -s tests -p 'test_simular_consumo_101_7_e7_offline.py' -q` (33 pruebas nuevas). `python3 scripts/ingestion/simular_consumo_101_7_e7_offline.py --demo-offline`.

No ejecutar comandos de IAM, Cloud KMS, Cloud Storage, Dataproc ni HDFS. El script no tiene modo Cloud. Las autorizaciones oficiales, firma y anclas faltantes **no se sustituyen por valores de laboratorio**. Los 41 tests previos ya están aprobados; no se repiten aquí.
