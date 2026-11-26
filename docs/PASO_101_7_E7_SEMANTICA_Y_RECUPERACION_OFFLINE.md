# Paso 101.7-E.7 / E.7.8.6 — Compatibilidad semántica Quality (LOCAL)

**Autorización del cambio:** Quality permitió únicamente el ajuste puntual de compatibilidad en `pipeline-daniela`; no autorizó merge, GCS, KMS, IAM, transferencias ni ejecución. Este módulo permanece aislado del motor real de transferencia.

## Contrato oficial único para el piloto inicial

El módulo `scripts/ingestion/validar_semantica_recuperacion_e7_offline.py` genera exclusivamente **ocho campos de tipo `str`**:

1. `schema_version = BVL-OPERATION-FINGERPRINT-329C-v1`
2. `project_id = bvl-analytics-2026`
3. `run_id = bvl-bronze-inicial-20261125-001`
4. `operation_type = BRONZE_PILOT_TRANSFER`
5. `reservation_generation = 1795640338029794` (cadena, nunca entero)
6. `reservation_sha256 = 80cb92939b975659d32514fff3a64a30e02148434222f1391b1afd675913c232`
7. `pilot_json_sha256 = 82f384ab19231d89b4f4487ab7424eb24250943b4bdd40c85e88e8e08b5d16b7`
8. `canonical_destination_list_sha256 = acf864e31fc403faec3bf67ba13555474e4fa185c3475763781ca88fb0b1d923`

Serialización: `json.dumps(intent, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")`, sin salto de línea final. Resultado comprobado: **494 bytes**, SHA-256 **`87f93d0d1d84cdc4b346dee64e4c9a3a69f24e53ed94006d2f2fc0b7c6b56382`**.

La **huella NO se coloca como literal en el código de producción**: `semantic_sha256()` la calcula siempre a partir de los bytes canónicos. La huella esperada solo figura en las pruebas como aserción. El fichero local `fixtures/piloto_12_CANDIDATO_LOCAL.json` se lee sin modificar y su SHA-256 se compara con la referencia aprobada. La generación de reserva y sus hashes permanecen anclados a los valores documentales recibidos; **sin GCP no se afirma su verificación en Cloud**.

La URI exacta de reserva `gs://bvl26-control-7f3a9c/reservations/bronze/bvl-bronze-inicial-20261125-001.json` y `layer=bronze` siguen siendo controles de alcance obligatorios de `pilot_intent()` pero **no son parte de los ocho campos**.

Quedan excluidos UUID, timestamps, firmas, identificador de intento, `layer` y `reservation_uri` del objeto hasheado.

## Ruta inmutable aprobada del reclamo

`gs://bvl26-control-7f3a9c/authorizations/consumed/bronze/run_id=bvl-bronze-inicial-20261125-001/87f93d0d1d84cdc4b346dee64e4c9a3a69f24e53ed94006d2f2fc0b7c6b56382.json`

Esta ruta **no** contiene prefijo `sem-`. El módulo calcula la ruta, no crea objetos GCS. La implementación productiva futura deberá usar `if_generation_match=0`, bloquear ante `412` y ante resultados inciertos. No puede considerar `412` como una autorización de reutilización. El claim local `OfflineClaims` es solamente una simulación en memoria, no acredita atomicidad distribuida en GCS.

## Regresiones E.7.8.6

`python -m unittest discover -s tests -p 'test_semantica_recuperacion_e7_offline.py' -q`

- 17 pruebas preexistentes adaptadas al contrato aprobado.
- 10 pruebas nuevas: SHA-256 y 494 bytes exactos, lectura del piloto real en fixture, URI única, `reservation_generation` cadena, validación de `layer` y reserva externos, cambios de operación/esquema, rechazo de campos extra/tipos, estabilidad al cambiar UUID/fecha/firma, rechazo de fingerprint ajena.
- Simulación local de reclamaciones concurrentes y rechazo de reuso; sin Cloud.

## Recuperación permanece NO operativa

La función `recovery_claim_name()` es un **borrador local**: utiliza el mismo estilo de ruta sin `sem-`, pero **no está autorizada como contrato productivo de recuperación**. Sus booleanos de estado no prueban terminación, cierre de cargas reanudables, fencing, conciliación en GCS, control de secuencias, firmas oficiales ni autorización humana real. No se integra con el ejecutor productivo ni habilita transferencias.

**Bloqueo vigente:** `TRANSFERENCIAS_HABILITADAS = False` en `scripts/ingestion/transferir_hdfs_gcs.py`. No modificar, ni merge, ni aplicar permisos Cloud. Pendientes Quality/Infra: protocolo HTTP no reanudable, sesiones pendientes, IAM JIT y su prevención de bypass, confianza y recuperación segura.
