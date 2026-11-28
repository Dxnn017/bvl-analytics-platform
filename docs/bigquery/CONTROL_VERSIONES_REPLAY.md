# Control de versiones y replay Gold

## Estado

Implementación candidata local.
Publicación productiva no autorizada.

## Componentes

- SQL transaccional de los tres productos Gold.
- Control de continuidad de versiones Gold.
- Invalidación de cobertura SAB entre versiones Silver.

## Comportamiento

- Batch nuevo: PROCESS.
- Batch completado con identidad idéntica: REPLAY_NOOP.
- Identidad incompatible: REJECT.
- Replay Gold posterior a una actualización: EJECUCION_REPETIDA.
- Replay histórico no retrocede versiones vigentes.
- Nueva ejecución exige continuidad correcta.
- Productos no afectados conservan sus versiones.
- Cobertura SAB antigua sin evidencia vigente: DESCONOCIDA.
- Indicadores monetarios conservados.

## Evidencias

- replay_transaccional_fase6.txt
- control_version_cobertura_sab.txt
- control_versiones_gold.txt

## Limitaciones

- El control de versiones Gold es un componente local
  todavía pendiente de integración en el orquestador.
- El historial recibido debe provenir de ejecuciones
  completadas y verificadas.
- El registro Silver existente no fue modificado.
- El replay SQL requiere validación real en BigQuery.
- La exclusión concurrente SQLite no acredita
  exclusión distribuida en GCP.
- Pendiente integrar preflight firmado, autorización Quality
  y conteos reales de operaciones transaccionales.
- Pendiente acreditar metadatos SAB por jornada.
- Los bloqueos SQL y DELETE permanecen activos.

Los manifiestos SHA-256 de entregas anteriores corresponden
a sus respectivos commits históricos. No se sobrescriben.

Sin modificaciones en GCP ni releases ratificados.
