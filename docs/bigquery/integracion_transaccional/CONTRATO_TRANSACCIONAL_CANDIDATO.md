# Contrato transaccional candidato BigQuery

Estado: LOCAL NO AUTORIZADO.

Base incremental:
05d2b78f168b771a2a08a6cedb1b5bc3f3dd41b8

## Semantica candidata

INSERT:
Clave inexistente.

UPDATE_BUSINESS:
Al menos una columna de negocio modificada.

UPDATE_TECHNICAL:
Solo columnas tecnicas modificadas.

NOOP:
Todas las columnas Gold son iguales.

Ambos tipos de UPDATE conservan el contenido
completo de la nueva fila Gold.

No se modifica el comparador original.

La clasificacion tecnica utiliza sus exclusiones
de metadatos existentes.

La politica requiere ratificacion operativa.

## Identidad de batch

- Producto.
- batch_id.
- run_id.
- source_version.
- Version Gold anterior.
- Version Gold nueva.
- SHA-256 del payload.
- SHA-256 del staging.
- SHA-256 de la evidencia.

## Continuidad

La version anterior debe coincidir con la
version aplicada en el registro persistente.

No se permite reutilizar una version aplicada.

El replay exacto se resuelve antes de comprobar
la continuidad de versiones.

No se permite avanzar versiones con delta vacio
mediante este motor.

## Transaccion

Se utiliza BEGIN IMMEDIATE de SQLite.

En una misma transaccion:

1. Comprobar identidad y continuidad.
2. Clasificar e incorporar registros.
3. Registrar batch completado.
4. Registrar auditoria.
5. Actualizar version aplicada.
6. Registrar historial de versiones.
7. Confirmar.

Ante cualquier error se ejecuta rollback.

## Metricas

inserted_rows
updated_rows
business_updates
technical_updates
unchanged_rows
deleted_rows
received_rows

updated_rows =
business_updates + technical_updates.

received_rows =
inserted_rows + updated_rows + unchanged_rows.

deleted_rows debe permanecer en cero.

## Alcance

Transaccion por producto, no multiproducto.

SQLite local no constituye una implementacion
de las transacciones distribuidas BigQuery.

La inicializacion de snapshots es exclusivamente
sintetica y no sustituye la carga historica oficial.

Las firmas Quality no se verifican en este motor.

No se ejecutan consultas GoogleSQL.

No se conceden permisos ni autorizaciones.

No se modifican los SQL originales.

## Pendientes

- Pruebas funcionales nuevas.
- Rollback real local ante fallos inyectados.
- Pruebas de exclusion entre conexiones SQLite.
- Conciliacion del estado final.
- Generacion SQL candidata equivalente.
- Validacion historica fila a fila.
- Verificacion de layout fisico BigQuery.
- Verificacion de aliases de consultas historicas.
- Autorizacion y fuentes oficiales.
- Reserva distribuida.
- Validacion efectiva del motor BigQuery.
