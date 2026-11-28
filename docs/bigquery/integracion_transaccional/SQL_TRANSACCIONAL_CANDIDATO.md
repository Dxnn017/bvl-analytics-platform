# SQL transaccional candidato BigQuery

Estado: LOCAL NO AUTORIZADO.

## Componentes

- Control transaccional v2.
- MERGE Mercado.
- MERGE Actividad SAB.
- MERGE Empresa trimestre.

Los archivos originales no se modifican.

## Semantica

Se comparan todas las columnas Gold.

Las modificaciones exclusivamente tecnicas
se distinguen de las modificaciones de negocio,
pero ambas actualizan el registro completo.

Los campos de control staging no forman parte
del contenido Gold almacenado en destino.

## Parametros requeridos

batch_id: STRING
run_id: STRING
source_version: STRING
previous_version: STRING
new_version: STRING
source_manifest_sha256: STRING
gold_contract_sha256: STRING
payload_sha256: STRING
staging_content_sha256: STRING
evidence_sha256: STRING
transfer_sha256: STRING
expected_rows: INT64

## Prerrequisitos

Carga historica oficial acreditada.

Inicializacion autorizada del registro de versiones
y su historial.

Staging fisico acreditado e inmutable.

Verificacion criptografica externa de manifests,
payload, archivos y autorizacion Quality.

Reserva exclusiva distribuida por producto.

Identidad operativa y permisos acreditados.

## Transaccion candidata

Control de identidad.
Resolucion de replay.
Continuidad de versiones.
Validacion de staging.
Clasificacion de cambios.
MERGE sin DELETE.
Conciliacion de claves afectadas.
Auditoria persistente.
Avance de version.
COMMIT o ROLLBACK.

## Limitaciones

La validacion realizada es estatica.

El SQL no ha sido analizado ni ejecutado
por el motor GoogleSQL.

Las metricas se derivan de la clasificacion
previa y requieren validacion cloud posterior.

La conciliacion verifica las claves afectadas,
no acredita la igualdad de todo el producto.

La semantica logica es candidata.
No constituye equivalencia certificada entre
SQLite y BigQuery.

El registro de auditoria SQL no reproduce
todavia la huella canonica del informe SQLite.

BigQuery no garantiza unicidad por declarar
claves primarias no aplicadas.

Se requiere exclusion distribuida externa.

Los hashes recibidos como parametros no
autentican por si solos los datos staging.

No se implementa avance de version con delta vacio.

Todos los scripts mantienen ASSERT FALSE.

No ejecutar fragmentos para eludir el bloqueo.
