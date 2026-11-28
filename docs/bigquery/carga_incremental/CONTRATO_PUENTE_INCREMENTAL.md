# Puente incremental Gold a BigQuery

Estado: CANDIDATO LOCAL NO AUTORIZADO.

## Entrada

Lista de registros Gold completos con la columna
operation igual a UPSERT.

El consumidor debe obtener los registros de la
nueva version Gold y acreditar el alcance real
del cambio.

No se acepta un registro parcial como UPSERT.

No se permite DELETE.

## Representacion candidata

- DATE: texto ISO.
- STRING: texto sin sustituciones.
- BOOL: booleano.
- INT64: entero dentro de rango.
- FLOAT64: numero finito.
- NUMERIC: texto decimal exacto de escala dos.
- NULL: null, cuando el contrato lo permita.

El formato JSON es intermedio y local.

No constituye todavia un Parquet de transferencia
ni un payload directamente autorizado para BigQuery.

## Hash

Se reutiliza calcular_sha256 del proyecto.

El payload contiene las columnas Gold normalizadas
y operation.

El orden de filas forma parte de la identidad.

Las cinco columnas de control restantes no forman
parte de esta huella, para evitar circularidad.

Se calcula una segunda huella sobre staging completo.

La delimitacion del payload y su normalizacion son
una propuesta de integracion pendiente de ratificacion.

El hash declarado debe proceder posteriormente de
una evidencia acreditada e independiente.

## Versiones

source_version conserva la referencia que corresponda
al contrato incremental existente.

previous_gold_version y new_gold_version identifican
las versiones Gold involucradas.

current_gold_version representa la version aplicada
que debe acreditarse antes de una ejecucion real.

No confundir la version Silver de origen con la
version Gold publicada.

## Staging

- Mercado: 47 columnas.
- SAB: 47 columnas.
- Empresa: 64 columnas.

Se conserva el orden definido por los DDL existentes.

## Cobertura SAB

Conservar los datos de cobertura sin inventar valores.

Validar las restricciones de cobertura contenidas
en las consultas MERGE existentes.

No convertir DESCONOCIDA en COMPLETA.

## Bloqueos

- Batch vacio.
- DELETE.
- Claves invalidas o duplicadas.
- Tipos incompatibles.
- Campos obligatorios nulos.
- Particiones inconsistentes.
- Desbordamiento decimal.
- Hash incompatible.
- Continuidad de versiones incompatible.
- Ejecucion sin autorizacion.

## Pendientes

- Validacion funcional local del nuevo puente.
- Integracion con el detector real de cambios Gold.
- Acreditacion de manifests y versiones.
- Transferencia fisica de staging.
- Validacion de igualdad de contenido.
- Reserva exclusiva distribuida.
- MERGE real y auditoria.
- Replay y continuidad operativa.

No se ejecutaron ni modificaron los SQL existentes.
