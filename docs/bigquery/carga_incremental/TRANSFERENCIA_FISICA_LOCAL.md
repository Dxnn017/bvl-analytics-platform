# Transferencia fisica incremental BigQuery

Estado: CANDIDATO LOCAL NO AUTORIZADO.

## Formato

Parquet local mediante PyArrow.

Las columnas fisicas se derivan del contrato
Gold y del DDL staging existente.

Los registros Gold pueden ser nullable en staging.
Las seis columnas de control son obligatorias.

## Conversiones

- DATE: date32.
- STRING: string.
- BOOL: boolean.
- INT64: int32 o int64 segun tipo Spark original.
- FLOAT64: float64.
- NUMERIC: decimal128 con escala contractual.

Los tres decimales SAB de precision 34
se transfieren con precision 31 y escala 2.

No se autorizan redondeos.

## Integridad

Antes de escribir se valida:

- Huella de evidencia.
- Huella de payload.
- Huella de staging.
- Contratos vinculados.
- Correspondencia entre payload y staging.
- Identidad y metadatos del batch.
- Bloqueo de DELETE.
- Ausencia de autorizacion operativa.

Despues de escribir se comprueba:

- Esquema fisico.
- Conteo.
- Reconstruccion de registros.
- Igualdad del contenido.
- SHA-256 del archivo.

No se sobrescriben archivos existentes.

## Limitaciones

El procesamiento actual utiliza listas en memoria.

La prueba es local y sintetica.

No se acredita todavia el lector de BigQuery,
la inmutabilidad GCS ni la autenticidad
de manifests y autorizaciones oficiales.

La integracion con el detector real Gold
permanece pendiente.

Un hash de integridad no sustituye una firma.

No se autoriza ninguna carga cloud.
