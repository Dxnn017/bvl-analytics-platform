# Controles historicos complementarios BigQuery

Estado: CANDIDATO LOCAL NO AUTORIZADO.

## Igualdad de registros

Tres consultas generadas desde los contratos Gold.

Se comparan todas las columnas de cada producto.

Se comprueban:

- Claves invalidas.
- Claves duplicadas.
- Filas faltantes.
- Filas adicionales.
- Filas modificadas.
- Filas iguales.
- Conteo esperado.

La comparacion considera NULL mediante
IS DISTINCT FROM.

La igualdad solo es aplicable al staging
y destino de la misma version historica.

## Layout

Tres consultas INFORMATION_SCHEMA.COLUMNS.

Verifican:

- Columna de particionamiento.
- Campos de clustering.
- Orden del clustering.
- Columnas faltantes y adicionales.

No certifican granularidad de particion,
require_partition_filter ni todas las
opciones fisicas de tabla.

## Staging

BATCH_CANDIDATO es un identificador de ejemplo.

No identifica una carga real.

La ejecucion futura requiere una tabla historica
acreditada y vinculada a una version oficial.

## Alcance

Consultas de solo lectura.

No contienen DML ni DDL.

Validacion estatica exclusivamente.

No se ejecutaron consultas GoogleSQL.

No se verificaron tablas cloud.

No se modificaron componentes previos.

## Pendientes

Compatibilidad de aliases y tipos con
el conciliador historico existente.

Verificacion de metadata fisica completa.

Acreditacion de fuentes y autorizaciones.

Validacion efectiva BigQuery.
