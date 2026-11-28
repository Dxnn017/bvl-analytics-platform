# Integracion del detector Gold con BigQuery

Estado: CANDIDATO LOCAL NO AUTORIZADO.

## Detector utilizado

src/gold/incremental/detectar_cambios_gold.py

Se reutiliza su implementacion original.

El detector identifica claves anteriores, nuevas
y afectadas mediante diferencias entre DataFrames.

## Adaptador

Valida los tipos y nombres contractuales.

Obtiene registros completos de la version vigente.

Clasifica:

- Inserciones.
- Modificaciones.
- Eliminaciones.

Las eliminaciones bloquean el lote completo.

El orden de las filas se establece mediante
las claves contractuales del producto.

No se modifican los indicadores Gold.

## Delta vacio

No genera staging ni archivos Parquet.

No registra un batch MERGE.

No autoriza avance automatico de version.

El tratamiento operativo del avance sin cambios
requiere una decision independiente.

## Integridad

El hash esperado es un parametro independiente.

Los fixtures calculan referencias locales.

Una ejecucion oficial debe obtener la huella
de una evidencia acreditada, no del mismo
contenido que intenta verificar.

## Limite local

Hasta 100000 filas de delta por producto.

Es un limite candidato de proteccion de memoria,
no una capacidad productiva acreditada.

La extraccion depende de snapshots estables.

No se acredita el procesamiento de datos
concurrentemente modificados.

## Diferencia de semantica pendiente

El detector original compara todas las columnas,
incluyendo fecha_ingesta.

El comparador SQLite excluye fecha_ingesta.

Los MERGE existentes incluyen fecha_ingesta.

El adaptador conserva los cambios detectados
sin descartar metadatos Gold.

La equivalencia de NOOP, UPDATE y auditoria
se resolvera en el paso 545.5.

## Validacion

Pruebas sinteticas con los tres contratos reales.

Spark local y transferencia PyArrow local.

Sin ejecucion de constructores Gold.

Sin ejecucion BigQuery ni GCP.

Sin acreditacion de fuentes oficiales.

Sin autorizacion de publicacion.
