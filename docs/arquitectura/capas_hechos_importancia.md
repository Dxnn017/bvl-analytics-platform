# Pipeline de Hechos de Importancia SMV

## Fuente

Superintendencia del Mercado de Valores (SMV).

La fuente documental está compuesta por documentos PDF publicados
como parte de los Hechos de Importancia.

## Bronze

### PDF originales

Ruta HDFS:

`/datalake/bvl/bronze/smv/hechos_importancia/pdf/`

- 990 archivos PDF.
- Aproximadamente 299.1 MB.
- Se conservan sin modificar.
- Replicación HDFS: 1 en el entorno académico de un solo DataNode.

### Texto extraído

Ruta HDFS:

`/datalake/bvl/bronze/smv/hechos_importancia/texto_extraido/`

- 989 archivos TXT.
- Generados mediante extracción de texto de los PDF.
- Existe un PDF duplicado exacto, por lo que no se genera contenido
  textual redundante para dicho archivo.

## Metadata técnica

Ruta HDFS:

`/datalake/bvl/metadata/smv/hechos_importancia/`

Contiene:

- manifiesto_pdfs_hechos_importancia.csv
- mapeo_pdf_expediente.csv
- hechos_eventos_2025.csv
- hechos_documentos_2025.csv
- resumen_estructuracion_hechos_2025.json

## Data Quality

Se identificaron:

- 338 eventos temporalmente coherentes.
- 8 eventos clasificados como históricos o regularizaciones.
- 428 documentos asociados a eventos coherentes.
- 562 documentos asociados a registros históricos o regularizaciones.
- 1 PDF duplicado exacto.
- 897 documentos con texto suficiente.
- 92 documentos con texto insuficiente.

Los documentos no se eliminan. Las reglas de calidad determinan su
participación en la capa analítica.

## Flujo

PDF no estructurado
→ Bronze
→ extracción de texto
→ estructuración de eventos y documentos
→ reglas de calidad
→ Spark
→ Silver Parquet
→ integración con información bursátil y financiera.

## Silver

La transformación de los documentos se realiza mediante PySpark y genera
datasets estructurados en formato Parquet con compresión Snappy.

Ruta HDFS:

`/datalake/bvl/silver/hechos_importancia/`

### Eventos

`/eventos/`

- 346 eventos o expedientes.
- 346 números de expediente únicos.
- 338 eventos clasificados como `COHERENTE`.
- 8 eventos clasificados como `HISTORICO_REGULARIZACION`.

### Documentos

`/documentos/`

- 990 documentos.
- 990 GUID únicos.
- 428 documentos asociados a eventos temporalmente coherentes.
- 562 documentos asociados a eventos históricos o regularizaciones.

### Contenido textual

`/contenido_textual/`

- 990 registros estructurados.
- 989 textos provenientes directamente del PDF correspondiente.
- 1 registro utiliza el texto del archivo canónico debido a un PDF duplicado exacto.
- 897 documentos clasificados como `OK`.
- 92 documentos clasificados como `SIN_TEXTO`.
- 1 documento clasificado como `DUPLICADO`.

### Particionamiento

Los datasets Silver se encuentran particionados mediante:

`coherencia_temporal`

con los valores:

- `COHERENTE`
- `HISTORICO_REGULARIZACION`

Los documentos históricos o de regularización no se eliminan.
Se conservan para trazabilidad, pero pueden excluirse del análisis principal
mediante la bandera de calidad.

## Transformación no estructurado a estructurado

El flujo implementado es:

PDF oficial SMV
→ HDFS Bronze
→ extracción de texto
→ TXT
→ asociación documento-expediente
→ normalización y tipado con PySpark
→ reglas de Data Quality
→ Parquet/Snappy en Silver

De esta manera, los documentos originalmente no estructurados se convierten
en información analítica estructurada manteniendo la trazabilidad hacia el
PDF original.
