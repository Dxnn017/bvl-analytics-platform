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
