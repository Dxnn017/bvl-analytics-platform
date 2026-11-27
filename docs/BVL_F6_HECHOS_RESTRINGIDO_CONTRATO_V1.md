# BVL Analytics — Hechos de Importancia: Silver documental restringido, propuesta V1

**Estado: CANDIDATO PARA QUALITY, NO AUTORIZADO PARA PRODUCCIÓN.** La decisión escrita de Quality autoriza **preparar** el procesador, no ejecutarlo en Dataproc ni publicar datasets Silver, ni consumir claims.

## Motivación y alcance

- La existencia de **990 PDF** y **989 TXT** está documentada en la entrega Bronze oficial. El producto de salida preserva **una fila por cada PDF**, incluidos duplicados, más texto obtenido de TXT de origen verificado.
- El producto es **documental**, no representa 346 eventos oficiales ni 319 expedientes acreditados. La relación GUID ↔ expediente en los auxiliares HDFS no acredita el origen de metadatos del portal.
- Los 27 registros históricos sin PDF **no forman parte del dataset documental** y permanecen en cuarentena independiente. Los restantes vínculos histórico-documentales tampoco se incorporan al producto principal.
- Las fechas nominales del archivo son exclusivamente partes del nombre de archivo. No pueden interpretarse como fecha de presentación, acuerdo, publicación o fecha oficial del hecho.
- Cada identidad documental parte del nombre del PDF existente en el **manifest Bronze oficial fijado por SHA-256 y generación**. GUID del nombre ≠ ID oficial de expediente. No consulta URL SMV histórica ni usa metadatos HDFS de eventos.

## Orígenes autorizables para el candidato

La lectura de los objetos se ancla en el archivo **`entrega_bronze_silver_daniela.tar.gz` oficial**, validado por SHA-256 `ecc5c1d44fcf529c0efab5652dedb488a6149e5f208e4b94ec247847c3898487` y el manifest `smv__hechos_importancia.json` SHA-256 `d2c5f5d5a363e271a770497b4af6feedde454f999bc2fe7696a5c0ab0784eab0`, generación `1795680153321464`, run_id `bvl-bronze-inicial-20261125-001`.

El procesador inspecciona la ubicación `source_hdfs` que figura en el manifest **sin escribir en HDFS**. El modo `procesar-hdfs` compara cada byte leído con el tamaño y SHA-256 publicado en Bronze; esto acredita equivalencia por digest de bytes HDFS frente al **manifest oficialmente fijado**, pero **no es una nueva descarga de bytes desde GCS**. Quality deberá validar independientemente el procesamiento cloud antes de autorizar su ejecución productiva.

## Producto Silver propuesto

Identificador de producto sugerido: `smv/hechos_importancia/documentos_restringidos` (nombre y contrato sujetos a ratificación Quality). Granularidad: **una fila por PDF Bronze identificado por su URI + generación**, no por expediente.

| Campo | Tipo Spark | Regla de obtención |
|---|---|---|
| `guid_documento_archivo` | string | Extraído **únicamente del nombre** del PDF; etiqueta no acredita evento |
| `nombre_pdf` | string | Nombre PDF del manifest |
| `ruta_pdf_bronze` | string | URI de objeto oficial del manifest |
| `generacion_pdf_bronze` | string | Generación GCS fijada en manifest |
| `sha256_pdf` | string | SHA-256 del objeto fijado en manifest; byte HDFS contrastado |
| `tamano_pdf_bytes` | long | Tamaño fijado en manifest y verificado al leer HDFS |
| `fecha_nominal_archivo` | string ISO-8601 | Primeros 8 dígitos del nombre de PDF; **NO** fecha oficial del hecho |
| `fecha_nominal_no_es_fecha_oficial_hecho` | boolean | Constante `true`, etiqueta semántica obligatoria |
| `duplicado_por_sha256` | boolean | Marca **solo PDF no canónico** cuyo SHA-256 coincide con otro PDF |
| `nombre_pdf_canonico` | string | Nombre lexicográficamente menor dentro del grupo de igual SHA |
| `nombre_txt` | string nullable | Nombre de TXT oficial verificado o TXT canónico del PDF idéntico |
| `ruta_txt_bronze` | string nullable | URI oficial del TXT (no ruta HDFS histórica reconstruida) |
| `generacion_txt_bronze` | string nullable | Generación oficial del TXT |
| `sha256_txt` | string nullable | SHA del objeto TXT |
| `tamano_txt_bytes` | long nullable | Tamaño oficial TXT |
| `origen_texto` | string | `DIRECTO`, `CANONICO_POR_SHA_DOCUMENTAL` o `SIN_TXT` |
| `texto_extraido` | string | Contenido leído del TXT cuyos bytes pasan verificación; no se reextrae PDF |
| `caracteres_txt_verificados` | long | Longitud tras `strip` de texto verificado |
| `estado_calidad_txt_propuesto` | string | `SIN_TEXTO`, `CORTO_REVISAR`, `CON_TEXTO` o `SIN_TXT` |
| `verificacion` | string | Evidencia del método bytes HDFS vs manifest fijado |

**No se admiten** en el esquema principal: `numero_expediente`, `RUC`, `empresa`, `fecha_presentacion`, `fecha_acuerdo`, `fecha_consulta_portal`, `tipo_hecho`, `sector`, `descripcion`, `usar_silver_principal`, `coherencia_temporal` ni correspondencias históricas GUID–expediente.

## Conteos y reglas de calidad

- Entradas esperadas: **990 PDF + 989 TXT = 1,979 objetos**. La corrida se bloquea por conteos, SHA, generación, tamaño o rutas discrepantes.
- Se espera 989 hashes PDF únicos; 1 documento PDF no canónico marcado como duplicado exacto. **No se elimina el registro** y no se rellena información del portal.
- 989 TXT únicos: 989 PDF con TXT propio y 1 PDF duplicado con un TXT canónico previamente acreditado por SHA PDF; el texto de ambos queda marcado por su origen real.
- Todos los PDF y TXT se leen en HDFS y se comparan con los tamaños y SHA-256 del manifest oficial, sin modificar nada. No se emite éxito de lectura si falla alguno.
- Calidad de texto se deriva de los **bytes TXT**, no del campo histórico `estado_extraccion`. Umbral ilustrativo de 100 caracteres: `SIN_TEXTO` (0), `CORTO_REVISAR` (1–99), `CON_TEXTO` (>=100). **Quality debe ratificar el umbral**; el conteo de `SIN_TEXTO` no es extrapolable de los archivos históricos sin ejecutar este código.
- La salida Parquet es **local**, con compresión **Snappy**; se relee y concilia por conteo y esquema. No se publica en GCS/Dataproc ni se invoca ningún mecanismo protegido.

## Dependencias que deben ajustarse antes de Gold

1. `identidad_empresarial` históricamente hacía JOIN de eventos/expedientes a fuentes SMV. **No puede derivarse de este producto documental restringido**, pues no hay expediente, empresa ni RUC acreditados. Mantenerlo `BLOQUEADO_POR_FUENTE_EVENTOS_NO_ACREDITADA`, o solicitar nuevo contrato sobre una fuente oficial independiente con equivalencias validadas; **no inventar RUC por GUID**.
2. El Gold `impacto_hechos_valor` (u otros constructores de impacto por empresa/evento) **no puede inferir impacto** desde 990 documentos sin eventos, fecha oficial ni identidad empresarial. El resultado no es `SIN_IMPACTO`: debe ser `NO_CALCULABLE_FUENTE_NO_ACREDITADA` hasta contar con fuente externa independiente ratificada.
3. Los productos Gold no dependientes de eventos pueden seguir según sus contratos y versiones Silver oficiales. La publicación independiente de documentos no autoriza automáticamente activar todos los constructores Gold.
4. Dashboards/Streamlit deben exponer este producto solo como **inventario documental y calidad del texto**, no como noticias bursátiles verificadas, señales de compra/venta, atribución por empresa ni impacto en precio.
5. Quality debe revisar y ratificar modificaciones en DAG/puerta de dependencias y consumidores. **El paquete no altera dependencias ni código Gold existente**; entrega una propuesta para que los dueños implementen los cambios de manera coordinada.

## Restricciones

- No modifica Bronze, Fase 5, commits protegidos, variables de IAM, política KMS ni claims.
- No reutiliza los 27 eventos sin PDF para fabricar filas. Esos eventos solo permanecen documentados en los archivos de evidencia restringida ya aceptados por Quality.
- La rama y release finales deberán generarse desde `ea1400314f10ea4fd24f50ab1519abf87da0d03a` (baseline EMBIG), sin sobrescribir ni integrar automáticamente el hotfix financiero aún pendiente de ratificación.
- Para lanzar una ejecución productiva o crear una nueva versión Silver serán necesarias la ratificación formal de Quality, las autorizaciones específicas y la verificación cloud correspondiente.
