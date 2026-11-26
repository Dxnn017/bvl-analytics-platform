# BVL Analytics — Fase 6, integración local de fuentes restantes (V1)

**Naturaleza:** código candidato de transformación; NO es un release autorizado de producción ni un ejecutor Dataproc. Nunca consume `claim`, no escribe GCS, no publica `current.json` y no modifica la puerta original de Fase 5. Base de código compartida: `pipeline-daniela`, commit `387d55a9f8a8d6f26e9e74e991ef6da2d544a001`.

## Resultado concreto

El componente `fase6_integracion_10_local.py` reúne los 8 adaptadores generales aprobados en pruebas locales y los 2 especializados de Valores Inscritos y Hechos Valores Consulta dentro de **un único despachador local**, con alcance exacto por dataset, verificación estricta de bytes y inventario y validación de que la salida pertenece al mapeo ratificado.

- Los **11 procesos Bronze pendientes** equivalen a **12 productos Silver directos** porque `smv/valores_inscritos` produce dos salidas; el derivado `hechos_importancia/identidad_empresarial` es el **13.º producto restante**.
- El despachador tiene **10 procesos candidatos locales**; el 11.º (`smv/hechos_importancia`) está **deliberadamente bloqueado**.
- Valores Inscritos debe conciliar **289 XML + 1 JSON** con claves, multiplicidades y campos compartidos antes de transformar a `valores_inscritos` y `puente_ruc_nemonico`. **No** asumir un recuento fijo de registros basado en ejecuciones históricas.
- Hechos Valores Consulta exige **22 XML + 1 JSON**, verifica errores SMV 106/107 y conserva referencias XML serializadas; no se consulta SOAP.
- El derivado no consume Bronze directo ni staging: debe esperar **cinco manifests Silver oficiales** por versión, generación y SHA.

## Bloqueos materiales que no deben maquillarse

1. **Hechos de Importancia:** los scripts HDFS históricos requieren `hechos_eventos_2025.csv` y `hechos_documentos_2025.csv`, originalmente en `/datalake/bvl/metadata/smv/hechos_importancia/ingestion_date=2026-10-04/`. El inventario de 1,979 objetos Bronze oficiales no acredita aún estos CSV. Se necesitan referencias oficiales de objeto (URI, generación, SHA-256), política de consumo y ratificación Quality. Los 346 eventos históricos locales **no** se deben reconstruir o presentar como oficiales sin evidencia.
2. **Revisión contra datos reales:** todas las pruebas nuevas son sintéticas. Aunque los bytes de una copia local coincidan con hashes introducidos en memoria, ello no sustituye la firma KMS ni la comprobación contra manifests oficiales. Si los XML reales difieren, resolver el esquema con Quality; **no relajar validaciones para forzar una salida**.
3. **Integración operativa en Dataproc:** este módulo NO implementa el lanzador firmando 10 contextos. Falta agregar un ejecutor autorizado que obtenga un `GCSVerifiedPlan` real por intento mediante `check_gcs_signed_bundle`, valide lanzamiento F6 firmado y artefactos, compruebe origen de cada objeto, cree claim exclusivo *inmediatamente antes* del trabajo y escriba Parquet create-only, con evidencia y publicación posterior independiente Quality. Los módulos existentes `fase6_restantes_preflight.py` y `fase6_restantes_writer.py` son primitivas reutilizables, no un lanzador certificado.
4. **Cuota y batch:** no lanzar más de un batch 4+2×4 vCPU simultáneo con la cuota documentada de 12 vCPU. No reintentar automáticamente ni publicar desde el ejecutor.

## Archivos nuevos

- `scripts/processing/fase6_valores_conciliacion.py`: conciliación estricta del inventario 289 XML + un JSON de Valores Inscritos. No duplica requests ni descarta divergencias.
- `scripts/processing/fase6_integracion_10_local.py`: catálogo, snapshot de bytes pinneados, despacho Spark local de ocho fuentes generales, Valores y Hechos Valores. El resultado es un `LocalCandidate` con auditoría; no produce objetos cloud.
- `scripts/processing/fase6_planificar_release_local.py`: estado `PLAN_CANDIDATO_NO_AUTORIZA` en `~/Descargas/BVL_FASE6_INTEGRACION_10_ESTADO.json`, con hashes locales de módulos.
- `tests/test_fase6_integracion_10_puros.py`: casos de hash, manifest incompleto, landing prohibido, Valores 289+1, XML 106, duplicados, mapeo y bloqueo.
- `tests/test_fase6_integracion_10_spark_local.py`: pruebas Spark reales de EMBIG, las dos salidas de Valores y lineage de Hechos Valores. Ejecutar en Debian, no hay Spark instalado en el entorno de confección de este paquete.

## Instalación y validación en Debian (SIN CLOUD)

```bash
cd ~/bvl-analytics-platform || exit 1
unzip -n ~/Descargas/BVL_FASE6_INTEGRACION_10_V1.zip -d .
python3 -m unittest discover -s tests -p 'test_fase6_integracion_10_puros.py' -v
SPARK_LOCAL_IP=127.0.0.1 spark-submit --master 'local[1]' --conf spark.ui.enabled=false --conf spark.sql.ansi.enabled=true tests/test_fase6_integracion_10_spark_local.py
python3 scripts/processing/fase6_planificar_release_local.py --repo .
```

Criterio para continuar: todas las pruebas nuevas pasan, sin editar la puerta F5; no realizar push/commit automático antes de verificar la salida. Con este resultado podemos consolidar el paquete técnico, y solicitar a Lizeth **una sola resolución de los metadatos de Hechos junto con la validación del ejecutor y los artefactos**, no contactos repetitivos.

## Evidencia para la sustentación del profesor

Todavía falta un **nuevo lote** introducido desde un punto de entrada visible en la aplicación, con `run_id` nuevo y trazabilidad de Bronze original a Silver versión nueva, Gold y BigQuery/Streamlit. El piloto inicial de Tipo de Cambio (7,751 registros publicados) prueba el primer enlace Bronze→Silver; no equivale a la demostración completa de ingesta incremental y extremo a extremo.
