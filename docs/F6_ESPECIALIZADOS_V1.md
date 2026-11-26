# BVL Analytics — Adaptadores especializados Fase 6 (candidatos V1)

Base: `pipeline-daniela`, commit de referencia `387d55a9f8a8d6f26e9e74e991ef6da2d544a001`; el paquete **no modifica** esa base ni la puerta operativa del commit F5 `41266f0`.

## Alcance de esta entrega

| Proceso | Implementación local | Restricción operativa |
|---|---|---|
| `smv/valores_inscritos` | `transform_valores_inscritos(df)` devuelve **dos** DataFrames (`valores_inscritos`, `puente_ruc_nemonico`) y métricas; prohíbe conflictos por nemónico o atributos y conversiones silenciosas | Recibir únicamente DataFrame construido desde JSON oficial verificado; dos staging outputs en un mismo intento firmado; Quality valida y publica ambas salidas como unidad |
| `smv/hechos_valores_consulta` | `reconcile(xml_items,json_bytes)` compara la multiplicidad de `(NemonicoValor,RazonSocial)` de **22 XML** y **1 JSON** preservados; conserva lineage por XML, distingue códigos SMV 106/107 | Los bytes deben llegar desde el manifest verificado con generación/hash exactos; no SOAP nuevo. El esquema de conciliación es candidato sujeto a datos reales; Quality debe revisar los XML con errores y la identidad del registro |
| `smv/hechos_importancia` | Requiere metadatos `hechos_eventos_2025.csv` y `hechos_documentos_2025.csv` identificados en código HDFS histórico; bloqueo explícito | Los CSV se ubicaban en `/datalake/bvl/metadata/` y **no constan ratificados como parte de los 1979 objetos Bronze oficiales**. No ejecutar hasta fijar fuente oficial, generaciones/hashes y autorización Quality de esos metadatos |
| `hechos_importancia/identidad_empresarial` | La función `validate_derived_dependencies()` ya existe en `fase6_restantes_core.py` (paquete anterior) | Esperar cinco manifests Silver oficiales y generaciones verificadas; NO usar Bronze directo como sustituto |

## Qué NO demuestra este paquete

- No son 13 productos procesados en GCS ni publicación Silver.
- No verifica por sí solo firmas KMS ni autoriza claims.
- No crea batch Dataproc ni modifica IAM.
- No sustituye `check_gcs_signed_bundle()` + `consume_once()` del commit aprobado.
- El procesador histórico de valores fijó 1008 filas y 831 pares **en Debian de 2026-10-05**: son referencia histórica, **no** un conteo supuesto para nuevo batch GCS.
- No da por resuelto el origen de los CSV de eventos. Para productos derivados se exige consumir `referencias_smv/hechos_valores_consulta` oficial, no el JSON Bronze directo como hacía el HDFS histórico.

## Calidad y pruebas

Pruebas offline sintéticas: XML mal formado/DTD, error 106/107, error desconocido, multiplicidades inconsistentes, claves JSON repetidas, historial de duplicados, y bloqueo de fuentes no acreditadas.

Pruebas Spark locales (Debian): dos salidas, deduplicación de pares, fechas inválidas con ANSI, valores no numéricos y conflictos de identidad.

Comandos, solo lectura cloud:

```bash
cd ~/bvl-analytics-platform || exit 1
unzip -n ~/Descargas/BVL_FASE6_ESPECIALIZADOS_V1.zip -d .
python3 -m unittest discover -s tests -p 'test_fase6_especializados_puros.py' -v
SPARK_LOCAL_IP=127.0.0.1 spark-submit --master 'local[1]' --conf spark.ui.enabled=false --conf spark.sql.ansi.enabled=true tests/test_fase6_especializados_spark_local.py
```

## Para terminar el release consolidado (paso siguiente)

1. Ejecutar las 5 pruebas Spark en Debian y resolver cualquier incompatibilidad observada.
2. Conciliar el resultado con registros reales oficiales, sin consumir autorización: comprobar esquema/números del manifest e identificar alcance de metadatos de Hechos con Quality.
3. Integrar con runner firmado por dataset, preflight GCS exacto, claim exclusivo, writer create-only del paquete anterior y dos salidas atómicas de Valores. **Debe revisarlo Quality antes de cualquier batch nuevo.**
4. Completar Hechos Eventos con metadatos oficiales; publicar los 12 productos directos restantes y, al final, el derivado con cinco versiones Silver.

La exigencia del docente de una ingesta nueva y visualización extremo a extremo sigue pendiente para Fase 9. No presentar el piloto inicial como prueba de esa ingesta nueva.
