# BVL Analytics — Release Silver restante y demostración E2E

**Fecha de referencia:** 26/11/2026. **Estado:** diseño e inventario de implementación, NO autorización operativa.

## 1. Evidencias que ya no repetimos

- Fases 1–5 cerradas; Bronze oficial con 12 datasets, 3,599 archivos y 393,623,640 bytes.
- Primer Silver publicado internamente por Quality: `macroeconomia/tipo_cambio` (7,751 registros / 357 Parquet verificados). Su manifest KMS y `current.json` oficial ya existen, según reporte Quality. Gold aún no autorizado.
- El commit `387d55a9f8a8d6f2be9e74e991ef6da2d544a001` y la puerta F5 permanecen inmutables.
- No repetir los 35 tests de Fase 5, 20 tests Quality del piloto o la migración de Bronze por rutina.

## 2. Lo que demuestra el ZIP del repositorio

El inventario tiene 11 fuentes Bronze pendientes, 12 productos directos, más el derivado `hechos_importancia/identidad_empresarial`: **13 productos por publicar**. El script adjunto registra rutas y hashes de procesadores existentes; no importa ni ejecuta ninguno.

Se encontró código histórico reutilizable, pero NO adaptado para ejecución segura sobre GCS para todas las fuentes:

- `bronze_to_silver_dataproc_candidato.py` permite reaprovechar EMBIG y cuatro fuentes financieras, pero el `main()` histórico está bloqueado y el writer usa `df.write`.
- `cotizaciones_to_silver.py`, `montos_sab_to_silver.py`, `principales_cuentas_to_silver.py` y `valores_smv_to_silver.py` tienen rutas HDFS, ejecución al importar y escrituras `overwrite` históricas. **No ejecutar esos archivos directamente en Cloud**.
- `smv/valores_inscritos`: **un único** intento SINGLE_SOURCE debe producir dos salidas, ambas aceptadas antes de promocionar ninguna.
- `smv/hechos_valores_consulta`: 22 XML originales + 1 JSON. `query_hechos_valores_smv.py` contiene adquisición SOAP y rutas locales, no es un transformador cloud seguro. Crear transformación offline de objetos oficiales, validar errores del servicio, deduplicación, conciliación XML/JSON y lineage; no repetir peticiones SOAP para construir Silver oficial.
- `smv/hechos_importancia`: el procesador histórico consume CSV auxiliares desde `/datalake/bvl/metadata/` además de TXT. **No asumir que esos CSV están en el manifest Bronze oficial**; Quality debe validar origen/versionado antes de usar. Excluir basura documentada (88 archivos TXT de un byte `0C`) según reglas de calidad, no inventar eventos.
- `hechos_identidad_gcp_candidato.py` permanece bloqueado; requiere precisamente **cinco Silver oficiales versionados**, incluyendo `referencias_smv/hechos_valores_consulta`. No sustituir esa dependencia por XML Bronze directo en el derivado.

Cada nuevo adaptador debe permitir: lectura de cada objeto oficial por generación + SHA, transformación validada, pruebas de esquema/granularidad, salida Arrow/Parquet Snappy create-only en staging exclusivo, manifest candidato con lineage, y lanzamiento sujeto a `check_gcs_signed_bundle()` + `consume_once()` con cliente Silver, más contexto firmado de ejecutable. Quality publica y actualiza `current.json`. Una autorización por intento; sin retry automático tras consumir claim.

## 3. Orden recomendado de implementación y ejecución supervisada

Primero EMBIG y cuatro fuentes financieras (reutilizar funciones ya existentes), después principales cuentas y cotizaciones, luego Montos SAB, Valores Inscritos (dos salidas), Hechos Valores Consulta y Hechos de Importancia. **El derivado se ejecuta al final**, solo tras cinco publicaciones oficiales. Este orden es una propuesta de ejecución, no un lote autorizado. Con 4 vCPU driver + 2×4 vCPU workers, planear inicialmente **un batch a la vez** dentro de la cuota histórica 12 vCPU. Quality puede modificar orden y recurso con evidencias.

Para evitar repetir a mano la administración por dataset, crear un orquestador de control que tome una **lista de autorizaciones firmadas y vigentes por intento**, inmovilice los hashes de ejecutables, envíe batch solo bajo aprobación supervisora y concilie individualmente cada resultado. Ninguna plantilla o mapeo local habilita a saltar la firma o el claim. Eventarc/Workflows aún no están desplegados integralmente según evidencia anterior.

## 4. Exigencia nueva del profesor — demostración extremo a extremo

El profesor requiere **una ingesta realmente nueva durante la demostración** y ver su efecto hasta el consumo final. No es suficiente presentar 3,599 archivos históricos, enseñar pantallas o ejecutar Bronze→Silver de la carga inicial.

**Escenario sugerido para acordar con equipo:** un nuevo registro de tipo de cambio BCRP o una corrección/inserción con clave `fecha` verificable, usando una **nueva fuente autorizada**. Antes del evento, se toma captura/consulta de la fecha y KPI en la aplicación; se conserva el identificador de la corrida. Durante la presentación, se introduce el dato mediante el punto de entrada seleccionado; la ingesta registra lote nuevo con `run_id` independiente del `bvl-bronze-inicial-20261125-001` y almacena original en Bronze landing. Tras Quality Bronze y autorizaciones Silver, Silver procesa incremento seguro y publica versión oficial nueva. Gold detecta partición/clave afectada, recalcula lo que corresponda y Quality publica. BigQuery/Looker/Streamlit refrescan usando solo Gold oficial. Se vuelve a consultar la misma clave/indicador y se comprueba el cambio. Finalmente, se repite el mismo evento para demostrar idempotencia/sin duplicación (si Quality permite el escenario de replay). Alternativa segura para demo: ejecutar en ambiente de prueba con contratos y datos independientes, pero debe ser **una cadena real**, no interfaz simulada presentada como producción.

**Evidencia mínima a mostrar en secuencia:**

1. **Ingreso:** origen exacto, hora, archivo o API, hash y `run_id` nuevo.
2. **Bronze:** URI, generación, SHA y manifest oficial del nuevo lote.
3. **Silver:** intento firmado, claim, job ID SUCCEEDED, controles de calidad, manifest, versión y `current.json` actualizado si hubo cambio.
4. **Gold:** manifest Silver–Gold aprobado, dependencias/versiones fijadas, claves/particiones recalculadas, versión Gold oficial.
5. **Consumo:** consulta BigQuery/SQL y filtro/indicador visible en dashboard/Streamlit; antes/después sobre la misma clave.
6. **Fiabilidad:** prueba de no cambio/idempotencia y tiempos de cada capa. Si hay un rechazo, explicar y demostrar el bloqueo seguro, sin ocultar error.

**Dueños:** Daniela (ingreso, Bronze/Silver e incremental), Lizeth (Quality, firmas, publicaciones y operación cloud), Piero (Gold, incremental y BigQuery), Matthew (ML y aplicación/punto de entrada, si se acuerda). Diseñar el **punto de entrada** ahora con Matthew, porque la demo requiere interactuar con él. Puede ser carga desde Streamlit, CLI supervisado o interfaz de ingesta, pero debe tener su **flujo real** en GCP.

**Riesgo de planificación:** el piloto Silver publicado para Tipo Cambio procede de una carga inicial; **no demuestra la ingesta nueva extremo a extremo**. No dar por terminada la Fase 9 antes de probar este recorrido en la nube.

## 5. Puertas de entrega Quality

- Entrega 1: mapeo/fuentes reconciliados con anexo firmado, adaptadores con tests y release reproducible.
- Entrega 2: firma/claim por intento, batches supervisados, Quality staging, versiones y manifests oficiales. Emitir única entrega consolidada del código, pero no confundirla con una autorización única para todas las fuentes.
- Entrega 3: dependencias para Piero, integración Gold/consumo y demo incremental.

**No enviar todavía el plan como autorización.** Es un inventario técnico para programar el release. Los hallazgos críticos son: transformadores HDFS históricos con overwrite, consulta XML/JSON, metadata de Hechos y el derivado de cinco dependencias.
