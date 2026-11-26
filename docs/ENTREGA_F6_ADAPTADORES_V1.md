# BVL Analytics Platform — implementación inicial consolidada F6

**Estado: CÓDIGO CANDIDATO, NO AUTORIZA CLOUD.** Este paquete incorpora únicamente archivos nuevos. Se basa en el ZIP de 207 archivos proporcionado por Daniela y en el plan local ya probado de 11 procesos / 12 productos directos + 1 derivado. **No modifica** el commit Quality `387d55a9f8a8d6f2be9e74e991ef6da2d544a001`, el commit de puerta F5 `41266f0efba4477c30266e19ca3280838ff3d802`, contratos, manifests, GCS ni IAM.

## 1. Implementación candidata de fuentes (8 / 11)

| Fuente Bronze | Estado del adaptador puro | Transformación reutilizada y nuevo control |
|---|---|---|
| `bcrp/embig` | Candidato | Mes español y siglo, tolerancia `n.d.`, NULL, ANSI, conflictos de fecha, métricas separadas |
| `smv/cambios_patrimonio` | Candidato | Wrapper JSON `data`, mapeo columnas, ejercicio/RUC, duplicado exacto; sin overwrite |
| `smv/estado_resultados` | Candidato | Igual familia financiera, sin alterar colisiones de negocio |
| `smv/flujo_efectivo` | Candidato | Igual familia financiera |
| `smv/situacion_financiera` | Candidato | Igual familia financiera |
| `smv/principales_cuentas` | Candidato | Esquema histórico 16 campos, `clave_negocio` y `id_registro`, preserva colisiones en vez de borrarlas |
| `smv/cotizaciones` | Candidato | Fechas, clave fecha + valor, dedup exacta, bloqueo de registros conflictivos |
| `smv/montos_sab` | Candidato | 1221 objetos esperados, fecha negociación y código SAB, clave única, fecha y particiones |

**Estas 8 transformaciones no están certificadas contra el lote completo de GCS.** Las pruebas sintéticas de Spark deben pasar en Debian y después debe comprobarse la compatibilidad de los esquemas, hashes y conteos de los datos oficiales. La lectura controlada actual usa un máximo de 256 MiB y 200 mil registros; si una fuente supera límites, falla explícitamente y requerirá lectura por streaming aprobada, no ampliar memoria/roles a ciegas.

## 2. Fuentes que siguen bloqueadas (3 / 11)

- `smv/valores_inscritos` (290 objetos). No es equivalente al JSON HDFS histórico único: debe reconciliar originales y entregar dos productos bajo **una** autorización/claim; ambas salidas requieren Quality.
- `smv/hechos_valores_consulta` (22 XML y JSON consolidado). Crear normalizador offline con auditoría XML/JSON, errores 106/107, cobertura, deduplicación y lineage; **no usar el cliente SOAP de extracción** como procesador Silver.
- `smv/hechos_importancia` (1979 objetos). El script antiguo lee CSV auxiliares de `/datalake/bvl/metadata/` que NO están demostrados como parte de los objetos Bronze oficiales. Obtener procedencia y ratificación Quality antes de crear registros de eventos. Los TXT de 1 byte `0C` y XML de error no son filas útiles.

**Derivado (`hechos_importancia/identidad_empresarial`)**: bloqueado hasta la publicación y verificación por generación/hash de cinco versiones oficiales Silver (`eventos`, `principales_cuentas`, `puente_ruc_nemonico`, `cotizaciones`, `hechos_valores_consulta`). Se validan dependencias inmutables, nunca `current.json` sin resolver.

## 3. Arquitectura de este paquete

- `fase6_restantes_core.py`: alcance estricto para SINGLE_SOURCE, conteos de fuentes, nombre de producto, intento, prefijos `staging/`, dos salidas de Valores e identidad con cinco versiones.
- `fase6_restantes_transform.py`: funciones Spark puras, sin ejecutar Spark al importar. CSV/JSON verificados, control ANSI, nulos, duplicados y conflictos; no `overwrite`.
- `fase6_restantes_preflight.py`: lector GCS de generaciones/hashes exactos, **solo tras recibir un `GCSVerifiedPlan` real de la puerta original**. No consume claim. Límites conservadores.
- `fase6_restantes_writer.py`: serialización Parquet/Snappy controlada, escritor `if_generation_match=0`, `retry=None` y sin rename/update/delete. La primitiva de staging requiere plan real y generación de claim, pero **no constituye una autorización independiente ni un lanzador**. Su ejecutor de producción aún requiere revisión y firma Quality.
- `fase6_restantes_estado.py`: CLI **solo local**, genera inventario de adaptadores en Descargas. Nunca invoca GCS ni Spark.

## 4. Pruebas y criterios de cierre

Las pruebas offline verifican catálogo, scopes, un intento para dos salidas, excepciones Bronze, cinco dependencias oficiales, CSV/JSON e I/O create-only simulado. Las pruebas Spark comprueban EMBIG, fechas inválidas bajo ANSI, deduplicación, colisiones financieras y Montos SAB. **No confundir tests sintéticos con conformidad sobre GCS.**

Antes de certificar como release para Quality: comparar esquemas y métricas oficiales con las evidencias históricas; agregar pruebas por fuente (incluidas tres especializadas), simulación de manifest de 290/1979 objetos, canditatos con lineage y contexto firmado por intento, empaquetado 2.2.91 de entrada/dependencias y una corrida supervisada individual. Jamás consumir claim como prueba.

## 5. Demostración solicitada por el profesor

Se debe demostrar una **nueva ingesta real** y seguimiento de punta a punta: ingreso en aplicación o CLI autorizado -> archivo inmutable Bronze con `run_id` nuevo -> Quality Bronze -> Silver lote incremental firmado -> Quality publica versión Silver -> Gold integra y publica nueva versión -> consulta BigQuery y cambio visible en dashboard/Streamlit. Registrar URI, generation, SHA, run_id, attempt_id, batch_id, conteos, duración y resultado anterior/posterior. Repetir idéntico contenido en un intento **independiente autorizado** para probar idempotencia sin duplicados, nunca reutilizar claim.

El piloto tipo de cambio de 7751 registros es un baseline, **no es aún esa demostración E2E**. Coordinar el punto de entrada con Matthew y las claves/particiones impactadas con Piero cuando estén preparados para integrar, sin interrumpir el trabajo en marcha.

## 6. Restricciones de operación

- Un solo batch a la vez con la cuota documentada de 12 vCPU (driver 4 + 2×4), sujeto a aprobación Quality.
- No auto-retry, no promoción oficial desde el procesador, no `overwrite`, no permisos IAM adicionales.
- Las autorizaciones Silver del lote inicial y las firmas F5 existentes no cubren automáticamente las nuevas transformaciones ni los nuevos `run_id` de demostración.
- Cualquier incidencia tras consumir claim exige conciliación y una nueva autorización/attempt firmados.

**Próximo paso**: ejecutar la batería de pruebas en Debian sobre este paquete. No hacer commit/push si las pruebas fallan; después preparar único commit candidato y revisión Quality junto a las fuentes aún pendientes, no presentar este subpaquete como release final de los 13 productos.
