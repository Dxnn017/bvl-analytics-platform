# BVL — Entrega técnica Fase 5 / actividad 14.5

**Estado:** CANDIDATO PARA REVISIÓN QUALITY. **NO ES SILVER_AUTORIZADO.**
**Proyecto:** `bvl-analytics-2026` · región `southamerica-west1` · run `bvl-bronze-inicial-20261125-001`.
**Contrato base inmutable:** `BVL-BRONZE-SILVER-CONTRACT-v1`, SHA-256 `6f33f3a88e04b38a5e75863d3ccc67005231f05d474b9916d34fd7030edef20a`.
**Rama:** `pipeline-daniela`. Últimos commits ya reportados: `6119487`, `201963a`, `8c603e7`. El commit de este módulo será posterior.

## 1. Mapeo definitivo **propuesto** para ratificación firmada

El contrato base ya contiene las propuestas para 10 fuentes. El anexo debe ratificar expresamente los 12 procesamientos, incluyendo los dos faltantes. El total de datos Bronze es 12 datasets, 3,599 archivos y 393,623,640 bytes.

| Dataset Bronze | Producto(s) Silver | Alcance |
|---|---|---|
| `bcrp/embig` | `macroeconomia/embig` | SINGLE_SOURCE |
| `bcrp/tipo_cambio` | `macroeconomia/tipo_cambio` | SINGLE_SOURCE |
| `smv/cambios_patrimonio` | `finanzas_empresariales/cambios_patrimonio` | SINGLE_SOURCE |
| `smv/cotizaciones` | `mercado_diario/cotizaciones` | SINGLE_SOURCE |
| `smv/estado_resultados` | `finanzas_empresariales/estado_resultados` | SINGLE_SOURCE |
| `smv/flujo_efectivo` | `finanzas_empresariales/flujo_efectivo` | SINGLE_SOURCE |
| `smv/hechos_importancia` | `hechos_importancia/eventos` | SINGLE_SOURCE |
| `smv/hechos_valores_consulta` | `referencias_smv/hechos_valores_consulta` | SINGLE_SOURCE |
| `smv/montos_sab` | `mercado_diario/montos_sab` | SINGLE_SOURCE |
| `smv/principales_cuentas` | `finanzas_empresariales/principales_cuentas` | SINGLE_SOURCE |
| `smv/situacion_financiera` | `finanzas_empresariales/situacion_financiera` | SINGLE_SOURCE |
| `smv/valores_inscritos` | **`referencias_smv/valores_inscritos` y `referencias_smv/puente_ruc_nemonico`** | SINGLE_SOURCE, `silver_outputs` de dos entradas, éxito conjunto |

Producto adicional (no es otra fuente Bronze ni un Silver directo): `hechos_importancia/identidad_empresarial` — `MULTI_SOURCE`, depende de versiones Silver **oficiales** de:

1. `hechos_importancia/eventos`
2. `finanzas_empresariales/principales_cuentas`
3. `referencias_smv/puente_ruc_nemonico`
4. `mercado_diario/cotizaciones`
5. `referencias_smv/hechos_valores_consulta`

**Resultado:** 12 procesamientos fuente; 13 productos directos (uno doble); 1 producto derivado MULTI_SOURCE; **14 productos distintos**. El puente se genera del mismo Bronze `smv/valores_inscritos` que su tabla principal, de acuerdo con el procesador existente. No se presenta como un proceso MULTI_SOURCE adicional.

## 2. Documentos firmados y cadena de confianza

No se modifica `phase_5_closed:false` del contrato base. La puerta valida de forma acumulativa:

- Contrato base **bytes exactos** y SHA-256 fijado externamente.
- Anexo operativo firmado, **esquema propuesto** `BVL-SILVER-OPERATIONAL-ANNEX-v1`. Debe contener los 12 procesamientos, `silver_products`, identidad derivada y las prohibiciones. El nombre final del esquema y las URIs del anexo/acta requieren que Quality los emita / ratifique como artefactos oficiales.
- Acta firmada `BVL-PHASE5-CLOSURE-v1`: `phase_5_closed:true`, aprobación Quality y SHA del anexo. Esta acta no reemplaza la autorización específica.
- Autorización firmada `BVL-SILVER-AUTH-v1` con `mode:SINGLE_SOURCE|MULTI_SOURCE`, `silver_outputs`, referencias fijas, cuenta de servicio, `attempt_id`, ventana UTC <=4 h, vínculo SHA al anexo/acta y `BUILD_SILVER_STAGING` únicamente.

Firmas: **ECDSA P-256 SHA-256, DER**, JSON UTF-8 canónico ordenado (`sort_keys=true`, `separators=(',', ':')`, `ensure_ascii=false`), sin BOM, NaN ni claves duplicadas. Huella SHA-256 SPKI de confianza pinneada independiente de los documentos: `5c70c40088a680cfd7ced1859c4d6b335652d96991cdcae4268819fb4f90f5ce`. No se adjuntan ni solicitan claves privadas.

Para leer firmas/artefactos GCS se exige **un contexto de lanzamiento Quality confiable independiente** con URI, generación y SHA-256 de cada artefacto y firma. Nunca aceptar autodeclaraciones de hashes/generaciones de un JSON sin verificar.

## 3. Rutas Quality ratificadas

**Simple** (dataset con `/` en la ruta):

```
gs://bvl26-control-7f3a9c/authorizations/silver/<dataset>/run_id=<run_id>/attempt_id=<attempt_id>/authorization.json
```

**Múltiple**:

```
gs://bvl26-control-7f3a9c/authorizations/silver/derived/<producto>/run_id=<run_id>/attempt_id=<attempt_id>/authorization.json
```

La firma es `authorization.sig` en el mismo directorio.

**Registro atómico de consumo** (solo tras comprobar todo):

```
gs://bvl26-control-7f3a9c/authorizations/consumed/silver/run_id=<run_id>/<fingerprint>.json
```

**Staging independiente para cada salida**:

```
gs://bvl26-silver-7f3a9c/staging/<producto>/run_id=<run_id>/attempt_id=<attempt_id>/
```

El fingerprint es SHA-256 de un JSON canónico del **alcance semántico**, incluyendo proyecto, región, contrato/anexo/acta, `mode`, dataset/producto, run, `attempt_id`, operación, identidad, dependencias con URI+generación+SHA+versión, y `silver_outputs`, ordenados por identificador. **No** depende de la fecha de vencimiento ni de la firma: reemitir otra autorización para el **mismo intento y alcance** no permite otro consumo. Quality debe garantizar también unicidad de `attempt_id` a nivel de emisión.

El consumo se implementa usando `GCSExclusiveClaimStore.create_exclusive(..., if_generation_match=0, retry=None)`; un segundo intento de creación del mismo objeto falla. No borrar ni reescribir el registro de consumo.

## 4. Recovery y éxito conjunto

- En caso de fallo: conservar staging parcial y auditoría; la autorización queda consumida y **no se reutiliza**.
- Para recuperar: conciliación firmada o registrada por Quality, **nuevo `attempt_id`**, nueva autorización firmada y **nuevo staging**. Adjuntar `recovery_of_fingerprint`, `previous_attempt_id`, `quality_reconciliation_id` en nueva autorización.
- Sin retries automáticos, `overwrite`, borrados ni publicación oficial desde `BUILD_SILVER_STAGING`.
- Cuando un procesamiento tiene 2 salidas (`valores_inscritos` + `puente_ruc_nemonico`), **ambas** deben existir y superar Quality; un resultado parcial nunca equivale a éxito.
- Para MULTI_SOURCE solo se aceptan versiones Silver publicadas y aprobadas previamente. Los manifests Silver oficiales deben tener versiones, URI, generación, SHA-256 y lista de objetos; su forma final y ubicación requieren contrato de publicación Silver ratificado. La implementación falla cerrada si faltan.
- El procesamiento Spark debe consumir únicamente los objetos versionados del plan. Si los conectores Spark no fijan versiones GCS directamente, exigir inmutabilidad efectiva de objetos/ACL y un plan intermedio versionado; no convertir automáticamente `gs://` genérico en permiso para leer contenido posterior.

## 5. Limitaciones y pruebas

**Componente entregado:** `puerta_silver_operativa_v1_candidata.py`. Usa los módulos previos `puerta_silver_v1_candidata.py` y `adaptador_gcs_silver_v1.py`. Contiene validación completa **en memoria**, preflight GCS read-only bajo contexto independiente y consumo atómico explícito (sin CLI). `consume_once()` requiere plan validado, identidad efectiva coincidente y preflight reciente. **No invocar contra GCS real sin autorización específica y supervisión Quality.**

**Pruebas:** 29 casos unitarios offline con firma P-256 **de prueba, efímera**, incluyendo fallos por firma/pin/acta/vigencia/mapeo/salida omitida/generación/versiones/identidad, consumo único y recovery. No validan IAM real, cuota, Eventarc/Workflows, Dataproc ni un `SILVER_AUTORIZADO` auténtico. Los 8 tests previos de mapeo y 29 tests de candidatos se ejecutaron anteriormente y **no se repiten**.

**Para cierre final todavía corresponde a Quality:** emitir anexo y acta reales firmados; fijar sus URIs/generaciones/hashes y el formato definitivo de manifests Silver derivados; revisar roles IAM, red, runtime 2.2 y ejecución supervisada contra GCS. **No cambiar contrato base ni activar Spark antes.**
