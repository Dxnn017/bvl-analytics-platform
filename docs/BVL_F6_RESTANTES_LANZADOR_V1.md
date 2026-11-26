# BVL Analytics — Fase 6, lanzador candidato para 10 procesos Silver restantes

**Estado:** PROPUESTA PARA QUALITY, NO AUTORIZA EJECUCIÓN. **No contiene credenciales.** No modifica el commit `41266f0`, el contrato base Bronze–Silver ni el cierre de Fase 5.

## Alcance real

- Se integraron **10 procesos SINGLE_SOURCE** candidatos: `bcrp/embig`, `smv/cambios_patrimonio`, `smv/estado_resultados`, `smv/flujo_efectivo`, `smv/situacion_financiera`, `smv/principales_cuentas`, `smv/cotizaciones`, `smv/montos_sab`, `smv/valores_inscritos`, `smv/hechos_valores_consulta`.
- Producen **11 salidas Silver candidatas**: Valores Inscritos genera `referencias_smv/valores_inscritos` **y** `referencias_smv/puente_ruc_nemonico` bajo una sola autorización, un claim y una revisión Quality conjunta.
- **No incluye** todavía `smv/hechos_importancia` (1 proceso, 1 producto) porque dos CSV de metadatos del pipeline HDFS no figuran como entradas autorizadas del inventario Bronze oficial. **No incluye** el derivado `hechos_importancia/identidad_empresarial` (1 producto), que espera cinco versiones Silver oficiales, no staging.
- La ejecución de Tipo de Cambio previamente publicada continúa intacta, fuera de este release.

## Primitivas reutilizadas y controles previstos

1. Un **contexto de lanzamiento nuevo** (propuesta `BVL-SILVER-LAUNCH-CONTEXT-REST-v1`) debe ser **ratificado y firmado por Quality**, por cada dataset/intento, con commit completo, SHA-256 y generaciones de entrypoint/`--py-files`, outputs, batch ID, cuenta Silver, runtime 2.2, ventana de ejecución y configuración 4+2×4 vCPU. Este esquema **aún no está ratificado**. No reutilizar el contexto fijo de Tipo de Cambio.
2. Lectura mediante `gate.check_gcs_signed_bundle` de F5 (P-256 pin, manifest Bronze oficial, hashes, identidad efectiva), seguida de `bind_launch_to_plan`. Los módulos de F5 se incluyen **sin modificar sus bytes**.
3. Verificar límites antes de claim: fuente completa, 256 MiB máximos, 32 MiB por objeto. El código candidato no es un lector general distribuido; confirmar tamaño/filas reales en preflight supervisado antes de emitir autorización, para evitar intentos que fallen tras claim. El entorno probado tiene runtime Dataproc 2.2.91, Python 3.12.3, Spark 3.5.3, Arrow 15.0.2, GCS 2.16.0.
4. `gate.consume_once(plan, GCSExclusiveClaimStore(client))` exclusivamente antes de ejecutar trabajo real, dentro de la ventana de preflight. Nunca reutilizar un intento consumido ni activar reintentos automáticos.
5. Lectura **por generación exacta y SHA-256** de cada objeto Bronze y transformación mediante el despachador local validado; escritura PyArrow/Snappy con `if_generation_match=0`, `retry=None`, solo `staging/` y sin `overwrite`/`delete`/`rename`.
6. Manifest candidato conjunto para todas las salidas, `quality_approved=false`. Quality valida conteos, esquemas, duplicados, lineage, generación, hash, logs y promociona versiones/cambia `current.json` **por separado**. El lanzador no puede publicar Silver oficial ni habilitar consumo Gold.

## Limitaciones que Quality debe revisar (bloqueos de salida a Cloud)

- Nuevo formato del contexto firmado y permiso de lectura de sus referencias independientes; no inferir aprobación del esquema a partir de tests locales.
- Confirmar particionado por producto, validación de tipos, esquema y el formato del manifest candidato antes de dar compatibilidad al Silver–Gold.
- Confirmar inventario, límites de memoria/filas y el tiempo real de preflight de las 1,221 fuentes de Montos SAB y las 290 fuentes de Valores Inscritos. Si un lote excede límites, diseñar **otro lector ratificado** antes de consumir el claim.
- Para Valores Inscritos: garantizar **aprobación conjunta de las dos salidas**. Ante fallo parcial, preservar staging y claim para conciliación; no reintentar a ciegas.
- Mantener concurrencia ≤12 vCPU y un batch a la vez. La incidencia histórica `EventLoggingListener` (GCS 404) requiere tratamiento independiente de registros de ejecución; no afecta por sí sola la integridad del piloto publicado.
- `smv/hechos_importancia`: obtener y ratificar procedencia de los CSV auxiliares, sin modificar manifests Bronze ya cerrados (ver consulta adjunta).
- Derivado MULTI_SOURCE: ratificar contrato de manifiesto Silver oficial y fijar los cinco SHA/generaciones antes de programar esa etapa.

## Uso en Debian (solo revisión)

```bash
cd ~/bvl-analytics-platform
python3 -m unittest discover -s tests -p 'test_fase6_launch_restantes_puros.py' -v
python3 -m unittest discover -s tests -p 'test_fase6_release_restantes_puros.py' -v
python3 scripts/processing/fase6_restantes_batch.py
python3 scripts/processing/fase6_release_restantes.py
```

El resultado debe decir `BLOQUEADO_POR_DEFECTO` y `PROPUESTA_NO_FIRMADA`. Los archivos estarán en `~/Descargas/BVL_FASE6_RESTANTES_SIN_COMMIT_.../`. **No usar `--execute` ni subir artefactos a GCS.** Luego de revisar todo el código, añadir los archivos del release a un commit separado y probar `--from-commit`, que exige archivos exactos registrados y puerta F5 intacta. No automatizar el push antes de verificar la evidencia.

## Demostración exigida por el profesor

El cierre del proyecto requiere **ingesta nueva visible desde la aplicación** → nuevo run/control/autorización → Bronze → Silver → Gold → BigQuery/dashboard, con trazabilidad e idempotencia. **Ni las pruebas locales ni el piloto histórico sustituyen esa demostración E2E.** Coordinar con Piero y Matthew cuando existan Silver y Gold oficiales.
