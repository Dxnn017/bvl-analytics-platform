# Coordinador de transferencias Bronze v0.1 (preparación)

**Estado:** NO OPERATIVO. Paso 101.7 vigente. Este módulo no autoriza transferencias.

## Propósito

Conecta la selección de los 12 archivos del piloto o la lista completa de 3,599 operaciones con el formato de recibos del núcleo `transferir_hdfs_gcs.py`. Valida SHA-256, tamaño, generación y estado de cada recibo, registra cada intento en JSONL, limita reintentos transitorios y detiene el lote ante inconsistencias. La reconciliación real 412 y la comprobación de bytes de GCS permanecen en el núcleo original, actualmente bloqueado.

La función interna `_procesar_ensayo_con_inyeccion` es una prueba con un transferidor **falso**: sus registros son `ENSAYO_LOCAL_SIN_CLOUD` y no constituyen evidencia de transferencia. No es un método de autorización operativa. No se permite omitir un archivo confiando solo en un journal local: en una recuperación verdadera deberá consultarse GCS y conciliarse su generación y SHA-256.

## Interfaz prevista para Quality (pendiente)

1. Autoridad debe verificar manifests oficiales inmutables, 12 destinos/prefijos del contrato y 3,599 rutas exactas, con hashes oficiales fuera de banda.
2. Solo `sa-bvl-ingestion` podrá leer reserva y aprobación por generation, una vez autorizado IAM.
3. Una **atestación de liberación operativa autenticada y limitada al lote** debe resolver `step_101_7_release_id`, identidad de emisor, conjunto de destinos permitido, generaciones y hashes exactos de reserva/aprobación. JSON pasado por CLI, metadata.emitter_sa, booleans y SHA entregados por un mismo actor no son autenticidad.
4. Antes de permitir la subida, incorporar enlace en metadata/recibo con `reservation_uri`, `reservation_generation`, `approval_uri`, `approval_generation`, `step_101_7_release_id`; comprobar identidad efectiva de `sa-bvl-ingestion` sin claves JSON.
5. Quality debe acordar y autorizar el reporte por archivo/lote, el piloto y el mecanismo de reanudación antes de ejecutar. Ni este módulo ni los reportes declaran `BRONZE_COMPLETADO`.

## Uso permitido ahora

```bash
python3 scripts/ingestion/orquestar_transferencias_bronze_v01.py --preflight-only \
  --alcance piloto --plan outputs/quality/bronze_runid_lab/PLAN_BRONZE_12_bvl-bronze-inicial-20261124-001.json \
  --piloto outputs/quality/bronze_runid_lab/PILOTO_CANDIDATO_bvl-bronze-inicial-20261124-001.json \
  --hdfs-evidencia outputs/quality/bronze_runid_lab/PILOTO_HDFS_VERIFICADO_bvl-bronze-inicial-20261124-001.json

python3 scripts/ingestion/orquestar_transferencias_bronze_v01.py --preflight-only \
  --alcance completo --plan outputs/quality/bronze_runid_lab/PLAN_BRONZE_12_bvl-bronze-inicial-20261124-001.json
```

**Prohibido** activar `TRANSFERENCIAS_HABILITADAS` manualmente, usar cuentas privilegiadas o ejecutar operaciones Cloud sin autorizaciones independientes de Quality. No hay modo `--execute`.
