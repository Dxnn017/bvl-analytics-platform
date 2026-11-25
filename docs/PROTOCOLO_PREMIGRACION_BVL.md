# Protocolo de premigracion BVL Analytics

## Responsabilidad
Rama: pipeline-daniela
Alcance: ingesta Bronze, procesamiento Silver,
incrementalidad, integridad y trazabilidad.

## Infraestructura existente
Proyecto: bvl-analytics-2026
Region: southamerica-west1
Buckets: Bronze, Silver, Gold y Control.
No se requiere aprovisionamiento adicional.

## Inventario Bronze
Datasets: 12
Archivos: 3599
Bytes: 393623640
Origen: HDFS /datalake/bvl/bronze
Destino: GCS landing
Manifests originales: inmutables

## Evidencias locales
Inventario HDFS: 3599/3599.
Tamanos correctos: 3599/3599.
Muestras SHA-256: 12/12 correctas.
Planificacion local: aprobada por Quality.
Reintentos e idempotencia: simulacion aprobada.
Transferencias reales: 0.

## Condiciones antes de migrar
1. Quality autoriza levantar la parada 101.7.
2. Matthew completa las pruebas IAM T01-T10.
3. Quality valida la reserva persistente.
4. Se asigna el run_id definitivo.
5. Se confirma presupuesto y auditoria Cloud.
6. Se verifica el contrato del ejecutor de ingesta.

## Procedimiento operativo previsto
1. Reservar run_id de manera persistente.
2. Consultar el registro consolidado.
3. Verificar identidad de cada archivo.
4. Transferir exclusivamente a landing.
5. Validar tamano, SHA-256 y generacion GCS.
6. Reconciliar fallos y reintentos.
7. Entregar resultados a Quality.
8. Publicar Bronze solo con aprobacion Quality.
9. Habilitar Silver solo tras BRONZE_COMPLETADO.
10. Entregar versiones Silver aprobadas a Piero.

## Restricciones actuales
Run ID definitivo: PENDIENTE.
IAM T01-T10: PENDIENTE.
Concurrencia Cloud: NO VALIDADA.
Integridad SHA-256 completa: PENDIENTE.
Migracion real: NO AUTORIZADA.
Gold oficial: NO AUTORIZADO.

La simulacion local no equivale a una
transferencia efectiva ni a una publicacion.
