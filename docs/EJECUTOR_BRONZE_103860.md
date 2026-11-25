# Integración v0.1 de Ingestion Bronze — revisión 25/11/2026

**Estado: solo preflight local; Paso 101.7 vigente.**

## Cambios

- El validador `validar_control_bronze_local.py` debe usar la plantilla
  `.../run_id={run_id}/{manifest_relative_path}` de la revisión 0.1.1.
- `ejecutor_bronze_v01.py` reúne el preflight del plan completo (3,599 operaciones),
  la selección y la verificación HDFS del piloto (12 archivos).
- Implementa lectura puntual de reserva y aprobación por objeto canónico,
  generación exacta y SHA-256 suministrado por un **canal de referencia confiable**.
- Mantiene la barrera de ejecución del núcleo original y prohíbe iniciar
  transferencias por parámetros locales o por `is_operational` en un JSON.
- Las pruebas de GCS emplean dobles simulados: **no prueban IAM ni GCS reales**.

## Limitaciones operativas pendientes para Quality e Infraestructura

1. El origen/autenticidad del *release tuple* (run ID, generaciones, hashes y
   `step_101_7_release_id`) aún debe respaldarse con una fuente autenticada y
   oficial. Un JSON local o argumentos de CLI **no son fuente confiable**.
2. `metadata.emitter_sa` es informativo; la autoría debe verificarse por IAM y
   auditoría oficial. Las condiciones IAM deben comprobarse para el conjunto
   efectivo de identidades con `storage.objects.create` sobre ambos prefijos.
3. El `control` local `CANDIDATO` no sustituye el paquete de **12 manifests
   oficiales aprobados, inmutables y verificados** en el despliegue.
4. `sa-bvl-ingestion` aún no tiene `storage.objects.get` en Control.
5. `transferir_hdfs_gcs.py` continúa con `TRANSFERENCIAS_HABILITADAS=False`.
   `ejecutar_piloto()` deniega sin excepción; **no es un ejecutor Cloud habilitado**.
6. Aprobación de Quality e IAM; preflight autenticado; piloto de una sola
   transferencia o el lote específico aprobado; recibos con hashes,
   generaciones y vinculaciones; auditoría Quality antes de escalar.

## Uso local

```bash
python3 scripts/ingestion/ejecutor_bronze_v01.py \
 --plan outputs/quality/bronze_runid_lab/PLAN_BRONZE_12_bvl-bronze-inicial-20261124-001.json \
 --piloto outputs/quality/bronze_runid_lab/PILOTO_CANDIDATO_bvl-bronze-inicial-20261124-001.json \
 --hdfs-evidencia outputs/quality/bronze_runid_lab/PILOTO_HDFS_VERIFICADO_bvl-bronze-inicial-20261124-001.json
```

**Nunca ejecutar** `gcloud`, reservas GCS, IAM, Dataproc o transferencias sin
las autorizaciones separadas de Lizeth. El informe del piloto contiene
23,052,390 bytes y 12 archivos; SHA real HDFS validado para el piloto.
