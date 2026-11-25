# BVL Bronze: orquestador de preparación local del piloto

**Estado: LOCAL, NO OPERATIVO, NO AUTORIZADO.** Paso 101.7 vigente.

Este módulo integra: (i) plan de 3,599 operaciones validado, (ii) propuesta
piloto de 12 archivos reales, (iii) evidencia previa de `hdfs dfs -cat`
con SHA-256 observado, (iv) validación exacta de rutas por el mismo validador
del núcleo `transferir_hdfs_gcs.validar_operacion`, y (v) reporte agregado y
por archivo para Quality, en JSON/CSV/SHA-256.

La ejecución no utiliza credenciales, red ni ninguna API de GCS. El valor
`transfer_allowed` es **siempre falso**. No declara una reserva aprobada ni
simula autorización de Quality. La evidencia HDFS fue aportada por un proceso
local anterior; esta integración comprueba su consistencia contra el plan,
pero no vuelve a leer HDFS.

## Uso

```bash
RUN=bvl-bronze-inicial-20261124-001
BASE=outputs/quality/bronze_runid_lab
python3 scripts/ingestion/orquestar_piloto_bronze_local.py \
  --plan "$BASE/PLAN_BRONZE_12_${RUN}.json" \
  --piloto "$BASE/PILOTO_CANDIDATO_${RUN}.json" \
  --hdfs-evidencia "$BASE/PILOTO_HDFS_VERIFICADO_${RUN}.json" \
  --salida-dir "$BASE"
```

El resultado es `ORQUESTADOR_PILOTO_LOCAL_<run_id>.{json,csv,sha256}`;
repetir el comando no sobrescribe evidencia diferente.

## Pendientes para habilitar piloto real

1. Quality confirma las rutas con subcarpeta `pdf/` y la plantilla de destino
   del contrato v0.1; los manifests originales NO se alteran.
2. Matthew/Quality configuran IAM mínimo para lectura autenticada por
   `sa-bvl-ingestion` de los objetos exactos de reserva y aprobación.
3. GCS valida generation y SHA-256 de reserva y aprobación; fuentes de
   referencias, manifests y release operativa están autenticadas de forma
   independiente y vinculadas criptográficamente.
4. Se comprueba la procedencia Quality mediante controles IAM/auditoría;
   el campo JSON `approved_by_sa` no es prueba suficiente.
5. Quality emite **cuatro** autorizaciones separadas: IAM, reserva,
   aprobación y transferencia piloto; ninguna se infiere del estado `RESERVED`.
6. Un ejecutor Cloud distinto conecta el gate criptográfico/operativo con
   `transferir_archivo`; nunca cambiar el booleano del núcleo como sustituto
   de estas verificaciones. Registrar generation, bytes, SHA-256 y errores
   reales para todos los objetos y conciliar con el informe Quality.

**Sin cumplir estos puntos, no se migra.**
