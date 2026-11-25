# Cierre técnico del motor Bronze HDFS → GCS — local, NO autorizado

**Estado:** preparación técnica para pruebas locales. **Paso 101.7 vigente.** No existe autorización operativa, creación de reservas/aprobaciones, cambio IAM, uso de Dataproc ni transferencia GCS.

## Qué hace esta revisión

- `transferir_hdfs_gcs.py` preserva el bloqueo público, incluso si alguien modifica `TRANSFERENCIAS_HABILITADAS` a `True`. Solo la integración futura con un canal de liberación autenticado podrá habilitar una entrada operativa explícita.
- El motor privado `_transferir_motor_interno` ahora recibe un contexto estructurado con los identificadores de reserva, aprobación, generación, hashes, plan y release. **Esa validación es de coherencia, no de procedencia/autenticidad.** Está disponible para pruebas con dobles; no usarlo desde un orquestador operativo sin verificación externa de Quality.
- Se graba esa trazabilidad en los metadatos GCS de cada objeto hipotético y en el recibo. Los metadatos son etiquetas de auditoría, **no prueba criptográfica de autoridad**.
- La carga conserva `if_generation_match=0` para no sobrescribir; ante un 412 relee metadatos y descarga el contenido por generación para verificar SHA-256 y tamaño. Un objeto con distinto release o contenido se rechaza.
- `orquestar_transferencias_bronze_v01.py` transmite el contexto al núcleo y puede comprobar la correspondencia del recibo con la trazabilidad esperada, sin levantar el bloqueo.
- `tests/test_cierre_motor_bronze.py` usa clientes GCS/HDFS simulados y prueba bloqueo, escrituras protegidas, 412 idempotente, 412 con identidad distinta, SHA-256 remoto divergente y HDFS alterado. **No hace llamadas reales a Cloud**.

## Interfaz estructural prevista (NO es autorización)

```json
{
  "run_id": "bvl-bronze-inicial-20261124-001",
  "plan_sha256": "<64 hex>",
  "release_id": "<identificador oficial, no editable por el operador>",
  "alcance": "piloto",
  "reservation": {
    "uri": "gs://bvl26-control-7f3a9c/reservations/bronze/<run_id>.json",
    "generation": 123,
    "sha256": "<64 hex>"
  },
  "approval": {
    "uri": "gs://bvl26-control-7f3a9c/approvals/bronze/<run_id>.json",
    "generation": 456,
    "sha256": "<64 hex>"
  }
}
```

**El ejemplo anterior NO debe suministrarse manualmente a un ejecutor Cloud**. La autenticidad del release, los SHA-256 y generaciones de origen confiable, el alcance, los manifests oficiales aprobados y la identidad real de Ingestion deben verificarse por una interfaz autenticada de Quality, todavía por especificar/autorizar.

## Precondiciones de migración que siguen faltando

1. Quality aprueba un mecanismo concreto de liberación **fuera de banda** (por ejemplo firma asimétrica verificable con clave pública fijada en despliegue o equivalente autenticado) y una raíz de confianza para las referencias a los doce manifests oficiales. Un JSON, `approved_by_sa`, `emitter_sa` o un booleano `is_operational` por sí solo **no** es prueba.
2. Quality autoriza separadamente cambios IAM y la emisión de reserva y aprobación auténticas. Matthew aplica únicamente permisos aprobados e Ingestion usa su cuenta `sa-bvl-ingestion` sin JSON de llaves.
3. Calidad verifica que las rutas del lote piloto y luego las 3,599 rutas coinciden **exactamente** con los doce manifests, sus hashes y el alcance incluido en el release.
4. Tras una autorización específica, una implementación **distinta de la entrada bloqueada actual** verifica el release auténtico y liga esa decisión a cada llamada al motor. El coordinador debe persistir recibos reales y permitir que Quality audite el estado en GCS; la simulación local no certifica esa condición.
5. Migrar **solo el piloto aprobado**; Quality audita tamaño/hash/generación. Tras esa auditoría y **otra autorización**, migrar los 3,599 archivos. Quality declara `BRONZE_COMPLETADO` antes de Silver.

## Uso

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
grep -n '^TRANSFERENCIAS_HABILITADAS = False' scripts/ingestion/transferir_hdfs_gcs.py
```

No se añade un comando de ejecución Cloud. No modificar los manifests ni cambiar el bloqueo para intentar migrar sin liberación autorizada.
