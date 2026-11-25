# Propuesta de piloto Bronze - BVL Analytics

Estado: **CANDIDATO NO AUTORIZADO** (25/11/2026)

El preparador opera únicamente sobre el plan **local** original de 3599 operaciones.
Valida primero el inventario completo de 12 datasets y 393623640 bytes,
las rutas canónicas, ausencia de duplicados y formato de SHA-256.
Selecciona **uno por dataset**, el de menor tamaño (empate por URI GCS),
para reducir el costo y cubrir los 12 contratos físicos.

## Uso local

Desde la raíz del repositorio:

```bash
PLAN="outputs/quality/bronze_runid_lab/PLAN_BRONZE_12_bvl-bronze-inicial-20261124-001.json"
python3 scripts/ingestion/preparar_piloto_bronze_local.py --plan "$PLAN"
python3 -m unittest discover -s tests -p 'test_*.py'
```

Se producen tres archivos junto al plan (JSON, CSV y .sha256) con rutas HDFS/GCS,
tamaños, hashes esperados y campos de evidencia real aún vacíos (generación GCS,
hash observado y bytes observados). La propuesta es idempotente: no sobrescribe
archivos discrepantes.

**No** se comprueba el contenido real de HDFS durante la selección (los hashes
son los del plan ya revisado) ni se obtiene identidad/credencial, generación,
reserva o aprobación real de Control. Las pruebas unitarias usan datos sintéticos
únicamente para probar la lógica. No se genera reserva, no se realiza transferencia,
no se invoca el SDK GCP, no se modifica IAM. `TRANSFERENCIAS_HABILITADAS=False`
debe mantenerse inalterado.

## Para el cierre con Quality

1. Quality debe revisar el alcance (12 archivos, 1 por dataset, no 3599), aceptar
   el CSV y las rutas propuestas o solicitar menor/mayor número de archivos.
2. Matthew y Lizeth deben definir/autorizar la aplicación de los bindings IAM,
   atribución de autoría de Quality usando logs y política de escritura única,
   referencias confiables del canal oficial y liberación expresa del Paso 101.7.
3. El futuro ejecutor deberá revalidar HDFS, GCS real y autenticidad del release,
   generar evidencias persistentes por archivo/lote con generation GCS, SHA-256
   observado, estados, errores y publicación (Quality decide BRONZE_COMPLETADO).
4. Solo tras autorizaciones **separadas** de IAM, reserva, aprobación y transferencia
   podrá ejecutarse un piloto real. No iniciar carga masiva hasta su auditoría.


## Variante física pendiente de conformidad Quality

El inventario original contiene rutas con carpetas intermedias,
por ejemplo `pdf/ingestion_date=...`. El generador conserva exactamente
las rutas HDFS y GCS del plan, sin mover archivos ni alterar manifests.
Antes de operar en Cloud, Quality debe confirmar esta variante
respecto a la plantilla del contrato Bronze v0.1.
