# BVL — Incrementalidad segura de Cotizaciones y auditoría de escalabilidad

**Alcance:** diagnóstico y primera implementación para Cotizaciones de la SMV. Este paquete añade scripts nuevos: no sobrescribe los existentes. No modifica la capa Silver oficial mientras se prepara staging.

## 1. Diagnóstico del repositorio recibido

- `scripts/ingestion/ingest_bronze.py`: controla SHA-256, pero un archivo diferente con igual nombre y fecha de ingesta puede colisionar en Bronze.
- `scripts/processing/cotizaciones_to_silver.py`: fecha fija `2026-10-04`, validaciones exactas `60` wrappers y `83130` filas, escritura `.mode("overwrite")`; es un procesador **histórico**, no incremental.
- `scripts/processing/montos_sab_to_silver.py`, `principales_cuentas_to_silver.py`, `hechos_importancia_to_silver.py` y otros: también sobrescriben sus salidas; falta generalizar la actualización por clave de negocio.
- `scripts/ingestion/ingest_valores_smv_bronze.sh` y `ingest_hechos_valores_smv_bronze.sh`: **eliminan** directorios HDFS con `hdfs dfs -rm -r -f` antes de cargar. No ejecutarlos como parte de la actualización incremental.
- `src/gold/build_gold.py`: genera los cinco productos Gold mediante overwrite completo; **este paquete no convierte todavía Gold en incremental**. Requiere tratamiento de dependencias temporales, incluida `lag()` de precios.
- `src/gold/build_gold.py`: la variable llamada `volatilidad_retorno_pct` se calcula como `stddev(precio_cierre)`, lo que no mide desviación de retornos. Corregir definición antes de emplearla en negocio.
- `scripts/performance/benchmark_scalability.py`: informa filas esperadas, pero no cuenta las filas realmente materializadas después de escribir. Además, es un benchmark con réplica y JOIN; no constituye el pipeline de ingesta completo.

## 2. Scripts nuevos

| Archivo | Función | Seguridad |
|---|---|---|
| `scripts/ingestion/ingest_cotizaciones_incremental.py` | Carga JSON originales y deduplica archivos por SHA-256. | No sobrescribe archivos Bronze anteriores. |
| `scripts/processing/cotizaciones_incremental_desde_bronze.py` | Normaliza el lote, compara claves `(fecha_cotizacion, valor)`, revisa correcciones y genera SOLO meses afectados en staging. | No toca Silver oficial. |
| `scripts/processing/publicar_cotizaciones_incremental.py` | Valida manifest y publica particiones con copia de respaldo y bloqueo de una publicación simultánea. | **Sin `--confirm PUBLICAR` es solamente lectura.** |
| `scripts/performance/benchmark_escalabilidad_auditado.py` | Verifica filas escritas, réplicas, bytes comprimidos, throughput y tiempos separados. | Escribe solo resultados de rendimiento en staging. |

## 3. Instalar en Debian (desde la raíz del repositorio)

```bash
cd ~/bvl-analytics-platform
tar -xzf ~/Descargas/bvl_mejoras_incrementales.tar.gz -C ~/bvl-analytics-platform
python3 -m py_compile scripts/ingestion/ingest_cotizaciones_incremental.py scripts/processing/cotizaciones_incremental_desde_bronze.py scripts/processing/publicar_cotizaciones_incremental.py scripts/performance/benchmark_escalabilidad_auditado.py
git status --short
```

## 4. Primera demostración — datos reales 2021–2024 y 2025 en staging

Aprovecha las rutas de pruebas que se prepararon antes en tu VM:

```bash
source /tmp/rutas_prueba_incremental_bvl.sh
spark-submit scripts/processing/cotizaciones_incremental_desde_bronze.py \
    --current "$BASE_BVL_TEST" --incoming-parquet "$LOTE_BVL_TEST" \
    > /tmp/bvl_delta_staging.log 2>&1

grep -E 'AUDITORIA INCREMENTAL|silver_antes:|nuevas:|sin_cambio:|corregidas:|PARTICION_STAGE|STAGING_VALIDADO|MANIFEST|SILVER_OFICIAL' /tmp/bvl_delta_staging.log
```

**Resultados esperados:** `silver_antes=59209`, `nuevas=23921`, `corregidas=0`. Debe crear **12 particiones mensuales de 2025 en staging**, mientras que las particiones históricas de 2021–2024 permanecen inalteradas.

Lee el manifest antes de cualquier publicación:

```bash
MANIFEST=$(sed -n 's/^MANIFEST|//p' /tmp/bvl_delta_staging.log | tail -1)
python3 scripts/processing/publicar_cotizaciones_incremental.py --manifest "$MANIFEST"
```

El comando anterior es una **simulación de publicación, no publica**. No ejecutes `--confirm PUBLICAR` hasta verificar el plan y tener el visto bueno del equipo, puesto que modifica el destino especificado en el manifest. Haz la primera publicación exclusivamente en el conjunto de **prueba** (`$BASE_BVL_TEST`), nunca directamente en Silver oficial.

## 5. Ingesta de un archivo REAL nuevo de la SMV (cuando exista)

No generes información simulada para el modelo ni los dashboards. Descarga archivos oficiales, en formato JSON con el campo `Resultado`, y conserva su procedencia.

```bash
python3 scripts/ingestion/ingest_cotizaciones_incremental.py \
  --files ~/Descargas/Cotizaciones_Nuevas_Oficiales.json
```

Copiará el original a una ruta inmutable, por ejemplo:

`/datalake/bvl/bronze/smv/cotizaciones/ingestion_date=YYYY-MM-DD/cotizaciones_<sha256>.json`

Utiliza la fecha impresa por el ingestor para normalizarlo:

```bash
spark-submit scripts/processing/cotizaciones_incremental_desde_bronze.py \
  --bronze '/datalake/bvl/bronze/smv/cotizaciones/ingestion_date=YYYY-MM-DD/cotizaciones_*.json' \
  --ingestion-date YYYY-MM-DD \
  > /tmp/bvl_delta_bronze.log 2>&1
```

Reemplaza `YYYY-MM-DD` por la fecha real. **Si hay correcciones de cotizaciones existentes, el programa se detendrá por defecto** y las auditará; solo se aceptarán tras revisar su fuente y ejecutarlo explícitamente con `--allow-corrections`.

## 6. Evidencias para la sustentación

1. Conteos antes/después, registros insertados, modificados, repetidos y número de particiones afectadas.
2. Comparación de archivos/particiones sin cambios antes y después.
3. Persistencia de un lote real y segunda ejecución del mismo lote: `nuevas=0`, `corregidas=0`, `SIN_CAMBIOS`.
4. Auditoría de procedencia (fuente, URL, fecha de descarga, SHA-256, ruta Bronze y run-id).
5. Mediciones con el benchmark auditado y evidencia Spark UI/YARN (Shuffle, Spill, CPU, tareas); no confundir este benchmark con la corrida completa.
6. Actualización posterior de Gold y dashboard basada en los productos realmente publicados (siguiente módulo de implementación).

## 7. Advertencias de producción

- Una sustitución de varias particiones en Parquet/HDFS **no es una transacción ACID**. El publicador conserva backups y ofrece rollback ante fallos, pero debe ejecutarse en una ventana sin lectores/escritores simultáneos. Para despliegue real con concurrencia se recomiendan Iceberg, Hudi o Delta Lake, o snapshots de tabla con cambio de puntero controlado.
- No utilizar el script histórico `cotizaciones_to_silver.py` después de publicar cambios incrementales: su `overwrite` borraría los registros añadidos fuera de la muestra fija.
- No ejecutar scripts de extracción Bronze que incluyen `hdfs dfs -rm -r -f` sobre directorios históricos.
- No afirmar que Gold es incremental todavía: los cinco productos actuales siguen reconstruyéndose con `overwrite`.
- No atribuir mejora de negocio a estas pruebas sin medir una línea base y un indicador de decisión de la BVL o del proceso analítico simulado de la plataforma.
