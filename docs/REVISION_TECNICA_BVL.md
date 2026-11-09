# Informe corto de revisión técnica — rama `pipeline-daniela`

**Material recibido:** `bvl_codigo_revision.tar.gz`: carpeta `scripts/ingestion`, `scripts/processing`, `scripts/performance` y `src/gold`.

## Riesgos demostrados por el código

1. **Bronze no totalmente inmutable:** el ingestor general `ingest_bronze.py` detecta duplicados por checksum, pero deposita versiones nuevas con el mismo nombre en una carpeta fechada. Puede haber colisiones dentro del mismo día.
2. **Cargas destructivas en SMV:** `ingest_valores_smv_bronze.sh` y `ingest_hechos_valores_smv_bronze.sh` tienen instrucciones explícitas para eliminar sus directorios HDFS antes de escribir.
3. **Silver de Cotizaciones histórico:** fija `FECHA_INGESTA`, `60` wrappers y `83130` filas; al finalizar utiliza `overwrite` sobre el dataset completo.
4. **Otros Silver no incrementales:** `montos_sab_to_silver.py`, `principales_cuentas_to_silver.py`, `hechos_importancia_to_silver.py`, `hechos_identidad_to_silver.py` y `valores_smv_to_silver.py` utilizan sobrescritura.
5. **Gold completo no incremental:** los cinco productos se escriben con `overwrite`.
6. **Definición equivocada de indicador:** `stddev(precio_cierre)` aparece etiquetado como `volatilidad_retorno_pct` en Gold. Es una desviación de precio, no de retorno en porcentaje.
7. **Escalabilidad:** el benchmark multiplica filas por `crossJoin` con `replica_id`, pero solo imprime registros *esperados*; para descartar pérdidas debe verificar el conteo realmente almacenado. La pequeña diferencia en MB puede explicarse por compresión de valores repetidos, algo que debe corroborarse con resultados experimentales.

## Hallazgos positivos

- Uso de claves `fecha_cotizacion + valor` en las auditorías de Cotizaciones.
- Uso generalizado de Parquet y Snappy.
- Rutinas de control de calidad, auditorías y perfiles de fuentes.
- Separación explícita de Silver, Gold, metadatos y evidencias.
- Pruebas previas de 83,130 registros únicos, fusión de 23,921 registros 2025 y reingesta idempotente, comunicadas por la usuaria (no verificadas en esta ejecución local).

## Estado de este paquete

- **Implementado en código:** ingesta inmutable de nuevos JSON oficiales de Cotizaciones, preparación incremental de Silver por meses afectados, publicador con backup, y benchmark de escalabilidad con conteos verificados.
- **Probado aquí:** compilación estática de Python y revisión de rutas. No se dispone de tu VM, HDFS ni PySpark para ejecutar pruebas de integración contra datos reales.
- **Pendiente:** confirmar resultados del nuevo código en Debian, extender el patrón a fuentes Silver restantes, implementar actualización de Gold preservando dependencias temporales y conectarlo al dashboard.
