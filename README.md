# BigData---Bolsa-de-Valores

## Riesgo BVL en Matt-core

Los tres productos Gold alimentan un pipeline PySpark MLlib. El generador entrega CSV sintéticos en `data/sample`; el adaptador GCP lee las tablas homónimas mediante el conector Spark–BigQuery. Ambas rutas usan las mismas ventanas, controles de clave, entrenamiento, predicción y evaluación. La publicación local usa SQLite y la remota usa una tabla intermedia y una transacción BigQuery. Streamlit lee los resultados publicados.

### Ejecución local

Desde la raíz del repositorio, con Python 3.11 o 3.12, Java compatible con Spark 3.5 y el entorno virtual activo:

```bash
python -m pip install -r requirements.txt
python -m scripts.ingestion.muestra --lote 1
python -m scripts.processing.riesgo --env local --model outputs/tmp/rf-v1 --version rf-v1 --gold muestra1 muestra1 muestra1
python -m streamlit run app.py
```

En Windows, configurar `JAVA_HOME`, `HADOOP_HOME` y añadir su directorio `bin` con los componentes nativos de Hadoop al PATH del proceso. En esta máquina de desarrollo se utilizaron los binarios 3.3.5 en `.venv/hadoop/bin`. En PowerShell, antes de los comandos anteriores:

```powershell
.\.venv\Scripts\Activate.ps1
$env:HADOOP_HOME = (Resolve-Path .venv/hadoop).Path
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"
$env:SPARK_HOME = (Resolve-Path .venv/Lib/site-packages/pyspark).Path
$env:SPARK_LOCAL_IP = '127.0.0.1'
$env:PYSPARK_PYTHON = (Get-Command python).Source
```

El primer entrenamiento requiere una ruta de modelo nueva. Conservar la correspondencia entre artefacto y `--version`. Para aplicar el snapshot corregido con el modelo ya entrenado:

```bash
python -m scripts.ingestion.muestra --lote 2 --dir outputs/tmp/lote2
python -m scripts.processing.riesgo --env local --load-model --data outputs/tmp/lote2 --model outputs/tmp/rf-v1 --version rf-v1 --gold muestra1 muestra1 muestra1
python -m pytest -q
```

Repetir el segundo comando registra otra ejecución con cero inserciones y actualizaciones. En la app, refrescar y seleccionar la última fecha para ver las nuevas cotizaciones. En la serie de SIM0, la corrección del 30 de mayo cambia el hash. `DATA_BACKEND=sample` es el valor predeterminado y `ML_DB` permite seleccionar otra base SQLite. Para una ubicación distinta, proporcionar también `--db` al pipeline.

### Contrato Gold–ML

El diccionario versionado en `config/diccionario_gold.csv` proviene del commit `2034a909` de Piero. Se consume una proyección de sus columnas. Los identificadores conservan su tipo cadena y los montos SAB su decimal original.

| Producto | Grano y organización física Gold | Uso en ML |
|---|---|---|
| mercado_valor_diario | nemonico, fecha · partición fecha · cluster nemonico | Una fila por valor y día, retorno Gold, rango, macro y monto individual. |
| actividad_sab_diaria | fecha, codigo_sab · partición fecha · cluster codigo_sab | Un HHI por fecha cuando todas las filas tienen cobertura completa, trazabilidad y el mismo HHI presente. |
| empresa_trimestre | id_registro · partición fecha_fin_trimestre · cluster id_registro, rpj | Por RUC, último trimestre apto cuyo cierre e ingesta ya ocurrieron. Si varias filas disponibles compiten en ese trimestre, los ratios quedan nulos. |

Las claves nulas o duplicadas interrumpen el proceso. La fecha de ingesta aproxima la disponibilidad en la plataforma. La reconstrucción histórica exacta requiere snapshots disponibles en cada corte. La frecuencia cuenta cierres válidos en 20 días calendario divididos entre 20, incluidos fines de semana. La volatilidad es la desviación estándar muestral del retorno Gold en esa ventana. Los primeros cortes pueden tener historia parcial. El modelo imputa faltantes con medianas aprendidas en entrenamiento. Los KPI visibles cuentan valores por fecha y presentan el HHI diario observado. Los ratios financieros se consumen por observación.

### Etiqueta y evaluación

Para la demostración, el objetivo representa el mayor movimiento absoluto de un retorno diario Gold en los cinco días calendario siguientes. BAJO corresponde a menos de 1 %, MEDIO a entre 1 % inclusive y 2,5 % exclusivo, y ALTO a 2,5 % o más. Son umbrales de trabajo pendientes de validación de negocio. Solo se evalúan horizontes completos con al menos un retorno observado. La predicción sí incluye los cortes recientes sin etiqueta. Los rendimientos suministrados heredan el tratamiento de eventos corporativos de Gold.

`--split` fija el último día permitido para las etiquetas de entrenamiento. La evaluación comienza en fechas de observación posteriores. El bosque usa 24 árboles, profundidad máxima 5 y seed 42 configurable. El baseline asigna la clase más frecuente en entrenamiento. `outputs/performance/evaluacion.json` conserva matrices (filas reales, columnas predichas en orden 0, 1, 2), soporte, precisión, recall y F1 de ALTO. El score en [0, 1] es la esperanza de la clase dividida entre dos.

### Publicación y auditoría

`config/salidas_ml.sql` define las dos salidas con sus nombres y tipos. La clave de predicción es (nemonico, fecha_corte, horizonte_dias, modelo_version). El SHA-256 cubre RUC y el vector ordenado antes de imputar, incluidos los nulos. La publicación reevalúa el snapshot para capturar cambios en ventanas históricas. Compara el hash y las tres versiones declaradas por el productor. Una versión global nueva actualiza también la procedencia de filas con iguales variables. Usar snapshots completos que conserven las claves vigentes. La eliminación de claves retiradas requiere una política de bajas del productor.

La clave de auditoría es run_id. La publicación de cambios y el registro de conteos se confirman en una transacción. Un reintento idéntico conserva el run_id original de cada predicción y agrega una corrida con cero cambios. La muestra no necesita credenciales.

### Despliegue previsto en Dataproc y BigQuery

Usar un clúster existente Spark 3.5 en southamerica-west1 con el conector Spark–BigQuery compatible, cliente Python google-cloud-bigquery y ADC de la cuenta de ejecución. Aprovisionar previamente las dos tablas de salida con el DDL entregado. Empaquetar el directorio scripts como paquete Python importable (desde la raíz, `python -m zipfile -c bvl.zip scripts`). Usar versiones reales de origen, corte apropiado para el histórico y un prefijo GCS nuevo por modelo. Ejemplo documental para una futura ejecución autorizada:

```bash
gcloud dataproc jobs submit pyspark scripts/processing/riesgo.py --py-files=bvl.zip --project=bvl-analytics-2026 --region=southamerica-west1 --cluster="$CLUSTER" -- --env gcp --model "gs://$BUCKET/modelos/rf-v1" --version rf-v1 --split 2024-04-30 --gold "$V_MERCADO" "$V_SAB" "$V_EMPRESA"
```

Las actualizaciones usan el mismo comando con `--load-model`. Streamlit usa `DATA_BACKEND=bigquery` y ADC. El despliegue remoto queda pendiente de validación del equipo.

| Identidad | Acceso requerido |
|---|---|
| App | Crear jobs de consulta en el proyecto y leer exclusivamente las dos tablas ML. |
| Cuenta del job Spark | Lectura de las tres Gold, sesiones de Storage Read en el proyecto, creación de jobs y lectura/escritura de las dos salidas. La tabla intermedia ml_stage por corrida requiere creación, escritura, lectura y eliminación en el dataset. Restringir el acceso al prefijo de modelos y a los artefactos del job en GCS. |
| Operador de clúster existente | Enviar y consultar jobs, usar el clúster designado y depositar el paquete de trabajo en el bucket de staging. |

Consultar las referencias de [Storage Read](https://docs.cloud.google.com/bigquery/docs/reference/storage), [roles BigQuery](https://docs.cloud.google.com/bigquery/docs/access-control) y [permisos Dataproc](https://docs.cloud.google.com/managed-spark/docs/concepts/iam/iam).
