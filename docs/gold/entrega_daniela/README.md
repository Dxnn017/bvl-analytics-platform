# Entrega tecnica Gold para integracion Silver-Gold

## 1. Identificacion

- Proyecto: bvl-analytics-2026
- Responsable: Piero
- Destinataria: Daniela
- Rama: analytics-piero
- Commit base: e20bd5c068a87f7b586e2b0736f06d7d5ef8f42d
- Fecha de validacion: 2026-11-27
- Entorno: local
- Estado: evidencia tecnica para revision
- Publicacion oficial autorizada: NO

## 2. Objetivo

Entregar evidencia tecnica reproducible sobre el
procesamiento Gold, sus dependencias Silver y las
limitaciones identificadas durante la integracion.

## 3. Entregables

| Entregable | Ubicacion |
|---|---|
| Matriz Silver-Gold | MATRIZ_SILVER_GOLD.md |
| Commit base | e20bd5c |
| Comando reproducible | reproducir_pruebas.sh |
| Fixtures sinteticos | fixtures/ |
| Evidencias de ejecucion | evidencias/resultados_pruebas.txt |
| Hashes de integridad | evidencias/SHA256SUMS.txt |
| Informe de limitaciones | LIMITACIONES_BIGQUERY.md |

## 4. Productos incluidos

- mercado_valor_diario
- actividad_sab_diaria
- empresa_trimestre

Productos pendientes de acreditacion Quality:

- hechos_empresa
- impacto_hechos_valor

## 5. Requisitos de reproduccion

Entorno utilizado durante la validacion:

- Debian 12.
- Python 3.11.2.
- PySpark 3.5.1.
- Java compatible con Spark.
- Git.
- Bash.
- SQLite mediante la biblioteca estandar de Python.
- Repositorio con los archivos de esta entrega.

No se requieren credenciales de Google Cloud para
ejecutar los escenarios sinteticos incluidos.

## 6. Comando reproducible

Desde la raiz del repositorio, con el entorno Python
compatible previamente activado:

```bash
PYTHON_BIN="$(command -v python3)" \
bash docs/gold/entrega_daniela/reproducir_pruebas.sh
```

Para conservar una nueva evidencia sin sobrescribir
el registro original:

```bash
(
    set -o pipefail

    PYTHON_BIN="$(command -v python3)" \
    bash docs/gold/entrega_daniela/reproducir_pruebas.sh \
    2>&1 | tee "/tmp/bvl_gold_$(date +%Y%m%d_%H%M%S).txt"
)
```

Resultado final esperado:

```text
ESCENARIOS_EVALUADOS: 4
LIMITACIONES_DIAGNOSTICADAS: NEMONICOS Y SAB
PUBLICACION_AUTORIZADA: False
VALIDACION_CONJUNTA_LOCAL_FINALIZADA_OK
```

## 7. Resultados obtenidos

| Escenario | Resultado |
|---|---|
| Normalizacion de nemonicos | Limitacion reproducida |
| Cuarentena SAB sintetica | Limitacion reproducida |
| Sustitucion financiera | Aprobada localmente |
| Incrementalidad y trazabilidad | Aprobada localmente |

Resultados adicionales:

- Suite incremental: 8 de 8 pruebas aprobadas.
- Diferencias financieras en grupos no afectados: 0.
- Diferencias de idempotencia financiera: 0.
- Claves financieras invalidas: 0.
- Grupos financieros duplicados: 0.
- ROA sintetico actualizado: 20%.
- Codigo de salida observado en terminal: 0.

El codigo de salida fue mostrado fuera del archivo
capturado mediante tee.

Los escenarios de nemonicos y SAB son diagnosticos.
Su ejecucion exitosa no significa que las limitaciones
hayan sido corregidas.

## 8. Verificacion de integridad

Desde la raiz del repositorio:

```bash
sha256sum -c docs/gold/entrega_daniela/evidencias/SHA256SUMS.txt
```

Los hashes corresponden a los archivos entregados,
no a una certificacion de Quality.

Una nueva ejecucion puede generar diferencias en los
timestamps, mensajes de Spark y tiempos de ejecucion.
Por ello, no se exige que un nuevo log tenga el mismo
hash que la evidencia original.

## 9. Limitaciones y dependencias pendientes

- Acordar normalizacion canonica de nemonicos.
- Definir tratamiento de cobertura parcial SAB.
- Confirmar ruta fisica del dataset financiero Silver.
- Acreditar identidad y sustituciones financieras oficiales.
- Validar replay del control de versiones Gold.
- Validar SQL, concurrencia y transacciones en BigQuery real.
- Obtener las aprobaciones Quality correspondientes.

La prueba financiera adicional incluida en la suite local
no formaba parte del commit base e20bd5c.

El commit base identifica la implementacion original
utilizada durante la validacion.

## 10. Restricciones de entrega

- No se publicaron versiones oficiales.
- No se modificaron datos Silver oficiales.
- No se ejecutaron operaciones en GCP.
- No se modificaron permisos IAM.
- No se acredita BigQuery productivo.
- No se acredita Quality.
- No se incluyen Parquet historicos ni archivos PEM pendientes.

Esta entrega se proporciona para revision tecnica
y coordinacion de la integracion Silver-Gold.

PUBLICACION_AUTORIZADA: False
