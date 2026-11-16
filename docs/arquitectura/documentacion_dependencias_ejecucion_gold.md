# BVL Analytics Platform
## Documentación de dependencias y ejecución Gold

**Versión del documento:** 1.0  
**Contrato de integración:** Silver–Gold v0.1  
**Responsable de Gold:** Piero Alexis Mendoza Santos  
**Destinataria:** Lizeth — Infraestructura, seguridad y orquestación  
**Fecha:** 16 de noviembre de 2026  
**Proyecto GCP:** bvl-analytics-2026  
**Rama:** analytics-piero  
**Commit de referencia:** 833eeaf  

---

## 1. Objetivo del documento

El presente documento describe las dependencias, interfaces, mecanismos de ejecución y requisitos de infraestructura de la capa Gold del proyecto BVL Analytics Platform.

Su finalidad es facilitar la integración de los componentes Bronze, Silver y Gold en Google Cloud, manteniendo la compatibilidad con el contrato Silver–Gold v0.1 y con el procesamiento incremental previamente validado en el entorno local.

La implementación actual ha sido validada mediante Apache Spark 3.5.1 en Debian 12. La adaptación a Google Cloud Storage y Dataproc constituye la siguiente etapa del proyecto.

## 2. Arquitectura general

El proyecto utiliza una arquitectura Medallion compuesta por tres capas:

- Bronze: almacenamiento de datos originales provenientes de las fuentes bursátiles, financieras y macroeconómicas.
- Silver: limpieza, normalización, validación y publicación de versiones completas de los datasets.
- Gold: integración de datos, generación de productos analíticos y procesamiento incremental.

La arquitectura objetivo en Google Cloud contempla:

```text
Fuentes de datos
        |
        v
Cloud Storage - Bronze
        |
        v
Dataproc / PySpark
        |
        v
Cloud Storage - Silver
        |
        v
Manifest Silver-Gold v0.1
        |
        v
Eventarc + Workflows
        |
        v
Dataproc / Gold incremental
        |
        v
Cloud Storage - Gold
        |
        v
BigQuery
        |
        v
Looker Studio / Machine Learning
```

La infraestructura definitiva será coordinada con Lizeth. No se consideran aprobados todavía los nombres de buckets, las cuentas de servicio ni la región de ejecución.

## 3. Productos de la capa Gold

Actualmente existen cinco productos Gold históricos construidos y validados.

| Producto Gold | Granularidad | Clave de negocio | Particiones | Registros históricos |
|---|---|---|---|---:|
| mercado_valor_diario | Instrumento por fecha | nemonico, fecha | anio, mes | 83130 |
| actividad_sab_diaria | SAB por fecha | codigo_sab, fecha | anio, mes | 22156 |
| empresa_trimestre | Registro financiero trimestral | id_registro | anio, trimestre | 5249 |
| hechos_empresa | Expediente empresarial | numero_expediente | anio | 346 |
| impacto_hechos_valor | Expediente por instrumento | numero_expediente, nemonico | anio | 175 |

Total de registros Gold históricos: 111056.

Los productos se encuentran almacenados en formato Parquet con compresión Snappy y estructuras particionadas.

### 3.1. mercado_valor_diario

Integra las cotizaciones diarias con información macroeconómica y referencias empresariales.

Incluye precios, montos negociados, tipo de cambio, EMBIG, indicadores de calidad y retornos bursátiles.

### 3.2. actividad_sab_diaria

Consolida la actividad diaria de las sociedades agentes de bolsa.

### 3.3. empresa_trimestre

Organiza la información financiera empresarial por ejercicio y trimestre, incluyendo indicadores y controles de calidad.

### 3.4. hechos_empresa

Integra los hechos de importancia empresarial con información de identidad, documentos y contenido textual.

### 3.5. impacto_hechos_valor

Relaciona hechos empresariales con instrumentos bursátiles y analiza las cotizaciones anteriores y posteriores al acontecimiento.

La ventana de análisis comprende siete días calendario antes y después del hecho, excluyendo el día del acontecimiento.

Los resultados representan asociaciones temporales y no constituyen demostraciones de causalidad.

## 4. Dependencias directas Silver–Gold

Las dependencias se encuentran definidas en el componente dependency_registry.py.

| Dataset Silver | Producto Gold dependiente | Unidad de recálculo |
|---|---|---|
| cotizaciones | mercado_valor_diario | Instrumento y fecha |
| montos_sab | actividad_sab_diaria | Fecha completa de negociación |
| tipo_cambio | mercado_valor_diario | Fecha macroeconómica |
| embig | mercado_valor_diario | Fecha macroeconómica |
| principales_cuentas | empresa_trimestre | Grupo financiero por RPJ, ejercicio y trimestre |
| eventos | hechos_empresa | Expediente |
| documentos | hechos_empresa | Expedientes asociados a documentos |
| contenido_textual | hechos_empresa | Expedientes asociados a textos |
| identidad_empresarial | hechos_empresa | Expediente |
| puente_ruc_nemonico | mercado_valor_diario | Histórico de los instrumentos afectados |
| indices | Sin dependencia Gold actual | No requiere recálculo |

Un cambio Silver no implica necesariamente una actualización Gold. Primero debe determinarse si existen claves analíticas efectivamente afectadas.

Las dependencias anteriores se encuentran definidas en la planificación incremental. La ejecución operativa actual está especializada en el dataset tipo_cambio.

## 5. Dependencias internas Gold

Además de las dependencias Silver–Gold, existen dos dependencias entre productos Gold.

```text
mercado_valor_diario ---------+
                              |
                              v
                      impacto_hechos_valor
                              ^
                              |
hechos_empresa ---------------+
```

Los productos mercado_valor_diario y hechos_empresa constituyen fuentes del producto impacto_hechos_valor.

Por ello, cuando existan modificaciones efectivas en mercado o hechos empresariales, deberá evaluarse su propagación hacia impacto.

El producto impacto_hechos_valor consume, entre otros campos:

- Precios de cierre.
- Montos negociados.
- Moneda de referencia.
- Tipo de cambio anterior y posterior.
- EMBIG anterior y posterior.

Una modificación macroeconómica puede afectar indirectamente este producto si existen instrumentos y expedientes relacionados dentro de la ventana temporal correspondiente.

## 6. Orden de ejecución Gold

El orden lógico de construcción completa es:

```text
1. mercado_valor_diario
2. actividad_sab_diaria
3. empresa_trimestre
4. hechos_empresa
5. impacto_hechos_valor
```

Los primeros cuatro productos no tienen dependencias entre ellos y pueden ejecutarse de manera independiente si sus fuentes están disponibles.

El producto impacto_hechos_valor debe ejecutarse después de contar con las versiones consistentes de mercado_valor_diario y hechos_empresa.

Para el procesamiento incremental no es obligatorio ejecutar los cinco productos. Únicamente deben ejecutarse aquellos que presenten registros candidatos a recalcular, respetando las dependencias internas.

## 7. Contrato Silver–Gold v0.1

La comunicación entre Silver y Gold se realiza mediante manifests JSON.

El documento de referencia es:

```text
docs/arquitectura/contrato_manifest_silver_gold.md
```

El manifest proporciona información sobre:

- Versión del contrato.
- Entorno.
- Identificador único de ejecución.
- Estado de publicación Silver.
- Fecha de publicación.
- Tipo de publicación.
- Aprobación de validaciones.
- Datasets modificados.
- Versiones anteriores y nuevas.
- Rutas de almacenamiento.
- Versión de esquema.
- Claves de negocio.
- Particiones afectadas.
- Inserciones, correcciones y eliminaciones.
- Conteos anteriores y posteriores.
- Autorización de eliminaciones.

Las versiones Silver deben ser completas e inmutables, de forma que Gold pueda consultar exactamente los datos asociados al manifest.

El manifest oficial debe tener:

```text
estado = SILVER_PUBLICADO_APROBADO
validacion_aprobada = true
```

Los manifests de laboratorio no deben activar automáticamente el pipeline oficial.

### 7.1. Integridad y trazabilidad

El procesamiento Gold utiliza:

- SHA-256 del manifest.
- Identificadores de versiones Silver.
- Inventario de archivos Parquet.
- Huellas SHA-256 individuales.
- Huella consolidada de cada dataset.
- Registro de versiones.
- Evidencias de validación y ejecución.

El SHA-256 del manifest permite identificar su contenido exacto y prevenir la reutilización incompatible de un mismo identificador de ejecución.

En Google Cloud deberá conservarse este mecanismo y complementarse con identificadores de generación de objetos cuando corresponda.

Los hashes nativos de GCS no deben asumirse equivalentes al SHA-256 utilizado por el contrato.

## 8. Componentes de procesamiento incremental

Los principales componentes se encuentran en:

```text
src/gold/incremental/
```

| Componente | Responsabilidad |
|---|---|
| manifest_validator.py | Validación estructural del manifest |
| dependency_registry.py | Catálogo de dependencias Silver–Gold y Gold–Gold |
| incremental_planner.py | Elaboración del plan preliminar incremental |
| version_registry.py | Registro, resolución e integridad de versiones Silver |
| operational_validator.py | Validación física y lógica mediante Spark |
| scope_resolver.py | Determinación de claves y particiones candidatas |
| incremental_executor.py | Recálculo incremental, equivalencia e idempotencia |

Los constructores históricos se encuentran dentro de src/gold y se reutilizan para el cálculo de los productos analíticos.

El ejecutor incremental no debe utilizar una lógica de transformación diferente a la del constructor histórico.

## 9. Flujo operativo incremental

El flujo lógico de procesamiento es:

```text
Silver publica una version completa
                |
                v
Manifest aprobado
                |
                v
Validacion estructural
                |
                v
Planificacion preliminar
                |
                v
Resolucion de versiones e integridad
                |
                v
Validacion operativa con Spark
                |
                v
Resolucion del alcance Gold
                |
                v
Control de idempotencia
                |
                v
Existen registros afectados?
        |                |
       NO               SI
        |                |
        v                v
Conservar Gold      Recalculo incremental
        |                |
        |                v
        |          Validar candidata
        |                |
        +-------+--------+
                |
                v
Registrar resultado
```

La implementación experimental actual utiliza diferentes comandos y reportes para cada etapa. La integración con Workflows deberá coordinar su ejecución y el intercambio de evidencias.

### 9.1. Validación estructural

Comprueba que el manifest cumple con los campos, tipos de datos, operaciones y reglas del contrato v0.1.

### 9.2. Planificación incremental

Identifica los productos Gold potencialmente afectados y propone un orden de ejecución.

Un plan preliminar no constituye una autorización de publicación.

### 9.3. Validación operativa

Comprueba mediante Spark:

- Esquemas físicos.
- Conteos reales.
- Unicidad de claves.
- Claves nulas.
- Particiones inconsistentes.
- Cambios efectivos entre versiones.
- Coincidencia con el manifest.
- Integridad de los archivos.

### 9.4. Resolución del alcance

Determina qué claves y particiones Gold podrían requerir recálculo.

También evalúa las dependencias indirectas hacia impacto_hechos_valor.

### 9.5. Ejecución incremental

El ejecutor reutiliza los constructores históricos para recalcular los registros afectados.

Los registros y particiones no afectados deben conservarse.

Si no existen cambios efectivos, se mantiene la versión Gold anterior.

### 9.6. Validación de equivalencia

Durante las pruebas locales se comparó el resultado incremental con una reconstrucción completa de referencia.

Se utilizaron comparaciones bidireccionales mediante exceptAll para verificar que no existieran registros diferentes.

Esta reconstrucción completa constituye una comprobación experimental y no debe interpretarse como una obligación de reconstruir Gold en cada ejecución productiva.

### 9.7. Idempotencia

Una ejecución previamente completada no debe volver a modificar Gold cuando coincidan su run_id, contenido del manifest y evidencias correspondientes.

La implementación local devuelve:

```text
GOLD_OMITIDO_IDEMPOTENCIA
```

La idempotencia secuencial local está validada. La gestión de concurrencia distribuida y el registro transaccional todavía deben adaptarse a Google Cloud.

## 10. Estados de procesamiento

El contrato contempla los siguientes estados generales:

| Estado | Significado |
|---|---|
| GOLD_RECIBIDO | Manifest recibido |
| GOLD_VALIDANDO | Validación en curso |
| GOLD_RECHAZADO | Manifest o datos incompatibles |
| GOLD_PLANIFICADO | Plan incremental generado |
| GOLD_PROCESANDO | Procesamiento en curso |
| GOLD_VALIDANDO_RESULTADOS | Validación de resultados |
| GOLD_PUBLICADO_APROBADO | Publicación Gold aprobada |
| GOLD_SIN_CAMBIOS | No existen modificaciones analíticas |
| GOLD_OMITIDO_IDEMPOTENCIA | Ejecución previamente completada |
| GOLD_FALLIDO | Fallo durante el procesamiento |

En las pruebas locales también se utilizan estados específicos, como:

```text
VALIDACION_OPERATIVA_PRUEBA_APROBADA
GOLD_ALCANCE_CANDIDATO_RESUELTO
GOLD_SIN_REGISTROS_AFECTADOS
GOLD_CANDIDATO_PRUEBA_VALIDADO
EJECUCION_INCREMENTAL_PRUEBA_COMPLETADA
```

Los estados experimentales no autorizan una publicación productiva.

## 11. Resultados de las pruebas incrementales BCRP

Se han validado tres escenarios en el entorno local.

| Indicador | Inserción propia | Corrección propia | Inserción Daniela |
|---|---:|---:|---:|
| Registros Silver anteriores | 7751 | 7751 | 7751 |
| Registros Silver posteriores | 7752 | 7751 | 7752 |
| Inserciones | 1 | 0 | 1 |
| Correcciones | 0 | 1 | 0 |
| Eliminaciones | 0 | 0 | 0 |
| Registros Gold recalculados | 0 | 116 | 0 |
| Registros Gold modificados | 0 | 116 | 0 |
| Registros Gold de mercado conservados | 83130 | 83014 | 83130 |
| Particiones reescritas | 0 | 1 | 0 |
| Equivalencia aprobada | Si | Si | Si |
| Idempotencia secuencial | Si | Si | Si |

### 11.1. Inserción propia

Fecha: 2026-09-18.

Tipo de cambio insertado: 3.37.

No existen cotizaciones bursátiles para esa fecha en el histórico disponible. Por ello, no se requiere recalcular ningún producto Gold.

Resultado:

```text
GOLD_SIN_CAMBIOS
```

### 11.2. Corrección histórica

Fecha: 2025-12-30.

Valor anterior: 3.36435714285714.

Valor corregido: 3.40.

Se recalcularon y modificaron 116 registros de mercado_valor_diario.

Se reescribió exclusivamente la partición anio=2025/mes=12, que contiene 2198 registros.

Las otras 59 particiones conservaron su contenido original.

Se conservaron 83014 registros de mercado sin modificaciones.

La reducción del alcance de transformación fue aproximadamente del 99.86 % respecto a los 83130 registros históricos de mercado.

Este porcentaje representa reducción de registros recalculados, no una medición del tiempo total de procesamiento.

Resultado:

```text
GOLD_CANDIDATO_PRUEBA_VALIDADO
```

### 11.3. Integración con Daniela

Run ID:

```text
20261116T003053Z_78127719
```

Se incorporaron las versiones Silver anterior y nueva entregadas por Daniela.

Se verificaron los 714 archivos Parquet y se confirmó que la versión anterior coincidía físicamente con el Silver utilizado para construir Gold histórico.

Fecha insertada: 2026-09-18.

Tipo de cambio: 3.50.

No existen cotizaciones bursátiles para dicha fecha, por lo que Gold no requiere modificaciones.

Resultado:

```text
GOLD_SIN_CAMBIOS
```

El manifest original de Daniela conserva su estado de laboratorio. No se activó una publicación oficial.

### 11.4. Dependencias indirectas

En los tres escenarios evaluados no se encontraron pares de expedientes e instrumentos que requirieran recalcular impacto_hechos_valor.

La ausencia de cambios fue comprobada mediante reconstrucción completa de referencia.

El recálculo incremental efectivo de impacto, cuando existan expedientes afectados, todavía requiere implementación y pruebas adicionales.

## 12. Comandos de ejecución local validados

Los siguientes comandos corresponden al entorno Debian 12 y no deben ejecutarse directamente en Dataproc sin adaptar las rutas y dependencias.

Directorio del repositorio:

```bash
cd "$HOME/bvl-analytics-platform"
```

### 12.1. Validación operativa

```bash
PYSPARK_PYTHON=/usr/bin/python3 \
PYSPARK_DRIVER_PYTHON=/usr/bin/python3 \
spark-submit \
  --master 'local[2]' \
  --conf spark.sql.shuffle.partitions=8 \
  src/gold/incremental/operational_validator.py \
  --manifest "$HOME/bvl_entregas/incremental_pruebas_20261114/manifests/manifest_bcrp_insercion_operativo.json" \
  --registro "$HOME/bvl_entregas/incremental_pruebas_20261114/control/registro_versiones_silver_integracion.json" \
  --salida "$HOME/bvl_entregas/incremental_pruebas_20261114/reportes/validacion_operativa_bcrp_insercion.json"
```

### 12.2. Resolución del alcance

```bash
PYSPARK_PYTHON=/usr/bin/python3 \
PYSPARK_DRIVER_PYTHON=/usr/bin/python3 \
spark-submit \
  --master 'local[2]' \
  --conf spark.sql.shuffle.partitions=8 \
  src/gold/incremental/scope_resolver.py \
  --escenario todos
```

### 12.3. Ejecución incremental

```bash
PYSPARK_PYTHON=/usr/bin/python3 \
PYSPARK_DRIVER_PYTHON=/usr/bin/python3 \
spark-submit \
  --master 'local[2]' \
  --conf spark.sql.shuffle.partitions=8 \
  src/gold/incremental/incremental_executor.py \
  --escenario todos \
  --ejecutar-prueba
```

### 12.4. Comprobación de idempotencia

La repetición del comando anterior sobre las ejecuciones exitosas produjo el estado GOLD_OMITIDO_IDEMPOTENCIA para los tres escenarios.

No se recalcularon registros ni se generaron nuevas versiones candidatas.

Los comandos dependen de la preparación previa de los datasets, manifests, registros y reportes.

Los validadores y el resolvedor protegen los reportes existentes contra sobrescrituras.

## 13. Propuesta de almacenamiento en Google Cloud

Se propone utilizar cuatro buckets independientes:

```text
gs://BUCKET_BRONZE/
    landing/
    smv/
    bcrp/
    quarantine/

gs://BUCKET_SILVER/
    staging/
    versions/
    backups/

gs://BUCKET_GOLD/
    mercado_valor_diario/
    actividad_sab_diaria/
    empresa_trimestre/
    hechos_empresa/
    impacto_hechos_valor/

gs://BUCKET_CONTROL/
    manifests/
    version_registry/
    audit/
    events/approved/
```

BUCKET_BRONZE, BUCKET_SILVER, BUCKET_GOLD y BUCKET_CONTROL son marcadores de posición. No representan nombres de recursos existentes.

### 13.1. Versionado Silver

Ejemplo conceptual:

```text
gs://BUCKET_SILVER/versions/tipo_cambio/
    version=VERSION_ID/
        anio=2026/
            mes=9/
                part-*.parquet
```

La ruta registrada debe identificar la raíz de la versión completa del dataset.

El esquema definitivo de las rutas debe coordinarse con Daniela y Lizeth, debido a que el registro local utiliza actualmente otra estructura física.

### 13.2. Versionado Gold

Para facilitar publicaciones seguras, se recomienda definir rutas independientes para las versiones candidatas y aprobadas.

Ejemplo conceptual:

```text
gs://BUCKET_GOLD/versions/VERSION_GOLD/
    mercado_valor_diario/
    actividad_sab_diaria/
    empresa_trimestre/
    hechos_empresa/
    impacto_hechos_valor/
```

Se recomienda que un puntero de publicación identifique la versión Gold vigente.

Esta estructura de versionado es una propuesta pendiente de aprobación conjunta.

### 13.3. Bucket Control

Debe permitir conservar:

- Manifests Silver.
- Registros de versiones.
- Evidencias de validación operativa.
- Planes incrementales.
- Reportes de alcance.
- Reportes de ejecución Gold.
- Registros de auditoría.
- Eventos de lotes aprobados.
- Punteros de publicación acordados.

Se recomienda diferenciar los artefactos de control de Silver y Gold mediante prefijos independientes.

## 14. Requisitos de permisos para Gold

Los permisos definitivos serán configurados por Lizeth siguiendo el principio de mínimo privilegio.

| Recurso | Acceso requerido por Gold |
|---|---|
| Bucket Bronze | No requerido para el procesamiento Gold |
| Bucket Silver | Listar y leer versiones aprobadas |
| Bucket Control | Leer manifests y registros Silver |
| Prefijos de control Gold | Crear y leer reportes y registros de ejecución autorizados |
| Bucket Gold | Leer versiones anteriores y crear versiones candidatas |
| Dataproc | Ejecutar procesos autorizados de Spark |
| BigQuery | Gestionar las tablas analíticas autorizadas en una etapa posterior |

Para la ejecución automatizada se deberá utilizar una cuenta de servicio específica.

No se recomienda ejecutar los pipelines automatizados con credenciales personales.

La cuenta de servicio Gold no necesita permisos para modificar ni eliminar versiones Silver.

La publicación de punteros y los mecanismos de limpieza de versiones deberán contar con permisos específicos y coordinados.

Los roles IAM exactos dependerán de la configuración de almacenamiento, del modo de ejecución Dataproc y del diseño definitivo de publicación.

## 15. Propuesta de orquestación con Eventarc y Workflows

La ejecución automática de Gold debe iniciarse únicamente después de la publicación aprobada de un lote Silver.

Flujo propuesto:

```text
Daniela procesa Bronze -> Silver
                |
                v
Valida y publica una version Silver
                |
                v
Genera manifest aprobado
                |
                v
Genera evento de lote aprobado
                |
                v
Eventarc
                |
                v
Workflows
                |
                v
Valida condiciones de ejecucion
                |
                v
Ejecuta procesamiento Gold en Dataproc
                |
                v
Consulta resultado
                |
                v
Registra estado y evidencias
```

### 15.1. Condiciones de activación

Gold no debe activarse por cada archivo Parquet generado en Silver.

El disparador debe asociarse a un evento de publicación completa, representado mediante un objeto de control acordado.

Workflows deberá comprobar como mínimo:

- Entorno permitido.
- Existencia del manifest.
- Estado aprobado.
- Validación Silver aprobada.
- Identificador de ejecución.
- Disponibilidad de la versión registrada.
- Ausencia de conflictos de publicación.

La forma exacta del evento, sus campos y la configuración del disparador deben coordinarse antes del despliegue.

### 15.2. Parámetros que deberá recibir Gold

La futura ejecución portable de Gold debería recibir:

- Proyecto GCP.
- Entorno.
- Ruta GCS del manifest.
- Ruta del registro de versiones Silver.
- Raíz Silver.
- Versión Gold base.
- Raíz Gold candidata.
- Raíz de evidencias y auditoría.
- Identificador de ejecución.

Estos parámetros representan la interfaz objetivo para Google Cloud. Todavía no constituyen una interfaz implementada y validada en Dataproc.

### 15.3. Manejo de resultados

La orquestación deberá distinguir entre:

- Procesamiento exitoso con cambios.
- Procesamiento exitoso sin cambios.
- Ejecución omitida por idempotencia.
- Rechazo por incumplimiento del contrato.
- Fallo técnico durante la ejecución.

Los reintentos no deben producir duplicidad de resultados ni sobrescribir versiones previamente aprobadas.

## 16. Publicación segura en Google Cloud

La publicación Gold deberá realizarse mediante versiones independientes.

No se debe trasladar directamente a GCS el mecanismo de renombrado de directorios utilizado en HDFS.

Flujo objetivo:

1. Identificar la versión Gold base.
2. Generar una nueva versión candidata.
3. Conservar los productos y particiones no afectados.
4. Validar los resultados de la candidata.
5. Registrar sus huellas e inventario.
6. Actualizar el puntero de publicación mediante una condición de generación.
7. Registrar el resultado de la ejecución.

Si ocurre un error antes de la publicación, la versión Gold aprobada anterior deberá mantenerse disponible.

El puntero solamente debe actualizarse cuando todos los productos afectados hayan sido validados.

La implementación de este mecanismo en GCS todavía se encuentra pendiente.

## 17. Requisitos pendientes de coordinación con Lizeth

Para adaptar Gold a Google Cloud se requiere confirmar:

1. ID definitivo del proyecto.
2. Región de almacenamiento y procesamiento.
3. Nombres de los cuatro buckets.
4. Estructura de rutas Silver, Gold y Control.
5. Identidades y cuentas de servicio.
6. Permisos de lectura y escritura.
7. Modalidad de ejecución Dataproc.
8. Versión de Spark y dependencias disponibles.
9. Mecanismo de distribución del código PySpark.
10. Configuración de Eventarc y Workflows.
11. Ubicación del registro de ejecuciones.
12. Estrategia de versionado y publicación.
13. Presupuesto, cuotas y controles de costos.
14. Ubicación de los datasets BigQuery futuros.

La región southamerica-east1 constituye una propuesta inicial, pendiente de revisión técnica y económica.

## 18. Estado actual y limitaciones

### Completado

- Construcción y validación de los cinco productos Gold históricos.
- Contrato Silver–Gold v0.1.
- Validación estructural de manifests.
- Planificación incremental.
- Registro local de versiones Silver.
- Validación operativa BCRP con Spark.
- Integración del paquete Silver de Daniela.
- Resolución de dependencias directas e indirectas para los escenarios evaluados.
- Ejecución incremental BCRP local.
- Equivalencia lógica y física.
- Idempotencia secuencial local.

### Pendiente

- Adaptación de operaciones de almacenamiento a GCS.
- Parametrización completa de rutas.
- Lectura y escritura de registros y reportes remotos.
- Ejecución operativa en Dataproc.
- Migración de Gold histórico a Cloud Storage.
- Procesamiento incremental de los demás datasets Silver.
- Recálculo incremental efectivo de impacto_hechos_valor.
- Control de concurrencia distribuida.
- Publicación Gold mediante puntero de versión.
- Integración con Eventarc y Workflows.
- Validación conjunta del pipeline en Google Cloud.
- Integración con BigQuery y Looker Studio.

La implementación actual no debe considerarse un pipeline Gold productivo completo.

## 19. Referencias y archivos de apoyo

Repositorio:

https://github.com/Dxnn017/bvl-analytics-platform

Rama:

```text
analytics-piero
```

Commits de referencia:

| Commit | Descripción |
|---|---|
| 294bf33 | Implementación y validación Gold histórica |
| 64ce56c | Validación y planificación incremental |
| 833eeaf | Procesamiento incremental BCRP |

Archivos entregados junto con este documento:

```text
contrato_manifest_silver_gold.md
manifest_bcrp_insercion_operativo.json
manifest_bcrp_correccion_operativo.json
documentacion_dependencias_ejecucion_gold.md
```

Los manifests adjuntos constituyen ejemplos operativos de pruebas locales. Sus rutas no corresponden todavía a Google Cloud Storage.

## 20. Conclusión

La capa Gold cuenta con una implementación histórica validada y un procesamiento incremental BCRP funcional en el entorno local.

Las pruebas realizadas demuestran que es posible consumir versiones Silver completas, determinar el alcance de los cambios, recalcular únicamente los registros afectados, conservar los datos históricos y reconocer ejecuciones previamente completadas.

La siguiente etapa consiste en adaptar los componentes existentes a Cloud Storage y Dataproc, manteniendo el contrato Silver–Gold v0.1 y coordinando la infraestructura, permisos y mecanismos de publicación con Lizeth y Daniela.

No se requiere reconstruir la lógica analítica desde cero. Se requiere adaptar las interfaces de almacenamiento, ejecución, orquestación y publicación para operar de manera segura en Google Cloud.

---

Fin del documento.
