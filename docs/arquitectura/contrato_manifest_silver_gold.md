# CONTRATO DE MANIFEST SILVER → GOLD

## 1. Información general

- Proyecto: BVL Analytics.
- Documento: Contrato de integración incremental Silver → Gold.
- Versión: 0.1.
- Fecha: 2026-11-14.
- Estado: PROPUESTA PENDIENTE DE APROBACIÓN.
- Componente emisor: Pipeline Silver.
- Componente consumidor: Pipeline Gold.
- Rama de desarrollo Gold: analytics-piero.

Este documento complementa el contrato técnico Gold v1.1.

Su finalidad es establecer un mecanismo verificable de comunicación
entre Silver y Gold para ejecutar actualizaciones incrementales.

La estructura se basa en la propuesta inicial proporcionada por
Daniela y contempla ampliaciones necesarias para garantizar
trazabilidad, calidad, idempotencia y conservación del histórico.

Las ampliaciones deberán ser aprobadas conjuntamente antes de
utilizar este contrato en una publicación oficial.


## 2. Objetivo

El manifest debe permitir que Gold:

1. Identifique una publicación Silver aprobada.
2. Determine qué datasets fueron modificados.
3. Identifique las claves y particiones afectadas.
4. Determine los productos Gold dependientes.
5. Recalcule únicamente los registros afectados.
6. Conserve los registros históricos no modificados.
7. Controle ejecuciones repetidas.
8. Bloquee publicaciones inconsistentes.
9. Registre las versiones utilizadas.
10. Genere evidencias de procesamiento incremental.


## 3. Principios de integración

- Silver constituye la fuente de entrada autorizada de Gold.
- Gold no modifica Bronze ni Silver.
- Gold no consume datos Silver sin aprobación.
- Gold no procesa manifests de staging como publicaciones oficiales.
- Gold no realiza append sin verificar claves.
- Gold no sobrescribe indiscriminadamente el histórico.
- Gold no interpreta archivos ausentes como eliminaciones.
- Gold conserva los registros históricos no afectados.
- Gold registra la versión Silver utilizada.
- Gold valida sus resultados antes de publicarlos.
- Gold no publica resultados parciales ante errores.
- Una ejecución repetida no debe generar duplicados.
- Una ejecución sin cambios efectivos debe registrarse.
- Los datos de prueba deben mantenerse separados de los oficiales.


## 4. Momento de generación del manifest

El pipeline Silver debe generar el manifest oficial únicamente
después de completar satisfactoriamente:

1. Recepción del lote.
2. Validación de entrada.
3. Procesamiento Bronze.
4. Transformación incremental Silver.
5. Detección de inserciones y correcciones.
6. Validación de calidad.
7. Publicación segura de Silver.
8. Confirmación de la versión publicada.

El manifest oficial constituye la señal de autorización para Gold.

Los manifests correspondientes a resultados temporales o staging
no deben activar publicaciones Gold.


## 5. Propuesta inicial de Daniela

La estructura inicial comunicada por Daniela es:

```json
{
  "run_id": "ejemplo_run_001",
  "estado": "SILVER_PUBLICADO_APROBADO",
  "fecha_publicacion": "YYYY-MM-DDTHH:MM:SSZ",
  "datasets": [
    {
      "dataset": "tipo_cambio",
      "ruta_silver": "/datalake/bvl/silver/macroeconomia/tipo_cambio",
      "clave": ["fecha"],
      "particiones_afectadas": [
        {
          "anio": 2026,
          "mes": 9
        }
      ],
      "registros_nuevos": 1,
      "registros_corregidos": 0,
      "version_publicada": "version_ejemplo"
    }
  ],
  "validacion_aprobada": true
}
```

Esta estructura constituye la base del contrato.

El ejemplo no representa una publicación Silver real y no debe
utilizarse para activar una ejecución oficial.


## 6. Ampliaciones propuestas para el manifest

Se propone incorporar:

- version_manifest.
- entorno.
- tipo_publicacion.
- version_esquema.
- version_anterior.
- claves_afectadas.
- ruta_detalle_cambios.
- registros_eliminados.
- registros_antes.
- registros_despues.
- autorizacion_eliminaciones.

Estas ampliaciones permiten identificar modificaciones exactas,
comprobar conservación de registros y controlar versiones.

El formato definitivo deberá acordarse entre Silver y Gold.


## 7. Estructura general propuesta

Ejemplo exclusivamente simulado:

```json
{
  "version_manifest": "0.1",
  "entorno": "PRUEBA",
  "run_id": "simulado_bcrp_001",
  "estado": "SILVER_PUBLICADO_APROBADO",
  "fecha_publicacion": "2026-11-14T22:00:00Z",
  "tipo_publicacion": "INCREMENTAL",
  "validacion_aprobada": true,
  "datasets": [
    {
      "dataset": "tipo_cambio",
      "ruta_silver": "file:///ruta_de_prueba/macroeconomia/tipo_cambio",
      "version_esquema": "ejemplo_1",
      "version_anterior": "simulada_v1",
      "version_publicada": "simulada_v2",
      "clave": ["fecha"],
      "particiones_afectadas": [
        {
          "anio": 2026,
          "mes": 9
        }
      ],
      "claves_afectadas": [
        {
          "operacion": "INSERT",
          "clave_anterior": null,
          "clave_nueva": {
            "fecha": "2026-09-18"
          }
        }
      ],
      "ruta_detalle_cambios": null,
      "registros_nuevos": 1,
      "registros_corregidos": 0,
      "registros_eliminados": 0,
      "registros_antes": 7751,
      "registros_despues": 7752,
      "autorizacion_eliminaciones": false
    }
  ]
}
```

Todos los identificadores, versiones, fechas de publicación y rutas
de este ejemplo corresponden a una simulación.

El registro del 18 de septiembre de 2026 se utiliza únicamente
como ejemplo de diseño y no representa el nuevo registro real
detectado por Daniela.


## 8. Definición de campos generales

| Campo | Tipo | Obligatorio propuesto | Descripción |
|---|---|---|---|
| version_manifest | string | Sí | Versión del formato JSON |
| entorno | string | Sí | PRUEBA o PRODUCCION |
| run_id | string | Sí | Identificador único de ejecución |
| estado | string | Sí | Estado de publicación Silver |
| fecha_publicacion | string | Sí | Fecha ISO 8601 con zona horaria |
| tipo_publicacion | string | Sí | INCREMENTAL |
| validacion_aprobada | boolean | Sí | Resultado de validación Silver |
| datasets | array | Sí | Datasets incluidos en la publicación |

La versión inicial del consumidor incremental deberá aceptar
únicamente versiones del manifest explícitamente soportadas.

Las publicaciones completas deberán gestionarse mediante un
procedimiento independiente y no como actualizaciones incrementales.


## 9. Definición de campos por dataset

| Campo | Tipo | Obligatorio propuesto | Descripción |
|---|---|---|---|
| dataset | string | Sí | Identificador del dataset |
| ruta_silver | string | Sí | Ruta de la versión publicada |
| version_esquema | string | Sí | Versión del esquema |
| version_anterior | string | Sí | Versión base de comparación |
| version_publicada | string | Sí | Nueva versión disponible |
| clave | array de strings | Sí | Columnas de identificación |
| particiones_afectadas | array | Sí | Particiones modificadas |
| claves_afectadas | array o null | Condicional | Detalle de cambios |
| ruta_detalle_cambios | string o null | Condicional | Archivo con detalle de cambios |
| registros_nuevos | integer | Sí | Cantidad de inserciones |
| registros_corregidos | integer | Sí | Cantidad de correcciones |
| registros_eliminados | integer | Sí | Cantidad de eliminaciones |
| registros_antes | integer | Sí | Conteo antes de publicar |
| registros_despues | integer | Sí | Conteo después de publicar |
| autorizacion_eliminaciones | boolean | Sí | Permiso explícito de eliminación |

Los campos opcionales o condicionales no deben provocar una
interpretación ambigua sobre las modificaciones realizadas.

Si existe algún cambio, debe estar disponible su detalle mediante
claves_afectadas o ruta_detalle_cambios.

No deben coexistir ambas modalidades como fuentes independientes
de verdad para una misma modificación.


## 10. Detalle de modificaciones

El detalle de cambios debe distinguir las operaciones:

- INSERT.
- UPDATE.
- DELETE.

### 10.1. Inserción

```json
{
  "operacion": "INSERT",
  "clave_anterior": null,
  "clave_nueva": {
    "fecha": "2026-09-18"
  }
}
```

Representa un registro que no existía en la versión anterior.

### 10.2. Corrección

```json
{
  "operacion": "UPDATE",
  "clave_anterior": {
    "fecha": "2025-12-30"
  },
  "clave_nueva": {
    "fecha": "2025-12-30"
  }
}
```

Representa un registro existente cuyo contenido fue corregido.

La clave puede mantenerse o modificarse, según la política
autorizada del dataset.

Cuando una corrección afecta la clave o la partición de origen,
deben identificarse tanto los valores anteriores como los nuevos.

### 10.3. Eliminación

```json
{
  "operacion": "DELETE",
  "clave_anterior": {
    "fecha": "2025-12-30"
  },
  "clave_nueva": null
}
```

Representa una eliminación explícita y autorizada.

Gold debe bloquear eliminaciones no autorizadas.

La ausencia de un registro en un archivo de entrada no constituye
por sí sola una eliminación.

Todos los ejemplos de esta sección son simulados.


## 11. Reglas de validación del manifest

Gold debe comprobar como mínimo:

1. El archivo contiene JSON válido.
2. La versión del manifest es compatible.
3. El entorno corresponde al modo de ejecución.
4. El run_id existe y no está vacío.
5. El estado es SILVER_PUBLICADO_APROBADO.
6. validacion_aprobada es exactamente true.
7. La fecha de publicación tiene formato válido.
8. El tipo de publicación es compatible.
9. datasets tiene una estructura válida.
10. Los identificadores de datasets son reconocidos.
11. No existen datasets repetidos dentro del manifest.
12. Las rutas pertenecen al entorno autorizado.
13. Las claves declaradas coinciden con el contrato.
14. Las particiones tienen una estructura válida.
15. Los conteos son enteros no negativos.
16. Las versiones Silver están identificadas.
17. Las versiones son accesibles y consistentes.
18. Las operaciones declaradas son reconocidas.
19. Las claves de cada operación están completas.
20. No existen operaciones contradictorias.
21. Las eliminaciones cuentan con autorización.
22. El detalle de cambios concuerda con los conteos.
23. La versión anterior coincide con la versión base de Gold.
24. El conteo final es consistente con las operaciones.
25. No existen cambios de esquema incompatibles.

La validación estructural debe diferenciarse de la validación
operativa de rutas, versiones, archivos y datos.

Un manifest estructuralmente correcto no garantiza por sí solo
que la información publicada en Silver sea válida o accesible.


## 12. Conservación de registros

Para inserciones, correcciones uno a uno y eliminaciones:

registros_despues =
    registros_antes
    + registros_nuevos
    - registros_eliminados

Una corrección uno a uno no incrementa por sí misma la cantidad
total de registros.

Si un dataset utiliza correcciones que transforman una fila
en varias filas o viceversa, deberá acordarse una regla específica.

Los conteos Silver no deben confundirse con los conteos Gold.

Una modificación Silver puede:

- No generar cambios en Gold.
- Modificar una fila Gold.
- Modificar varias filas Gold.
- Incorporar nuevas filas Gold.
- Retirar filas Gold derivadas que ya no sean válidas.

La cantidad de filas afectadas dependerá de las relaciones,
granularidades y reglas de transformación de cada producto.


## 13. Inventario de datasets Silver consumidos

Las siguientes rutas son relativas a la raíz Silver.

| Dataset | Ruta relativa | Clave operativa histórica |
|---|---|---|
| cotizaciones | mercado_diario/cotizaciones | valor, fecha_cotizacion |
| montos_sab | mercado_diario/montos_sab | codigo_sab, fecha_negociacion |
| tipo_cambio | macroeconomia/tipo_cambio | fecha |
| embig | macroeconomia/embig | fecha |
| principales_cuentas | finanzas_empresariales/principales_cuentas | id_registro |
| eventos | hechos_importancia/eventos | numero_expediente |
| documentos | hechos_importancia/documentos | guid_documento |
| contenido_textual | hechos_importancia/contenido_textual | guid_documento |
| identidad_empresarial | hechos_importancia/identidad_empresarial | numero_expediente |
| puente_ruc_nemonico | referencias_smv/puente_ruc_nemonico | nemonico_valor |

Estas claves corresponden al histórico Silver auditado.

Para principales_cuentas debe considerarse adicionalmente el
grupo de negocio:

- rpj.
- ejercicio.
- trimestre_num.

La estabilidad de id_registro frente a correcciones debe
confirmarse con Silver.

Para puente_ruc_nemonico se verificó también la unicidad de:

- ruc.
- nemonico_valor.

Los nombres de identificadores utilizados en los manifests
deberán acordarse con Daniela.

Los demás datasets Silver inventariados no participan
directamente en los cinco productos Gold históricos actuales.

Un dataset reconocido pero no consumido podrá generar una
ejecución sin cambios Gold.

Un dataset desconocido deberá bloquearse hasta confirmar
su contrato.


## 14. Matriz de dependencias Gold

| Dataset Silver | Productos Gold afectados |
|---|---|
| cotizaciones | mercado_valor_diario, impacto_hechos_valor |
| montos_sab | actividad_sab_diaria |
| tipo_cambio | mercado_valor_diario, impacto_hechos_valor |
| embig | mercado_valor_diario, impacto_hechos_valor |
| principales_cuentas | empresa_trimestre |
| eventos | hechos_empresa, impacto_hechos_valor |
| documentos | hechos_empresa, impacto_hechos_valor |
| contenido_textual | hechos_empresa, impacto_hechos_valor |
| identidad_empresarial | hechos_empresa, impacto_hechos_valor |
| puente_ruc_nemonico | mercado_valor_diario, impacto_hechos_valor |

La matriz permite identificar productos potencialmente afectados.

La existencia de una dependencia no garantiza que se produzcan
cambios efectivos en el producto Gold.


## 15. Claves de los productos Gold

| Producto | Clave |
|---|---|
| mercado_valor_diario | nemonico, fecha |
| actividad_sab_diaria | codigo_sab, fecha |
| empresa_trimestre | id_registro |
| hechos_empresa | numero_expediente |
| impacto_hechos_valor | numero_expediente, nemonico |

Las claves deberán conservar su unicidad después de cada
actualización incremental.


## 16. Reglas de recálculo incremental

### 16.1. mercado_valor_diario

Una modificación de cotizaciones afecta directamente la clave:

- nemonico.
- fecha.

Una modificación de tipo de cambio o EMBIG afecta las
cotizaciones correspondientes a las fechas modificadas.

Una modificación del puente RUC-nemónico puede afectar todas
las filas históricas asociadas a los nemónicos modificados.

Deben considerarse las claves anteriores y nuevas cuando
existan correcciones de identidad o fecha.

Si Silver modifica cierres anteriores de otras cotizaciones,
también deberá informar esas modificaciones.

### 16.2. actividad_sab_diaria

Una modificación de una sociedad agente de bolsa afecta
potencialmente a todas las sociedades de la fecha modificada.

Deben recalcularse:

- Totales diarios.
- Participaciones de mercado.
- Ranking diario.
- Concentración HHI.
- Indicadores dependientes.

No es suficiente recalcular únicamente la sociedad modificada.

### 16.3. empresa_trimestre

Una modificación financiera debe considerar todos los registros
del grupo:

- rpj.
- ejercicio.
- trimestre_num.

Deben recalcularse las banderas de colisión, los controles
contables y los indicadores financieros correspondientes.

Si cambia el grupo de negocio, deben revisarse tanto el
grupo anterior como el nuevo.

No deben eliminarse colisiones reales de manera arbitraria.

### 16.4. hechos_empresa

Una modificación de eventos, documentos, contenido textual
o identidad empresarial afecta los expedientes relacionados.

Deben recalcularse:

- Identificación empresarial.
- Calidad documental.
- Cantidades de documentos.
- Cobertura textual.
- Indicadores de suficiencia.
- Elegibilidad para análisis de impacto.

Cuando un documento cambia de expediente, deben revisarse
el expediente anterior y el nuevo.

### 16.5. impacto_hechos_valor

Una modificación puede afectar:

- Pares expediente-nemónico.
- Fecha del acontecimiento.
- Cotizaciones anteriores.
- Cotizaciones posteriores.
- Datos macroeconómicos asociados.
- Identificación empresarial.
- Información documental incorporada al producto.

Se mantiene la metodología histórica oficial:

CIERRES_CERCANOS_7D_SIN_DIA_EVENTO_V1

La ventana comprende siete días calendario anteriores y
siete días calendario posteriores al acontecimiento,
excluyendo la fecha del propio evento.

Una modificación de cotizaciones puede afectar múltiples
acontecimientos relacionados con el mismo nemónico.

Una modificación de hechos o identidad puede incorporar
o retirar pares derivados de expediente y nemónico.

Los pares retirados deben justificarse mediante los cambios
aprobados en Silver.

Las demás filas históricas deben conservarse.


## 17. Particiones Gold

| Producto | Particiones |
|---|---|
| mercado_valor_diario | anio, mes |
| actividad_sab_diaria | anio, mes |
| empresa_trimestre | anio, trimestre |
| hechos_empresa | anio |
| impacto_hechos_valor | anio |

Las particiones Silver afectadas no necesariamente coinciden
con las particiones Gold que deben actualizarse.

Gold debe determinar sus particiones afectadas a partir de:

- Claves anteriores.
- Claves nuevas.
- Relaciones entre productos.
- Dependencias temporales.
- Reglas de transformación.
- Granularidad del producto.

No deben actualizarse particiones no afectadas sin una
justificación técnica documentada.


## 18. Versionado y trazabilidad

Cada ejecución Gold debe registrar:

- run_id del manifest Silver.
- Versión del manifest.
- Entorno de ejecución.
- Versiones Silver utilizadas.
- Versión Gold anterior.
- Versión Gold generada.
- Productos identificados.
- Productos efectivamente modificados.
- Particiones afectadas.
- Fecha y hora de inicio.
- Fecha y hora de finalización.
- Resultado de validaciones.
- Conteos anteriores y posteriores.
- Estado final.

La ruta Silver debe permitir identificar de manera inequívoca
la versión declarada.

Una ruta mutable sin control de versión no garantiza
reproducibilidad.

El mecanismo definitivo de almacenamiento de versiones
Silver deberá acordarse con Daniela.

Las versiones de fuentes Silver no modificadas también
deben quedar identificadas para garantizar una reconstrucción
reproducible de los productos Gold.


## 19. Idempotencia

Gold debe identificar las ejecuciones previamente procesadas.

Se propone registrar el run_id junto con una huella SHA-256
del contenido del manifest.

### Caso 1: Primera ejecución

Si el run_id no ha sido publicado anteriormente y el manifest
es válido, Gold puede iniciar el procesamiento.

### Caso 2: Ejecución repetida

Si el run_id y la huella coinciden con una publicación
Gold previamente completada, no deben generarse duplicados
ni repetirse innecesariamente las transformaciones.

### Caso 3: Identificador reutilizado

Si el mismo run_id presenta una huella diferente, la
ejecución debe bloquearse por inconsistencia.

### Caso 4: Ejecución fallida

Una ejecución fallida puede reintentarse bajo controles,
siempre que las versiones de entrada y la identidad
del manifest permanezcan consistentes.

### Caso 5: Versión ya procesada

Si un nuevo run_id referencia versiones Silver ya aplicadas,
Gold deberá verificar su estado y evitar duplicaciones.

El registro de ejecuciones debe distinguir intentos fallidos
de publicaciones finalizadas.

La política definitiva de concurrencia y recuperación deberá
garantizar que dos ejecuciones no publiquen simultáneamente
resultados incompatibles.


## 20. Estados de ejecución Gold

Se proponen los siguientes estados:

| Estado | Descripción |
|---|---|
| GOLD_RECIBIDO | Manifest recibido |
| GOLD_VALIDANDO | Validación del manifest |
| GOLD_RECHAZADO | Manifest inválido o incompatible |
| GOLD_PLANIFICADO | Dependencias identificadas |
| GOLD_PROCESANDO | Transformaciones en ejecución |
| GOLD_VALIDANDO_RESULTADOS | Controles sobre staging |
| GOLD_PUBLICADO_APROBADO | Publicación correcta |
| GOLD_SIN_CAMBIOS | Sin modificaciones efectivas |
| GOLD_OMITIDO_IDEMPOTENCIA | Ejecución previamente aplicada |
| GOLD_FALLIDO | Error durante procesamiento o publicación |

Los estados transitorios no constituyen una publicación oficial.

Los consumidores analíticos deben utilizar únicamente
versiones Gold publicadas y aprobadas.

Una ejecución GOLD_SIN_CAMBIOS debe conservar la versión
analítica válida anterior y registrar las versiones Silver
que fueron evaluadas.


## 21. Staging y publicación segura

Flujo propuesto:

1. Recibir manifest.
2. Validar estructura y autorización.
3. Verificar versiones Silver.
4. Comprobar idempotencia.
5. Identificar productos y claves afectados.
6. Leer la versión Gold anterior.
7. Recalcular los registros necesarios.
8. Construir los resultados en staging.
9. Verificar unicidad de claves.
10. Verificar conservación del histórico.
11. Validar métricas y relaciones.
12. Verificar particiones no afectadas.
13. Publicar una nueva versión Gold aprobada.
14. Registrar el resultado de ejecución.
15. Emitir evidencias para los consumidores.

La publicación lógica debe considerar el conjunto completo
de productos Gold afectados.

Si falla uno de los productos, no deben exponerse resultados
inconsistentes como una publicación aprobada.

La versión oficial anterior debe permanecer disponible
hasta completar correctamente la nueva publicación.

La implementación física de este mecanismo se desarrollará
inicialmente en local y posteriormente se adaptará
a Cloud Storage.


## 22. Ejecuciones sin cambios

Un manifest aprobado puede no producir cambios Gold.

Ejemplos:

- Registro macroeconómico sin cotizaciones relacionadas.
- Dataset Silver no consumido actualmente por Gold.
- Corrección sin modificación de campos utilizados.
- Publicación cuyo resultado analítico es equivalente.

En estos casos Gold debe:

1. Registrar el manifest recibido.
2. Verificar sus versiones y dependencias.
3. Comprobar que no existen cambios efectivos.
4. Conservar la versión Gold válida anterior.
5. Registrar GOLD_SIN_CAMBIOS.
6. Evitar reescrituras innecesarias.

El histórico actual de cotizaciones finaliza el
31 de diciembre de 2025.

Una nueva fecha macroeconómica de septiembre de 2026
podría no tener cotizaciones relacionadas.

No deben generarse cotizaciones artificiales para producir
una modificación analítica.


## 23. Estrategia de pruebas

Las pruebas locales utilizarán copias independientes de Silver
y Gold junto con manifests identificados como simulados.

Se deben implementar los siguientes escenarios:

| Código | Escenario | Resultado esperado |
|---|---|---|
| P01 | Manifest JSON válido | Aceptado estructuralmente |
| P02 | JSON malformado | Rechazado |
| P03 | Silver no aprobado | Rechazado |
| P04 | Versión incompatible | Rechazado |
| P05 | Dataset desconocido | Rechazado |
| P06 | Clave incorrecta | Rechazado |
| P07 | Conteos negativos | Rechazado |
| P08 | Inserción válida | Actualización incremental |
| P09 | Corrección válida | Actualización incremental |
| P10 | Eliminación no autorizada | Rechazado |
| P11 | Repetición del mismo lote | Sin duplicados |
| P12 | Mismo run_id con contenido diferente | Rechazado |
| P13 | Lote sin impacto Gold | GOLD_SIN_CAMBIOS |
| P14 | Error durante transformación | Sin publicación |
| P15 | Error durante validación | Sin publicación |
| P16 | Corrección que cambia partición | Actualizar origen y destino |
| P17 | Modificación SAB | Recalcular fecha completa |
| P18 | Modificación financiera | Recalcular grupo empresarial |
| P19 | Modificación documental | Recalcular expediente |
| P20 | Modificación de cotización | Recalcular impactos relacionados |
| P21 | Reconstrucción completa de referencia | Equivalencia lógica |
| P22 | Versiones Silver fuera de secuencia | Bloqueado |
| P23 | Ejecuciones concurrentes incompatibles | Bloqueadas |
| P24 | Manifest de prueba en producción | Rechazado |

Ninguna de estas pruebas debe considerarse ejecutada
únicamente por estar documentada.


## 24. Criterios de aceptación

El procesamiento incremental Gold se considerará aprobado
cuando se demuestre:

- Consumo de manifests válidos.
- Rechazo de manifests inválidos.
- Identificación correcta de dependencias.
- Identificación correcta de claves afectadas.
- Incorporación de nuevos registros.
- Procesamiento controlado de correcciones.
- Conservación de registros históricos no afectados.
- Unicidad de claves Gold.
- Reproducción correcta de indicadores.
- Ausencia de modificaciones fuera del alcance previsto.
- Idempotencia.
- Recuperación ante errores.
- Publicación segura.
- Equivalencia con una reconstrucción completa.
- Trazabilidad de versiones y ejecuciones.

La comparación contra una reconstrucción completa se utilizará
como referencia de validación y no como estrategia ordinaria
de procesamiento incremental.

La validación local no equivale a una certificación del
despliegue distribuido en Google Cloud.


## 25. Acuerdos pendientes con Silver

Se requiere confirmar conjuntamente:

1. Versión definitiva del formato manifest.
2. Nombres oficiales de los datasets.
3. Identificación de esquemas.
4. Estabilidad de claves de negocio.
5. Estabilidad de id_registro en principales_cuentas.
6. Formato del detalle de cambios.
7. Formato de claves anteriores y nuevas.
8. Formato de particiones afectadas.
9. Política de correcciones.
10. Política de eliminaciones.
11. Ubicación de manifests oficiales.
12. Versionado de rutas Silver.
13. Disponibilidad de versiones anteriores.
14. Versiones de las fuentes no modificadas.
15. Mecanismo de notificación a Gold.
16. Secuencia de publicaciones.
17. Política de reintentos.
18. Diferenciación entre prueba y producción.
19. Control de concurrencia.
20. Integración posterior con Google Cloud.


## 26. Estado actual

Completado:

- Recepción de la propuesta inicial de Daniela.
- Auditoría histórica de Silver.
- Construcción de cinco productos Gold.
- Validación histórica Gold.
- Definición de claves y granularidades.
- Identificación de dependencias.
- Contrato técnico Gold v1.1.
- Respaldo histórico.
- Publicación del código histórico en analytics-piero.

En desarrollo:

- Contrato del manifest Silver → Gold v0.1.

Pendiente:

- Aprobación conjunta del contrato definitivo.
- Implementación del lector de manifests.
- Implementación del validador.
- Implementación del planificador incremental.
- Implementación de procesadores incrementales Gold.
- Registro de ejecuciones.
- Publicación segura.
- Pruebas locales.
- Prueba conjunta con Daniela.
- Integración con Google Cloud.
- Publicación en BigQuery.
- Dashboards Looker Studio.

Este documento representa una propuesta de integración.

No acredita que el procesamiento incremental Gold,
la publicación segura Silver o la integración automática
entre ambos componentes estén implementados.


## 27. Historial de versiones

| Versión | Fecha | Descripción |
|---|---|---|
| 0.1 | 2026-11-14 | Propuesta inicial de contrato incremental Silver → Gold |
