# Informe de limitaciones BigQuery y Gold incremental

## 1. Identificacion

- Proyecto: bvl-analytics-2026
- Responsable Gold: Piero
- Rama: analytics-piero
- Commit base: e20bd5c068a87f7b586e2b0736f06d7d5ef8f42d
- Fecha: 2026-11-27
- Entorno de validacion: local
- Publicacion autorizada: NO

## 2. Alcance validado

Productos incluidos:

- mercado_valor_diario
- actividad_sab_diaria
- empresa_trimestre

La validacion utiliza Spark local, simulaciones Python y
pruebas del adaptador incremental basado en SQLite.

No constituye una certificacion de BigQuery en Google Cloud.

## 3. Resultados de las pruebas locales

| Control | Resultado |
|---|---|
| INSERT y UPDATE | Aprobado localmente |
| DELETE autorizado y no autorizado | Aprobado localmente |
| Idempotencia de operaciones | Aprobado localmente |
| Idempotencia de batch | Aprobado localmente |
| Concurrencia de dos procesos | Aprobado localmente |
| Trazabilidad run_id y source_version | Aprobado localmente |
| Cambios de negocio frente a metadatos | Aprobado localmente |
| Rechazos sin mutaciones | Aprobado localmente |
| Rollback por fallo de auditoria | Aprobado localmente |
| Sustitucion financiera candidata | Aprobado localmente |

Suite incremental: 8 pruebas ejecutadas y 8 aprobadas.

La ejecucion conjunta de los cuatro escenarios finalizo
con codigo de salida 0.

Dos pruebas financieras pertenecen a un archivo inicialmente
no incluido en el commit base.

## 4. Limitaciones de integracion confirmadas

### L01. Normalizacion de nemonicos

Severidad propuesta: Alta.

Evidencia:

- Nemonico anterior: DEMO1.
- Nemonico nuevo: " demo1 ".
- Claves afectadas detectadas: 2.
- Normalizacion canonica correcta: False.

El detector incremental distingue representaciones que
pueden corresponder al mismo nemonico logico.

Riesgo:

- Recalculos innecesarios.
- Inconsistencias de claves.
- Cruces empresariales incompletos.

Accion propuesta:

Acordar una normalizacion canonica Silver-Gold basada
en upper(trim()), validar colisiones y repetir las pruebas
antes de cualquier publicacion.

Estado: limitacion reproducida, no corregida.

### L02. Cobertura parcial SAB

Severidad propuesta: Alta.

| Indicador | Completo | Parcial |
|---|---:|---:|
| Registros SAB | 2 | 1 |
| Actividad diaria | 200 | 100 |
| Participacion SAB A | 50% | 100% |
| HHI | 5000 | 10000 |
| Actividad diaria valida | True | True |

No se imputaron registros faltantes.

Sin embargo, el constructor calcula indicadores sobre los
registros disponibles sin distinguir la cobertura parcial.

La politica Quality rechaza el fixture sintetico debido
a que exige cantidades historicas especificas.

Accion propuesta:

- Incorporar metadatos de cobertura declarados por Silver.
- Diferenciar validez aritmetica de cobertura completa.
- Mantener trazabilidad de consultas afectadas.
- Evitar imputacion artificial de datos faltantes.
- No debilitar la politica Quality historica.

Estado: limitacion reproducida, no corregida.

La cuarentena fue simulada mediante exclusion de un registro.
No se ejecuto el mecanismo real de cuarentena Silver.

### L03. Sustitucion financiera

Severidad propuesta: Alta para publicacion oficial.

Resultados sinteticos:

- Registro anterior eliminado de la candidata: True.
- Registro corregido incorporado: True.
- Registros anteriores: 2.
- Registros resultantes: 2.
- Diferencias no afectadas: 0.
- Diferencias de idempotencia: 0.
- ROA anterior: 10%.
- ROA actualizado: 20%.

El recalculador sustituye correctamente el grupo sintetico.

Esto no autoriza sustituciones financieras oficiales.

La clasificacion financiera probada en unittest se encuentra
definida dentro del archivo de prueba. Su integracion en el
pipeline productivo no queda acreditada.

Accion propuesta:

Validar identidad financiera, colisiones, procedencia
y autorizacion antes de aplicar sustituciones oficiales.

### L04. Continuidad y repeticion de versiones Gold

Severidad propuesta: Media.

La inspeccion de control_versiones_gold.py identifica que
la continuidad se valida antes de comprobar si el run_id
corresponde a una ejecucion previa.

Una repeticion posterior al avance de la version vigente
podria ser rechazada por continuidad incorrecta.

Estado: riesgo identificado mediante revision de codigo,
pendiente de reproduccion experimental.

Accion propuesta:

Evaluar una prueba especifica de replay posterior al cambio
de version, sin debilitar los controles de continuidad.

## 5. Limitaciones especificas de BigQuery

| Aspecto | Estado |
|---|---|
| Ejecucion real de DDL | No validada |
| Ejecucion real de MERGE | No validada |
| Transacciones SQL en BigQuery | No validadas |
| Concurrencia distribuida en BigQuery | No validada |
| Rollback real ante fallo de auditoria | No validado |
| Compatibilidad efectiva de esquemas | Pendiente |
| Permisos IAM de ejecucion | No verificados aqui |
| Costos y bytes procesados | No medidos |
| Rendimiento con volumen real | No medido |
| Integracion GCS-BigQuery | No validada aqui |
| Publicacion oficial | No autorizada |

SQLite y las simulaciones Python permiten verificar
comportamientos locales, pero no reproducen todas las
garantias ni restricciones del motor BigQuery.

## 6. Validaciones previas a una ejecucion cloud

1. Confirmar contratos Silver vigentes.
2. Acordar normalizacion canonica de nemonicos.
3. Definir tratamiento de cobertura parcial SAB.
4. Acreditar identidad y sustitucion financiera.
5. Validar los SQL en un entorno BigQuery autorizado.
6. Verificar permisos IAM y ubicacion de los datasets.
7. Evaluar concurrencia, auditoria y rollback reales.
8. Medir bytes procesados y costos.
9. Obtener las aprobaciones de Quality correspondientes.
10. Conservar las versiones oficiales de productos no afectados.

Estas actividades quedan pendientes. No se ejecutaron como
parte de la validacion local.

## 7. Productos fuera del alcance funcional conjunto

- hechos_empresa
- impacto_hechos_valor

Ambos permanecen pendientes de las dependencias y
acreditaciones Quality correspondientes.

## 8. Evidencias disponibles

- MATRIZ_SILVER_GOLD.md
- fixtures/prueba_nemonicos.py
- fixtures/prueba_cuarentena_sab.py
- fixtures/prueba_sustitucion_financiera.py
- reproducir_pruebas.sh
- evidencias/resultados_pruebas.txt
- evidencias/SHA256SUMS.txt
- tests/bigquery/test_sustitucion_financiera_local.py

## 9. Conclusion

La validacion local conjunta fue ejecutada satisfactoriamente.

Los cuatro escenarios solicitados fueron evaluados:

- Dos escenarios con limitaciones reproducidas.
- Dos escenarios aprobados dentro del alcance local.

Las ocho pruebas de la suite incremental fueron aprobadas.

Los resultados permiten entregar evidencia tecnica
reproducible para revision de integracion.

No permiten declarar BigQuery productivo validado,
Quality aprobado ni publicacion oficial autorizada.

PUBLICACION_AUTORIZADA: False
