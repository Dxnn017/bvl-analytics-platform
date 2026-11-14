
# Contrato de datos Silver → Gold

## 1. Información general

- Proyecto: BVL Analytics.
- Responsable: Piero Alexis Mendoza Santos.
- Rama: analytics-piero.
- Componente: Gold, BigQuery y dashboards.
- Fecha de actualización: 14/11/2026.
- Versión del contrato: 1.1.
- Versión anterior: 1.0.
- Estado: Construcción histórica implementada y validada localmente.
- Aprobación conjunta del equipo: pendiente.
- Procesamiento incremental: pendiente de implementación.
- Despliegue en Google Cloud: pendiente.

## 2. Objetivo

Establecer las reglas de integración, calidad, transformación,
procesamiento incremental y publicación de los datasets Silver
para construir productos analíticos orientados al mercado
bursátil peruano.

La capa Gold proporciona indicadores bursátiles, financieros
y empresariales destinados al consumo analítico mediante
BigQuery, Looker Studio y los componentes predictivos
del proyecto.

Los productos Gold deben conservar la trazabilidad hacia
Silver, mantener su granularidad y evitar la pérdida,
duplicación o modificación injustificada de información.

## 3. Fuente de datos

La fuente histórica inicial corresponde al paquete Silver
entregado por Daniela:

- Archivo: bvl_silver_20261109.tar.gz.
- Integridad SHA-256: verificada correctamente.
- Formato: Parquet con compresión Snappy.
- Datasets disponibles: 16.
- Datasets leídos correctamente: 16.
- Errores de lectura: 0.

Ruta local de entrada:

/home/piero/bvl_entregas/silver_20261109/silver

Ruta local Gold validada:

/home/piero/bvl_entregas/gold_staging_20261114

Ruta de respaldos:

/home/piero/bvl_entregas/gold_respaldos_20261114

Las rutas de entrada y salida se parametrizan para facilitar
la ejecución local y su futura adaptación a HDFS y Google
Cloud Storage.

La ejecución histórica documentada utiliza almacenamiento
local y no constituye una publicación en Google Cloud.

## 4. Inventario Silver

| Dominio | Dataset | Registros |
|---|---|---:|
| Mercado | cotizaciones | 83130 |
| Mercado | montos_sab | 22156 |
| Mercado | indices | 16 |
| Macroeconomía | tipo_cambio | 7751 |
| Macroeconomía | embig | 7481 |
| Finanzas | principales_cuentas | 5249 |
| Finanzas | situacion_financiera | 18983 |
| Finanzas | estado_resultados | 11609 |
| Finanzas | flujo_efectivo | 16912 |
| Finanzas | cambios_patrimonio | 512 |
| Eventos | eventos | 346 |
| Eventos | documentos | 990 |
| Eventos | contenido_textual | 990 |
| Eventos | identidad_empresarial | 338 |
| Referencias | puente_ruc_nemonico | 831 |
| Referencias | valores_inscritos | 1008 |

## 5. Selección de fuentes

### 5.1. Fuentes principales

- mercado_diario/cotizaciones
- mercado_diario/montos_sab
- macroeconomia/tipo_cambio
- macroeconomia/embig
- finanzas_empresariales/principales_cuentas
- hechos_importancia/eventos
- hechos_importancia/documentos
- hechos_importancia/contenido_textual
- hechos_importancia/identidad_empresarial
- referencias_smv/puente_ruc_nemonico

### 5.2. Fuentes complementarias

- mercado_diario/indices
- finanzas_empresariales/situacion_financiera
- finanzas_empresariales/estado_resultados
- finanzas_empresariales/flujo_efectivo
- finanzas_empresariales/cambios_patrimonio
- referencias_smv/valores_inscritos

Las fuentes complementarias se conservan en Silver.

No se incorporan directamente a los productos Gold actuales
debido a sus limitaciones de cobertura, granularidad o
necesidad de validaciones adicionales.

El dataset de índices contiene 16 registros correspondientes
a una sola fecha: 2025-01-06.

Por ello, no se utiliza para construir tendencias históricas
ni calcular retornos anormales ajustados al mercado.

Los estados financieros detallados presentan cobertura
temporal parcial y requieren una integración específica.

## 6. Inventario consolidado Gold

| Producto | Registros | Columnas | Parquet |
|---|---:|---:|---:|
| mercado_valor_diario | 83130 | 40 | 60 |
| actividad_sab_diaria | 22156 | 33 | 60 |
| empresa_trimestre | 5249 | 58 | 20 |
| hechos_empresa | 346 | 54 | 4 |
| impacto_hechos_valor | 175 | 61 | 1 |
| TOTAL | 111056 | - | 145 |

Los conteos corresponden al snapshot Silver histórico
del 09/11/2026.

Los conteos no constituyen restricciones permanentes para
futuros lotes incrementales.

### 6.1. Organización física

Formato: Apache Parquet.

Compresión: Snappy.

Rutas relativas:

- mercado/mercado_valor_diario
- mercado/actividad_sab_diaria
- empresas/empresa_trimestre
- eventos/hechos_empresa
- eventos/impacto_hechos_valor

Particionamiento:

| Producto | Particiones |
|---|---|
| mercado_valor_diario | anio, mes |
| actividad_sab_diaria | anio, mes |
| empresa_trimestre | anio, trimestre |
| hechos_empresa | anio |
| impacto_hechos_valor | anio |

La construcción histórica utiliza escritura protegida
mediante errorifexists para impedir sobrescrituras
accidentales.

El particionamiento incremental y la publicación de
versiones deberán implementarse de manera controlada.

## 7. Productos analíticos Gold

### 7.1. mercado_valor_diario

Objetivo:

Analizar el comportamiento de los instrumentos bursátiles
y su contexto macroeconómico.

Fuentes:

- cotizaciones
- tipo_cambio
- embig
- puente_ruc_nemonico

Granularidad:

Una fila por instrumento bursátil y fecha de cotización.

Clave:

- nemonico
- fecha

Resultados históricos:

- Registros: 83130.
- Columnas: 40.
- Nemónicos distintos: 656.
- Fechas de cotización: 1221.
- Fecha inicial: 2021-01-04.
- Fecha final: 2025-12-31.
- Particiones mensuales: 60.
- Archivos Parquet: 60.

Indicadores principales:

- precio_anterior
- precio_cierre
- precio_apertura
- precio_maximo
- precio_minimo
- precio_promedio
- monto_negociado
- variacion_precio
- retorno_diario_pct
- rango_intradia
- rango_intradia_pct
- dias_desde_cierre_anterior
- tipo_cambio_venta
- embig_peru

Indicadores de calidad:

- precio_anterior_valido
- precio_cierre_valido
- rango_intradia_valido
- retorno_calculable
- monto_negociado_valido
- monto_negociado_positivo
- tipo_cambio_disponible
- embig_disponible
- macro_disponible
- empresa_vinculada
- inscripcion_compatible

Reglas:

1. Conservar todas las cotizaciones Silver.
2. Integrar variables macroeconómicas por fecha.
3. Integrar el puente SMV por nemónico.
4. Utilizar LEFT JOIN.
5. No multiplicar registros.
6. Calcular retornos con precios finitos y positivos.
7. Mantener nulos los retornos con denominadores inválidos.
8. Conservar los instrumentos sin vinculación empresarial.
9. No sustituir tipos de cambio faltantes por cero.
10. No inventar precios ni información empresarial.
11. Mantener la moneda como referencia del snapshot SMV.
12. No asumir que la moneda de referencia es históricamente
    válida para todas las cotizaciones.
13. No agregar montos con unidades monetarias incompatibles.

Fórmulas:

Variación absoluta:

precio_cierre - precio_anterior

Retorno porcentual:

((precio_cierre / precio_anterior) - 1) * 100

Rango intradiario:

precio_maximo - precio_minimo

Rango intradiario porcentual:

(rango_intradia / precio_anterior) * 100

Distancia temporal:

fecha - fecha_anterior

El campo retorno_diario_pct representa el retorno respecto
al cierre anterior reportado.

No implica necesariamente que las cotizaciones estén
separadas por un solo día calendario.

En los dashboards deberá utilizarse una descripción
compatible con esta definición.

### 7.2. actividad_sab_diaria

Objetivo:

Analizar la actividad, participación y concentración de
las sociedades agentes de bolsa.

Fuente:

- montos_sab

Granularidad:

Una fila por sociedad agente de bolsa y fecha.

Clave:

- codigo_sab
- fecha

Resultados históricos:

- Registros: 22156.
- Columnas: 33.
- Sociedades agentes de bolsa distintas: 22.
- Fechas de negociación: 1221.
- Periodo: 2021-01-04 a 2025-12-31.
- Particiones mensuales: 60.
- Archivos Parquet: 60.

Indicadores:

- monto_compras
- monto_ventas
- monto_total
- saldo_neto
- participacion_compras_pct
- participacion_ventas_pct
- participacion_total_pct
- ranking_diario
- aporte_hhi
- hhi_diario
- total_compras_diario
- total_ventas_diario
- total_actividad_diaria
- cantidad_sab_diaria

Reglas:

1. Conservar los importes monetarios reportados.
2. Utilizar Decimal(24,2) para los montos.
3. Calcular participaciones sobre grupos diarios completos.
4. Conservar los porcentajes originales.
5. Comparar porcentajes originales y recalculados.
6. Validar que los montos sean finitos y no negativos.
7. Aplicar una tolerancia monetaria de 0.01.
8. No unir directamente filas SAB con cotizaciones.
9. Utilizar agregaciones con granularidades compatibles.
10. No interpretar compras más ventas como volumen único
    de transacciones del mercado.
11. No sumar indicadores diarios repetidos por cada SAB.

Fórmulas:

Saldo neto:

monto_compras - monto_ventas

Diferencia monetaria:

monto_total - monto_compras - monto_ventas

Consistencia monetaria:

abs(diferencia_monetaria) <= 0.01

Participación de compras:

(monto_compras / total_compras_diario) * 100

Participación de ventas:

(monto_ventas / total_ventas_diario) * 100

Participación total:

(monto_total / total_actividad_diaria) * 100

Ranking diario:

Orden descendente por monto_total.

Desempate:

Orden ascendente por codigo_sab.

Aporte HHI:

participacion_total_pct al cuadrado.

HHI diario:

Suma de los aportes HHI de todas las SAB de una fecha.

El HHI mide concentración de la actividad reportada
por las sociedades agentes de bolsa.

No representa concentración de propiedad empresarial
ni concentración de posiciones de inversión.

Los indicadores dependientes de una fecha deberán
recalcularse para todas las SAB de esa fecha cuando
se inserte, modifique o elimine un registro.

### 7.3. empresa_trimestre

Objetivo:

Analizar el desempeño financiero empresarial.

Fuente:

- principales_cuentas

Granularidad:

Una fila por registro financiero reportado.

Identificador único:

- id_registro

Clave de agrupación empresarial:

- rpj
- ejercicio
- trimestre_num

Resultados históricos:

- Registros: 5249.
- Columnas: 58.
- RPJ distintos: 295.
- Periodo: 2021-T1 a 2025-T4.
- Particiones trimestrales: 20.
- Archivos Parquet: 20.

Indicadores:

- activo_total
- pasivo_total
- patrimonio_total
- total_ingreso
- utilidad_neta
- roa_aproximado_pct
- roe_aproximado_pct
- endeudamiento_pct
- margen_neto_pct
- autonomia_financiera_pct
- resultado_financiero

Indicadores de calidad:

- colision_financiera
- diferencia_contable
- identidad_contable_valida
- sin_incidencias_estructurales
- apto_kpi_general
- roa_calculable
- roe_calculable
- endeudamiento_calculable
- margen_neto_calculable
- autonomia_calculable
- patrimonio_negativo
- motivos_calidad

Reglas:

1. Conservar todos los registros financieros.
2. Mantener el identificador id_registro.
3. No eliminar las colisiones de clave de negocio.
4. Excluir registros conflictivos de los KPIs oficiales.
5. Validar la identidad contable.
6. No dividir entre cero.
7. Conservar las utilidades negativas.
8. Conservar e identificar patrimonios negativos.
9. Normalizar las etiquetas monetarias a PEN y USD.
10. Mantener la moneda original para trazabilidad.
11. No agregar importes de diferentes monedas.
12. No sumar resultados de distintos periodos sin confirmar
    si los estados son trimestrales o acumulados.
13. No asumir que id_registro permanece estable después de
    correcciones de origen.
14. No duplicar indicadores por cada nemónico de una empresa.

Fórmulas:

Diferencia contable:

activo_total - pasivo_total - patrimonio_total

ROA aproximado:

(utilidad_neta / activo_total) * 100

ROE aproximado:

(utilidad_neta / patrimonio_total) * 100

Endeudamiento:

(pasivo_total / activo_total) * 100

Margen neto:

(utilidad_neta / total_ingreso) * 100

Autonomía financiera:

(patrimonio_total / activo_total) * 100

Condiciones:

- Registros aptos para el KPI correspondiente.
- Valores numéricos finitos.
- Denominadores estrictamente positivos.
- Ausencia de colisiones pendientes.
- Identidad contable válida.
- Periodo y moneda válidos.

La tolerancia contable utilizada es una unidad.

Esta tolerancia es provisional y requiere confirmación
de las unidades monetarias y reglas de origen.

ROA y ROE utilizan saldos de cierre, no saldos promedio.

Estos indicadores son aproximados y no están anualizados.

### 7.4. hechos_empresa

Objetivo:

Analizar los acontecimientos empresariales y la
disponibilidad de documentación asociada.

Fuentes:

- eventos
- identidad_empresarial
- documentos
- contenido_textual

Granularidad:

Una fila por expediente.

Clave:

- numero_expediente

Resultados históricos:

- Registros: 346.
- Columnas: 54.
- Archivos Parquet: 4.
- Particionamiento por año.
- Fecha histórica mínima: 2010-06-23.
- Fecha histórica máxima: 2025-01-31.
- Cobertura principal: enero de 2025.

Indicadores principales:

- empresa_identificada
- cantidad_nemonicos
- tiene_nemonicos
- total_documentos
- documentos_duplicados
- documentos_no_duplicados
- documentos_principales
- documentos_con_texto
- textos_suficientes
- textos_insuficientes
- textos_principales
- textos_principales_suficientes
- documentos_principales_con_texto_suficiente
- documentos_con_incidencia_referencial
- tamano_documental_bytes
- caracteres_textuales_totales
- cobertura_textual_pct
- suficiencia_textual_pct
- suficiencia_textual_principal_pct
- duplicidad_documental_pct
- evento_principal
- integridad_documental_valida
- candidato_impacto_bursatil
- dias_acuerdo_presentacion
- motivos_calidad_gold

Reglas:

1. Conservar todos los expedientes.
2. Diferenciar eventos principales y regularizaciones.
3. Utilizar numero_expediente para integrar eventos.
4. Utilizar guid_documento para integrar documentos y textos.
5. Agregar documentos antes de unirlos con los eventos.
6. Evitar multiplicar filas por cantidad de documentos.
7. Conservar los indicadores de calidad documental.
8. No duplicar innecesariamente los textos completos.
9. Mantener trazabilidad hacia Silver.
10. Identificar los candidatos al análisis bursátil.
11. No interpretar la cobertura documental como evidencia
    automática de suficiencia del contenido.
12. Conservar eventos sin identificación empresarial.

Fórmulas:

Cobertura textual:

(documentos_con_texto / total_documentos) * 100

Suficiencia textual:

(textos_suficientes / total_documentos) * 100

Suficiencia textual principal:

(textos_principales_suficientes / textos_principales) * 100

Duplicidad documental:

(documentos_duplicados / total_documentos) * 100

Las métricas se mantienen nulas cuando su denominador
no es positivo.

La asociación de un documento con una fila de contenido
textual no garantiza que su texto sea suficiente.

### 7.5. impacto_hechos_valor

Objetivo:

Analizar el comportamiento bursátil observado alrededor
de acontecimientos empresariales.

Fuentes Gold directas:

- hechos_empresa
- mercado_valor_diario

La identidad empresarial se incorpora mediante
hechos_empresa.

Granularidad:

Una fila por expediente e instrumento bursátil.

Clave:

- numero_expediente
- nemonico

Resultados históricos:

- Registros: 175.
- Columnas: 61.
- Eventos candidatos: 136.
- Instrumentos candidatos: 66.
- Archivos Parquet: 1.
- Particionamiento por año.
- Periodo de eventos: 2025-01-02 a 2025-01-31.

Metodología:

CIERRES_CERCANOS_7D_SIN_DIA_EVENTO_V1

Ventana temporal:

Siete días calendario anteriores y siete días calendario
posteriores a la fecha de presentación.

No se utiliza la cotización correspondiente al mismo
día del evento.

Selección anterior:

Cierre válido más cercano estrictamente anterior al
acontecimiento, dentro de siete días calendario.

Selección posterior:

Cierre válido más cercano estrictamente posterior al
acontecimiento, dentro de siete días calendario.

Se considera válido un precio de cierre cuando es
finito, no nulo y estrictamente positivo.

Indicadores:

- fecha_anterior
- fecha_posterior
- precio_anterior
- precio_posterior
- monto_anterior
- monto_posterior
- moneda_anterior
- moneda_posterior
- cotizaciones_ventana_anterior
- cotizaciones_ventana_posterior
- dias_desde_precio_anterior
- dias_hasta_precio_posterior
- dias_entre_cotizaciones
- ventana_dias
- cobertura_completa
- retorno_calculable
- variacion_precio
- retorno_observado_pct
- direccion_movimiento
- variacion_monto_negociado_pct
- variacion_tipo_cambio_pct
- variacion_embig
- estado_cobertura
- motivos_calidad

Fórmulas:

Variación absoluta:

precio_posterior - precio_anterior

Retorno observado:

((precio_posterior / precio_anterior) - 1) * 100

Variación del monto negociado:

((monto_posterior / monto_anterior) - 1) * 100

Variación del tipo de cambio:

((tipo_cambio_posterior / tipo_cambio_anterior) - 1) * 100

Variación EMBIG:

embig_posterior - embig_anterior

Reglas:

1. Utilizar únicamente eventos candidatos.
2. Conservar todos los pares evento-valor candidatos.
3. No exigir cobertura completa para conservar una fila.
4. No utilizar cotizaciones del día del acontecimiento.
5. No inventar precios anteriores o posteriores.
6. No calcular retornos sin cobertura completa.
7. No calcular retornos cuando exista un conflicto
    identificado de moneda de referencia.
8. No interpretar una moneda de referencia desconocida
    como confirmación de equivalencia monetaria histórica.
9. Calcular la variación del monto solamente cuando
    los montos sean válidos, el monto anterior sea
    positivo y exista coincidencia de moneda de referencia.
10. Conservar indicadores de cobertura y calidad.
11. No interpretar las variaciones como evidencia causal.
12. No presentar los precios como ajustados por dividendos
    o eventos corporativos sin confirmación.
13. No utilizar información posterior al evento como
    característica predictiva disponible antes del evento.

Limitaciones:

- No se confirma el ajuste histórico de los precios.
- No se confirma la moneda histórica de cada cotización.
- No se dispone de un índice histórico suficiente.
- No se calculan retornos anormales ajustados al mercado.
- Pueden existir eventos simultáneos y ventanas superpuestas.
- Los resultados no representan un estudio causal.

## 8. Relaciones y cardinalidades

### 8.1. Cotizaciones y macroeconomía

Relaciones:

cotizaciones.fecha_cotizacion = tipo_cambio.fecha

cotizaciones.fecha_cotizacion = embig.fecha

Cardinalidad: muchos a uno.

Resultados:

- Fechas bursátiles: 1221.
- Fechas presentes en tipo de cambio: 1221.
- Fechas presentes en EMBIG: 1221.
- Cotizaciones antes de integrar: 83130.
- Cotizaciones después de integrar: 83130.
- Tipo de cambio nulo: 301.
- EMBIG nulo: 0.

### 8.2. Cotizaciones y referencias empresariales

Relación:

cotizaciones.valor = puente_ruc_nemonico.nemonico_valor

Cardinalidad observada: muchos a uno.

Resultados:

- Pares RUC-nemónico: 831.
- RUC distintos: 148.
- Nemónicos ambiguos: 0.
- Valores cotizados distintos: 656.
- Valores vinculados: 109.
- Valores sin vínculo: 547.
- Cotizaciones con RUC: 26557.
- Cotizaciones sin RUC: 56573.

Se utiliza LEFT JOIN para conservar el mercado completo.

La referencia actual no garantiza la vigencia histórica
de todas las relaciones.

No debe asumirse que todos los valores sin vinculación
corresponden a instrumentos extranjeros.

### 8.3. Finanzas y referencias empresariales

Relación candidata:

principales_cuentas.ruc = puente_ruc_nemonico.ruc

Resultados:

- RUC financieros válidos distintos: 289.
- RUC vinculados: 148.
- RUC sin vínculo: 141.

Una empresa puede tener varios instrumentos.

No se incorpora esta relación directamente a la tabla
empresa_trimestre para evitar la multiplicación de
registros financieros.

### 8.4. Eventos y documentos

Relación:

eventos.numero_expediente = documentos.numero_expediente

Cardinalidad: uno a muchos.

Los documentos se agregan por expediente antes de
integrarse con los acontecimientos.

### 8.5. Documentos y contenido textual

Relación:

documentos.guid_documento = contenido_textual.guid_documento

Cardinalidad observada: uno a uno.

También se verifica la correspondencia del expediente.

No se detectaron registros huérfanos en el snapshot
histórico auditado.

### 8.6. Eventos e identidad empresarial

Relación:

eventos.numero_expediente =
identidad_empresarial.numero_expediente

Cardinalidad observada: uno a cero o uno.

El arreglo de nemónicos se utiliza para reconstruir
los candidatos al análisis bursátil.

### 8.7. Impacto y cotizaciones

Relación:

hechos_empresa.nemonicos_cotizables =
mercado_valor_diario.nemonico

La relación requiere una restricción temporal.

No se deben unir todos los eventos con todas las
cotizaciones de un instrumento sin controlar la
granularidad de salida.

## 9. Resultados de calidad histórica

### 9.1. Mercado bursátil

- Cotizaciones conservadas: 83130.
- Cotizaciones faltantes: 0.
- Cotizaciones adicionales: 0.
- Claves inválidas: 0.
- Claves duplicadas: 0.
- Retornos nulos: 372.
- Montos negociados iguales a cero: 32.
- Precios negativos: 0.
- Indicadores no finitos: 0.
- Precios originales modificados: 0.

Las incidencias de origen se conservan mediante
indicadores de calidad.

### 9.2. Macroeconomía

Tipos de cambio nulos:

| Fecha | Cotizaciones |
|---|---:|
| 2023-01-02 | 24 |
| 2024-01-02 | 46 |
| 2024-07-26 | 64 |
| 2024-12-31 | 58 |
| 2025-12-26 | 109 |
| TOTAL | 301 |

La auditoría complementaria confirma que los valores
nulos de Gold corresponden a valores nulos de Silver.

No se utilizan datos futuros para completar valores
históricos.

### 9.3. Sociedades agentes de bolsa

Resultados:

- Registros: 22156.
- Montos inválidos: 0.
- Inconsistencias monetarias después de aplicar
  la tolerancia decimal: 0.
- Participaciones totales nulas: 0.
- Rankings nulos: 0.
- Participación diaria recalculada: aproximadamente 100%.
- HHI diario mínimo: 1148.7809.
- HHI diario máximo: 9645.7677.
- HHI diario promedio: 2838.4715.

Los porcentajes originales de participación diaria
presentaban valores entre 99.96% y 100.04%.

Se conservan los porcentajes originales y se calculan
nuevas participaciones sobre los importes reportados.

### 9.4. Colisiones financieras

Se detectaron tres claves empresariales repetidas
correspondientes a seis registros.

Empresa:

BNB VALORES PERU S.A. SOCIEDAD AGENTE DE BOLSA.

RPJ:

SG0005.

Periodos:

- 2021-T1.
- 2021-T2.
- 2021-T3.

Los registros contienen importes distintos.

Se conservan las seis filas, pero se excluyen de los
KPIs financieros oficiales mientras no exista una
regla de resolución validada.

### 9.5. Identidad contable

Registros con diferencias absolutas superiores a una unidad:

- J40886, 2023-T4: -348162.
- OE5376, 2023-T1: -648661.

Ambos presentan activo total igual a cero.

No se modifican los importes de origen.

La tolerancia de una unidad permanece pendiente de
confirmación con las reglas contables de origen.

### 9.6. Elegibilidad financiera

| Indicador | Registros |
|---|---:|
| Registros conservados | 5249 |
| Colisiones financieras | 6 |
| Identidad contable inválida | 2 |
| Aptos para KPI general | 5241 |
| ROA calculable | 5237 |
| ROE calculable | 5186 |
| Endeudamiento calculable | 5237 |
| Margen neto calculable | 4839 |
| Autonomía financiera calculable | 5237 |

La elegibilidad general no garantiza que todos los
indicadores individuales sean calculables.

Cada KPI debe utilizar su indicador de elegibilidad.

Clasificación financiera:

- Ganancia: 3836.
- Pérdida: 1395.
- Equilibrio: 10.
- No evaluable: 8.

### 9.7. Monedas financieras

- PEN: 4717 registros.
- USD: 532 registros.

Se conserva la etiqueta monetaria original.

No se agregan importes de distintas monedas sin una
metodología de conversión validada.

Las unidades monetarias de los estados financieros
requieren confirmación antes de publicar agregados
monetarios absolutos.

### 9.8. Eventos empresariales

| Indicador | Cantidad |
|---|---:|
| Eventos totales | 346 |
| Eventos principales | 338 |
| Regularizaciones históricas | 8 |
| Eventos identificados | 292 |
| Eventos sin identificar | 54 |
| Eventos principales sin identificar | 46 |
| Documentos asociados | 990 |
| Documentos duplicados | 1 |
| Documentos con fila textual asociada | 990 |
| Textos suficientes | 897 |
| Textos insuficientes | 93 |
| Textos principales | 428 |
| Textos principales suficientes | 427 |
| Incidencias referenciales | 0 |
| Eventos candidatos a impacto | 136 |
| Pares evento-valor candidatos | 175 |

La cobertura de eventos principales corresponde
a enero de 2025.

No debe generalizarse esta muestra como si cubriera
todos los acontecimientos ocurridos entre 2021 y 2025.

### 9.9. Impacto bursátil

Resultados de la ventana oficial:

| Indicador | Cantidad |
|---|---:|
| Pares candidatos | 175 |
| Pares con precio anterior | 74 |
| Pares con precio posterior | 86 |
| Pares completos | 66 |
| Pares incompletos | 109 |
| Pares únicamente con precio anterior | 8 |
| Pares únicamente con precio posterior | 20 |
| Pares sin ambos precios | 81 |
| Retornos calculables | 66 |
| Variaciones de monto calculables | 57 |
| Conflictos de moneda de referencia | 0 |

Cobertura de pares:

66 de 175 = 37.71%.

Dirección del movimiento:

- Subida: 23.
- Bajada: 25.
- Sin variación: 18.

Retorno observado:

- Mínimo: aproximadamente -7.6375%.
- Máximo: aproximadamente 12.4324%.
- Promedio: aproximadamente 0.0606%.
- Mediana: 0%.

Los 66 pares completos presentan montos negociados
positivos en ambos cierres seleccionados.

### 9.10. Sensibilidad temporal

| Ventana | Completos | Incompletos | Cobertura |
|---|---:|---:|---:|
| 7 días | 66 | 109 | 37.71% |
| 15 días | 83 | 92 | 47.43% |
| 30 días | 97 | 78 | 55.43% |

Diagnóstico de pares incompletos:

- Recuperables en 15 días: 17.
- Recuperables en 30 días adicionales: 14.
- Cierres fuera de 30 días: 55.
- Sin cierre posterior histórico: 21.
- Sin cierre anterior histórico: 2.

La cobertura se reprodujo directamente desde Silver.

Las ventanas alternativas son exclusivamente
diagnósticas y no modifican la metodología Gold.

La ventana oficial se mantiene en siete días calendario.

No se atribuyen automáticamente las ausencias de
cotizaciones a iliquidez, suspensión o deslistado.

### 9.11. Cobertura por evento

- Eventos candidatos: 136.
- Eventos con algún instrumento evaluable: 60.
- Eventos con todos los instrumentos evaluables: 40.
- Eventos con cobertura parcial: 20.
- Eventos sin instrumentos evaluables: 76.
- Cobertura de eventos candidatos: 44.12%.

La cobertura por evento debe diferenciarse de la
cobertura por par evento-valor.

### 9.12. Cobertura de instrumentos

- Instrumentos candidatos: 66.
- Instrumentos con cotizaciones en enero de 2025: 38.
- Instrumentos sin cotizaciones en enero de 2025: 28.
- Instrumentos con cobertura completa en siete días: 27.
- Pares sin cotizaciones durante enero de 2025: 64.
- Pares con cotización el mismo día del evento: 45.

La cotización del mismo día no se utiliza en la
metodología oficial.

## 10. Optimización física

La primera construcción de actividad_sab_diaria
generó 450 archivos Parquet.

Se optimizó su escritura mediante reparticionamiento
por año y mes antes del almacenamiento.

Resultados:

| Indicador | Original | Optimizado |
|---|---:|---:|
| Registros SAB | 22156 | 22156 |
| Columnas | 33 | 33 |
| Archivos Parquet SAB | 450 | 60 |
| Espacio ocupado aproximado | 9.4M | 3.4M |
| Particiones mensuales | 60 | 60 |

Reducción de archivos SAB: 86.67%.

Reducción aproximada del espacio ocupado: 63.83%.

Se ejecutaron:

- 16 controles independientes sobre la versión optimizada.
- 15 controles de comparación entre versiones.
- Comparación exacta de registros en ambas direcciones.
- Verificación de claves.
- Verificación de columnas.
- Comparación de valores.
- Verificación de particiones.
- Verificación física de la copia incorporada a staging.

Resultado:

Equivalencia lógica confirmada entre las versiones
original y optimizada.

La versión original permanece respaldada.

Inventario Gold global:

- Archivos antes de optimizar: 535.
- Archivos después de optimizar: 145.
- Reducción global: 72.90%.
- Espacio ocupado aproximado actual: 12 MiB.

Los tamaños corresponden a estimaciones de du y
pueden variar según el sistema de archivos.

## 11. Procesamiento incremental

Estado: diseño pendiente de implementación y pruebas.

Gold deberá actualizarse únicamente después de una
publicación aprobada de Silver.

No se procesarán lotes rechazados o incompletos.

### 11.1. Contrato de entrada incremental

Cada lote deberá proporcionar, como mínimo:

- Identificador único del lote.
- Versión Silver publicada.
- Versión del esquema.
- Fecha y hora de publicación.
- Estado de validación.
- Datasets modificados.
- Rutas de datos aprobados.
- Tipo de operación.
- Claves afectadas.
- Particiones afectadas.
- Registros recibidos.
- Registros válidos.
- Registros rechazados.
- Información de correcciones.
- Información de eliminaciones autorizadas.
- Referencia al lote anterior cuando corresponda.

El formato definitivo del manifest deberá acordarse
con Daniela y Lizeth.

Un archivo ausente no se interpretará automáticamente
como eliminación de registros.

### 11.2. Reglas generales

1. No procesar versiones Silver rechazadas.
2. Identificar los cambios reales del lote.
3. Identificar todas las dependencias directas e indirectas.
4. Recalcular las unidades afectadas.
5. Conservar los registros históricos no afectados.
6. Ejecutar las transformaciones en staging.
7. Validar integridad, claves y métricas.
8. Publicar únicamente resultados aprobados.
9. Garantizar idempotencia.
10. Registrar inserciones, modificaciones y eliminaciones.
11. Conservar trazabilidad del lote.
12. Permitir recuperación ante fallos.
13. Evitar sobrescribir la versión válida antes de aprobar
    completamente la nueva versión.
14. Evitar publicaciones parciales entre productos dependientes.
15. Gestionar concurrencia y reintentos de forma controlada.

### 11.3. Dependencias incrementales

| Fuente Silver modificada | Productos Gold afectados |
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

Dependencias entre productos:

mercado_valor_diario -> impacto_hechos_valor

hechos_empresa -> impacto_hechos_valor

Los cambios en fuentes macroeconómicas, empresariales
o documentales pueden propagarse indirectamente
al producto impacto_hechos_valor.

### 11.4. Incrementalidad de mercado

Clave afectada:

- nemonico
- fecha

Una inserción o corrección de cotización requiere
recalcular la fila correspondiente.

Si Silver modifica los cierres anteriores reportados
de otras filas, también deberán incluirse esas claves.

Una corrección macroeconómica requiere actualizar
todas las cotizaciones de la fecha afectada.

Un cambio en el puente SMV requiere identificar
las cotizaciones del nemónico afectado.

Las modificaciones de identidad pueden afectar
múltiples particiones históricas.

Las eliminaciones deberán procesarse únicamente
cuando estén autorizadas y sean identificables.

### 11.5. Incrementalidad SAB

Unidad lógica de recálculo:

- fecha

Una modificación individual puede cambiar:

- Totales diarios.
- Participaciones.
- Ranking.
- Aportes HHI.
- HHI diario.
- Indicadores de validez de la fecha.

Por ello, se recalculará el grupo diario completo.

No es suficiente actualizar únicamente la fila
de la sociedad agente de bolsa modificada.

### 11.6. Incrementalidad financiera

Unidad lógica de recálculo:

- rpj
- ejercicio
- trimestre_num

Los cambios deben considerar el grupo financiero
completo para recalcular las colisiones.

La identificación de registros nuevos, corregidos o
eliminados requiere confirmar el comportamiento de
id_registro en Silver.

Cuando un registro cambie de grupo, deberán considerarse
tanto el grupo original como el nuevo.

No se utilizará únicamente id_registro como criterio
de actualización hasta verificar su estabilidad.

### 11.7. Incrementalidad de hechos empresariales

Unidad lógica de recálculo:

- numero_expediente

Una modificación de documentos, textos o identidad
requiere recalcular los indicadores del expediente.

Cuando un documento cambie de expediente se deberán
recalcular los expedientes anterior y nuevo.

La modificación del arreglo de nemónicos requiere
reconstruir los pares candidatos de impacto.

### 11.8. Incrementalidad de impacto bursátil

Unidad lógica de recálculo:

- numero_expediente
- nemonico

Una modificación de evento requiere recalcular
sus pares candidatos.

Una modificación de cotización puede afectar los
eventos del mismo nemónico que se encuentren dentro
de siete días calendario de la fecha modificada,
excluyendo la fecha del propio evento.

Una nueva cotización puede reemplazar la referencia
anterior o posterior seleccionada.

Una eliminación puede provocar pérdida de cobertura
o selección de una referencia alternativa válida.

Los cambios macroeconómicos y de moneda de referencia
pueden modificar indicadores de los pares afectados.

Los cambios en hechos_empresa pueden modificar
metadatos, indicadores documentales y elegibilidad.

Cuando cambie una fecha, clave o relación, deberán
considerarse los valores anteriores y nuevos.

La actualización incremental deberá reconstruir
los candidatos de los expedientes afectados y
retirar únicamente los pares que dejen de ser válidos
según cambios autorizados de Silver.

### 11.9. Orden de ejecución

Primero se procesarán las fuentes Silver aprobadas.

Después se construirán o actualizarán:

- mercado_valor_diario
- actividad_sab_diaria
- empresa_trimestre
- hechos_empresa

Finalmente:

- impacto_hechos_valor

El producto de impacto utilizará versiones compatibles
y aprobadas de mercado y hechos empresariales.

### 11.10. Idempotencia

La reejecución de un mismo lote aprobado no deberá:

- Duplicar claves.
- Duplicar particiones.
- Duplicar registros financieros.
- Modificar indicadores sin cambios de entrada.
- Alterar datos históricos no afectados.
- Crear publicaciones inconsistentes.

La idempotencia se verificará comparando los
resultados lógicos de ejecuciones repetidas.

No se exigirá igualdad de nombres de archivos Parquet
si los contenidos lógicos son equivalentes.

### 11.11. Publicación y recuperación

Estrategia propuesta:

1. Identificar el lote.
2. Verificar aprobación Silver.
3. Obtener cambios y dependencias.
4. Construir salidas en staging.
5. Ejecutar controles de calidad.
6. Comparar con la versión Gold vigente.
7. Aprobar la nueva versión.
8. Publicar mediante un mecanismo controlado.
9. Registrar métricas y trazabilidad.
10. Mantener capacidad de recuperación.

La publicación no deberá destruir la última versión
válida antes de verificar la nueva.

El mecanismo definitivo de publicación deberá
adaptarse a las capacidades de Google Cloud Storage
y BigQuery.

## 12. Publicación y consumo analítico

Arquitectura objetivo:

Silver aprobado
    -> Procesamiento PySpark en Dataproc
    -> Gold staging
    -> Validaciones de calidad
    -> Gold publicado
    -> BigQuery
    -> Looker Studio

Google Cloud Storage conservará los productos Gold
necesarios para procesamiento y trazabilidad.

BigQuery proporcionará acceso a los productos
analíticos destinados a dashboards y consultas.

Se evitará duplicar innecesariamente datasets.

La estrategia definitiva deberá considerar:

- Tablas nativas o externas.
- Particionamiento.
- Clustering.
- Costos de almacenamiento.
- Costos de consulta.
- Frecuencia de actualización.
- Permisos.
- Trazabilidad.
- Recuperación ante fallos.

Estas decisiones todavía requieren coordinación
con el equipo.

### 12.1. Consumo en dashboards

Los dashboards deberán respetar la granularidad
y elegibilidad de cada producto.

Mercado:

- Evolución de precios.
- Retorno respecto al cierre anterior.
- Actividad negociada por instrumento.
- Cobertura macroeconómica.
- Cobertura empresarial.

Actividad SAB:

- Participación por sociedad.
- Ranking.
- Compras y ventas.
- Saldo neto.
- Concentración HHI.

Finanzas:

- Indicadores de rentabilidad.
- Endeudamiento.
- Autonomía financiera.
- Resultados financieros.
- Calidad y elegibilidad de registros.

Hechos empresariales:

- Cantidad de acontecimientos.
- Tipos de hechos.
- Sectores.
- Identificación empresarial.
- Calidad documental.

Impacto bursátil:

- Cobertura por par.
- Cobertura por evento.
- Retornos observados.
- Dirección del movimiento.
- Distancias temporales.
- Limitaciones de cobertura.

Los denominadores de cobertura deben permanecer
explícitos en todas las visualizaciones.

No se sumarán indicadores diarios repetidos.

No se agregarán importes de unidades monetarias
no confirmadas o incompatibles.

No se interpretarán los resultados de impacto
como evidencia causal.

### 12.2. Consumo predictivo

Los componentes MLlib deberán considerar:

- Granularidad de las características.
- Disponibilidad temporal.
- Valores faltantes.
- Elegibilidad de los registros.
- Diferencias entre fechas de cotización.
- Posibles sesgos de cobertura.
- Prevención de fuga de información.

Los precios posteriores a un evento no podrán
utilizarse como características disponibles antes
de dicho evento.

Los estados financieros requieren confirmar su
fecha efectiva de disponibilidad pública antes
de utilizarlos en modelos temporales.

## 13. Criterios de aceptación

### 13.1. Construcción histórica

- Lectura correcta de las fuentes Silver.
- Claves Gold completas y únicas.
- Conservación de registros.
- Ausencia de multiplicaciones no autorizadas.
- Indicadores calculados según reglas documentadas.
- Conservación de incidencias de calidad.
- Trazabilidad hacia Silver.
- Particiones compatibles con las fechas.
- Archivos Parquet disponibles.
- Validaciones independientes aprobadas.

Estado:

Cumplidos para el snapshot histórico auditado,
dentro del alcance de los controles ejecutados.

### 13.2. Procesamiento incremental

- Identificación de cambios de origen.
- Conservación de datos históricos no afectados.
- Reprocesamiento de dependencias.
- Correcciones controladas.
- Eliminaciones autorizadas.
- Idempotencia.
- Recuperación ante fallos.
- Comparación con una reconstrucción completa.
- Publicación consistente.

Estado:

Pendiente de implementación y pruebas.

### 13.3. Consumo analítico

- Consultas correctas en BigQuery.
- Disponibilidad de productos aprobados.
- Dashboards conectados.
- Comparación antes y después de una nueva ingesta.
- Coherencia de métricas y granularidad.
- Validación con el equipo.

Estado:

Pendiente de implementación.

## 14. Coordinación entre componentes

Daniela:

- Publicación aprobada de Silver.
- Esquemas y claves.
- Identificación de cambios.
- Correcciones y eliminaciones.
- Manifest incremental.
- Confirmación de unidades monetarias.
- Revisión de cobertura histórica.
- Ampliación de referencias empresariales.

Piero:

- Transformaciones Gold.
- Indicadores analíticos.
- Validaciones independientes.
- Procesamiento incremental Gold.
- Publicación en BigQuery.
- Dashboards en Looker Studio.

Matthew:

- Consumo de características Gold para MLlib.
- Prevención de fuga de información.
- Integración de resultados predictivos.
- Portal de carga y seguimiento.

Lizeth:

- Infraestructura Google Cloud.
- Dataproc y Cloud Storage.
- Permisos y cuentas de servicio.
- Orquestación.
- Calidad y monitoreo.
- Coordinación de mecanismos de publicación.

Las responsabilidades deberán confirmarse durante
la integración del proyecto.

## 15. Evidencias de auditoría

### 15.1. Auditorías Silver

- src/auditoria/silver/inventario_silver.py
- src/auditoria/silver/auditoria_claves_silver.py
- src/auditoria/silver/auditoria_colisiones_financieras.py
- src/auditoria/silver/auditoria_puente_smv.py
- src/auditoria/silver/auditoria_cobertura_temporal.py
- src/auditoria/silver/auditoria_integracion_silver.py
- src/auditoria/silver/auditoria_metricas_silver.py

### 15.2. Construcción Gold

- src/gold/mercado/build_mercado_valor_diario.py
- src/gold/mercado/build_actividad_sab_diaria.py
- src/gold/empresas/build_empresa_trimestre.py
- src/gold/eventos/build_hechos_empresa.py
- src/gold/eventos/build_impacto_hechos_valor.py

### 15.3. Validaciones independientes

- src/gold/mercado/validate_mercado_valor_diario.py
- src/gold/mercado/validate_actividad_sab_diaria.py
- src/gold/empresas/validate_empresa_trimestre.py
- src/gold/eventos/validate_hechos_empresa.py
- src/gold/eventos/validate_impacto_hechos_valor.py

### 15.4. Comparación y auditorías complementarias

- src/gold/mercado/compare_actividad_sab_versiones.py
- src/gold/mercado/audit_mercado_valor_diario.py
- src/gold/eventos/audit_cobertura_impacto.py

### 15.5. Resultados acumulados

| Validación | Aprobados | Fallidos |
|---|---:|---:|
| Mercado valor diario | 8 | 0 |
| Actividad SAB diaria | 16 | 0 |
| Empresa trimestre | 20 | 0 |
| Hechos empresa | 24 | 0 |
| Impacto hechos valor | 30 | 0 |
| Equivalencia SAB | 15 | 0 |
| Auditoría complementaria mercado | 37 | 0 |
| Auditoría cobertura impacto | 8 | 0 |
| TOTAL | 158 | 0 |

Los 158 controles corresponden al acumulado
documentado de las suites de validación.

Algunas comprobaciones se superponen entre suites.

La reejecución de los 16 controles SAB sobre la
versión optimizada no se contabiliza nuevamente.

Este total no constituye una certificación integral
de todas las posibles condiciones de producción.

### 15.6. Entorno de validación

- Debian 12.
- Apache Spark 3.5.1.
- PySpark 3.5.1.
- Python 3.11.2.
- Java 17.
- Procesamiento local con dos hilos.
- spark.sql.shuffle.partitions=8.
- Almacenamiento local mediante rutas file://.

La ejecución distribuida en Dataproc requiere
validación adicional.

## 16. Hallazgos pendientes de coordinación

### 16.1. Cobertura del puente SMV

- 547 instrumentos sin vinculación empresarial.
- 56573 cotizaciones sin RUC asociado.

Se requiere evaluar la ampliación de las referencias
empresariales y confirmar su vigencia temporal.

### 16.2. Cobertura macroeconómica

- Cinco fechas con tipo de cambio nulo.
- 301 cotizaciones afectadas.

Se requiere verificar la disponibilidad de estos
valores en la fuente original.

### 16.3. Cobertura de acontecimientos

- Eventos principales limitados a enero de 2025.
- 28 instrumentos candidatos sin cotizaciones
  durante enero de 2025.
- 109 pares sin cobertura completa de siete días.

Se requiere distinguir las limitaciones de extracción
de las condiciones reales del mercado.

### 16.4. Finanzas

- Tres grupos financieros con colisiones.
- Dos registros con inconsistencias contables.
- Confirmación pendiente de unidades monetarias.
- Confirmación pendiente de periodos acumulados.
- Confirmación pendiente de estabilidad de id_registro.

### 16.5. Metodología bursátil

- Moneda histórica no confirmada.
- Precios ajustados no confirmados.
- Índices históricos insuficientes.
- Posible superposición de acontecimientos.
- Limitaciones de interpretación causal.

### 16.6. Contrato incremental

Se requiere acordar con Daniela:

- Formato definitivo del manifest.
- Versionado del esquema.
- Identificación de registros modificados.
- Correcciones de datos históricos.
- Eliminaciones autorizadas.
- Semántica de las particiones publicadas.
- Estabilidad de las claves.
- Mecanismo de aprobación Silver.

Los esquemas físicos de los productos y fuentes
deberán consolidarse mediante inventarios verificables
antes de cerrar el contrato incremental definitivo.

## 17. Estado actual

### Completado

- Preparación del entorno.
- Verificación de integridad Silver.
- Inventario de 16 datasets Silver.
- Auditoría de claves.
- Auditoría de colisiones financieras.
- Auditoría de referencias empresariales.
- Auditoría de cobertura temporal.
- Validación de integraciones.
- Auditoría de métricas.
- Construcción de cinco productos Gold.
- Validación independiente de los cinco productos.
- Optimización física de actividad SAB.
- Verificación de equivalencia de versiones.
- Inventario físico consolidado.
- Auditoría complementaria de mercado.
- Auditoría de cobertura de impacto.
- Actualización técnica del contrato a versión 1.1.

### Pendiente

- Verificación final del contrato actualizado.
- Respaldo histórico consolidado.
- Registro de avances en Git.
- Aprobación conjunta del contrato.
- Implementación incremental.
- Pruebas de inserciones y correcciones.
- Pruebas de eliminaciones autorizadas.
- Pruebas de idempotencia.
- Pruebas de recuperación ante fallos.
- Integración con publicaciones Silver.
- Despliegue distribuido en Dataproc.
- Almacenamiento Gold en Google Cloud Storage.
- Publicación en BigQuery.
- Dashboards en Looker Studio.
- Integración con MLlib.
- Integración final con el equipo.

## 18. Conclusión

La capa Gold histórica cuenta con cinco productos
analíticos implementados y validados localmente.

Se conservan 111056 registros distribuidos en
145 archivos Parquet.

Las auditorías documentadas acumulan 158 controles
aprobados y cero fallidos.

Los productos mantienen la granularidad definida,
la trazabilidad hacia Silver y los indicadores de
calidad necesarios para interpretar sus limitaciones.

La construcción histórica proporciona una base
para implementar posteriormente el procesamiento
incremental y la publicación analítica en Google Cloud.

La versión 1.1 documenta resultados históricos
verificados y reglas propuestas para las siguientes
fases.

No constituye evidencia de que la incrementalidad,
BigQuery, Looker Studio o la infraestructura cloud
ya estén implementados.
