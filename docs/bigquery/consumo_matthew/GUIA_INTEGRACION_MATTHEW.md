# Guia de integracion para Matthew

Estado: CANDIDATO LOCAL NO AUTORIZADO.

## 1. Prerrequisitos

Recibir confirmacion de versiones Gold oficiales.

Verificar manifests aprobados y registro vigente.

Confirmar carga BigQuery y conciliacion efectiva.

Confirmar esquema, granularidad y layout fisico.

Verificar autorizacion de lectura.

No conectar dashboards productivos antes
de completar estos requisitos.

## 2. BigQuery

Proyecto propuesto: bvl-analytics-2026.

Region acordada: southamerica-west1.

Dataset propuesto: gold_analytics.

Productos:

- mercado_valor_diario.
- actividad_sab_diaria.
- empresa_trimestre.

Consultar contratos_consumo.json.

Consultar diccionario_datos.csv o JSON.

Las consultas sql_consumo son ejemplos
candidatos, no consultas operativas aprobadas.

Parametros obligatorios:

- fecha_inicio: DATE.
- fecha_fin: DATE.

Las fechas delimitan un intervalo inclusivo.

Los scripts conservan un bloqueo preventivo.

No ejecutar fragmentos para eludirlo.

La adaptacion operativa a consultas SELECT
parametrizadas requiere autorizacion y revision.

No crear tablas, vistas o datasets sin
las autorizaciones correspondientes.

## 3. Looker Studio

Una vez autorizado:

1. Seleccionar el conector BigQuery.
2. Seleccionar el proyecto autorizado.
3. Seleccionar el dataset oficial.
4. Configurar una fuente por producto.
5. Verificar tipos, fechas y claves.
6. Revisar agregaciones predeterminadas.
7. Configurar controles de fechas.
8. Exponer version y estado de calidad.

No utilizar staging como fuente oficial.

Evitar consultas sin filtros temporales.

Verificar costos antes de uso recurrente.

No compartir informes sin autorizacion.

### Dashboard Mercado

Dimension: nemonico y fecha.

Serie: precio_cierre.

Indicadores: retorno_diario_pct
y rango_intradia_pct.

Mostrar controles de validez.

No sumar precios ni retornos.

No mezclar monedas sin conversion acreditada.

El retorno es un retorno simple de precio,
no un retorno total ajustado por dividendos.

### Dashboard SAB

Dimension: fecha y codigo_sab.

Indicadores: compras, ventas, saldo neto
y participacion total.

Mostrar cobertura y trazabilidad.

Los totales diarios y HHI se repiten
en varias filas del detalle.

Utilizar el resumen diario candidato
para evitar doble contabilizacion.

No aplicar filtros por SAB al universo
antes de obtener los totales diarios.

Los dias parciales o desconocidos
deben identificarse como tales.

### Dashboard Empresa

Dimension: id_registro y fecha_fin_trimestre.

Atributos: empresa, moneda y tipo_informacion.

Indicadores: ROA y ROE aproximados,
endeudamiento, margen neto y autonomia.

Utilizar los flags de calculabilidad.

No sumar ratios.

No anualizar indicadores automaticamente.

No mezclar moneda ni tipo de informacion.

## 4. Machine Learning

Mantener una fuente oficial versionada.

Registrar version Gold, fecha de corte,
contrato y huella del dataset de entrenamiento.

Separar entrenamiento, validacion y prueba
segun orden temporal cuando corresponda.

No usar particiones aleatorias como unica
evaluacion de modelos temporales.

La fecha del evento no acredita por si sola
la disponibilidad historica del dato.

Para estados financieros verificar fecha
real de publicacion o disponibilidad.

No unir estados financieros con mercado
usando solamente el cierre del trimestre.

Evitar fuga por precios, retornos, resultados
o revisiones posteriores al instante objetivo.

No utilizar como feature la etiqueta objetivo
ni variables que la contengan directamente.

No sustituir NULL por cero sin justificacion.

Preservar estados de cobertura SAB.

Ajustar imputadores y escaladores solamente
con datos de entrenamiento.

Evaluar la conversion de NUMERIC para ML
sin alterar el dato monetario original.

Los identificadores no se consideran
features numericas continuas.

Registrar restricciones, sesgos y metricas.

## 5. Integracion entre productos

No existe una clave universal garantizada.

Mercado utiliza nemonico y fecha.

SAB utiliza fecha y codigo_sab.

Empresa utiliza id_registro.

RUC requiere validacion de cardinalidad
y vigencia antes de unir productos.

No asumir relacion uno a uno entre
registros financieros y valores bursatiles.

No unir SAB con mercado solamente por fecha
sin controlar la multiplicacion de filas.

## 6. Versionado

La referencia oficial debe corresponder
a una version efectivamente aplicada.

El identificador de version y manifest
debe conservarse en la evidencia de consumo.

No inferir la version oficial exclusivamente
a partir de fecha_ingesta.

Los deltas no se consideran aplicados
hasta acreditar continuidad y transaccion.

## 7. Limites de esta entrega

No incluye dashboards desplegados.

No incluye modelos entrenados.

No incluye credenciales ni tokens.

No concede permisos IAM.

No certifica fuentes oficiales.

No acredita disponibilidad BigQuery.

No ejecuta GoogleSQL.

Los KPI documentados existen en Gold,
pero su uso productivo sigue pendiente.

Las unidades monetarias no acreditadas
permanecen sin asignar.
