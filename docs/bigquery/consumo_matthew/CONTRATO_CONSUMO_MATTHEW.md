# Contratos de consumo Gold para Matthew

Estado: CANDIDATO LOCAL NO AUTORIZADO.

## Alcance

Tres productos Gold para analitica,
BigQuery, Looker Studio y Machine Learning.

Proyecto propuesto:
bvl-analytics-2026

Dataset propuesto:
gold_analytics

No se acredita la existencia operativa
de las tablas ni su disponibilidad.

## Productos

### mercado_valor_diario

Clave: nemonico, fecha.

Granularidad: valor financiero por dia.

Columnas: 41.

Referencia historica candidata: 83130 filas.

Particion BigQuery: fecha.

Clustering: nemonico.

### actividad_sab_diaria

Clave: fecha, codigo_sab.

Granularidad: SAB por dia.

Columnas: 41.

Referencia historica candidata: 21911 filas.

Particion BigQuery: fecha.

Clustering: codigo_sab.

### empresa_trimestre

Clave: id_registro.

Granularidad: registro financiero trimestral.

Columnas: 58.

Referencia historica candidata: 5158 filas.

Particion BigQuery: fecha_fin_trimestre.

Clustering: id_registro, rpj.

No se garantiza unicidad por empresa
y trimestre utilizando RUC.

## Diccionario

140 columnas derivadas de contratos reales.

Incluye tipo Spark, tipo BigQuery,
nulabilidad, claves, particiones y clustering.

Las clasificaciones de uso analitico
son propuestas.

Las descripciones no documentadas
se identifican como pendientes.

No se inventan unidades ni formulas.

## Control de acceso

Consumo propuesto exclusivamente de lectura.

No se requieren permisos de escritura
Gold para construir visualizaciones.

El acceso real depende de IAM
y de las autorizaciones correspondientes.

No utilizar staging, backups ni versiones
candidatas como fuentes productivas.

## Calidad

Las versiones oficiales deben estar acreditadas.

NULL no equivale a cero.

Los campos monetarios conservan NUMERIC.

Los indicadores de cobertura SAB
deben permanecer visibles.

No mezclar versiones historicas e incrementales
sin continuidad acreditada.

Los 302 controles locales aprobados
no sustituyen validaciones cloud.

## Responsabilidades propuestas

Piero: contratos Gold y preparacion tecnica
de integracion BigQuery.

Matthew: consumo analitico, visualizaciones
y modelos ML, sujeto a sus responsabilidades
acordadas con el equipo.

Quality: acreditacion de fuentes y decisiones
operativas segun autorizaciones correspondientes.

## Seguridad

Sin autorizacion de consumo productivo.

Sin autorizacion de publicacion.

Sin ejecucion GCP ni BigQuery.
