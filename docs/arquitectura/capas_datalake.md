# Arquitectura de capas del Data Lake BVL

El proyecto implementa una arquitectura de datos basada en HDFS y PySpark, organizada principalmente en capas Bronze y Silver.

La ruta raíz del Data Lake es:

```text
/datalake/bvl/
```

## 1. Capa Bronze

La capa Bronze conserva los datos con la mayor fidelidad posible respecto de las fuentes originales.

```text
/datalake/bvl/bronze/
```

### 1.1 Cotizaciones SMV

Ruta:

```text
/datalake/bvl/bronze/smv/cotizaciones/
```

Características:

- 60 archivos JSON históricos.
- Periodo: 2021-2025.
- Los archivos conservan la estructura original de la fuente SMV.
- Total estructurado posteriormente en Silver: 83,130 registros.

### 1.2 Montos intermediados por SAB

Ruta:

```text
/datalake/bvl/bronze/smv/montos_sab/
```

Características:

- 1,221 archivos JSON.
- Periodo: 2021-2025.
- Fuente oficial SMV.
- Total estructurado posteriormente en Silver: 22,156 registros.

### 1.3 Principales cuentas financieras

Ruta:

```text
/datalake/bvl/bronze/smv/principales_cuentas/
```

Características:

- 20 archivos JSON.
- Ejercicios 2021-2025.
- Cuatro trimestres por ejercicio.
- Total estructurado posteriormente en Silver: 5,249 registros.

### 1.4 Hechos de importancia

Ruta:

```text
/datalake/bvl/bronze/smv/hechos_importancia/
```

La fuente contiene información semiestructurada y no estructurada publicada por la SMV.

Estructura:

```text
hechos_importancia/
├── pdf/
└── texto_extraido/
```

Resultados principales:

- 990 documentos PDF.
- 989 archivos TXT físicos.
- 990 documentos estructurados.
- 346 eventos o expedientes.
- 1 documento utiliza el texto de un archivo canónico debido a la existencia de un PDF duplicado exacto.

La metadata y trazabilidad se conserva en:

```text
/datalake/bvl/metadata/smv/hechos_importancia/
```

### 1.5 Valores inscritos SMV

Los valores inscritos fueron obtenidos mediante consultas al servicio oficial SOAP de la SMV utilizando los RUC del universo de principales cuentas financieras.

Ruta:

```text
/datalake/bvl/bronze/smv/valores_inscritos/
```

Estructura:

```text
valores_inscritos/
├── json/
└── xml/
```

Resultados:

- 289 RUC consultados.
- 289 respuestas XML originales.
- 1 JSON consolidado.
- 1,008 registros de valores inscritos.
- 148 RUC con al menos un valor inscrito.
- 0 errores durante la extracción.

La integridad del JSON almacenado en HDFS fue comprobada mediante SHA-256 contra el archivo local.

La metadata de extracción se conserva en:

```text
/datalake/bvl/metadata/smv/valores_inscritos/
```

### 1.6 BCRP - Tipo de cambio

Ruta:

```text
/datalake/bvl/bronze/bcrp/tipo_cambio/
```

Características:

- Fuente oficial BCRP.
- 7,751 observaciones reales.
- Periodo: 1997-01-02 a 2026-09-17.

### 1.7 BCRP - EMBIG Perú

Ruta:

```text
/datalake/bvl/bronze/bcrp/embig/
```

Características:

- Fuente oficial BCRP.
- 7,481 observaciones reales.
- Periodo: 1998-01-01 a 2026-09-03.

---

## 2. Capa Silver

La capa Silver contiene datos estructurados, tipados y normalizados.

Los datasets se almacenan principalmente en Apache Parquet con compresión Snappy.

```text
/datalake/bvl/silver/
```

### 2.1 Mercado diario

#### Cotizaciones

Ruta:

```text
/datalake/bvl/silver/mercado_diario/cotizaciones/
```

Resultados:

- 83,130 registros.
- 83,130 claves únicas.
- Sin fechas nulas.
- Sin valores nulos en el identificador del valor.
- Periodo: 2021-2025.
- Particionamiento por año y mes.

La granularidad principal corresponde a una cotización de un valor en una fecha determinada.

#### Montos intermediados SAB

Ruta:

```text
/datalake/bvl/silver/mercado_diario/montos_sab/
```

Resultados:

- 22,156 registros.
- 22 SAB distintas.
- 22,156 claves únicas.
- Periodo: 2021-2025.

La clave utilizada considera la fecha de negociación y el código de la Sociedad Agente de Bolsa.

### 2.2 Finanzas empresariales

#### Principales cuentas financieras

Ruta:

```text
/datalake/bvl/silver/finanzas_empresariales/principales_cuentas/
```

Resultados:

- 5,249 registros.
- 289 RUC válidos distintos.
- 295 empresas distintas.
- Ejercicios 2021-2025.

La granularidad fue auditada utilizando dimensiones de negocio como:

```text
RUC
+ Ejercicio
+ Trimestre
+ TipoInformacion
+ RPJ
+ MetodoFlujoEfectivo
+ Moneda
```

Se detectaron tres grupos donde esta combinación no distingue completamente los registros.

Por ello, los registros originales se conservan y se utiliza adicionalmente un identificador técnico para garantizar trazabilidad, evitando eliminar información válida.

### 2.3 Hechos de importancia

Ruta:

```text
/datalake/bvl/silver/hechos_importancia/
```

Estructura:

```text
hechos_importancia/
├── eventos/
├── documentos/
└── contenido_textual/
```

Resultados:

- 346 eventos.
- 990 documentos.
- 990 registros de contenido textual.
- 338 eventos clasificados como `COHERENTE`.
- 8 eventos clasificados como `HISTORICO_REGULARIZACION`.
- 428 documentos temporalmente coherentes.
- 562 documentos vinculados a eventos históricos o regularizaciones.
- 897 documentos con estado de extracción `OK`.
- 92 documentos con estado `SIN_TEXTO`.
- 1 documento con estado `DUPLICADO`.

Los datos se particionan mediante la clasificación de coherencia temporal.

### 2.4 Referencias SMV

#### Valores inscritos

Ruta:

```text
/datalake/bvl/silver/referencias_smv/valores_inscritos/
```

Resultados:

- 1,008 registros.
- 148 RUC distintos.
- 831 nemónicos distintos.
- 831 pares RUC + nemónico distintos.
- 0 RUC nulos o vacíos.
- 0 nemónicos nulos o vacíos.

El campo `monto_inscrito` se conserva porque forma parte de la respuesta oficial de la SMV.

La auditoría detectó que, en los RUC con múltiples observaciones por nemónico, los distintos nemónicos de una misma empresa comparten sistemáticamente los mismos conjuntos de montos inscritos.

Por este motivo, `monto_inscrito` no se utiliza como parte de la clave del puente empresarial.

#### Puente RUC - Nemónico

Ruta:

```text
/datalake/bvl/silver/referencias_smv/puente_ruc_nemonico/
```

Resultados:

- 831 relaciones únicas.
- 148 RUC.
- 831 nemónicos.
- 0 duplicados RUC + nemónico.
- 0 nemónicos asociados a más de un RUC.
- 0 pares con atributos de identidad ambiguos.

Este dataset permite relacionar oficialmente la información financiera empresarial con los valores negociados.

La relación principal es:

```text
Principales Cuentas
        |
        | RUC
        v
Valores Inscritos SMV
        |
        | NemonicoValor
        v
Cotizaciones
```

La auditoría de cobertura obtuvo:

- 109 nemónicos del puente presentes en Cotizaciones.
- 26,557 registros de Cotizaciones vinculables mediante el puente.
- 83,130 registros totales de Cotizaciones.
- Cobertura sobre filas de Cotizaciones: 31.95 %.

Entre los valores sin puente se observaron también instrumentos internacionales, ETF, fondos y otros valores que no necesariamente poseen una empresa peruana equivalente dentro del universo de RUC utilizado.

### 2.5 Macroeconomía

#### Tipo de cambio

Ruta:

```text
/datalake/bvl/silver/macroeconomia/tipo_cambio/
```

Resultados:

- 7,751 registros.
- Una observación por fecha.
- 0 fechas nulas.
- 0 fechas duplicadas.
- Periodo: 1997-01-02 a 2026-09-17.
- Particionamiento por año y mes.

#### EMBIG Perú

Ruta:

```text
/datalake/bvl/silver/macroeconomia/embig/
```

Resultados:

- 7,481 registros.
- Una observación por fecha.
- 0 fechas nulas.
- 0 fechas duplicadas.
- Periodo: 1998-01-01 a 2026-09-03.
- Particionamiento por año y mes.

---

## 3. Relaciones previstas para integración

La integración del proyecto no utiliza únicamente la fecha como criterio general de JOIN.

La relación empresarial principal utiliza identificadores oficiales de negocio:

```text
Principales Cuentas
        |
        | RUC
        v
Valores Inscritos
        |
        | RUC + NemonicoValor
        v
Cotizaciones
```

En Cotizaciones, el campo `Valor` se relaciona con `NemonicoValor` del puente SMV.

Para la información macroeconómica se utiliza la fecha:

```text
Cotizaciones.Fecha
        |
        +------> Tipo de Cambio.Fecha
        |
        +------> EMBIG.Fecha
```

Estos JOIN por fecha corresponden únicamente al enriquecimiento macroeconómico diario.

Los JOIN empresariales utilizan RUC, nemónico y otras dimensiones de negocio según la granularidad de cada fuente.

---

## 4. Formatos de almacenamiento

La capa Bronze conserva los formatos originales de las fuentes utilizadas:

- JSON.
- CSV.
- XML.
- PDF.
- TXT.

La capa Silver utiliza principalmente:

- Apache Parquet.
- Compresión Snappy.
- Tipado explícito.
- Normalización de fechas.
- Normalización de variables numéricas.
- Validaciones de nulidad.
- Validaciones de unicidad y granularidad.


## Identidad empresarial de Hechos de Importancia

Los Hechos de Importancia se mantienen en Silver como eventos independientes y su resolución de identidad empresarial se almacena en una tabla complementaria.

### Bronze

Las consultas adicionales realizadas al servicio oficial SOAP de la SMV se conservan en:

`/datalake/bvl/bronze/smv/hechos_valores_consulta/`

Estructura almacenada:

- `xml/ingestion_date=2026-10-05/`: contiene las 22 respuestas XML originales de las consultas SOAP.
- `json/ingestion_date=2026-10-05/`: contiene el JSON consolidado con los valores recuperados.

Se almacenan las respuestas originales y el resultado consolidado para mantener la trazabilidad de la extracción.

La metadata de extracción se encuentra en:

`/datalake/bvl/metadata/smv/hechos_valores_consulta/`

### Silver

La identidad empresarial resuelta se encuentra en:

`/datalake/bvl/silver/hechos_importancia/identidad_empresarial/`

La granularidad es un registro por expediente principal de Hechos de Importancia.

Resultados de validación:

- 338 eventos principales.
- 338 números de expediente distintos.
- 277 eventos vinculados mediante `RUC_EXACTO`.
- 15 eventos adicionales vinculados mediante `NEMONICO_SMV`.
- 46 eventos conservados como `SIN_PUENTE`.
- 292 eventos identificados.
- Cobertura de identidad: 86.39 %.
- 129 empresas distintas.
- 113 empresas resueltas.
- 16 empresas sin puente.
- Cobertura empresarial: 87.60 %.
- 0 duplicados adicionales.

La resolución se realiza mediante dos mecanismos determinísticos:

1. Razón social normalizada hacia RUC utilizando las fuentes oficiales integradas en Silver.
2. Consulta oficial de valores inscritos de la SMV para obtener nemónicos, conservando únicamente aquellos presentes en el histórico de Cotizaciones.

No se utiliza fuzzy matching para asignar identidades no verificadas.

La presencia de un RUC válido no implica necesariamente que la empresa tenga un nemónico disponible en el histórico de Cotizaciones. Por ello se mantienen separados los conceptos de identidad empresarial y vinculación bursátil.

