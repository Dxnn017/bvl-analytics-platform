# Inventario definitivo de fuentes de datos

## 1. Fuentes activas del proyecto

| Fuente | Organismo | Formato original | Periodo / cobertura | Registros Silver | Granularidad / clave principal | Ruta Silver | Función |
|---|---|---|---|---:|---|---|---|
| Cotizaciones históricas | SMV | JSON | 2021-01-04 a 2025-12-31 | 83,130 | valor + fecha_cotizacion | /datalake/bvl/silver/mercado_diario/cotizaciones | Comportamiento bursátil diario de los valores |
| Montos intermediados por SAB | SMV | JSON | 2021-2025 | 22,156 | fecha_negociacion + codigo_sab | /datalake/bvl/silver/mercado_diario/montos_sab | Actividad e intermediación de las Sociedades Agentes de Bolsa |
| Principales cuentas financieras | SMV | JSON | 2021-2025, trimestral | 5,249 | RUC + ejercicio + trimestre + dimensiones financieras | /datalake/bvl/silver/finanzas_empresariales/principales_cuentas | Situación financiera de las empresas supervisadas |
| Tipo de cambio venta | BCRP | CSV | 1997-01-02 a 2026-09-17 | 7,751 | fecha | /datalake/bvl/silver/macroeconomia/tipo_cambio | Contexto macroeconómico diario |
| EMBIG Perú | BCRP | CSV | 1998-01-01 a 2026-09-03 | 7,481 | fecha | /datalake/bvl/silver/macroeconomia/embig | Riesgo país y contexto macroeconómico |
| Hechos de Importancia - eventos | SMV | JSON / documentos | 2025 | 346 eventos; 338 principales | numero_expediente | /datalake/bvl/silver/hechos_importancia/eventos | Eventos relevantes comunicados por las empresas |
| Hechos de Importancia - documentos | SMV | PDF / metadata | 2025 | 990 | guid_documento | /datalake/bvl/silver/hechos_importancia/documentos | Trazabilidad documental de los hechos |
| Hechos de Importancia - contenido textual | SMV | TXT derivado de PDF | 2025 | 990 | guid_documento | /datalake/bvl/silver/hechos_importancia/contenido_textual | Contenido no estructurado para análisis textual |
| Identidad empresarial de Hechos | Derivada de fuentes SMV | Parquet | 2025 | 338 | numero_expediente | /datalake/bvl/silver/hechos_importancia/identidad_empresarial | Resolver empresa/RUC/nemónico de cada evento principal |
| Valores inscritos SMV | SMV SOAP | XML + JSON consolidado | Consulta vigente al 2026-10-05 | 1,008 | RUC + nemónico + atributos del valor | /datalake/bvl/silver/referencias_smv/valores_inscritos | Maestro oficial de valores inscritos |
| Puente RUC-Nemónico | Derivada de Valores inscritos SMV | Parquet | Consulta vigente al 2026-10-05 | 831 | RUC + nemónico | /datalake/bvl/silver/referencias_smv/puente_ruc_nemonico | Vincular información financiera con cotizaciones |

## 2. Resultados principales de integración

### Cotizaciones y macroeconomía

Las Cotizaciones contienen 1,221 fechas distintas para el periodo 2021-2025 utilizado en el proyecto.

Cobertura temporal:

- Tipo de cambio BCRP: 1,221 de 1,221 fechas, equivalente a 100 %.
- EMBIG Perú: 1,221 de 1,221 fechas, equivalente a 100 %.
- Montos SAB: 1,221 de 1,221 fechas, equivalente a 100 %.

Los indicadores BCRP se integran por fecha debido a que su granularidad natural es una observación macroeconómica diaria.

### Identidad empresarial y valores

El servicio oficial de Valores Inscritos de la SMV permitió obtener:

- 1,008 registros.
- 148 RUC con valores inscritos.
- 831 nemónicos distintos.
- 831 relaciones únicas RUC-nemónico.
- 0 nemónicos asociados a más de un RUC.

El puente permite la relación:

Principales Cuentas -> RUC -> Valores Inscritos -> Nemónico -> Cotizaciones.

### Hechos de Importancia

Se validaron 338 eventos principales.

Métodos de identificación:

- RUC_EXACTO: 277 eventos.
- NEMONICO_SMV: 15 eventos.
- SIN_PUENTE: 46 eventos.
- Total identificado: 292 eventos.
- Cobertura final de identidad: 86.39 %.

Cobertura por empresa:

- Empresas totales: 129.
- Empresas resueltas: 113.
- Empresas sin puente: 16.
- Cobertura empresarial: 87.60 %.

No se utiliza fuzzy matching para asignar identidades no verificadas.

## 3. Fuentes evaluadas pero no activas

| Fuente | Estado | Motivo |
|---|---|---|
| ValoresPatrimonialesRPMV | NO ACTIVA | El recurso JSON oficial evaluado devolvía HTTP 404. Se conservan únicamente diccionario y metadata como referencia documental. |
| EmpresasSMV_RUC.json | NO ACTIVA | El archivo obtenido contenía solamente una respuesta individual y no un padrón completo de empresas. |
| Indices.json | ARCHIVADA | Solo contenía 16 registros y una cobertura temporal insuficiente para el histórico 2021-2025. |
| EstadoResultadosSMV.json | ARCHIVADA | Fuente anterior con periodo desalineado respecto del universo final. |
| SituacionFinancieraSMV.json | ARCHIVADA | Fue sustituida por Principales Cuentas Financieras, que ofrece una estructura homogénea 2021-2025. |
| FlujoEfectivoSMV.json | ARCHIVADA | Fuente anterior con periodo desalineado respecto del universo final. |
| CambiosPatrimonioSMV.json | ARCHIVADA | Presentaba granularidad y capacidad de integración insuficientes para el modelo final. |

## 4. Archivos complementarios

Los directorios locales `diccionarios/` y `metadatos/` conservan documentación oficial de las fuentes SMV.

Estos archivos se utilizan para:

- verificar el significado oficial de cada campo;
- definir tipos de datos y transformaciones;
- justificar claves de negocio;
- documentar la procedencia de los datasets;
- diferenciar fuentes activas de fuentes evaluadas y posteriormente descartadas.

La existencia de un diccionario o metadata no implica que la fuente correspondiente forme parte del pipeline analítico final.

## 5. Relaciones que serán utilizadas para Gold

Relación bursátil principal:

Principales Cuentas
-> RUC
-> Puente RUC-Nemónico
-> Cotizaciones

Relaciones macroeconómicas:

Cotizaciones
-> fecha
-> Tipo de Cambio BCRP

Cotizaciones
-> fecha
-> EMBIG Perú

Relación con actividad de intermediación:

Cotizaciones agregadas por fecha
-> fecha
-> Montos SAB agregados por fecha

Relación con Hechos de Importancia:

Hechos de Importancia
-> Identidad empresarial
-> RUC y/o nemónico
-> empresa o valor cotizado

## 6. Principio de diseño

Los datasets no se integrarán indiscriminadamente en una única tabla.

Cada tabla Gold deberá mantener una granularidad explícita y responder a una necesidad analítica o decisión de negocio concreta.

Las uniones se realizarán según la naturaleza del dato:

- claves empresariales mediante RUC;
- claves bursátiles mediante nemónico/valor;
- series macroeconómicas mediante fecha;
- información financiera mediante RUC + ejercicio + trimestre;
- Hechos de Importancia mediante numero_expediente y la tabla de identidad empresarial.

Esto evita relaciones muchos-a-muchos no controladas y la multiplicación artificial de registros.
