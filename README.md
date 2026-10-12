# Análisis del comportamiento del mercado bursátil peruano mediante información financiera, bursátil y macroeconómica

Proyecto académico desarrollado para el curso de **Big Data y Analítica de Datos**, orientado al análisis del comportamiento del mercado bursátil peruano mediante la integración de información oficial de la **Superintendencia del Mercado de Valores (SMV)** y del **Banco Central de Reserva del Perú (BCRP)**.

La solución implementa un pipeline de datos basado en una arquitectura **Bronze – Silver – Gold**, utilizando **Hadoop HDFS, Apache Spark, PySpark y YARN**, con mecanismos de calidad, trazabilidad, integración, optimización y pruebas de escalabilidad.

---

## 1. Objetivo del proyecto

Construir un pipeline analítico que permita integrar información bursátil, financiera, empresarial, macroeconómica y hechos de importancia para apoyar el monitoreo de valores y empresas del mercado peruano.

La solución busca analizar aspectos como:

- evolución de la negociación bursátil;
- comportamiento de precios y retornos;
- volatilidad de los valores;
- contexto macroeconómico;
- situación financiera de las empresas;
- actividad de las Sociedades Agentes de Bolsa;
- relación entre hechos de importancia y comportamiento bursátil.

---

## 2. Fuentes de datos

Las fuentes utilizadas corresponden principalmente a información pública institucional.

| Fuente | Entidad | Contenido |
|---|---|---|
| Cotizaciones históricas | SMV | Precios, montos negociados y valores bursátiles |
| Montos intermediados por SAB | SMV | Compras, ventas y montos negociados por SAB |
| Principales cuentas financieras | SMV | Activos, pasivos, patrimonio, ingresos y utilidad |
| Valores inscritos | SMV | RUC, razón social, nemónico, ISIN y datos del instrumento |
| Hechos de importancia | SMV | Eventos empresariales y documentación asociada |
| Tipo de cambio | BCRP | Tipo de cambio interbancario |
| EMBIG Perú | BCRP | Indicador diario de riesgo país |

Los archivos originales se conservan en la zona Bronze para garantizar trazabilidad y reproducibilidad.

---

## 3. Arquitectura de la solución

La plataforma sigue el flujo:

```text
Fuentes oficiales
      ↓
Ingesta Batch
      ↓
Bronze / RAW
      ↓
Procesamiento y calidad
      ↓
Silver
      ↓
Integración
      ↓
Gold
      ↓
EDA y Modelado
      ↓
Consumo y visualización
```

### Bronze

Conserva los datos originales sin modificar.

Incluye mecanismos de:

- validación técnica;
- checksum;
- control de duplicados;
- cuarentena;
- auditoría de ingesta;
- conservación del dato fuente.

### Silver

Contiene datos limpios y normalizados en **Parquet con compresión Snappy**.

Los principales dominios son:

```text
silver/
├── mercado_diario/
├── macroeconomia/
├── finanzas_empresariales/
├── referencias_smv/
└── hechos_importancia/
```

En esta capa se realizan:

- tipado y normalización de fechas;
- tratamiento de nulos;
- deduplicación según granularidad;
- estandarización de categorías;
- validaciones de calidad;
- construcción del puente RUC–nemónico;
- resolución de identidad empresarial.

### Gold

La capa Gold genera cinco productos analíticos independientes según su granularidad:

```text
gold/
├── mercado/
│   ├── mercado_valor_diario/
│   └── actividad_sab_diaria/
├── empresas/
│   └── empresa_trimestre/
└── eventos/
    ├── hechos_empresa/
    └── impacto_hechos_valor/
```

| Producto Gold | Granularidad |
|---|---|
| `mercado_valor_diario` | valor + fecha |
| `actividad_sab_diaria` | fecha + código SAB |
| `empresa_trimestre` | RUC + ejercicio + trimestre |
| `hechos_empresa` | número de expediente |
| `impacto_hechos_valor` | número de expediente + nemónico |

Los productos Gold se almacenan en **HDFS utilizando Parquet + Snappy**.

---

## 4. Tecnologías utilizadas

- Python 3.11
- Apache Hadoop 3.3.6
- HDFS
- Apache Spark 3.5.1
- PySpark
- YARN
- Spark SQL
- Spark MLlib
- Parquet
- Snappy
- Git
- GitHub
- Debian 12
- VirtualBox

---

## 5. Estructura del repositorio

```text
bvl-analytics-platform/
├── config/
├── data/
│   └── sample/
├── docs/
│   ├── arquitectura/
│   ├── evidencias/
│   └── procedencia/
├── notebooks/
│   ├── eda/
│   └── model/
├── outputs/
│   ├── figures/
│   ├── performance/
│   └── quality/
├── scripts/
│   ├── analytics/
│   ├── evidencias/
│   ├── ingestion/
│   ├── integration/
│   ├── model/
│   ├── performance/
│   ├── processing/
│   ├── quality/
│   └── scalability/
├── src/
│   └── gold/
├── requirements.txt
└── README.md
```

---

## 6. Organización del Data Lake

La implementación principal utiliza el siguiente namespace en HDFS:

```text
/datalake/bvl/
├── bronze/
├── silver/
├── gold/
├── metadata/
└── quarantine/
```

### Bronze

```text
/datalake/bvl/bronze/
├── bcrp/
└── smv/
```

### Silver

```text
/datalake/bvl/silver/
├── mercado_diario/
├── macroeconomia/
├── finanzas_empresariales/
├── referencias_smv/
└── hechos_importancia/
```

### Gold

```text
/datalake/bvl/gold/
├── mercado/
├── empresas/
└── eventos/
```

---

## 7. Calidad de datos

Se implementaron validaciones de:

- completitud;
- unicidad;
- coherencia temporal;
- rangos válidos;
- duplicados;
- correspondencia empresarial;
- granularidad de las claves.

Algunos resultados obtenidos fueron:

- 831 pares RUC–nemónico únicos;
- 0 pares RUC–nemónico duplicados;
- 0 nemónicos asociados a más de un RUC;
- EMBIG con 100 % de completitud en fecha y valor;
- Tipo de Cambio con 100 % de completitud en fecha y 95.46 % en valor;
- cobertura temporal EMBIG–Cotizaciones: 100 %;
- cobertura temporal Tipo de Cambio–Cotizaciones: 99.59 %;
- 292 de 338 Hechos de Importancia con identidad empresarial resuelta;
- cobertura de identidad: 86.39 %.

Los registros sin correspondencia verificable se mantienen como casos sin puente y no se asignan mediante coincidencias aproximadas.

---

## 8. Optimización del rendimiento

Se evaluó el impacto del particionamiento temporal sobre el conjunto de Cotizaciones.

| Escenario | Mediana |
|---|---:|
| Parquet sin particionar | 0.5506 s |
| Parquet particionado por año y mes | 0.3343 s |

Resultado:

- reducción del tiempo: **39.27 %**;
- factor de aceleración: **1.65x**.

---

## 9. Prueba de escalabilidad

El pipeline fue probado utilizando réplicas del conjunto real exclusivamente para medir rendimiento.

| Escala | Registros | Tiempo |
|---:|---:|---:|
| 1x | 83,130 | 28.03 s |
| 10x | 831,300 | 358.37 s |
| 100x | 8,313,000 | 642.10 s |

Las tres pruebas se ejecutaron manteniendo:

```text
1 executor
1 core
512 MB executor
512 MB driver
```

Los datos replicados no son utilizados en el análisis exploratorio, entrenamiento del modelo ni conclusiones de negocio.

---

## 10. Análisis exploratorio

El análisis exploratorio permitió identificar:

### Evolución del monto negociado

El mayor monto mensual observado ocurrió en mayo de 2024:

```text
S/ 6,470,692,281.72
```

### Relación entre retorno y riesgo país

La correlación de Pearson entre el retorno promedio diario y el EMBIG Perú fue:

```text
-0.1876
```

lo que evidencia una relación negativa débil.

### Valores con mayor monto negociado

Entre los valores con mayor monto acumulado se encontraron:

```text
ENGEPEC1
BAP
ALICORC1
INRETC1
CVERDEC1
```

Las figuras generadas se encuentran en:

```text
outputs/figures/
```

---

## 11. Modelo base

Se implementó un modelo de **Regresión Logística con Spark MLlib** para clasificar si un valor presentará alta volatilidad en la siguiente jornada bursátil.

### División temporal

```text
Train: 2021–2024
Test : 2025
```

### Variables predictoras

- retorno actual;
- rango intradía;
- monto negociado;
- Tipo de Cambio;
- EMBIG Perú.

### Línea base técnica

```text
ROC-AUC = 0.5927
```

Métricas complementarias:

| Métrica | Resultado |
|---|---:|
| ROC-AUC | 0.5927 |
| F1 ponderado | 0.6844 |
| Accuracy | 0.7787 |
| Precision ponderada | 0.7758 |
| Recall ponderado | 0.7787 |

El resultado constituye una línea base para comparar mejoras posteriores mediante ingeniería de características, ajuste del umbral, tratamiento del desbalance y modelos de mayor capacidad.

---

## 12. Ejecución

### Verificar servicios

```bash
jps
```

Los principales procesos esperados son:

```text
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
```

### Construcción de Gold

```bash
spark-submit \
  --master yarn \
  --driver-memory 512m \
  --executor-memory 512m \
  --executor-cores 1 \
  --num-executors 1 \
  src/gold/build_gold.py
```

### Análisis exploratorio

```bash
spark-submit \
  --master yarn \
  --driver-memory 512m \
  --executor-memory 512m \
  --executor-cores 1 \
  --num-executors 1 \
  scripts/analytics/eda_semana7.py
```

### Modelo base

```bash
spark-submit \
  --master yarn \
  --driver-memory 512m \
  --executor-memory 512m \
  --executor-cores 1 \
  --num-executors 1 \
  scripts/model/modelo_base_volatilidad.py
```

---

## 13. Seguridad y trazabilidad

En el entorno académico se implementaron:

- permisos HDFS;
- separación de zonas del Data Lake;
- checksum SHA-256;
- auditoría de ingesta;
- metadata de procesamiento;
- cuarentena;
- versionamiento mediante Git y GitHub.

Para un entorno productivo se plantea incorporar:

- autenticación centralizada;
- RBAC;
- principio de mínimo privilegio;
- cifrado en reposo;
- TLS para comunicaciones;
- gestor de secretos.

---

## 14. Estado actual

Actualmente se encuentran implementadas las siguientes etapas:

```text
[✓] Ingesta
[✓] Bronze
[✓] Calidad y limpieza
[✓] Silver
[✓] Integración empresarial
[✓] Gold
[✓] Optimización
[✓] Escalabilidad
[✓] Análisis exploratorio
[✓] Modelo base
[ ] Dashboard final
[ ] Modelos mejorados
```

---

## 15. Reproducibilidad

El repositorio conserva:

- scripts de ingesta y procesamiento;
- reglas de calidad;
- scripts de integración;
- construcción de Gold;
- benchmarks;
- pruebas de escalabilidad;
- evidencias de ejecución;
- resultados analíticos;
- código del modelo base.

Los datos de gran volumen no se almacenan directamente en GitHub. El repositorio contiene únicamente código, documentación, evidencias y archivos de muestra necesarios para reproducir el flujo.

---

## Autores

Proyecto desarrollado por el equipo del curso de **Big Data y Analítica de Datos – UPAO**.
