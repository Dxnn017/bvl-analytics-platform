# Criterios candidatos de KPI y calidad

## Mercado valor diario

Usos previstos:

- Evolucion temporal de precios por nemonico.
- Comparacion de comportamiento de valores.
- Analisis de volumen e indicadores disponibles.
- Analisis de riesgo y tendencias.

No sumar precios de diferentes valores.

No calcular rendimientos sin definir
el tratamiento de dividendos, splits
y otros eventos corporativos.

## Actividad SAB diaria

Usos previstos:

- Actividad diaria por intermediario.
- Comparacion de montos negociados.
- Participacion relativa cuando corresponda.
- Evolucion temporal de intermediarios.

Los totales diarios que se repiten por SAB
no deben sumarse directamente.

La cobertura DESCONOCIDA o PARCIAL
no permite declarar representatividad completa.

No sustituir montos NULL por cero.

## Empresa trimestre

Usos previstos:

- Evolucion de activos y otros campos
  financieros disponibles.
- Analisis comparativo de periodos.
- Analisis de ratios existentes.
- Caracteristicas financieras para ML.

No sumar ratios.

No asumir que todos los registros
son comparables por RUC y trimestre.

No reincorporar registros financieros
excluidos por decisiones Quality.

## Catalogo numerico

Los campos numericos del contrato se enumeran
como candidatos de consumo.

Esto no convierte automaticamente
cada campo numerico en un KPI.

Las formulas, unidades, filtros,
denominadores y agregaciones requieren
revision de significado de negocio.

## Machine Learning

Separar entrenamiento y evaluacion
de forma temporal cuando corresponda.

Evitar fuga de informacion futura.

No convertir estados de cobertura
desconocidos en cobertura completa.

Conservar referencia a version Gold,
fecha de corte y contrato utilizado.

No interpretar los modelos
como recomendaciones de inversion.

## Estado

Propuesta local pendiente de ratificacion
de consumo y calidad.

No autoriza uso productivo.
