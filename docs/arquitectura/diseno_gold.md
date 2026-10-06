# Diseño de la capa Gold

## 1. Objetivo analítico

La capa Gold generará productos de datos orientados a la toma de decisiones,
sin unir indiscriminadamente todos los datasets Silver.

La decisión de negocio que soportará la solución es:

Priorizar el monitoreo de valores y empresas del mercado peruano que presenten
cambios relevantes en su comportamiento bursátil, situación financiera o hechos
de importancia, considerando además el contexto macroeconómico del Perú.

## 2. mercado_valor_diario

Ruta:

/datalake/bvl/gold/mercado/mercado_valor_diario

Granularidad:

valor + fecha

Fuentes:

- Cotizaciones SMV.
- Tipo de Cambio BCRP.
- EMBIG Perú.

La integración con las variables macroeconómicas se realiza por fecha, debido a
que Tipo de Cambio y EMBIG tienen granularidad diaria.

Indicadores propuestos:

- variacion_precio
- retorno_diario_pct
- rango_intradia
- rango_intradia_pct

## 3. actividad_sab_diaria

Ruta:

/datalake/bvl/gold/mercado/actividad_sab_diaria

Granularidad:

fecha + codigo_sab

Fuente:

- Montos Intermediados por SAB.

Indicadores propuestos:

- monto_compras
- monto_ventas
- monto_total
- porcentaje_compras
- porcentaje_ventas
- porcentaje_total
- ranking_sab_dia_por_monto

Montos SAB no se unirá directamente a cada valor cotizado porque ambas fuentes
poseen granularidades diferentes.

## 4. empresa_trimestre

Ruta:

/datalake/bvl/gold/empresas/empresa_trimestre

Granularidad:

RUC + ejercicio + trimestre

Fuentes:

- Principales Cuentas Financieras.
- Puente RUC-Nemónico.
- Cotizaciones agregadas trimestralmente.

Relación:

Principales Cuentas
-> RUC
-> Puente RUC-Nemónico
-> Nemónico
-> Cotizaciones

Indicadores financieros propuestos:

- razon_endeudamiento
- margen_neto
- roa
- roe

Indicadores bursátiles propuestos:

- nemonicos_cotizados
- dias_con_cotizacion
- retorno_promedio_pct
- volatilidad_retorno_pct

Los registros financieros ambiguos no serán seleccionados arbitrariamente.

## 5. hechos_empresa

Ruta:

/datalake/bvl/gold/eventos/hechos_empresa

Granularidad:

numero_expediente

Fuentes:

- Hechos de Importancia.
- Identidad empresarial.
- Documentos.
- Contenido textual.

Universo principal:

- 338 eventos.
- 277 vinculados mediante RUC_EXACTO.
- 15 vinculados mediante NEMONICO_SMV.
- 46 SIN_PUENTE.
- 292 eventos identificados.
- Cobertura de identidad: 86.39 %.

Los documentos serán agregados previamente por expediente para evitar
multiplicar los eventos.

## 6. impacto_hechos_valor

Ruta:

/datalake/bvl/gold/eventos/impacto_hechos_valor

Granularidad:

numero_expediente + nemonico

Fuentes:

- hechos_empresa
- Cotizaciones

Indicadores propuestos:

- fecha_evento
- fecha_cotizacion_previa
- fecha_cotizacion_evento_o_siguiente
- cierre_previo
- cierre_evento
- retorno_evento_pct
- monto_negociado_evento
- cambio_monto_negociado

No se asumirá causalidad únicamente por coincidencia temporal.

## 7. Reglas de calidad

1. Cada tabla Gold debe declarar su granularidad.
2. Las claves deben validarse antes de escribir el dataset.
3. No se permiten JOIN muchos-a-muchos no controlados.
4. No se utilizará fuzzy matching para asignar empresas.
5. Las divisiones por cero producirán valores nulos.
6. Las series BCRP se integrarán por fecha.
7. Los documentos de Hechos se agregarán antes de unirse a eventos.
8. Los valores múltiples de una empresa se agregarán antes de construir
   empresa_trimestre.
9. Toda transformación Gold tendrá auditoría de conteo, unicidad y cobertura.

## 8. Orden de implementación

1. mercado_valor_diario
2. actividad_sab_diaria
3. empresa_trimestre
4. hechos_empresa
5. impacto_hechos_valor

El desarrollo de Gold será realizado a partir de los datasets Silver ya
validados y documentados.
