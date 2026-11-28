# Entrega tecnica Gold y BigQuery para Matthew

Version: v1.

Estado: CANDIDATO LOCAL NO AUTORIZADO.

Responsable de entrega:
Piero Alexis Mendoza Santos.

Destinatario:
Matthew - Analytics, BI y ML.

## Objetivo

Proporcionar interfaces de consumo analitico
para los tres productos Gold preparados
para integracion BigQuery.

## Proyecto

bvl-analytics-2026

Region acordada:
southamerica-west1

Dataset propuesto:
gold_analytics

## Productos

| Producto | Columnas | Clave |
|---|---:|---|
| mercado_valor_diario | 41 | nemonico, fecha |
| actividad_sab_diaria | 41 | fecha, codigo_sab |
| empresa_trimestre | 58 | id_registro |

Total: 140 columnas Gold.

## Referencias historicas candidatas

Mercado: 83130 registros.

SAB: 21911 registros.

Empresa: 5158 registros.

Total: 110199 registros.

No son conteos oficiales acreditados.

## Orden de lectura

1. LEEME_ENTREGA_MATTHEW.md.
2. CONTRATO_CONSUMO_MATTHEW.md.
3. contratos_consumo.json.
4. diccionario_datos.csv.
5. CRITERIOS_KPI_Y_CALIDAD.md.
6. indicadores_gold_documentados.json.
7. GUIA_INTEGRACION_MATTHEW.md.
8. sql_consumo.
9. Evidencias de validacion.
10. CIERRE_LOCAL_BIGQUERY.md.

## Recursos disponibles

Contratos de consumo para tres productos.

Diccionario de 140 columnas en CSV y JSON.

Catalogo numerico candidato.

Trece indicadores seleccionados.

Cuatro consultas SQL de referencia.

Guia para BigQuery, Looker Studio y ML.

Evidencias de integridad.

## Resultados de validacion

302 controles BigQuery previamente aprobados.

42 controles adicionales de consumo.

344 controles acumulados.

Los controles incluyen pruebas funcionales
locales, sinteticas y comprobaciones estaticas.

No constituyen certificacion productiva.

## Restricciones de consumo

No utilizar staging ni backups como fuentes
productivas.

No asumir que NULL equivale a cero.

No sumar precios, porcentajes ni ratios
sin una definicion de negocio aprobada.

No sumar totales diarios repetidos por SAB.

Preservar cobertura y trazabilidad SAB.

No asumir unicidad financiera por RUC
y trimestre.

No unir productos sin validar cardinalidad.

No utilizar datos futuros en modelos ML.

Los indicadores y sus definiciones requieren
ratificacion semantica y operativa.

## Prerrequisitos operativos

- Fuentes y versiones Gold oficiales.
- Manifests aprobados.
- Autorizacion Quality.
- Carga BigQuery acreditada.
- Conciliacion real de resultados.
- Validacion efectiva GoogleSQL.
- Permisos de lectura autorizados.
- Ratificacion de indicadores.

## Bases tecnicas

Gold v3 ratificado:

62e0f0f76743822bc19859459e4307b2fb9b6ef2

Cierre local BigQuery:

0b68dac7a26ff43ed09b9218958185cd79514407

Repositorio:

https://github.com/Dxnn017/bvl-analytics-platform

Rama:

analytics-piero

El commit exacto de entrega se encuentra
en MANIFIESTO_PAQUETE.json dentro del ZIP.

## Seguridad

No se incluyen credenciales ni claves privadas.

No se incluyen datos financieros de ejecucion.

No se conceden permisos IAM.

No se autoriza ejecucion de SQL bloqueados.

No se autoriza consumo productivo.

No se autoriza publicacion.

## Recepcion solicitada

Confirmar integridad SHA-256 del paquete.

Confirmar recepcion de los tres contratos.

Confirmar recepcion del diccionario.

Confirmar revision de las restricciones.

La recepcion documental no constituye
autorizacion de consumo productivo.
