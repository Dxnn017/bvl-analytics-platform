# Procedimiento de carga historica Gold a BigQuery

## 1. Estado

PROPUESTA_LOCAL_NO_AUTORIZADA.

Proyecto: bvl-analytics-2026
Region propuesta: southamerica-west1
Rama: analytics-piero

Base Gold ratificada tecnicamente:
62e0f0f76743822bc19859459e4307b2fb9b6ef2

La conformidad tecnica de Gold v3 no constituye
autorizacion de ejecucion ni publicacion.

## 2. Objetivo

Incorporar los tres productos historicos Gold
oficialmente aprobados en BigQuery.

Conteos historicos candidatos:

- mercado_valor_diario: 83130.
- actividad_sab_diaria: 21911.
- empresa_trimestre: 5158.
- Total: 110199.

Los conteos definitivos deben corresponder
a los manifests Gold oficialmente aprobados.

Las 91 excepciones financieras permanecen
separadas del producto analitico.

## 3. Arquitectura

Origen:

gs://bvl26-gold-7f3a9c/versions/

Control documental:

gs://bvl26-control-7f3a9c/

Datasets propuestos:

- gold_analytics
- gold_analytics_staging
- gold_analytics_control

La existencia, ubicacion, permisos y configuracion
de estos datasets requieren acreditacion operativa.

## 4. Condiciones previas

1. Publicacion oficial Gold aprobada por Quality.
2. Manifest y firma acreditados por producto.
3. Version vigente y registro de versiones consistente.
4. Autorizacion expresa de carga historica BigQuery.
5. Identidad efectiva del ejecutor acreditada.
6. Ubicaciones GCS y BigQuery compatibles.
7. Inventario exacto de archivos y generaciones.
8. Huellas SHA-256 y tamanos verificados.
9. Contratos de salida y destino acreditados.
10. Destinos historicos iniciales vacios.
11. Identidad unica del lote y control de replay.
12. Mecanismo de exclusion de escritores concurrentes.

La autorizacion Quality para generar Gold no
implica automaticamente autorizacion BigQuery.

## 5. Preflight historico

Verificar:

- Firma de manifest y autoridad firmante.
- Identidad de producto y version.
- Vigencia de autorizacion.
- Correspondencia exacta de archivos.
- Generaciones, tamanos y hashes.
- Esquema fisico Parquet.
- Particiones Hive.
- Columnas obligatorias.
- Claves no nulas ni duplicadas.
- Conteos por archivo, particion y producto.
- Valores numericos representables.
- Ausencia de archivos no acreditados.
- Identidad y huella del lote.

No utilizar comodines para seleccionar
archivos no incluidos en el inventario aprobado.

BigQuery no admite generaciones GCS en las URI
de carga Parquet. Debe acreditarse una ubicacion
de transferencia protegida contra sustituciones,
o un mecanismo equivalente de lectura exacta.

No se permite sustituir la acreditacion anterior
por una simple comprobacion de hashes antes
y despues de la carga.

## 6. Compatibilidad numerica

Spark string -> BigQuery STRING.
Spark date -> BigQuery DATE.
Spark double -> BigQuery FLOAT64.
Spark boolean -> BigQuery BOOL.
Spark integer -> BigQuery INT64.
Spark long -> BigQuery INT64.

Decimal(24,2), Decimal(25,2) y Decimal(26,2)
son compatibles con el rango NUMERIC.

Decimal(34,2) no garantiza compatibilidad
por rango con NUMERIC.

Columnas SAB sujetas a control:

- total_compras_diario
- total_ventas_diario
- total_actividad_diaria

Validar representabilidad exacta de cada valor.

La representacion de transferencia candidata
para estas columnas sera Decimal(31,2),
si todos sus valores son compatibles.

No aplicar redondeos, truncamientos,
saturaciones ni reemplazos por NULL.

No modificar el Parquet Gold oficial.

Ante desbordamiento, bloquear el lote y
solicitar una decision de contrato.

## 7. Staging historico

Utilizar tablas historicas aisladas por lote.

No reutilizar directamente las tablas delta
de integracion incremental.

Configurar explicitamente el esquema destino,
tipos decimales y reconstruccion de particiones.

La carga inicial del staging debe ser exclusiva
y no sobrescribir tablas de otros lotes.

No utilizar WRITE_TRUNCATE sobre destinos
definitivos.

No ejecutar DDL ni DML sin autorizacion.

## 8. Validacion previa a incorporacion

Validar por producto:

- Conteo exacto autorizado.
- Esquema y tipos compatibles.
- Claves de negocio no nulas.
- Ausencia de duplicados.
- Particiones correctas.
- Comparacion de valores e indicadores.
- Precision monetaria SAB.
- Ausencia de excepciones financieras
  dentro del producto aprobado.
- Correspondencia con version Gold.

No acreditar cobertura SAB completa cuando
el estado ratificado sea DESCONOCIDA.

## 9. Incorporacion historica definitiva

Preparar los tres destinos vacios previamente
y acreditar que no existen cargas concurrentes.

La incorporacion definitiva debe utilizar
una estrategia transaccional validada.

La confirmacion del lote debe registrar:

- batch_id
- run_id
- versiones Gold
- hashes de manifests
- contratos
- huella del payload
- conteos
- referencias de jobs
- estado del lote

No confirmar parcialmente una entrega de tres
productos como si estuviera completa.

Los mecanismos de exclusion concurrente,
transaccion y replay deben comprobarse en
los pasos posteriores.

## 10. Control de replay

Mismo lote y mismo contenido:

No duplicar registros.

Mismo lote y contenido diferente:

Rechazar.

Lote historico nuevo con destino no vacio:

Bloquear y solicitar decision expresa.

No convertir una carga historica inicial
en una actualizacion incremental implicita.

## 11. Restricciones

No modificar Gold v3 ratificado.
No modificar Silver oficial.
No publicar datos.
No ejecutar GCP.
No eliminar datos.
No utilizar autorizaciones simuladas
como autorizaciones productivas.

hechos_empresa e impacto_hechos_valor
quedan fuera del alcance.

## 12. Referencias tecnicas

https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet

https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types

https://cloud.google.com/bigquery/docs/transactions
