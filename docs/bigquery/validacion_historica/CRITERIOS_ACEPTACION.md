# Criterios de aceptacion historica BigQuery

Estado: CANDIDATO LOCAL NO AUTORIZADO.

## Controles implementados

- Identidad de producto y version.
- Referencia SHA-256 del manifest.
- Contrato Gold vinculado.
- Conteo exacto.
- Claves validas y sin duplicados.
- Esquema contractual completo.
- Particiones y extremos temporales.
- Conteos de valores presentes y nulos.
- Minimos, maximos y promedios.
- Comparacion decimal exacta.
- Comparacion FLOAT64 con tolerancia candidata.

## Representacion de resultados

- DATE: texto ISO YYYY-MM-DD.
- INT64: entero JSON.
- FLOAT64: numero JSON finito.
- NUMERIC y BIGNUMERIC: texto decimal exacto.
- NULL: null JSON.

Los promedios de INT64 y NUMERIC se representan
como texto decimal, conforme al perfil BIGNUMERIC.

Los promedios decimales del origen deben obtenerse
con una politica de precision compatible.

## Tolerancias candidatas

FLOAT64:

- Relativa: 1e-9.
- Absoluta: 1e-8.

Las tolerancias requieren ratificacion antes
de emitir conformidad productiva.

No se permiten tolerancias monetarias implicitas.

## Condiciones pendientes

- Publicacion Gold oficial.
- Autenticidad de manifests y firmas.
- Autorizacion independiente BigQuery.
- Consultas ejecutadas y resultados acreditados.
- Verificacion de particionamiento y clustering.
- Identidad efectiva del ejecutor.
- Referencias de jobs BigQuery.
- Conciliacion fila a fila contra el origen.
- Validacion de los indicadores de negocio.

La coincidencia de perfiles agregados no acredita
igualdad completa de los datos.

La prueba local utiliza resultados SQL sinteticos.
No ejecuta el motor BigQuery.

Ningun informe generado autoriza produccion.
