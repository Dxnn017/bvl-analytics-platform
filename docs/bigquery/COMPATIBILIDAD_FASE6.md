# Compatibilidad Gold - BigQuery

## Estado

Propuesta local. Sin autorización productiva.

Commit Gold de referencia:
308e063d2a2d310fad8af76a86228ad441a61f21

## Cambios

Mercado valor diario:
- 40 a 41 columnas.
- Incorporación de nemonico_original.
- Validación de claves canónicas.
- Destinos históricos no canónicos requieren migración autorizada.

Actividad SAB diaria:
- 33 a 41 columnas.
- Incorporación de ocho campos de cobertura y trazabilidad.
- Estados COMPLETA, PARCIAL y DESCONOCIDA.
- Independencia entre cobertura y validez aritmética.

Empresa trimestre:
- Sin modificaciones.

## Componentes adecuados

- Contratos JSON.
- DDL de destino.
- DDL de staging.
- MERGE incremental.
- SQL transaccional.

## Validaciones

- Correspondencia estructural JSON y DDL.
- Correspondencia de columnas INSERT y VALUES.
- Propagación de campos nuevos mediante UPDATE.
- Detección de cambios de trazabilidad y cobertura.
- Replay local sin reaplicación.
- Actualización sin cambios contabilizada como NOOP.
- SHA-256 sensible a cambios de contenido.
- Conservación de bloqueos de seguridad.

## Reproducción

Desde la raíz del repositorio:

    PYTHONPATH="$PWD" python tests/bigquery/test_compatibilidad_fase6.py

## Limitaciones

- Simulación local mediante SQLite.
- Validación estructural, no ejecución real de SQL BigQuery.
- No se acredita concurrencia distribuida.
- No se valida todavía el preflight firmado en GCP.
- No se autoriza DELETE ni publicación.
- No se migran tablas existentes automáticamente.
- Las columnas obligatorias nuevas requieren una estrategia
  de migración autorizada si existen tablas históricas.
- Pendiente verificar consumidores BI y ML.
- Pendiente garantizar que la cobertura de una versión
  Silver anterior no se conserve indebidamente al cambiar
  de versión sin nueva evidencia.
- Pendiente verificar coherencia de metadatos SAB por jornada
  sobre el candidato completo.
- El control transaccional oficial de replay sigue pendiente.

No se modificaron recursos GCP ni releases ratificados.
