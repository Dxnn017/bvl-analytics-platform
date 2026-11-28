# Matriz actualizada Silver-Gold — Fase 6

| Gold | Dependencias | Clave Gold | Particiones | Estado |
|---|---|---|---|---|
| mercado_valor_diario | cotizaciones, tipo_cambio, embig, puente_ruc_nemonico | nemonico, fecha | anio, mes | Candidato a revisión |
| actividad_sab_diaria | montos_sab y metadatos de cobertura | codigo_sab, fecha | anio, mes | Candidato condicionado |
| empresa_trimestre | principales_cuentas | id_registro | anio, trimestre | Conciliación pendiente |
| hechos_empresa | identidad_empresarial y fuentes autorizadas de hechos | numero_expediente | Por verificar en candidato | Bloqueado |
| impacto_hechos_valor | hechos_empresa y mercado_valor_diario | numero_expediente, nemonico | Por verificar en candidato | Bloqueado |

## Reglas transversales

### Mercado

- Clave canónica: upper(trim(nemonico)).
- Conservación de trazabilidad.
- Incidencia ante colisiones canónicas.
- Sin modificación de releases ratificados.

### SAB

Contrato BVL-SILVER-SAB-COVERAGE-v0.1.

- Una fila de metadatos por fecha y versión Silver.
- COMPLETA, PARCIAL o DESCONOCIDA.
- Validez aritmética independiente de completitud.
- Sin imputación ni distribución artificial de cuarentena.
- Cambios de cobertura generan alcance incremental.

### Finanzas

- id_registro como clave primaria.
- Conservación de colisiones empresariales.
- Exclusión de registros no elegibles de los KPIs.
- Sustituciones productivas sujetas a autorización.

### Hechos e impacto

- documental_restringido no habilita consumo oficial Gold.
- identidad_empresarial continúa bloqueada.
- impacto no puede publicarse sin hechos acreditados.

## Dependencias físicas

Mercado:
- mercado_diario/cotizaciones
- macroeconomia/tipo_cambio
- macroeconomia/embig
- referencias_smv/puente_ruc_nemonico

SAB:
- mercado_diario/montos_sab
- Archivo independiente de metadatos de cobertura.

Finanzas:
- finanzas_empresariales/principales_cuentas
- Confirmar versión y ruta física oficial.

La presente matriz es candidata y requiere ratificación.
