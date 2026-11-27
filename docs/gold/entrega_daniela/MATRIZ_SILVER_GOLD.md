# Matriz de dependencias Silver -> Gold

## 1. Contexto

- Proyecto: bvl-analytics-2026
- Rama: analytics-piero
- Commit base: e20bd5c068a87f7b586e2b0736f06d7d5ef8f42d
- Fecha de validacion: 2026-11-27
- Entorno: local
- Python: 3.11.2
- PySpark: 3.5.1
- Publicacion autorizada: NO

## 2. Matriz de dependencias

| Producto Gold | Dataset Silver | Dependencia | Alcance de recalculo |
|---|---|---|---|
| mercado_valor_diario | mercado_diario/cotizaciones | Directa | Nemonico y fecha |
| mercado_valor_diario | macroeconomia/tipo_cambio | Complementaria | Valores de fechas afectadas |
| mercado_valor_diario | macroeconomia/embig | Complementaria | Valores de fechas afectadas |
| mercado_valor_diario | referencias_smv/puente_ruc_nemonico | Identidad empresarial | Fechas de nemonicos afectados |
| actividad_sab_diaria | mercado_diario/montos_sab | Directa | Jornada completa afectada |
| empresa_trimestre | estados financieros/principales_cuentas | Directa | RPJ, ejercicio y trimestre |

Nota: la denominacion de la ruta financiera es logica.
La ruta fisica debe verificarse contra el contrato Silver vigente.

## 3. Granularidad, claves y particiones

| Producto | Clave Gold | Particionamiento |
|---|---|---|
| mercado_valor_diario | nemonico, fecha | anio, mes |
| actividad_sab_diaria | fecha, codigo_sab | anio, mes |
| empresa_trimestre | id_registro | anio, trimestre |
| hechos_empresa | Pendiente de acreditacion Quality | anio |
| impacto_hechos_valor | Pendiente de acreditacion Quality | anio |

## 4. Reglas de procesamiento incremental

### mercado_valor_diario

- Detectar cambios en las cotizaciones.
- Identificar fechas afectadas por cambios macroeconomicos.
- Identificar nemonicos afectados por cambios en el puente.
- Recalcular las claves afectadas.
- Conservar los registros no afectados.
- Validar unicidad de la clave Gold.

Limitacion confirmada:
el detector no normaliza de manera canonica los nemonicos.
DEMO1 y " demo1 " generan dos claves afectadas.

### actividad_sab_diaria

- Identificar las fechas afectadas.
- Recalcular la jornada completa disponible en Silver.
- Recalcular participaciones, concentracion y rankings.
- Conservar las fechas no afectadas.
- No imputar registros faltantes como cero.

Limitacion confirmada:
la cobertura parcial puede generar indicadores aparentemente
validos sobre un universo incompleto.

La politica Quality actual rechaza el fixture sintetico porque
sus cantidades no coinciden con la evidencia historica esperada.

### empresa_trimestre

- Identificar grupos afectados mediante RPJ, ejercicio y trimestre.
- Recalcular el grupo financiero completo.
- Sustituir los registros afectados en la candidata.
- Conservar los grupos no afectados.
- Validar unicidad de id_registro.
- Evaluar colisiones financieras.

La sustitucion tecnica en una candidata no constituye autorizacion
de eliminacion o sustitucion en una version oficial.

## 5. Resultados sinteticos

| Escenario | Resultado | Estado |
|---|---|---|
| Normalizacion de nemonicos | Dos claves para un mismo nemonico logico | Limitacion reproducida |
| Cuarentena SAB | Participacion 50% a 100%; HHI 5000 a 10000 | Limitacion reproducida |
| Sustitucion financiera | ROA 10% a 20%; cero diferencias no afectadas | Aprobado localmente |
| Incrementalidad y trazabilidad | 8 pruebas unitarias aprobadas | Aprobado localmente |

## 6. Conservacion de versiones

Los productos no afectados deben conservar sus versiones vigentes.

Los productos hechos_empresa e impacto_hechos_valor permanecen
fuera de la validacion funcional conjunta por sus dependencias
pendientes de Quality.

Ninguna candidata local sustituye automaticamente una version oficial.

## 7. Evidencias

- fixtures/prueba_nemonicos.py
- fixtures/prueba_cuarentena_sab.py
- fixtures/prueba_sustitucion_financiera.py
- reproducir_pruebas.sh
- evidencias/resultados_pruebas.txt
- evidencias/SHA256SUMS.txt

## 8. Restricciones

- No se ejecutaron operaciones en BigQuery real.
- No se publicaron versiones oficiales.
- No se modificaron datos Silver oficiales.
- No se modificaron permisos IAM.
- No se acredita la autorizacion de Quality.
- La cuarentena SAB fue simulada, no ejecutada en Silver.
