# BVL Analytics — Informe técnico de cierre Fase 6

## 1. Alcance

Responsable Gold: Piero.
Coordinación Silver: Daniela.
Coordinación GCP y Quality: Lizeth.

Commit de referencia: b290fc2bc7bc7de8792698c98c54307c86b059b8.

Entrega técnica candidata, sin autorización de publicación oficial.

## 2. Validaciones previas

Daniela reprodujo independientemente el commit de referencia:

- SHA-256: 9/9.
- Pruebas unitarias: 8/8.
- Escenarios Spark: 3/3.

Estas validaciones se conservan como antecedentes.
No se repitieron las ocho pruebas unitarias durante este cierre.

## 3. Ajustes de integración Gold

### Normalización de nemónicos

Regla acordada: upper(trim(nemonico)).

- Normalización en constructor, detector y recalculador.
- Rechazo de claves nulas y vacías.
- Conservación de nemonico_original.
- Detección de colisiones canónicas.
- Sin eliminación automática para resolver colisiones.
- Vinculación empresarial mediante clave canónica.
- Conservación de registros no afectados en fixture.
- Idempotencia comprobada localmente.

En bases históricas sin nemonico_original se conserva
el nemónico histórico disponible como referencia.

La ampliación del esquema requiere coordinación
con los consumidores y contratos de publicación.

### Cobertura SAB

Contrato candidato: BVL-SILVER-SAB-COVERAGE-v0.1.

Campos incorporados:

- registros_elegibles
- registros_cuarentena
- archivos_excluidos
- estado_cobertura
- version_silver
- scope_quality_id
- trazabilidad_verificada
- cobertura_completa

Estados soportados:

- COMPLETA
- PARCIAL
- DESCONOCIDA

Se conserva actividad_diaria_valida como indicador aritmético
independiente de cobertura_completa.

Se comprobó:

- Propagación de los tres estados.
- Ausencia de metadatos como DESCONOCIDA.
- Conservación de filas e indicadores existentes.
- Rechazo de fechas duplicadas.
- Rechazo de versiones incompatibles.
- Rechazo de conteos inconsistentes.
- Recálculo exclusivo por cambios de metadatos.
- Conservación de jornadas no afectadas.
- Idempotencia local.

No se imputaron registros faltantes.

Los fixtures empleados reproducen los casos sintéticos
comunicados por Daniela, sin constituir evidencia oficial.

## 4. Conciliación de productos

| Producto | Histórico local | GCP reportado | Diferencia |
|---|---:|---:|---:|
| mercado_valor_diario | 83130 | 83130 | 0 |
| actividad_sab_diaria | 22156 | 21911 | 245 |
| empresa_trimestre | 5249 | 5158 | 91 |
| TOTAL | 110535 | 110199 | 336 |

Los tres productos locales presentan:

- Claves primarias inválidas: 0.
- Grupos duplicados de clave primaria: 0.

Los resultados GCP fueron comunicados por Lizeth.
No se realizó lectura directa de los Parquet GCP.

### Mercado

Coincidencia exacta de conteos.

Pendiente verificar versiones, contenido y normalización
sobre los archivos candidatos oficiales.

### SAB

22156 - 245 = 21911.

La diferencia coincide con la cuarentena comunicada.

Pendiente acreditar los 245 registros y los 14 archivos
excluidos mediante inventario verificable.

No se atribuyen exclusiones a jornadas sin evidencia.

### Empresa trimestre

5249 - 5158 = 91.

La diferencia permanece pendiente de conciliación nominal.

No existe evidencia suficiente para atribuir los 91 registros
a exclusiones, correcciones o cambios de versión concretos.

Las seis colisiones BNB no explican esta diferencia.

## 5. Tratamiento financiero BNB

Empresa: BNB Valores Perú S.A. Sociedad Agente de Bolsa.
RPJ: SG0005.

| Periodo | Activo total | Utilidad neta |
|---|---|---|
| 2021-T1 | 11408 / 11408 | 433 / 196 |
| 2021-T2 | 11417 / 11418 | 333 / 147 |
| 2021-T3 | 12495 / 12495 | 529 / 164 |

Se verificaron seis filas con id_registro distintos.

Todas presentan:

- colision_financiera = true.
- apto_kpi_general = false.
- roa_calculable = false.
- roe_calculable = false.
- endeudamiento_calculable = false.
- margen_neto_calculable = false.
- autonomia_calculable = false.
- motivos_calidad = COLISION_FINANCIERA.

Propuesta:

1. Conservar las seis filas e identificadores originales.
2. Mantener su exclusión de indicadores financieros oficiales.
3. No sumar, promediar ni seleccionar registros arbitrariamente.
4. Contrastar documentos y versiones de origen.
5. Resolver únicamente mediante evidencia acreditada y Quality.

El procesador financiero existente cumple localmente
las condiciones de conservación y exclusión.

No se modificó dicho procesador.

Pendiente verificar las seis filas en el candidato GCP.

## 6. Viabilidad de hechos e impacto

### hechos_empresa

Procesador existente:
src/gold/eventos/build_hechos_empresa.py

Viabilidad técnica: disponible para desarrollo experimental.

Viabilidad oficial: bloqueada.

identidad_empresarial no se encuentra acreditada.

documental_restringido no autoriza consumo Gold ni
sustituye la identificación empresarial requerida.

### impacto_hechos_valor

Procesador existente:
src/gold/eventos/build_impacto_hechos_valor.py

Viabilidad técnica: disponible para desarrollo experimental.

Viabilidad oficial: bloqueada por dependencia de hechos_empresa.

No se deben generar impactos oficiales mediante
asociaciones empresariales no acreditadas.

## 7. Procesadores ajustados

- src/gold/mercado/build_mercado_valor_diario.py
- src/gold/incremental/detectar_alcance_mercado.py
- src/gold/incremental/recalcular_mercado_valor.py
- src/gold/mercado/build_actividad_sab_diaria.py
- src/gold/mercado/cobertura_sab.py
- src/gold/incremental/recalcular_actividad_sab.py
- src/gold/incremental/ejecutar_actividad_sab_local.py

## 8. Evidencias

- evidencias/pruebas_y_conciliacion.txt
- evidencias/validacion_recalculadores.txt
- evidencias/SHA256SUMS.txt

## 9. Restricciones

- Sin modificación de releases Silver ratificados.
- Sin modificación de versiones Gold oficiales.
- Sin ejecución adicional en GCP.
- Sin publicación oficial.
- Sin autorización Quality concedida.
- Sin validación real adicional en BigQuery.
- Sin eliminación productiva de registros financieros.

## 10. Conclusión

Los ajustes Silver-Gold fueron implementados y
validados localmente mediante pruebas focalizadas.

Los productos existentes pueden continuar su revisión
como candidatos sujetos a los controles Quality.

El cierre técnico local no equivale a aprobación productiva.

Pendiente revisión focalizada de Daniela y resolución
de los bloqueos oficiales por Lizeth y Quality.
