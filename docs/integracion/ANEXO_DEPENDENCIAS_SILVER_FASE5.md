# Anexo de dependencias Silver - Fase 5

**Estado: PROPUESTA PENDIENTE DE RATIFICACION QUALITY.**

## 1. Mapeo Bronze a Silver

| Dataset Bronze | Producto Silver propuesto |
|---|---|
| `bcrp/embig` | `macroeconomia/embig` |
| `bcrp/tipo_cambio` | `macroeconomia/tipo_cambio` |
| `smv/cambios_patrimonio` | `finanzas_empresariales/cambios_patrimonio` |
| `smv/cotizaciones` | `mercado_diario/cotizaciones` |
| `smv/estado_resultados` | `finanzas_empresariales/estado_resultados` |
| `smv/flujo_efectivo` | `finanzas_empresariales/flujo_efectivo` |
| `smv/hechos_importancia` | `hechos_importancia/eventos` |
| `smv/hechos_valores_consulta` | `referencias_smv/hechos_valores_consulta` |
| `smv/montos_sab` | `mercado_diario/montos_sab` |
| `smv/principales_cuentas` | `finanzas_empresariales/principales_cuentas` |
| `smv/situacion_financiera` | `finanzas_empresariales/situacion_financiera` |
| `smv/valores_inscritos` | `referencias_smv/valores_inscritos` |

## 2. Puente RUC-nemonico

- Fuente: `smv/valores_inscritos`.
- Productos del mismo procesamiento:
  - `referencias_smv/valores_inscritos`
  - `referencias_smv/puente_ruc_nemonico`
- No requiere una tabla Silver previa para construir el puente, según `valores_smv_to_silver.py`.
- Quality debe ratificar el alcance de autorización para ambas salidas.

## 3. Identidad empresarial

Dependencias Silver propuestas:
- `hechos_importancia/eventos`
- `finanzas_empresariales/principales_cuentas`
- `referencias_smv/puente_ruc_nemonico`
- `mercado_diario/cotizaciones`
- `referencias_smv/hechos_valores_consulta`

## 4. Restricciones de autorización

- Cada producto debe tener alcance de autorización explícito.
- Las dependencias deben identificarse mediante versiones y manifests concretos.
- Los destinos staging deben ser exclusivos por intento.
- No se permiten sobrescrituras ni publicación oficial durante BUILD_SILVER_STAGING.
- Un intento fallido no reutiliza la autorización consumida.
- La autorización múltiple y las rutas de recuperación requieren ratificación de Quality.

## 5. Estado

- Bronze: BRONZE_PUBLICADO.
- SILVER_AUTORIZADO: NO EMITIDO.
- Spark: BLOQUEADO.
- Publicación Silver: BLOQUEADA.
