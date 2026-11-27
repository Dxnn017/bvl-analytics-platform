# BVL Analytics — Entrega técnica Gold a Quality

## Repositorio
- Proyecto: bvl-analytics-platform
- Rama: analytics-piero
- Responsable: Piero
- Entorno validado: Debian 12, PySpark local.

## Productos Gold

| Producto | Registros históricos | Estado |
|---|---:|---|
| mercado_valor_diario | 83130 | Incremental local validado |
| actividad_sab_diaria | 22156 | Incremental local validado; requiere cobertura parcial oficial |
| empresa_trimestre | 5249 | Incremental local validado; identidad pendiente |
| hechos_empresa | 346 | Histórico experimental; publicación bloqueada |
| impacto_hechos_valor | 175 | Histórico experimental; publicación bloqueada |

## Evidencias de pruebas
- PRUEBA_CONJUNTA_CINCO_GOLD_APROBADA_OK
- INTEGRACION_SILVER_GOLD_APROBADA_OK
- ENCADENAMIENTO_CANDIDATAS_REALES_APROBADO_OK
- VALIDACION_MANIFESTS_APROBADA_OK
- RESTRICCIONES_QUALITY_APROBADAS_OK

## Controles
- Gold no consume Silver staging como versión oficial.
- Los cinco productos requieren habilitación explícita de Quality.
- Hechos e Impacto mantienen publicación oficial bloqueada.
- Montos SAB: 21911 candidatos válidos, 245 en cuarentena y 14 consultas afectadas.
- No se completan artificialmente datos faltantes.
- Empresa requiere acreditar dependencias de identidad.
- Validaciones locales de SHA-256, generación declarada y continuidad.
- Firmas, generaciones y permisos GCS reales pendientes de Quality.

## Componentes principales
- src/gold/incremental/ejecutar_cinco_gold_memoria.py
- src/gold/incremental/consumidor_versiones_gold.py
- src/gold/incremental/validar_publicacion_silver_local.py
- src/gold/incremental/politica_quality_gold.py

## Reproducción
Usar Python del entorno virtual:
`/home/piero/BigData_UPAO/bigdata_env/bin/python`

Para validar sintaxis:
`python3 -m py_compile src/gold/incremental/*.py`

Las pruebas integrales utilizan snapshots locales bajo
`/home/piero/bvl_entregas/`.

## Pendiente
1. Ratificación de contrato y versiones oficiales Silver.
2. Verificación criptográfica, firma y generación GCS real.
3. Consumidor operativo de current.json/manifests.
4. Publicación conjunta validada de puente y valores inscritos.
5. Pruebas de idempotencia y regresión del consumidor.
6. Integración autorizada con Dataproc y GCS.
7. Publicación Gold bajo control de Quality.

**Ninguna prueba local concede autorización productiva.**
