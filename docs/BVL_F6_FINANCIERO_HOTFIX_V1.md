# BVL Fase 6 — Hotfix financiero aislado

Origen: `BVL_F6_INCIDENCIA_FINANCIERA.zip`, informe independiente Quality de 24 objetos Bronze (5 datasets).

## Alcance

- `INC-F6-SMV-001`: aceptar **solo** `ResponseCode="OK"`, `Message="ConsultaGenerada"`, y `data` como lista con exactamente esas tres claves raíz. Conserva las validaciones de errores, claves JSON duplicadas y wrappers ambiguos. No cambia los códigos históricos admitidos.
- `INC-F6-SMV-002`: `smv/cambios_patrimonio` exige RPJ/Ejercicio; no crea ni imputa RUC ausente. Otros 3 financieros siguen exigiendo RUC/Ejercicio. RPJ vacío se rechaza.
- La tabla `smv/principales_cuentas` no cambia. Sus 5,249 registros, las 3 colisiones conservadas y las pruebas previas permanecen como evidencia independiente de Quality.
- No se modifican puertas Fase 5, core, writer, configuración IAM ni otros datasets.

## Reproducción y condiciones para Quality

1. Instalar el parche **una vez** en un checkout Git basado en el commit consolidado `ea1400314f10ea4fd24f50ab1519abf87da0d03a`.
2. Ejecutar 11 pruebas unitarias y 5 Spark locales (datos sintéticos) y regresar al flujo de revisión independiente.
3. **Quality** debe ejecutar con los 24 archivos reales las cinco transformaciones, comprobar salida Parquet local y conciliar filas, duplicados, esquemas, particiones y valores. Incluido `auditar_financieras_reales_local.py`, que lee el `inventario_local.json` aportado por Quality y verifica bytes locales contra SHA-256. Las rutas de dicho inventario pertenecen al entorno Quality: en Debian no estarán disponibles salvo que Quality las entregue.
4. Confirmar que no hay diferencias semánticas y que RPJ queda como identificador de origen sin RUC inventado; actualizar el contrato Gold si requiere RUC para ese producto.
5. Con revisión exitosa, Quality deberá aprobar el nuevo commit, reconstruir release firmado y autorizar nuevos intentos de producción. No lanzar Cloud antes.

## Riesgos

- Este hotfix no acredita por sí solo los registros Silver ni los objetos reales: esas evidencias se obtienen al ejecutar el script en el entorno autorizado de Quality.
- Evitar mezclar en el mismo commit limpiezas de documentación o el trabajo de Hechos/Identidad.
- La cobertura JSON se limita explícitamente al envelope SMV documentado. Para un envelope distinto, bloquear y solicitar nueva ratificación.

## Comandos mínimos

```bash
python3 -m unittest discover -s tests -p 'test_fase6_financiero_hotfix_puros.py' -v
SPARK_LOCAL_IP=127.0.0.1 spark-submit --master 'local[1]' --conf spark.ui.enabled=false --conf spark.sql.ansi.enabled=true tests/test_fase6_financiero_hotfix_spark_local.py
```
