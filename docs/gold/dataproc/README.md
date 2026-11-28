# Gold - Dataproc Serverless

## Estado

Preparación local completada.
Ejecución cloud y publicación no autorizadas.

## Configuración candidata

- Proyecto: bvl-analytics-2026.
- Región: southamerica-west1.
- Runtime previsto: 2.2 LTS.
- Cuenta: sa-bvl-gold.
- Driver: 4 vCPU.
- Ejecutores: 2 x 4 vCPU.
- Total configurado: 12 vCPU.
- Compute tier: Standard.
- Asignación dinámica deshabilitada.
- TTL propuesto para smoke test: 20 minutos.

La configuración de CPU no constituye un límite monetario.

## Productos candidatos

- mercado_valor_diario.
- actividad_sab_diaria.
- empresa_trimestre.

## Productos bloqueados

- hechos_empresa.
- impacto_hechos_valor.

## Componentes

- ejecutor_portable.py: constructores y recalculadores.
- main_gold.py: smoke test de driver y workers.
- empaquetar.py: ZIP reproducible.
- requirements-dataproc.txt: dependencias candidatas.
- config_dataproc.json: configuración propuesta.
- preparar_comando_dataproc.py: comando informativo.

## Reproducción local

Desde la raíz del repositorio:

    python src/gold/dataproc/empaquetar.py \
      --salida outputs/dataproc/bvl_gold_modules.zip

    sha256sum -c docs/gold/dataproc/evidencias/SHA256_PAQUETE.txt

    PYTHONPATH="$PWD/outputs/dataproc/bvl_gold_modules.zip" \
      python src/gold/dataproc/main_gold.py --modo smoke-test

    PYTHONPATH="$PWD/outputs/dataproc/bvl_gold_modules.zip" \
      python tests/gold/dataproc/test_ejecutor_portable.py

    python src/gold/dataproc/preparar_comando_dataproc.py

## Validaciones realizadas

- 76 módulos Python empaquetados.
- ZIP reproducible byte a byte.
- Importaciones desde paquete aislado.
- Cinco pruebas unitarias del ejecutor portable.
- Integración de tres recalculadores mediante mocks.
- Smoke test Spark 3.5.1 y Python 3.11.2.
- Importación de módulos en workers Spark locales.
- Conservación de productos bloqueados.

## Pendientes antes de ejecutar en GCP

- Confirmar disponibilidad y compatibilidad del runtime 2.2.
- Confirmar subred autorizada.
- Confirmar bucket autorizado para dependencias.
- Confirmar cuotas CPU disponibles.
- Confirmar permisos efectivos de la cuenta de servicio.
- Validar dependencias Python del runtime.
- Acreditar manifests, firmas y versiones Silver oficiales.
- Integrar acreditación Quality con la lectura Spark.
- Garantizar identidad e integridad de archivos consumidos.
- Validar procesamiento incremental real desde el paquete.
- Validar ejecución distribuida en Dataproc.
- Acreditar salida candidata en staging.

El punto de entrada solo admite smoke-test.
No admite procesamiento oficial ni publicación.

Los argumentos pendientes del comando son marcadores
deliberadamente no operativos.

No se modificaron recursos GCP.
No se utilizaron credenciales cloud.
No se consumieron créditos GCP durante estas pruebas.
