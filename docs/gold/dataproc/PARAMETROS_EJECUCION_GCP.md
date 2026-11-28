# Parametros candidatos para GCP

## Infraestructura prevista

| Parametro | Valor |
|---|---|
| Proyecto | bvl-analytics-2026 |
| Region | southamerica-west1 |
| Runtime previsto | 2.2 LTS |
| Compute tier | Standard |
| Driver | 4 vCPU |
| Ejecutores | 2 |
| CPU por ejecutor | 4 vCPU |
| CPU total configurada | 12 vCPU |
| Asignacion dinamica | Deshabilitada |
| TTL propuesto smoke test | 20 minutos |

Cuenta de servicio:

    sa-bvl-gold@bvl-analytics-2026.iam.gserviceaccount.com

Configuracion:

    src/gold/dataproc/config_dataproc.json

Generador informativo:

    src/gold/dataproc/preparar_comando_dataproc.py

El generador no ejecuta gcloud.

## Artefactos

Punto de entrada:

    src/gold/dataproc/main_gold.py

Paquete Python:

    outputs/dataproc/bvl_gold_modules.zip

Generacion desde la raiz del repositorio:

    python src/gold/dataproc/empaquetar.py \
      --salida outputs/dataproc/bvl_gold_modules.zip

Verificacion:

    sha256sum -c \
      docs/gold/dataproc/evidencias/SHA256_PAQUETE_QUALITY_V03.txt

## Dependencias candidatas

Archivo:

    src/gold/dataproc/requirements-dataproc.txt

Dependencias:

- google-cloud-storage >=2.18,<4
- google-auth >=2.35,<3
- cryptography >=43,<50
- pyarrow >=14,<20

Entorno local validado:

- Python 3.11.2.
- PySpark 3.5.1.

PySpark debe ser proporcionado por el runtime Dataproc.

Los rangos de dependencias son candidatos.
No constituyen un lockfile validado para GCP.
La compatibilidad efectiva debe comprobarse en el runtime.

## Modos

smoke-test:

- Verificacion de Spark y distribucion de modulos.
- Sin consumo Silver.
- Sin publicacion.

preflight-quality:

- Bloqueado deliberadamente en CLI.
- No inicia Spark.
- No admite ejecucion cloud por ahora.

## Interfaz Python candidata

    preparar_preflight_candidato(
        spark,
        cliente,
        producto,
        evidencias,
        pem_quality,
    )

Cada dependencia requiere:

- autorizacion_bytes
- firma_autorizacion_der
- tipos_esperados
- claves
- particiones_por_uri
- schema_spark

El llamador debe acreditar el origen de estos contratos.

## Pendientes de infraestructura

- Subred autorizada.
- Bucket de dependencias autorizado.
- Cuotas disponibles.
- Runtime disponible y compatible.
- Permisos efectivos de cuenta de servicio.
- Instalacion o disponibilidad de dependencias.
- Identidad del codigo desplegado.
- Insumos y autorizaciones oficiales.

No se debe ejecutar el comando candidato hasta
resolver los marcadores pendientes y recibir autorizacion.

La configuracion CPU y TTL no garantiza un coste maximo.
