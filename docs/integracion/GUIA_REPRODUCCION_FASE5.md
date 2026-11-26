# Guía de reproducción — Fase 5 BVL

## Requisitos
- Python 3
- Biblioteca cryptography
- Contrato oficial Bronze–Silver v1 entregado por Quality

## Preparación

Crear entorno virtual:

    python3 -m venv .venv-fase5
    .venv-fase5/bin/python -m pip install cryptography

Configurar contrato:

    export BVL_CONTRACT_V1_PATH="/ruta/al/contrato_bronze_silver_v1.json"

SHA-256 obligatorio:

    6f33f3a88e04b38a5e75863d3ccc67005231f05d474b9916d34fd7030edef20a

## Pruebas

Ejecutar desde la raíz del repositorio:

    .venv-fase5/bin/python -m unittest discover -s tests -p 'test_puerta_silver_operativa_v1_candidata.py' -v

    .venv-fase5/bin/python -m unittest discover -s tests -p 'test_validar_mapeo_silver_fase5.py' -v

Resultados de referencia:
- Puerta Silver: 29 pruebas aprobadas.
- Mapeo Silver: 8 pruebas aprobadas.

## Restricciones

- Las pruebas son locales y utilizan servicios GCS simulados.
- No se consumen autorizaciones reales.
- No se ejecuta Spark ni Dataproc.
- No se publica Silver.
- Las plantillas no son autorizaciones firmadas.
- Quality debe aprobar la puerta y emitir las autorizaciones.
