# Entrega conjunta Gold — Cierre Fase 6

## Documentación

- INFORME_CIERRE_FASE6.md
- MATRIZ_SILVER_GOLD_ACTUALIZADA.md
- CANDIDATOS_QUALITY.md

## Entorno

Python 3.11 y PySpark 3.5.1.

Desde la raíz del repositorio:

    export PYTHONDONTWRITEBYTECODE=1
    export PYTHONPATH="$PWD/src/gold/mercado:$PWD/src/gold/incremental:$PWD"

Pruebas focalizadas:

    python docs/gold/cierre_fase6/fixtures/pruebas_focalizadas.py
    python docs/gold/cierre_fase6/fixtures/validacion_recalculadores.py

Ambos scripts contienen verificaciones que requieren
los Parquet históricos locales disponibles en:

    outputs/integracion_daniela_local_20261127/

Los Parquet no se incluyen en Git.

Sin dichos archivos, la auditoría histórica no es
reproducible completamente desde un checkout limpio.

Las pruebas sintéticas y las auditorías históricas
se distinguen dentro de los resultados.

## Integridad

    sha256sum -c docs/gold/cierre_fase6/evidencias/SHA256SUMS.txt

## Restricciones

Entrega candidata para revisión de Daniela y Lizeth.
Sin autorización de publicación productiva.
