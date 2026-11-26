# Consulta puntual para Quality: procedencia de Hechos y formato launch REST

Para cerrar la Fase 6 hay un bloqueo documental que no puede corregirse solo en código: `hechos_importancia_to_silver.py` lee dos CSV históricos fuera del manifest Bronze firmado de 1,979 objetos:

- `/datalake/bvl/metadata/smv/hechos_importancia/ingestion_date=2026-10-04/hechos_eventos_2025.csv` (tamaño histórico indicado: 206,354 bytes)
- `/datalake/bvl/metadata/smv/hechos_importancia/ingestion_date=2026-10-04/hechos_documentos_2025.csv` (tamaño histórico indicado: 543,299 bytes)

Las cifras históricas no constituyen verificación SHA, generación GCS ni conformidad Quality. El cierre original de F5 y sus 12 manifests no deben modificarse.

**Decisión necesaria:** ¿existe una ubicación oficial versionada para estos dos archivos con URI, generación, SHA-256, fuente de origen y autorización Quality? Si no existe, determinar mediante protocolo formal cómo ingresarlos como dependencia adicional controlada/manifest suplementario, sin incorporarlos ficticiamente al lote histórico. Ratificar si `hechos_importancia/eventos` puede construirse desde las fuentes oficiales bajo ese modelo.

**En una sola respuesta, solicitar también** si Quality acepta revisar la propuesta `BVL-SILVER-LAUNCH-CONTEXT-REST-v1` para los 10 SINGLE_SOURCE pendientes y la publicación atómica de ambas salidas `smv/valores_inscritos`, o qué modificación precisa exige antes de emitir las firmas individuales.

Hasta esa decisión: no autorizar Hechos ni derivado `identidad_empresarial`, no ejecutar Dataproc ni consumir claims.
