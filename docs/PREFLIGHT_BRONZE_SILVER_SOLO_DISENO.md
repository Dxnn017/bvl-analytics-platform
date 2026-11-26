# Preflight Bronze → Silver (solo diseño y validación local)

Este módulo valida **exclusivamente** el borrador de contrato remitido por Quality.

- 12 datasets, 3,599 objetos y 393,623,640 bytes.
- Distingue `landing/` de prefijos oficiales propuestos sin `landing/`.
- Exige que el borrador esté bloqueado, sin manifest oficial ni generaciones definitivas.
- Enlaza conceptualmente los conjuntos con procesadores existentes; **no invoca** sus scripts.
- Nunca autoriza ni descarga datos, ni consulta GCS, ni lanza Spark, ni escribe Silver.

## Pendientes de integración productiva

1. Recibir el contrato **publicado y firmado**, con un anclaje de confianza fuera del contrato recibido.
2. Validar formalmente estados `BRONZE_COMPLETADO` y `SILVER_AUTORIZADO`, firmas, versiones y revocaciones con fuentes independientes; **no confiar en flags editables**.
3. Resolver URI por objeto, generación exacta, tamaño y SHA-256 contra el manifest oficial, sin condición de carrera entre comprobación y lectura en Spark. Se requiere una entrada de bytes inmutable/generación fijada.
4. Confirmar el mecanismo de publicación desde `landing/` a ruta oficial (objeto y generación pueden variar); no asumir que existe por estar propuesto.
5. Revisar rutas/CLI de `hechos_valores_consulta` y producción del candidato Silver de Hechos de Importancia.
6. Mantener Silver en staging, auditoría, control de idempotencia y aprobación antes de publicar; separar ejecución Gold de Piero.
7. Adaptar `ejecutar_incremental.sh` y cada procesador solo tras aprobar los puntos anteriores. No modificar por simple sustitución HDFS → gs://.

Este material **no** constituye conformidad Quality ni autorización de ejecución.
