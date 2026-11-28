# BVL Fase 6 — Launcher documental restringido, contrato candidato V1

**Estado:** implementación candidata, NO RATIFICADA, NO AUTORIZA EJECUCIÓN.
**Base inmutable Quality:** `8782d2a332a3492997e4e7af2bbac8df7db874f5`.
**Rama de trabajo:** `feature/f6-hechos-launcher-implementacion`.

## Alcance ratificado por Quality como diseño

- Proyecto `bvl-analytics-2026`, región `southamerica-west1`, cuenta designada `sa-bvl-silver` (Quality debe confirmar identificador IAM completo).
- Fuente `smv/hechos_importancia` con run `bvl-bronze-inicial-20261125-001`.
- Manifest Bronze generación `1795680153321464`, SHA-256 `d2c5f5d5a363e271a770497b4af6feedde454f999bc2fe7696a5c0ab0784eab0`.
- Inventario 1979 objetos (990 PDF, 989 TXT), 989 PDF únicos por SHA-256; 990 registros documentales, un TXT compartido justificadamente por PDF duplicado. No se aceptan TXT huérfanos.
- Producto **nuevo** `hechos_importancia/documental_restringido` (el histórico `hechos_importancia/eventos` permanece bloqueado).
- Staging exclusivo: `gs://bvl26-silver-7f3a9c/staging/hechos_importancia/documental_restringido/run_id=<RUN>/attempt_id=<ATTEMPT>/`.
- Versiones oficiales futuras: `gs://bvl26-silver-7f3a9c/versions/hechos_importancia/documental_restringido/version_id=<VERSION>/`.
- Registro Quality futuro: `gs://bvl26-control-7f3a9c/version_registry/silver/hechos_importancia/documental_restringido/current.json`.

## Contrato firmado candidato (requiere ratificación de formato)

`BVL-SILVER-DOCUMENTAL-LAUNCH-CONTEXT-CANDIDATE-v1` contiene proyecto, región, fuente, producto, run Bronze, URI/gen/SHA del manifest, run/attempt/batch, fingerprint de autorización, commit, URI/gen/SHA del entrypoint y dependencias, cuenta de servicio, prefijo staging, schema, vigencia, operación, runtime, y prohibiciones de overwrite/delete/retry/publicación/consumo Gold. **Solo Quality firma mediante KMS.** Ningún JSON local constituye una autorización. La ruta propuesta para referencias KMS de la extensión documental queda pendiente de confirmación Quality.

## Preflight y ejecución protegidos

1. Firma/documento de contexto nuevo, vigencia, integridad de artefactos, identidad e intento.
2. Reusar **sin modificar** `puerta_silver_operativa_v1_candidata.py` y la verificación `check_gcs_signed_bundle`. El plan F5 debe reconocer exactamente el **nuevo** producto documental; `eventos` se rechaza incluso si su firma es válida. Se requiere actualización firmada del catálogo/anexo por Quality.
3. Manifest oficial fijado por generación/SHA, inventario completo, rutas/nombres/duplicados/TXT y presupuesto de capacidad.
4. Prefijo staging sin artefactos previos; verificación de IAM de lectura fuente, creación de objetos y uso de KMS/claim bajo identidad autorizada corresponde a Quality.
5. **Único** claim exclusivo firmado; jamás usar como ensayo, ni reintentar tras consumirlo. Un fallo posterior preserva evidencias y exige conciliación Quality.
6. Lectura de 1979 objetos por URI y generación GCS; SHA y tamaño comprobados realmente. No tomar hechos/empresa/RUC/nemónico/expediente/fechas oficiales del CSV histórico.
7. Crear 990 filas, fijar `verificacion=BYTES_GCS_GENERATION_SHA256_SIZE_VERIFIED` solo **después** de la lectura verificada.
8. Serializar Parquet Snappy con control de conteo y esquema, emitir **solo staging** con `if_generation_match=0, retry=None`, y manifest candidato también create-only.
9. Quality concilia y publica las versiones oficiales con un proceso separado y firmado. El launcher jamás actualiza `versions/`, manifests oficiales ni `current.json`.

## Manifest candidato

`BVL-SILVER-DOCUMENTAL-STAGING-CANDIDATE-v1`, con las 1979 referencias fuente verificadas, manifest Bronze fijado, 990 filas, Parquet y sus URI/generación/SHA/tamaño/filas, métricas de PDF/TXT y duplicidad, etiqueta de procedencia GCS, contexto/fingerprint/claim, restricciones de 27 eventos sin PDF y banderas `quality_approved=false`, `official_publication_allowed=false`, `gold_consumption_allowed=false`.

## Esquema de datos Silver

El mismo esquema de 20 campos del procesador documental local ratificado como candidato: `guid_documento_archivo`, `nombre_pdf`, `ruta_pdf_bronze`, `generacion_pdf_bronze`, `sha256_pdf`, `tamano_pdf_bytes`, `fecha_nominal_archivo`, `fecha_nominal_no_es_fecha_oficial_hecho`, `duplicado_por_sha256`, `nombre_pdf_canonico`, `nombre_txt`, `ruta_txt_bronze`, `generacion_txt_bronze`, `sha256_txt`, `tamano_txt_bytes`, `origen_texto`, `texto_extraido`, `caracteres_txt_verificados`, `estado_calidad_txt_propuesto`, `verificacion`.

Solo la fecha nominal del **nombre del archivo** está permitida; no equivale a la fecha oficial del evento. Calidad de texto es indicativa, umbral sujeto a Quality. Se excluye identidad empresarial histórica y todo uso de Gold `hechos_empresa` / `impacto_hechos_valor` basado en relaciones no acreditadas.

## Dependencias y permisos requeridos

- Lectura bajo identidad Quality/Silver supervisada: objetos Bronze por **generación fijada**, manifest control, contextos y firmas, artifacts de runtime. Debe validarse ACL real y uso de identidad `sa-bvl-silver`.
- Firma: exclusivamente Quality KMS; lectura/verificación KMS y contexto previamente ratificados.
- Escritura: solo objetos nuevos en staging exacto; sin overwrite/delete/listado masivo fuera del intento; staging debe estar libre de artefactos previos y permisos de listado del prefijo específico para comprobarlo.
- Claim: operación exclusiva del mismo `attempt_id` y firma real F5; no reintentos, ni creación directa por CLI fuera del launcher.
- Version registry, `versions/`, manifests oficiales y Gold: permisos de escritura **denegados** al launcher.
- Requiere PyArrow/Snappy y Google Cloud Storage en el runtime además de módulos F5 protegidos; confirmar capacidades y límites antes de firmar.

## Limitaciones del candidato

Quality **aún no emitió** el nuevo contexto firmado, IAM ni el anexo de catálogo operativo. Por diseño `--execute` fallará antes de consumir claims en ausencia de dichos instrumentos. Las pruebas de esta entrega son **locales y simuladas**; no acreditan los bytes GCS. No ejecutar `--execute` hasta autorización expresa y revisión de la nueva implementación por Quality.

## Recuperación

No hay reintento automático. Si hay fallo tras claim, conservar claim, staging y evidencia; Quality debe reconciliar e emitir autorización y `attempt_id` nuevos para cualquier otro intento. **No borrar ni sobrescribir** artefactos previos.

**Preclaim IAM:** comprobación no destructiva de `storage.objects.get` en Bronze/control/artefactos y `storage.objects.create` + `storage.objects.list` en Silver. La concesión IAM debe ser confirmada por Quality.
**Fallo tras claim:** intenta crear una anotación inmutable `_quality/failure_candidate.json` una sola vez; si GCS falla, conserva logs del driver y no reintenta automáticamente.
