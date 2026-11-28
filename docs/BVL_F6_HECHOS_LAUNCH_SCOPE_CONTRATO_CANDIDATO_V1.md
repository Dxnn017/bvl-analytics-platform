# Propuesta para Quality — launcher Hechos documental restringido

**Estado:** CANDIDATO, NO RATIFICADO, SIN EJECUCIÓN.
**Base Git inmutable:** `8782d2a332a3492997e4e7af2bbac8df7db874f5`.

## 1. Cambio de contrato obligatorio antes de lanzar

El catálogo firmado histórico mapea `smv/hechos_importancia` a `hechos_importancia/eventos`. Quality restringió el alcance exclusivamente a documentos verificables. **No usar la autorización/eventos existente** para la salida documental. El identificador `hechos_importancia/documental_restringido` es **propuesto**, no oficial; Quality debe ratificar el nombre exacto, esquema y cambio de anexos/contratos antes de cualquier firma o claim.

Un contexto firmado nuevo debe vincular: fuente y manifest exactos (SHA + generación), nueva salida documental, `run_id`, `attempt_id`, `processor_commit`, SHA de entrypoint y dependencias, fingerprint de autorización, cuenta Silver, bucket/región, ruta staging create-only y expiración. El publicador Quality debe implementar la publicación oficial, no el launcher.

## 2. Preflight y secuencia segura propuesta

1. Exigir autorización nueva de alcance documental y contexto de lanzamiento con firma verificable Quality, junto a la puerta F5 original, sin modificar sus módulos protegidos.
2. Validar 1,979 referencias del manifest Bronze oficial: generación `1795680153321464`, hash `d2c5f5d5a363e271a770497b4af6feedde454f999bc2fe7696a5c0ab0784eab0`, 990 PDF y 989 TXT, sin duplicación de rutas; un duplicado PDF por SHA conocido.
3. Preclaim: verificar el conjunto firmado, identidad real, integridad del inventario, capacidad, vigencia, habilitación de producto y esquema; evitar un claim si cualquier requisito previo falla.
4. Tras la aprobación supervisada, **un solo claim exclusivo por intento firmado**, sin reintentos automáticos; todo fallo posterior requiere conciliación con Quality y nueva autorización.
5. Leer los bytes GCS por `uri+generation`, verificar `sha256+size_bytes` de cada objeto; nunca usar rutas sin generación ni archivos históricos de metadatos de expedientes.
6. El adaptador existente `fase6_hechos_documental_cloud_candidato.py` genera 990 filas. Revisar con Quality la etiqueta de procedencia `verificacion`, porque el modelo local heredado fija `BYTES_HDFS_SHA256_IGUAL_MANIFEST_BRONZE_PINNED` incluso si la lectura real provino de GCS. No declarar HDFS como fuente de verificación Cloud.
7. Validar contenido, conteo, esquema y serialización Parquet/Snappy, generar manifest de candidato con `quality_approved=false` y salida staging create-only. Verificar límites de tamaño del escritor antes de consumir claim.
8. Quality valida, ratifica y publica posteriormente, creando su versión y `current.json` conforme a contrato ratificado. El launcher nunca escribe versiones oficiales.

## 3. Campos excluidos

No acreditar empresa, RUC, nemónico, expediente, fecha oficial del hecho, descripción ni sector desde CSV históricos; no construir `identidad_empresarial` ni los Gold `hechos_empresa` / `impacto_hechos_valor` con relaciones no acreditadas. Los 27 eventos sin PDF siguen en cuarentena.

## 4. Estado de este paquete

`fase6_hechos_launch_scope_candidato.py` hace solamente validación estructural inerte; **no verifica firmas, no lee bytes GCS y no es el launcher**. Sus pruebas emplean datos sintéticos exclusivamente. Quality debe proporcionar/ratificar el nuevo contrato antes de programar y someter a revisión un launcher ejecutable con la puerta F5 y firma real.

## 5. Pruebas requeridas de la futura integración

Firma no válida, manifest cambiado, claves/generaciones incorrectas, cambio de salida/eventos, intento usado/expirado, claims repetidos, bytes incorrectos, 1978/1980 objetos, duplicidad inesperada, salida Parquet, recibos create-only, control de no overwrite/no delete, reporte candidato, fallo tras claim y recuperación; prueba GCS real supervisada solo después de autorización.
