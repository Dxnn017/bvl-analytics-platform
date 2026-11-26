# E.7.8.6 — Validación HTTP multipart local (SIN GCP)

**Aprobación:** Quality ratificó pruebas locales `uploadType=multipart`, `ifGenerationMatch=0`, inclusive tamaños >8 MiB. NO autorizó usar Cloud, modificar IAM, ejecutar la transferencia ni hacer merge.

El archivo `scripts/ingestion/verificar_multipart_e786_offline.py` ensambla una petición HTTP JSON API `multipart/related` con metadatos y contenido binario y la transmite **solo a un servidor HTTP de prueba en `127.0.0.1`**. El servidor recibe el cuerpo, valida el método POST, la consulta, el contenido, y produce respuestas sintéticas de éxito, `412`, `500` y encabezado `Location` no permitido. No solicita credenciales, no contacta `storage.googleapis.com` y no modifica el motor real.

- Se comprueba un contenido sintético de **8 MiB + 37 bytes** en una petición POST única.
- Query obligatoria `uploadType=multipart&ifGenerationMatch=0` y cabecera `Content-Type: multipart/related; boundary=...`.
- El SDK `google-cloud-storage` no se invoca: su `upload_from_filename()` actual podría iniciar sesión reanudable para >8 MiB.
- Rechazo de `uploadType=resumable`, precondición distinta de 0, destinos fuera de `landing/`, respuesta `Location`, estado `412`, estado `500` y hosts que no sean loopback.
- **No demuestra** que el ejecutor real utilice este transporte; una integración futura con credenciales operativas, timeout, integridad CRC32C/SHA-256, conciliación de respuestas inciertas, IAM JIT y detención de solicitudes pendientes debe diseñarse y validarse por separado.
- Tampoco demuestra cancelación de solicitudes HTTP en curso ni fencing de Cloud; esos controles siguen bloqueantes.

Validar: `python -m unittest discover -s tests -p 'test_verificar_multipart_e786_offline.py' -q` y `python scripts/ingestion/verificar_multipart_e786_offline.py`.

Estos archivos sirven como evidencia **local y separada** de las pruebas semánticas; no habilitan transferencia real.
