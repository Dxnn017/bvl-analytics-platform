# Puerta de integración Bronze v0.1 — Paso 101.7 vigente

## Alcance

- Reutiliza `seleccionar_lote` para piloto (12 archivos) o lote completo (3,599).
- Reutiliza `verificar_cadena_control_solo_lectura` para leer por URI canónica y generación exacta y comparar SHA-256, vínculo de reserva/aprobación y 12 manifests del contrato.
- Conserva los destinos exactos del plan, incluidas rutas intermedias como `pdf/ingestion_date=...`.
- En la interfaz CLI sólo está habilitado `--preflight-only` local, con cero red.
- **NUNCA llama a `transferir_archivo` ni habilita `TRANSFER_ALLOWED`.**

## Pendientes previos a migrar

1. **Quality** debe aprobar y desplegar el canal oficial de liberación del Paso 101.7, con referencias de generaciones/SHA recibidas de una fuente autenticada y anclaje inmutable de los manifests oficiales. No usar argumentos libres ni metadatos `approved_by_sa` como prueba.
2. **Matthew**, sólo tras permiso independiente de Quality, debe aplicar IAM mínimo de lectura `storage.objects.get` en Control e impersonación sin llaves JSON; Quality debe poder emitir aprobaciones auténticas con `if_generation_match=0`.
3. Integrar el verificador de liberación y el cliente autenticado en un gateway operativo con verificaciones de identidad, alcance autorizado (12 o 3599), expiración y protección de lotes antes de llamar al núcleo. No eliminar el guard de transferencia hasta esa revisión.
4. Quality debe auditar los recibos reales por archivo, generaciones GCS y SHA-256 y declarar éxito del piloto antes de autorizar los 3,599 archivos.

## Lectura de evidencia

Esta prueba valida integración funcional **local con cliente GCS simulado**, no demuestra permisos IAM, autenticidad de emisor ni operación Cloud. Es intencionalmente *fail-closed*.
