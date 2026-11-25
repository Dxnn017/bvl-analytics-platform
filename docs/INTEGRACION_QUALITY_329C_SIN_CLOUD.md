# Quality 329C — Integración OFFLINE del coordinador Bronze

## Procedencia

El código base se conserva de la rama `pipeline-daniela` (motor probado), y los cuatro verificadores Quality vienen del archivo original de Lizeth `ENTREGA_DANIELA_QUALITY_329C_INTEGRACION_SIN_CLOUD.zip` (SHA-256 y archivos fuente contrastados).

Las funciones `auditar_integracion_quality_329c_local()` y `revisar_quality_329c_desde_coordinador_local()` conectan el plan completo, la lista piloto y el **borrador** Quality con el coordinador existente. Se incluye la puerta de lectura GCS *inyectable* de Quality y el verificador Ed25519 para pruebas offline.

## Bloqueos que permanecen

- `TRANSFERENCIAS_HABILITADAS=False` en `transferir_hdfs_gcs.py`.
- `transferir_archivo()` y `ejecutar_operativo()` **rechazan** el despacho real incluso con JSON local `approved=True` o un flag cambiado.
- El borrador `QUALITY_APPROVED_BORRADOR_NO_PUBLICAR.json` es una fixture local, no un objeto emitido en GCS.
- `TRANSFER_ALLOWED` independiente NO existe ratificado ni implementado como autorización autenticada real.
- El verificador Ed25519 acepta claves suministradas por el ensayo local para comprobar criptografía, pero **no** dispone de raíces oficiales de confianza registradas. `transfer_allowed=False` siempre.
- No se hace ningún `GET`, `PUT`, `POST` ni cambios IAM en la ejecución del CLI.

## Comandos de comprobación (solo local)

```bash
python3 -m unittest discover -s tests -p 'test_*.py' -q
python3 tools/auditar_quality_329c_integracion_local.py \
  --plan fixtures/plan_3599_CANDIDATO_LOCAL.json \
  --piloto fixtures/piloto_12_CANDIDATO_LOCAL.json \
  --borrador fixtures/QUALITY_APPROVED_BORRADOR_NO_PUBLICAR.json
```

## Estado de cierre y pendientes

- Se acredita únicamente la consistencia documental de 3,599 operaciones, 12 elegidos y borrador Quality, con SHA-256 fijos vinculados a la entrega de Quality.
- Quality debe decidir la ruta y acreditación del emisor `TRANSFER_ALLOWED` y ofrecer atestación oficial independiente o evidencia auditoría comprobable.
- Quality tiene la reserva oficial, pero la autenticación productiva de `sa-bvl-ingestion` y las generaciones/hashes de los objetos de aprobación todavía requieren prueba autorizada en GCS.
- Para pasar de `PILOT_12` a `FULL_3599` se necesita un nuevo permiso independiente y reconciliar sin sobrescritura los 12 archivos.
- No se ejecuta migración piloto ni lote completo sin autorización 101.7 explícita.
