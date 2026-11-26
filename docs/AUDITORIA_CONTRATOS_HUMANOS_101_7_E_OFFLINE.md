# Auditoría local — Paso 101.7-E (BVL Bronze 329C)

**Estado: TRES_BORRADORES_COMPATIBLES_NO_OPERATIVOS**. No se ha emitido, firmado ni publicado autorización alguna. No se habilitan GCS, HDFS, IAM, KMS, `QUALITY_APPROVED`, `TRANSFER_ALLOWED` ni migración.

## Fuente y comprobantes

Paquete proporcionado por Lizeth: `BVL_101_7_E_CONTRATOS_HUMANOS_BORRADORES.zip`.

- SHA-256 ZIP: `78b0544ae1fea9a2d2b38c2206ac6aa65f09c5305894dde542b6a1c55f7c062a`.
- Los comprobantes `SHA256SUMS.txt` de los tres JSON y el LEEME coinciden sin modificar los bytes originales.
- Esquema común: `BVL-HUMAN-AUTHORIZATION-329C-v1-DRAFT`, `BORRADOR_NO_FIRMAR`.
- Acciones disjuntas: `ISSUE_QUALITY_ONLY`, `ISSUE_TRANSFER_ONLY`, `EXECUTE_PILOT_ONLY`.
- Run ID `bvl-bronze-inicial-20261125-001`; reserva oficial generación `1795640338029794`, SHA-256 `80cb92939b975659d32514fff3a64a30e02148434222f1391b1afd675913c232`.
- Alcance limitado a 12 destinos, 23,052,390 bytes, hash piloto `82f384ab19231d89b4f4487ab7424eb24250943b4bdd40c85e88e8e08b5d16b7`, lista canónica `acf864e31fc403faec3bf67ba13555474e4fa185c3475763781ca88fb0b1d923` y plan completo de referencia `1397f92911b159e8df257fc82a7dd01593fce0d2e898e796a82efc77ae35d90e`. Referencia al plan completo NO habilita sus 3,599 transferencias.
- Identidad humana (anclaje PEM) SHA-256 `21c2212a5d66f54e76cfd8dd00571dafd49af6984db273f9996473a9487492b6` y DER/SPKI SHA-256 `f37b987739c0fc12e4da1d7bb9192c2e4e1f0dad8e65120f1228c27260b4862a`; la identidad anterior se rechaza por huella revocada.

## Qué se ejecutó en el laboratorio

- Validación estricta de estructura, tipos, valores, claves JSON duplicadas, referencias, hashes, alcance e identidad pública.
- Anclaje adicional contra `plan_3599_CANDIDATO_LOCAL.json`, `piloto_12_CANDIDATO_LOCAL.json`, candidato Quality y borrador Transfer V2 originales que ya existen en el repositorio.
- Pruebas ECDSA P-256/SHA-256 sobre **bytes originales** con firmas ASN.1 DER **sintéticas** generadas por claves efímeras de prueba. Nunca se usó KMS ni la clave humana privada real.
- Validación de cadena sintética: documento Quality → documento Transfer → documento Execution; referencias SHA del documento y de sus firmas sin confundirlas con el SHA del payload KMS; rechazo de acción cruzada, alteraciones, expiración, futuro, UUID repetido, ejecutor incorrecto y ausencia de evidencia de identidad.
- Ejemplo **solo local** de consumo único con SQLite: pruebas de replay y 5 accesos concurrentes con 1 ganador. Este ledger local **no sustituye** el cerrojo atómico distribuido exigido en Cloud.
- Comprobación de hash de un registro de confianza contra un ancla suministrada independientemente; no se considera al hash `trust_record_candidate_sha256` incluido en el propio borrador una prueba suficiente de procedencia.

## Incompatibilidades y pendientes de contrato definitivo

1. **Borradores no ejecutables.** `authorization_id`, `issued_at_utc` y `expires_at_utc` son `null` en los tres; `status=BORRADOR_NO_FIRMAR`, sin firmas DER. No son convertibles a operativos sustituyendo solo `status`.
2. **Referencias no disponibles.** Transfer y Execution carecen de hashes de firmas Quality, documentos humanos previos, generaciones GCS y payload Transfer oficial. Execution aún no identifica al ejecutor ni su **commit Git completo de 40 caracteres**. No usar `262ff92` como commit completo ni suplir hashes ficticios.
3. **Firma y autenticidad.** Ratificar el formato binario exacto firmado (JSON UTF-8 inalterado), identidad y cadena de confianza fuera de banda, huella de clave pública emitida/retirada y firma DER separada por **cada** acción. Las tres acciones usan la misma identidad humana propuesta: su independencia es de **autorizaciones y firmas**, no de personas; Quality debe confirmar si esto cumple segregación de funciones.
4. **Confianza del registro.** El `trust_record_candidate_sha256=482795...` es una afirmación de un borrador, **no** anclaje confiable del registro real. Se necesita SHA anclado fuera de banda, procedencia, permisos de protección, revocación y evidencia del momento exacto de consumo del desafío.
5. **Vigencia y replay.** UUID único, hora UTC, máximo 86,400 segundos; definir consumo atómico durable **compartido** por acción (no SQLite local) con `RECLAMADO`, `CONSUMIDO` y `FALLO_AUDITADO` y semántica segura ante fallo parcial. Validar antecedentes consumidos correctamente sin exigir que sigan disponibles, pero impedir reutilizar el mismo permiso para la misma operación.
6. **Publicación y ejecución separadas.** Para cada objeto/documento/firma oficial: URI canónica, `generation`, SHA-256, actor autorizado y evidencia independiente. `QUALITY_APPROVED` oficial firmado es precondición de `TRANSFER_ALLOWED`; el permiso de emisión Transfer NO sustituye la autorización humana **EXECUTE_PILOT_ONLY** ni el levantamiento formal del Paso 101.7. Establecer la cadena confiable de entrega de los valores `generation`, SHA-256 y release efectivo.
7. **Identidad ejecutora.** Definir y autenticar `sa-bvl-ingestion@bvl-analytics-2026.iam.gserviceaccount.com`, identidad efectiva del proceso y commit completo; no confiar solo en datos JSON autoprovistos.
8. **Seguridad de los 12 archivos.** Coincidencia exacta de cada HDFS/GCS/bytes/SHA con manifest oficial, `ifGenerationMatch=0`, validación de origen y destino y conciliación de generación. Prohibir sobrescrituras y 3,599 archivos hasta otra autorización expresa.

## Punto de parada

La salida de la herramienta es **siempre** `transfer_allowed=false`, `operaciones_gcs=0`. Ni el resultado local ni las firmas sintéticas habilitan nada. No firmar los borradores, no publicar, no cambiar IAM, no ejecutar GCS ni Dataproc. Validar futuras versiones finales con contratos separados, firmados y autenticados y aprobación de Quality.

## Ejecución local en Debian

```bash
cd ~/bvl-analytics-platform
PY="$HOME/.venvs/bvl-quality329c/bin/python"
"$PY" scripts/ingestion/auditar_contratos_humanos_101_7_e_offline.py \
  --human-key "$HOME/bvl-329c-identidad/lizeth_human_public.pem"
"$PY" -m unittest discover -s tests -p 'test_*.py' -q
```

La CLI exige la presencia de las cuatro referencias locales ya versionadas y nunca se conecta a GCP.
