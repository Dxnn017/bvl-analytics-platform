# BVL Fase 6 — extensión contractual documental (CANDIDATA, SIN AUTORIZACIÓN)

## Problema exacto y alcance

El **anexo histórico** `BVL-SILVER-OPERATIONAL-ANNEX-v1` mantiene el mapeo
`smv/hechos_importancia → hechos_importancia/eventos`. El launcher F6 exige
`smv/hechos_importancia → hechos_importancia/documental_restringido`, en
`SINGLE_SOURCE`, sin evento oficial ni vinculación a empresa. Se resuelve mediante
**tres documentos Quality nuevos** y **dos documentos históricos firmados**, sin
alterar el contrato base ni reescribir registros oficiales.

La huella externa obligatoria del contrato base se mantiene exactamente:
`6f33f3a88e04b38a5e75863d3ccc67005231f05d474b9916d34fd7030edef20a`.

### Los cinco documentos y sus firmas

La puerta exige referencias independientes `{uri,generation,sha256,signature_uri,signature_generation,signature_sha256}` para **cinco parejas** JSON/firma DER P256:

1. `historical_annex`: anexo histórico original firmado (inmutable)
2. `historical_closure`: acta de cierre Fase 5 original firmada (inmutable)
3. `annex`: **nuevo anexo documental** `BVL-SILVER-OPERATIONAL-ANNEX-DOCUMENTAL-v1`
4. `closure`: **nueva acta documental** `BVL-PHASE5-DOCUMENTAL-EXTENSION-CLOSURE-v1`
5. `authorization`: **nueva autorización** `BVL-SILVER-AUTH-v1`, vinculada a los hashes de `annex` y `closure`

La cadena se verifica con la **misma clave pública Quality fijada** por el pin
externo existente. Se comprueban bytes canónicos exactos, firmas DER P256,
SHA-256 de parentesco, identidad, run_id, intento, vigencia y destino staging.
No se incorpora ninguna clave privada al repositorio.

### Plantilla exacta: nuevo anexo documental (`annex`)

La **plantilla ejecutable**, que produce el JSON canónico con los campos
reales completos, es `scripts/quality/generar_plantillas_f6_documental.py`.
Genera una copia profunda del anexo histórico (los 12 `direct_processes`,
`derived_processes`, contexto, controles) y admite **solo** estas diferencias:

- `schema_version = BVL-SILVER-OPERATIONAL-ANNEX-DOCUMENTAL-v1`;
- `direct_processes[bronze_dataset=smv/hechos_importancia].silver_products = ["hechos_importancia/documental_restringido"]`;
- `historical_annex_sha256`: hash de los bytes canónicos históricos, verificados con su firma;
- `historical_closure_sha256`: hash de los bytes históricos de acta, también verificados con firma;
- `documental_extension` (campos exactos, sin extensiones arbitrarias):

```json
{
  "schema_version":"BVL-SILVER-DOCUMENTAL-EXTENSION-v1",
  "source_dataset":"smv/hechos_importancia",
  "silver_product":"hechos_importancia/documental_restringido",
  "historical_product_not_authorized":"hechos_importancia/eventos",
  "mode":"SINGLE_SOURCE",
  "event_company_linkage_asserted":false,
  "official_event_date_asserted":false,
  "automatic_retry_allowed":false,
  "official_publication_allowed":false,
  "gold_consumption_allowed":false
}
```

La validación de `annex` exige que TODO el documento sea idéntico a la
transformación permitida del anexo histórico verificado. Cualquier otro cambio,
incluso otro producto directo o otra dependencia, bloquea el preclaim. La
estructura histórica sigue admisible sin extensión, sin modificación.

### Plantilla exacta: nueva acta documental (`closure`)

La plantilla ejecutable copia el acta histórica original y cambia **solo**:

- `schema_version = BVL-PHASE5-DOCUMENTAL-EXTENSION-CLOSURE-v1`;
- `status = DOCUMENTAL_EXTENSION_CLOSED`;
- `operational_annex_sha256 = SHA256(nuevo anexo JSON canónico)`;
- `historical_annex_sha256 = SHA256(anexo histórico firmado)`;
- `historical_closure_sha256 = SHA256(acta histórica firmada)`;
- `documental_product = hechos_importancia/documental_restringido`;
- `closed_at_utc = <momento real de cierre aprobado por Quality>`; no puede ser anterior al cierre histórico.

Los campos existentes `phase_5_closed`, `quality_approved`, contexto y
`base_contract_sha256` siguen obligatoriamente iguales al acta histórica
firmada; no se simula el cierre de Quality.

### Plantilla exacta: nueva autorización documental (`authorization`)

Conserva `BVL-SILVER-AUTH-v1` y restringe el alcance a:

```json
{
  "schema_version":"BVL-SILVER-AUTH-v1",
  "status":"SILVER_AUTORIZADO",
  "mode":"SINGLE_SOURCE",
  "dataset":"smv/hechos_importancia",
  "silver_product":null,
  "documental_scope":"DOCUMENTAL_RESTRINGIDO",
  "operation":"BUILD_SILVER_STAGING",
  "overwrite_allowed":false,
  "official_publication_allowed":false,
  "automatic_retry_allowed":false,
  "silver_dependencies":[]
}
```

La plantilla generadora añade **sin inventar** los campos restantes exactos:
`run_id`, `project_id`, `region`, `base_contract_sha256`, `service_account`,
`attempt_id`, `not_before_utc`, `expires_at_utc`,
`bronze_dependencies=[{dataset,manifest_uri,manifest_generation,manifest_sha256}]`,
`silver_outputs=[{name,staging_prefix}]`, `operational_annex_sha256`,
`phase5_closure_sha256`. El destino es exclusivamente
`gs://bvl26-silver-7f3a9c/staging/hechos_importancia/documental_restringido/run_id=<RUN>/attempt_id=<ATTEMPT>/`.

### Referencias firmadas e independientes para el preflight

Ejemplo de *rutas candidatas* (Quality debe ratificar los paths, generaciones,
hashes y publicar con `if_generation_match=0`):

- Nuevo anexo: `gs://bvl26-control-7f3a9c/contracts/bronze_silver/documental/run_id=<RUN>/attempt_id=<ATTEMPT>/operational_annex_documental.json` y `.sig`.
- Nueva acta: `gs://bvl26-control-7f3a9c/authorizations/phase5/documental/run_id=<RUN>/attempt_id=<ATTEMPT>/closure_documental.json` y `.sig`.
- Nueva auth: `gs://bvl26-control-7f3a9c/authorizations/silver/smv/hechos_importancia/run_id=<RUN>/attempt_id=<ATTEMPT>/authorization.json` y `.sig`.
- `historical_annex` y `historical_closure`: URIs **originales**, hashes y generaciones **originales**; nunca sobreescribirlos.

`trusted_refs` para el intento documental tiene cinco entradas: `annex`,
`closure`, `authorization`, `historical_annex`, `historical_closure`. Para
intentos históricos la puerta sigue aceptando solo las tres entradas originales.
Todas las referencias deben proceder de un contexto Quality independiente,
no de los JSON autopropuestos.

El launcher **no se modifica**; su `_verify_f5_new_product` comprueba el tipo
de plan verificado GCS, producto, modo, `run_id`, `attempt_id`, fingerprint,
identidad y prefijo staging, tal como ya estaba implementado. No se modifica el
mecanismo `consume_once`; continúa la prohibición de reutilizar un claim.

### Procedimiento de preparación (SIN GCP)

Quality debe proporcionar por separado el contrato base oficial y los cuatro
archivos históricos (anexo JSON, firma DER, acta JSON, firma DER), más el
archivo PEM de la clave pública Quality ya fijada. Este comando crea **solo**
tres JSON canónicos NO FIRMADOS en un directorio nuevo:

```bash
python3 scripts/quality/generar_plantillas_f6_documental.py \
 --contract /ruta/contrato_base.json \
 --historical-annex /ruta/operational_annex_historico.json \
 --historical-annex-sig /ruta/operational_annex_historico.sig \
 --historical-closure /ruta/closure_historica.json \
 --historical-closure-sig /ruta/closure_historica.sig \
 --quality-public-pem /ruta/quality_public.pem \
 --attempt-id ID_NUEVO_CONFIRMADO_POR_QUALITY \
 --not-before-utc '2026-11-28T15:00:00+00:00' \
 --expires-at-utc '2026-11-28T16:00:00+00:00' \
 --closed-at-utc '2026-11-28T14:59:00+00:00' \
 --output-dir /ruta/directorio_nuevo_no_existente
```

Las fechas ilustrativas NO son autorizaciones; deben reemplazarse por las
reales confirmadas por Quality. El programa valida firmas históricas y pin
externo, comprueba contrato base (incluido SHA), produce los tres JSON completos
y prohíbe sobreescribir una salida existente. No genera firmas.

Quality deberá firmar **cada uno** de los tres JSON canónicos con su clave
controlada, publicar los seis objetos con hashes y generaciones, suministrar
las cinco referencias externas y emitir además el contexto de lanzamiento e
IAM atestación correspondiente al intento exacto. Cualquier ausencia o
incompatibilidad detiene el flujo antes del claim. **Ninguna firma de Quality
se proporciona ni se suplanta en este paquete.**

### Alcance de las pruebas

Las pruebas focalizadas incluyen firmas P256 EFÍMERAS, validación de anexo,
acta y auth, comparación con históricos, preflight GCS **simulado sin red** y
`_verify_f5_new_product` real en memoria. El contrato de prueba es sintético
(inyección controlada para aislar la lógica); no sustituye las firmas, bytes,
manifest ni autorizaciones oficiales que validará Quality en su entorno.

**Estado:** propuesta contractual técnica, NO ratificada; publicación y Gold
bloqueados; cero GCP, cero claims, cero IAM modificado.
