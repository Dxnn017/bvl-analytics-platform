# Hotfix SMV 003–005 — candidato de alcance limitado

**Fuente diagnóstica:** `BVL_F6_INCIDENCIAS_RESTANTES.zip`, SHA-256 `eabf85ec670e2661f08b016687556a0abf8d230aa54399961ca1abb02dc2eef9`.

**Base:** `d6ca7f5ca6667a4b94c88702f63081d394b03e03` (incluye EMBIG integrado `ea14003` y hotfix financiero). Esta es una rama derivada de una corrección financiera todavía no ratificada formalmente. **No fusionar en `pipeline-daniela` sin aprobación**.

## Montos SAB — INC-F6-SMV-003

- **1,221 archivos** con 22,156 registros originales, según Quality.
- **1,207 archivos**, código `0` + mensaje exacto `Consulta generada` + único wrapper `Resultado`: 21,911 registros candidatos.
- **14 archivos**, código `-2147024864` + mensaje de bloqueo concurrente del log SMV: 245 registros **en cuarentena íntegra**, no declarados válidos y no aptos para producto Silver oficial.
- `fase6_montos_sab_quarantine.py` **no** modifica `load_json_records` ni acepta códigos de error como éxito. Requiere `uri`, `generation`, `size_bytes`, `sha256` y bytes fijados contra GCS. Devuelve registros candidatos y cuarentena con las filas originales para inspección. No escribe GCS.
- El launcher productivo **no** se cambia para saltarse 14 archivos. La publicación parcial, los 245 registros y sus porcentajes de cobertura exigen decisión/ratificación formal de Quality. Un estado parcial no equivale a completo.
- La lista de 14 objetos (URI/generación/SHA/tamaño/fecha nominal) está en el JSON que produce `fase6_smv_incidents_audit_zip.py`, usando reportes Quality. **Este ZIP no aporta los bytes reales**, por lo que la transformación completa solo puede ser reproducida por Quality o en un entorno con acceso legítimo a Bronze.

## Valores Inscritos — INC-F6-SMV-004

- 289 XML: 148 con registros, 94 respuestas 110 sin registros, 47 respuestas 107 sin registros; más un JSON consolidado.
- `parse_xml` admite `110` solo si es el único error y no hay `ValorObtenidoBE`, y existe `MensajeError = Empresa no tiene valores inscritos en SMV`.
- Claves XML duplicadas, DTD, códigos desconocidos, mensajes inválidos, mezcla de error/filas o errores contradictorios siguen rechazados.
- `reconcile_valores` compara todos los atributos XML presentes + RUC por nombre, conserva la multiplicidad y emite `xml_sin_valores_110` independiente de `xml_errors_106_107`.

## Hechos Valores Consulta — INC-F6-SMV-005

- 22 XML: 6 con registros, 11 código 106, 4 código 107, 1 código 110; 1 JSON consolidado.
- Comparación por multiset `(nemonico, razon social)` y linaje de objetos sin inventar registros para códigos 106/107/110.
- Métricas `xml_sin_valores_110`, `xml_error_106_107` separadas.

## Pruebas y límites de ratificación

Las pruebas unitarias usan casos **sintéticos basados en el contrato**. El análisis del ZIP solo compara el inventario y los patrones de Quality. La validación independiente de las 1,534 respuestas Bronze reales, transformación Spark y publicación no han sido ejecutadas por este paquete. Se mantienen bloqueadas hasta el procedimiento de Quality.

No se modifican Fase5, `fase6_restantes_transform.py` (EMBIG + financiero), ni ningún archivo GCS. No se consumen claims.
