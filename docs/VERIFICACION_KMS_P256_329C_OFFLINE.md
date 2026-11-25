# Quality 329C — compatibilidad de firmas KMS P-256 (solo local)

- Verifica el algoritmo Cloud KMS `EC_SIGN_P256_SHA256`: firma binaria DER, clave pública PEM sobre `secp256r1`, hash SHA-256 del **contenido exacto** firmado (sin recanonizar JSON ni añadir saltos de línea).
- La huella autorizada es SHA-256 de `DER SubjectPublicKeyInfo`. Quality debe entregar el PEM original y una huella **anclada por un canal independiente**; calcular la huella del PEM que acompaña a una supuesta autorización y confiar en ella no autentica al emisor.
- No incluye ninguna clave pública oficial, firma de Quality, orden `TRANSFER_ALLOWED`, ni credenciales. Las claves de pruebas se generan en memoria y se descartan.
- Este componente **no acredita** autoría IAM, Cloud Audit Logs, identidad real de emisor, integridad de generación GCS, alcance de los 12 archivos, ni la liberación del Paso 101.7. Su respuesta mantiene `transfer_allowed=False` incluso si la firma criptográfica es válida.
- No modifica `verificador_atestaciones_329c_offline.py` (Ed25519 de laboratorio); reemplazarlo o habilitar Cloud necesitará un contrato ratificado por Quality sobre objetos, bytes firmados, clave de confianza y **autorización independiente de transferencia**.

## Pruebas offline

```bash
$HOME/.venvs/bvl-quality329c/bin/python -m unittest discover -s tests -p 'test_*.py' -q
```

## Diagnóstico opcional con artefactos oficiales recibidos por canal seguro

```bash
$HOME/.venvs/bvl-quality329c/bin/python \
  scripts/ingestion/verificar_kms_p256_329c_offline.py \
  --payload DOCUMENTO_EXACTO_FIRMADO.json \
  --signature-der FIRMA_KMS.bin \
  --public-key-pem CLAVE_PUBLICA_QUALITY.pem \
  --trusted-spki-sha256 HUELLA_SPKI_OFICIAL_ANCLADA
```

**Este comando no habilita una transferencia ni lee de GCS.**
