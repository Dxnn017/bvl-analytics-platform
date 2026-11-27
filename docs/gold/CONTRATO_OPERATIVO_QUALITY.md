# Entrega Gold — Contrato operativo candidato

## 1. Matriz de dependencias

| Gold | Silver requerido | Restricción |
|---|---|---|
| mercado_valor_diario | cotizaciones, tipo_cambio, embig, puente_ruc_nemonico | Solo versiones autorizadas |
| actividad_sab_diaria | montos_sab | Cobertura parcial: 245 registros en cuarentena y 14 consultas afectadas |
| empresa_trimestre | principales_cuentas | Identidad empresarial únicamente acreditada |
| hechos_empresa | hechos_importancia/documental_restringido | BLOQUEADO para resultados oficiales |
| impacto_hechos_valor | hechos y mercado | BLOQUEADO para resultados oficiales |

No imputar días faltantes de SAB como cero.

## 2. Inventario GCS

Proyecto: bvl-analytics-2026
Región: southamerica-west1
Cuenta: sa-bvl-gold@bvl-analytics-2026.iam.gserviceaccount.com

| Recurso | Acceso previsto |
|---|---|
| gs://bvl26-silver-7f3a9c/versions/ | Lectura |
| gs://bvl26-gold-7f3a9c/versions/ | Lectura |
| gs://bvl26-gold-7f3a9c/staging/ | Lectura, escritura y limpieza controlada |
| gs://bvl26-control-7f3a9c/manifests/silver/ | Lectura |
| gs://bvl26-control-7f3a9c/manifests/gold/ | Lectura |
| gs://bvl26-control-7f3a9c/version_registry/silver/ | Lectura |
| gs://bvl26-control-7f3a9c/version_registry/gold/ | Lectura |
| gs://bvl26-control-7f3a9c/audit/ | Lectura y creación controlada |

Inventario de permisos esperados, NO verificación efectiva de IAM.

## 3. Firma Quality

KMS:
projects/bvl-analytics-2026/locations/southamerica-west1/keyRings/bvl-bronze-release-329c/cryptoKeys/quality-approved-signer/cryptoKeyVersions/1

Algoritmo: ECDSA-SHA256
Formato: DER
Huella SPKI SHA-256:
5c70c40088a680cfd7ced1859c4d6b335652d96991cdcae4268819fb4f90f5ce

Pendiente recibir PEM oficial. No confiar en claves proporcionadas por manifests.

## 4. Ejecución local

Entorno Python:
 /home/piero/BigData_UPAO/bigdata_env/bin/python

Pruebas:
 /home/piero/BigData_UPAO/bigdata_env/bin/python -m unittest discover -s tests/gold -p 'test_marcador_conjunto_v2.py' -v

Resultado obtenido: 5 pruebas aprobadas.

## 5. Limitaciones

- Contrato del marcador v0.2: pendiente de ratificación Quality.
- Lectura GCS: validada mediante mocks, no contra GCS real.
- Integración completa de marcador, current, manifest y archivos: pendiente.
- Esquemas y particiones de archivos: pendientes de validación integrada.
- Autorización productiva: NO concedida.
- Publicación Gold y actualización de registry: prohibidas hasta aprobación.
