BVL Analytics - Paso 103.8.54 (25-11-2026)

Objetivo:
  Validacion ESTRUCTURAL de contrato Bronze v0.1, sin acceder a GCS.

Archivos nuevos:
  scripts/ingestion/validar_control_bronze_local.py
  tests/test_control_bronze_local.py

Instalacion desde el directorio raiz del repositorio:
  python3 -m zipfile -e ~/Descargas/BVL_CONTROL_LOCAL_103854.zip .

Prueba:
  python3 -m unittest discover -s tests -p 'test_*.py'

El nuevo validador es offline y SOLO inspecciona estructuras locales.
NO comprueba identidad real de GCS, IAM, Cloud Audit Logs, generaciones
reales ni procedencia/firmas del canal oficial de Quality. Usa como
referencia local el control CANDIDATO, NO una lista confiable de manifests
oficiales. El resultado transfer_allowed siempre es False.
NO sustituye la validacion autenticada ni la autorizacion operativa
expresa del Paso 101.7. No genera reservas, aprobaciones ni ejecuciones.

En una fase posterior: anclar la referencia a manifiestos OFICIALES,
conectar lectura autenticada de Control, verificar procedencia Quality,
y validar liberacion operativa contra un canal realmente confiable.

NO cambiar TRANSFERENCIAS_HABILITADAS=False.
