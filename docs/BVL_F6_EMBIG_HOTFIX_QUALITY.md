# BVL Fase 6 — Hotfix candidato EMBIG (codificación)

**Base inmutable:** `722e2e131eb8e06d153197c5abe1c2dcc1688896`.

## Incidente verificado por Quality
- Dataset: `bcrp/embig`; serie `PD04709XD`.
- Fuente Bronze con generación `1795677945792399`, tamaño 89,858 bytes y SHA-256 `54de0c388cb6c81f6bbc04fbef953f0f1cd4725fe2ca2158753dc53c22802313`.
- Batch `bvl-f6-rest-adbdd35a77b94f878c69491e7cc6ce44`: **FAILED** tras consumir claim. No generó Parquet ni manifests. No reintentar el mismo intento.
- `utf-8`/`utf-8-sig` fallan; `cp1252` e `iso-8859-1` pasan según diagnóstico. No hay caracteres C1 en la muestra; Quality debe ratificar el uso explícito de CP1252 para el archivo/serie.

## Cambios mínimos
- Solo `load_bcrp_csv` de `scripts/processing/fase6_restantes_transform.py`: CP1252 **explícito** y estricto, limitado a `PD04709XD`. **Sin detector ni fallback silencioso**.
- Conserva `load_json_records` en UTF-8-SIG, la puerta Fase 5, los identificadores de fuente oficial, la validación GCS de generación/SHA y el resto de los diez procesadores.
- Tests focalizados offline y una prueba real de PySpark con ANSI activado y CSV sintético en CP1252.

## Antes de cualquier nuevo lanzamiento
1. Quality ratifica CP1252, reproduce tests y reconstruye artefactos desde el nuevo commit; comprueba nuevos SHA-256 y los publica mediante creación exclusiva.
2. Concilia formalmente el claim consumido y preserva evidencia del batch fallido. Prohibe reutilizar `attempt_id` o authorization fingerprint.
3. Emite **nuevo** intento, autorización independiente y contexto KMS firmado vinculado al nuevo commit y artefactos. Comprueba preflight, cuota y observabilidad.
4. Ejecuta EMBIG supervisado; valida conteos, calidad, manifests, logs y publica Silver solo después de la aprobación independiente.
5. Revisa los demás CSV BCRP **solo si están dentro del alcance del nuevo release**. No cambiar otras fuentes por una detección automática.

## Reproducción del parche
1. Crear worktree aislado basado en `722e2e1` en rama `fix/f6-embig-cp1252`.
2. Ejecutar `aplicar_hotfix_embig.py --repo <worktree>`.
3. Ejecutar `python3 -m unittest discover -s tests -p 'test_fase6_embig_encoding_puros.py' -v`.
4. Ejecutar `SPARK_LOCAL_IP=127.0.0.1 spark-submit --master 'local[1]' --conf spark.ui.enabled=false --conf spark.sql.ansi.enabled=true tests/test_fase6_embig_encoding_spark_local.py`.
5. Verificar `git diff --check` y las pruebas preexistentes; realizar commit separado cuando el resultado sea conforme.

**Estado:** CANDIDATO_LOCAL_NO_AUTORIZA. No se realizaron operaciones GCS, claims o Dataproc en la preparación del parche.
