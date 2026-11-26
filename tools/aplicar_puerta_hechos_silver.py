#!/usr/bin/env python3
"""Conecta un comando de diagnóstico offline a un candidato con BLOQUEO vigente."""
from pathlib import Path
import ast
import hashlib
import os
import tempfile

TARGET=Path('scripts/processing/hechos_identidad_gcp_candidato.py')
ORIGINAL=Path('scripts/processing/hechos_identidad_to_silver.py')
if not TARGET.is_file() or not ORIGINAL.is_file():
    raise SystemExit('ALTO: faltan el candidato o el procesador original')
if not Path('scripts/pipeline/puerta_hechos_silver_preparacion.py').is_file():
    raise SystemExit('ALTO: falta el módulo de preparación')
old=TARGET.read_text(encoding='utf-8')
if 'BVL_HECHOS_PREFLIGHT_OFFLINE_INTEGRADO_V1' in old:
    raise SystemExit('ALTO: puerta ya instalada, sin reescritura')
required=('SILVER_EJECUCION_HABILITADA = False','CANDIDATO_PARAMETRIZADO_V1','"errorifexists"')
if any(r not in old for r in required) or '"overwrite"' in old:
    raise SystemExit('ALTO: candidato no coincide con el estado revisado')
# El bloqueo se ejecuta al principio: el nuevo modo solo lectura se despacha antes.
pos=old.find('SILVER_EJECUCION_HABILITADA = False')
if pos<0 or pos>200 or 'if not SILVER_EJECUCION_HABILITADA' not in old[:500]:
    raise SystemExit('ALTO: bloqueo no localizado al inicio')
insert='''# BVL_HECHOS_PREFLIGHT_OFFLINE_INTEGRADO_V1
# SOLO permite comprobar bytes locales; nunca habilita Spark, HDFS o GCS.
import sys as _bvl_sys
if _bvl_sys.argv[1:2] == ['--preflight-offline']:
    import importlib.util as _bvl_importlib
    from pathlib import Path as _BvlPath
    _root = _BvlPath(__file__).resolve().parents[2]
    _path = _root / 'scripts/pipeline/puerta_hechos_silver_preparacion.py'
    _spec = _bvl_importlib.spec_from_file_location('bvl_hechos_preflight', _path)
    _module = _bvl_importlib.module_from_spec(_spec)
    _spec.loader.exec_module(_module)
    raise SystemExit(_module.main(_bvl_sys.argv[2:]))

'''
updated=old[:pos]+insert+old[pos:]
ast.parse(updated)
assert updated.count('SILVER_EJECUCION_HABILITADA = False')==1
assert updated.count('"errorifexists"')==1
assert '"overwrite"' not in updated
hash_original=hashlib.sha256(ORIGINAL.read_bytes()).hexdigest()
# Copia de seguridad local, fuera del repositorio
backup=Path.home()/'Descargas'/'hechos_identidad_candidato_antes_puerta.bak.py'
if backup.exists():
    raise SystemExit(f'ALTO: respaldo previo ya existe: {backup}')
backup.write_bytes(TARGET.read_bytes())
os.chmod(backup,0o600)
fd,tmp=tempfile.mkstemp(prefix='.candidato-bvl-',suffix='.py',dir=TARGET.parent)
try:
    with os.fdopen(fd,'w',encoding='utf-8') as f:
        f.write(updated)
    os.replace(tmp,TARGET)
finally:
    if os.path.exists(tmp):os.unlink(tmp)
assert hashlib.sha256(ORIGINAL.read_bytes()).hexdigest()==hash_original
print('PUERTA_OFFLINE_CONECTADA: OK')
print('EJECUCION_NORMAL_SILVER: BLOQUEADA')
print('PREFLIGHT_ENTRADA_SIN_SPARK: DISPONIBLE')
print('ORIGINAL_INTACTO: SI')
print('OPERACIONES_GCS: 0')
print('BACKUP_LOCAL:',backup)
