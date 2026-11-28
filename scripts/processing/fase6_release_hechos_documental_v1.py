#!/usr/bin/env python3
"""Release reproducible del NUEVO launcher documental; solo local, sin firma."""
from __future__ import annotations
import argparse
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts' / 'processing'))
from fase6_release_restantes import MODULES as BASE_MODULES

BASE='8782d2a332a3492997e4e7af2bbac8df7db874f5'
SCOPE_BASE='ac261565c0289aa80e9ba87222c43d4b5efebe71'
BRANCH='feature/f6-hechos-launcher-implementacion'
ENTRY='scripts/processing/fase6_hechos_launcher_documental_v1.py'
BUILDER='scripts/processing/fase6_release_hechos_documental_v1.py'
DOC='docs/BVL_F6_HECHOS_LAUNCHER_INTEGRACION_CONTRATO_V1.md'
TEST='tests/test_fase6_hechos_launcher_documental_v1.py'
MODULES={**BASE_MODULES,
 'fase6_hechos_launch_scope_candidato.py':'scripts/processing/fase6_hechos_launch_scope_candidato.py'}
PROTECTED=(
 'scripts/pipeline/puerta_silver_operativa_v1_candidata.py',
 'scripts/pipeline/puerta_silver_v1_candidata.py',
 'scripts/pipeline/adaptador_gcs_silver_v1.py',
)


def dzip(items):
    if type(items) is not dict or not items:
        raise ValueError('ZIP_ARCHIVOS_INVALIDOS')
    out=BytesIO()
    with zipfile.ZipFile(out,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=9) as z:
        for name,raw in sorted(items.items()):
            if type(raw) is not bytes or not name or name.startswith('/') or '..' in Path(name).parts or '\\' in name:
                raise ValueError('ARCHIVO_ZIP_INSEGURO')
            info=zipfile.ZipInfo(name,(1980,1,1,0,0,0))
            info.compress_type=zipfile.ZIP_DEFLATED
            info.create_system=3
            info.external_attr=0o100644<<16
            z.writestr(info,raw,compresslevel=9)
    return out.getvalue()


def git(*args):
    return subprocess.check_output(['git',*args],cwd=ROOT,stderr=subprocess.PIPE)


def from_head(path):
    raw=git('show','HEAD:'+path)
    disk=ROOT/path
    if not disk.is_file() or disk.is_symlink() or raw!=disk.read_bytes():
        raise ValueError('FILE_NO_COINCIDE_CON_COMMIT:'+path)
    return raw


def build_release():
    if git('branch','--show-current').decode().strip()!=BRANCH:
        raise ValueError('RAMA_INCORRECTA')
    git('merge-base','--is-ancestor',BASE,'HEAD')
    git('merge-base','--is-ancestor',SCOPE_BASE,'HEAD')
    for p in PROTECTED:
        if git('show',BASE+':'+p)!=from_head(p):
            raise ValueError('F5_PROTEGIDO_MODIFICADO:'+p)
    included=[ENTRY,BUILDER,DOC,TEST,*MODULES.values()]
    if git('status','--porcelain','--',*included).strip():
        raise ValueError('ARCHIVOS_SIN_COMMIT')
    commit=git('rev-parse','HEAD').decode().strip()
    entry=from_head(ENTRY)
    deps=dzip({name:from_head(path) for name,path in MODULES.items()})
    signed=False
    report={
      'schema_version':'BVL-F6-HECHOS-RELEASE-CANDIDATE-v1',
      'status':'CANDIDATO_PENDIENTE_FIRMA_Y_QUALITY',
      'quality_approved':False,'claim_allowed':False,'execution_allowed':False,
      'official_publication_allowed':False,'gold_consumption_allowed':False,
      'product':'hechos_importancia/documental_restringido',
      'blocked_historical_product':'hechos_importancia/eventos',
      'bronze_run_id':'bvl-bronze-inicial-20261125-001',
      'bronze_manifest_generation':'1795680153321464',
      'bronze_manifest_sha256':'d2c5f5d5a363e271a770497b4af6feedde454f999bc2fe7696a5c0ab0784eab0',
      'expected_source_objects':1979,'expected_documents':990,
      'processor_commit':commit,'entrypoint_sha256':sha256(entry).hexdigest(),
      'dependencies_sha256':sha256(deps).hexdigest(),
      'module_sha256':{name:sha256(from_head(path)).hexdigest()
                       for name,path in sorted(MODULES.items())},
      'artifact_bucket_proposal':'gs://bvl26-dataproc-staging-7f3a9c/bvl-phase6/releases/'+commit+'/',
      'requires_quality_kms_signature':True,'requires_f5_new_product_extension':True,
      'gcs_written':False,'claim_consumed':False,
    }
    files={
     'fase6_hechos_launcher_documental_v1.py':entry,
     'fase6_hechos_documental_dependencias.zip':deps,
     'release_request_NO_FIRMADA.json':(json.dumps(report,indent=2,sort_keys=True,ensure_ascii=False)+'\n').encode('utf-8'),
     'contrato/BVL_F6_HECHOS_LAUNCHER_INTEGRACION_CONTRATO_V1.md':from_head(DOC),
     'pruebas/test_fase6_hechos_launcher_documental_v1.py':from_head(TEST),
    }
    sums='\n'.join(f'{sha256(raw).hexdigest()}  {name}' for name,raw in sorted(files.items()))+'\n'
    files['SHA256SUMS.txt']=sums.encode('utf-8')
    payload=dzip(files)
    return payload,report


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--out',type=Path,required=True)
    args=ap.parse_args()
    payload, report=build_release()
    if args.out.exists():
        raise SystemExit('ALTO: DESTINO_EXISTE_NO_SOBRESCRIBIR')
    args.out.parent.mkdir(parents=True,exist_ok=True)
    args.out.write_bytes(payload)
    with zipfile.ZipFile(BytesIO(payload)) as z:
        assert z.testzip() is None
    print('RELEASE:',args.out)
    print('COMMIT:',report['processor_commit'])
    print('ENTRYPOINT_SHA256:',report['entrypoint_sha256'])
    print('DEPENDENCIAS_SHA256:',report['dependencies_sha256'])
    print('RELEASE_SHA256:',sha256(payload).hexdigest())
    print('ESTADO: CANDIDATO_PENDIENTE_FIRMA_QUALITY')
    print('GCS: NO | CLAIMS: NO | DATAPROC: NO')

if __name__=='__main__':
    main()
