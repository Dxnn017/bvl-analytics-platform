#!/usr/bin/env python3
"""Immutable candidate release from isolated Git commit; NEVER executes GCS/claim."""
from __future__ import annotations
import argparse,hashlib,json,subprocess,zipfile
from pathlib import Path
from fase6_release_restantes import MODULES, ENTRY, GATE_FILES, deterministic_zip

ROOT=Path(__file__).resolve().parents[2]
BRANCH='fix/f6-quality-candidato'
BASE='8381d36fd9054f46acd37ad7586511fa8b099cb5'
F5='41266f0efba4477c30266e19ca3280838ff3d802'

def raw_git(*args):
    return subprocess.check_output(['git',*args],cwd=ROOT,stderr=subprocess.PIPE)

def require(value,msg):
    if not value:raise ValueError(msg)

def payload(path):
    path=Path(path)
    require(path.is_file() and not path.is_symlink(),'Falta source de release: '+str(path))
    raw=raw_git('show','HEAD:'+str(path))
    require((ROOT/path).read_bytes()==raw,'Archivo difiere de commit: '+str(path))
    return raw

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--out',type=Path,required=True)
    args=ap.parse_args()
    require(raw_git('branch','--show-current').decode().strip()==BRANCH,'Rama candidata incorrecta')
    raw_git('merge-base','--is-ancestor',BASE,'HEAD')
    require(raw_git('status','--porcelain')==b'','Worktree candidato no limpio')
    commit=raw_git('rev-parse','HEAD').decode().strip()
    require(len(commit)==40,'Commit SHA invalido')
    for path in GATE_FILES:
        require(payload(path)==raw_git('show',F5+':'+path),'Fase5 modificada')
    require('fase6_montos_sab_quarantine.py' in MODULES,'Cuarentena NO empaquetada')
    require('fase6_montos_sab_scope_quality.py' in MODULES,'Scope NO empaquetado')
    require('fase6_hechos_documental_cloud_candidato.py' in MODULES,'Hechos Cloud NO empaquetado')
    mods={name:payload(path) for name,path in MODULES.items()}
    entry=payload(ENTRY)
    deps=deterministic_zip(mods)
    request={
      'schema':'BVL-F6-QUALITY-CANDIDATE-RELEASE-v1',
      'status':'CANDIDATO_NO_FIRMADO_NO_AUTORIZA',
      'processor_commit':commit,'quality_approved':False,
      'execution_allowed':False,'official_publication_allowed':False,
      'silver_to_gold_allowed':False,
      'entrypoint_sha256':hashlib.sha256(entry).hexdigest(),
      'dependencies_sha256':hashlib.sha256(deps).hexdigest(),
      'included_modules':{k:hashlib.sha256(v).hexdigest() for k,v in sorted(mods.items())},
      'partial_sab':{'source_files':1221,'normal_files':1207,'quarantine_files':14,
                     'normal_rows':21911,'quarantine_rows':245,
                     'signed_scope_required':True},
      'phase5_modules_unchanged':True,
      'needs_quality_ratification':['partial_scope_schema','launch_context','full_sab_real_bronze',
          'hechos_documental_cloud_mapping','silver_gold_contract']
    }
    q=json.dumps(request,ensure_ascii=False,sort_keys=True,indent=2).encode()
    out=args.out.expanduser().resolve()
    require(not out.exists(),'Release candidato ya existe, no sobreescribir')
    out.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(out,'x',compression=zipfile.ZIP_DEFLATED,
                         compresslevel=9) as z:
        def escribir(nombre, contenido):
            info = zipfile.ZipInfo(
                nombre, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            z.writestr(info, contenido, compresslevel=9)

        escribir('fase6_restantes_batch.py', entry)
        escribir('fase6_restantes_dependencias.zip', deps)
        escribir('CANDIDATO_NO_AUTORIZA.json', q)
        escribir('SHA256SUMS.txt',
            f'{hashlib.sha256(entry).hexdigest()}  fase6_restantes_batch.py\n'
            f'{hashlib.sha256(deps).hexdigest()}  fase6_restantes_dependencias.zip\n'
            f'{hashlib.sha256(q).hexdigest()}  CANDIDATO_NO_AUTORIZA.json\n')
    print('RELEASE_CANDIDATO:',out)
    print('COMMIT:',commit)
    print('ENTRYPOINT_SHA256:',request['entrypoint_sha256'])
    print('DEPENDENCIAS_SHA256:',request['dependencies_sha256'])
    print('GCS: NO | CLAIM: NO | DATAPROC: NO | QUALITY_RATIFICATION: PENDIENTE')

if __name__=='__main__':main()
