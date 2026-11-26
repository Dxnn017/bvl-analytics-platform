#!/usr/bin/env python3
"""Verifica SOLO bytes de snapshots locales frente a un manifest JSON SUMINISTRADO.
No verifica firmas, generaciones GCS reales, ni autorizaciones. Nunca ejecuta Silver.
"""
import argparse
import hashlib
import json
import re
from pathlib import Path

RUN = 'bvl-bronze-inicial-20261125-001'
BUCKET = 'bvl26-bronze-7f3a9c'
_HEX = re.compile(r'^[a-fA-F0-9]{64}$')


def _must(cond, msg):
    if not cond:
        raise ValueError(msg)


def _generation(x):
    return (type(x) is str and x.isdigit() and int(x)>0) or (type(x) is int and x>0)


def check(manifest, entries):
    """entries: lista [{uri, generation, local_path}]. No confiere autorización."""
    _must(isinstance(manifest, dict), 'Manifest JSON debe ser objeto')
    _must(manifest.get('run_id') == RUN, 'run_id diferente')
    ds = manifest.get('dataset')
    _must(isinstance(ds, str) and re.fullmatch(r'(smv|bcrp)/[a-z_]+', ds), 'dataset invalido')
    pref = f'gs://{BUCKET}/{ds}/run_id={RUN}/'
    files = manifest.get('files')
    _must(isinstance(files,list) and len(files)>0, 'No hay archivos')
    _must(isinstance(entries,list) and len(entries)==len(files), 'Mapeo incompleto o extra')
    idx = {}
    for item in entries:
        _must(isinstance(item,dict), 'Mapeo invalido')
        uri=item.get('uri'); gen=item.get('generation'); path=item.get('local_path')
        _must(isinstance(uri,str) and uri.startswith(pref) and len(uri)>len(pref), 'URI no oficial en mapeo')
        _must(_generation(gen), 'Generacion invalida en mapeo')
        _must(isinstance(path,str) and path.strip(), 'Archivo local faltante')
        _must(uri not in idx, 'Duplicado en mapeo')
        idx[uri]=(str(gen),Path(path))
    observed=set(); total=0
    for rec in files:
        _must(isinstance(rec,dict), 'Registro invalido')
        _must(all(k in rec for k in ('uri','generation','size_bytes','sha256')), 'Campos incompletos')
        uri=rec['uri']; gen=rec['generation']; size=rec['size_bytes']; digest=rec['sha256']
        _must(isinstance(uri,str) and uri.startswith(pref) and len(uri)>len(pref), 'URI fuera del prefijo oficial')
        _must('/../' not in uri and '/./' not in uri and '?' not in uri and '#' not in uri, 'URI invalida')
        _must(_generation(gen), 'Generacion invalida')
        _must(type(size) is int and size>=0, 'Tamano invalido')
        _must(isinstance(digest,str) and bool(_HEX.fullmatch(digest)), 'SHA256 invalido')
        _must(uri not in observed and uri in idx, 'Archivo ausente o duplicado')
        observed.add(uri)
        mapping_gen, path = idx[uri]
        _must(mapping_gen==str(gen), 'Generacion incompatible en el mapeo')
        _must(path.is_file() and not path.is_symlink(), f'Archivo local no valido: {path}')
        h=hashlib.sha256(); n=0
        with path.open('rb') as f:
            while block:=f.read(1024*1024):
                n+=len(block);h.update(block)
        _must(n==size, f'Tamano incorrecto: {uri}')
        _must(h.hexdigest()==digest.lower(), f'SHA256 incorrecto: {uri}')
        total+=n
    _must(observed==set(idx), 'Mapeo y manifest no coinciden')
    return dict(estado='SNAPSHOTS_CONCUERDAN_CON_MANIFEST_SUMINISTRADO', dataset=ds,
                archivos=len(observed), bytes_verificados=total, autorizacion_silver=False,
                firma_manifest_verificada=False, generacion_gcs_verificada=False,
                operaciones_cloud=0)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--manifest', required=True, type=Path)
    p.add_argument('--mapa', required=True, type=Path)
    a=p.parse_args()
    resultado=check(json.loads(a.manifest.read_text(encoding='utf-8')),
                    json.loads(a.mapa.read_text(encoding='utf-8')))
    print(json.dumps(resultado, indent=2, ensure_ascii=False))
    print('RESULTADO: SNAPSHOT_OFFLINE_VERIFICADO_SILVER_BLOQUEADO')

if __name__=='__main__':
    main()
