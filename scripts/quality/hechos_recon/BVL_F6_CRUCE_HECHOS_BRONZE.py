#!/usr/bin/env python3
"""Auditoría de solo lectura HDFS ↔ handoff Bronze oficial (si está disponible).
NO genera CSV, no autoriza Silver, no accede a GCS, no consume claims ni escribe en Git.
"""
import argparse
import csv
import hashlib
import io
import json
import re
import subprocess
import tarfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

BASE = '/datalake/bvl/metadata/smv/hechos_importancia/ingestion_date=2026-10-04'
PDF = '/datalake/bvl/bronze/smv/hechos_importancia/pdf/ingestion_date=2026-10-04'
TXT = '/datalake/bvl/bronze/smv/hechos_importancia/texto_extraido/ingestion_date=2026-10-04'
RUN = 'bvl-bronze-inicial-20261125-001'
GEN = '1795680153321464'
PACKAGE_SHA = 'ecc5c1d44fcf529c0efab5652dedb488a6149e5f208e4b94ec247847c3898487'
SHA_RE = re.compile(r'^[a-f0-9]{64}$')


def digest(data): return hashlib.sha256(data).hexdigest()

def hdfs_cat(path):
    r = subprocess.run(['hdfs', 'dfs', '-cat', path], stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, check=False, timeout=90)
    if r.returncode:
        raise ValueError(f'HDFS_NO_LEIDO: {path}: {r.stderr.decode("utf8","replace")[-200:]}')
    if len(r.stdout)>12_000_000: raise ValueError('Metadata >12MB; lectura rechazada')
    return r.stdout

def hdfs_list(dirpath, suffix):
    r = subprocess.run(['hdfs','dfs','-ls','-R',dirpath], capture_output=True,
                       text=True, check=False, timeout=90)
    if r.returncode: raise ValueError('HDFS_LS_ERROR: '+r.stderr[-250:])
    found=[]
    for line in r.stdout.splitlines():
        parts=line.split()
        if len(parts)>=8 and parts[0].startswith('-') and parts[-1].lower().endswith(suffix):
            found.append(parts[-1])
    if len(set(found))!=len(found): raise ValueError('HDFS_RUTAS_DUPLICADAS')
    return found

def csv_rows(raw):
    txt=raw.decode('utf-8-sig')
    try: dialect=csv.Sniffer().sniff(txt[:8192], delimiters=',;\t')
    except csv.Error: dialect=csv.excel
    r=csv.DictReader(io.StringIO(txt,newline=''), dialect=dialect)
    cols=[(n or '').strip() for n in (r.fieldnames or [])]
    if not cols or any(not c for c in cols) or len(cols)!=len(set(cols)):
        raise ValueError('CSV_CABECERAS_NO_VALIDAS')
    rows=[]
    for x in r:
        if None in x: raise ValueError('CSV_COLUMNAS_EXTRA')
        rows.append({str(k).strip():(v or '').strip() for k,v in x.items()})
    return cols,rows

def filename(p): return str(p).replace('\\','/').rstrip('/').rsplit('/',1)[-1]

def canonical_path(v):
    s=str(v or '')
    if s.startswith('hdfs://'): return urlparse(s).path
    return s

def historical_evidence(manifest_rows, docs_rows, pdf_paths, txt_paths):
    if len(manifest_rows)!=990 or len(docs_rows)!=990 or len(pdf_paths)!=990 or len(txt_paths)!=989:
        raise ValueError('CONTEOS_HISTORICOS_INESPERADOS')
    idx={}
    for r in manifest_rows:
        name=filename(r['archivo_pdf'])
        if name in idx: raise ValueError('PDF_DUPLICADO_NOMBRE_EN_MANIFIESTO: '+name)
        size=int(r['tamano_bytes'])
        sha=r['sha256'].lower()
        if size<0 or not SHA_RE.fullmatch(sha):raise ValueError('PDF_HASH_TAMANO_INVALIDOS: '+name)
        idx[name]={'size_bytes':size, 'sha256':sha}
    names=[filename(x) for x in pdf_paths]
    if len(set(names))!=990: raise ValueError('NOMBRES_PDF_HDFS_DUPLICADOS')
    if set(idx)!=set(names): raise ValueError('PDF_EN_HDFS_Y_METADATA_NO_COINCIDEN')
    ref=[filename(x['archivo_pdf']) for x in docs_rows]
    if Counter(ref)!=Counter(names): raise ValueError('PDF_DOCUMENTOS_NO_COINCIDEN')
    return idx

def official_manifest(bundle):
    # Pin de la entrega oficial del 26/11/2026; no confundir con auditoría GCS en vivo
    h=hashlib.sha256()
    with bundle.open('rb') as f:
        while block:=f.read(1024*1024):h.update(block)
    if h.hexdigest()!=PACKAGE_SHA: raise ValueError('HANDOFF_SHA_NO_ES_OFICIAL')
    with tarfile.open(bundle, mode='r:gz') as z:
        def load(name):
            members=z.getmembers()
            ms=[m for m in members if m.name==name and m.isfile()]
            if len(ms)!=1:raise ValueError('HANDOFF_MIEMBRO_NO_UNICO: '+name)
            return z.extractfile(ms[0]).read()
        handoff=json.loads(load('entrega_bronze_silver_preparada.json'))
        publication=json.loads(load('resultado_publicacion_bronze.json'))
        mraw=load('manifests_bronze_oficiales/smv__hechos_importancia.json')
        manifest=json.loads(mraw)
    if handoff.get('run_id')!=RUN or publication.get('run_id')!=RUN or manifest.get('run_id')!=RUN:
        raise ValueError('RUN_DIFERENTE')
    if manifest.get('dataset')!='smv/hechos_importancia' or manifest.get('status')!='BRONZE_COMPLETADO' or manifest.get('quality_approved') is not True:
        raise ValueError('MANIFEST_NO_APROBADO')
    entries=[x for x in handoff.get('datasets',[]) if x.get('dataset')=='smv/hechos_importancia']
    pubs=[x for x in publication.get('manifests',[]) if x.get('dataset')=='smv/hechos_importancia']
    if len(entries)!=1 or len(pubs)!=1:raise ValueError('REGISTRO_DATASET_NO_UNICO')
    e,p=entries[0],pubs[0]
    if str(e.get('bronze_manifest_generation'))!=GEN or str(p.get('bronze_manifest_generation'))!=GEN:
        raise ValueError('GENERACION_BRONZE_INCORRECTA')
    if e.get('bronze_manifest_sha256')!=digest(mraw) or p.get('bronze_manifest_sha256')!=digest(mraw):
        raise ValueError('MANIFEST_DIGEST_NO_CONCUERDA_CON_HANDOFF')
    files=manifest.get('files')
    if not isinstance(files,list) or len(files)!=1979:raise ValueError('BRONZE_NO_TIENE_1979')
    for f in files:
        if not f.get('uri','').startswith('gs://bvl26-bronze-7f3a9c/smv/hechos_importancia/run_id='+RUN+'/'):
            raise ValueError('URI_FUERA_DE_BRONZE_OFICIAL')
        if not SHA_RE.fullmatch(str(f.get('sha256','')).lower()) or int(f.get('size_bytes',-1))<0 or not str(f.get('generation','')).isdigit():
            raise ValueError('FILE_METADATA_INVALID')
    return files,digest(mraw)

def cross_match(hist_pdf, pdfpaths, txtpaths, bronze):
    # Coincide por ruta HDFS de origen (más fuerte que únicamente el nombre).
    hdfs_all={canonical_path(p) for p in pdfpaths+txtpaths}
    observed={}; pcount=tcount=0; pdf_hash_matches=0
    missing_origin=[]; mismatches=[]
    for f in bronze:
        origin=canonical_path(f.get('source_hdfs'))
        uri=f['uri']; name=filename(origin)
        if not origin:
            missing_origin.append(uri)
            continue
        if origin in observed:raise ValueError('HDFS_ORIGEN_DUPLICADO_BRONZE')
        observed[origin]=uri
        if origin.lower().endswith('.pdf'):
            pcount+=1
            recorded=hist_pdf.get(name)
            if not recorded or recorded['size_bytes'] != f['size_bytes'] or recorded['sha256'] != f['sha256'].lower():
                mismatches.append(name)
            else: pdf_hash_matches+=1
        elif origin.lower().endswith('.txt'): tcount+=1
        else: mismatches.append('EXTENSION_INESPERADA:'+origin)
    return dict(bronze_count=len(bronze),bronze_pdf=pcount,bronze_txt=tcount,
                pdf_sha_size_matches=pdf_hash_matches,
                origin_paths_matched=len(hdfs_all&set(observed)),origin_paths_expected=len(hdfs_all),
                origins_missing=len(missing_origin),mismatches_count=len(mismatches),
                mismatch_examples=mismatches[:8],
                recorded_hashes_match=(pdf_hash_matches==990 and not mismatches and pcount==990 and tcount==989 and len(hdfs_all&set(observed))==1979 and not missing_origin))

def locate_handoff(base, explicit):
    if explicit:return Path(explicit).expanduser()
    possible=[base/'Descargas/entrega_bronze_silver_daniela.tar.gz',
              base/'Downloads/entrega_bronze_silver_daniela.tar.gz',
              base/'entrega_bronze_silver_daniela.tar.gz']
    return next((p for p in possible if p.is_file()),None)

def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--handoff',help='Paquete original oficial entrega_bronze_silver_daniela.tar.gz; no modificar')
    ap.add_argument('--out',default=str(Path.home()/'Descargas'))
    a=ap.parse_args()
    out=Path(a.out).expanduser();out.mkdir(parents=True, exist_ok=True)
    report={'schema_version':'BVL-HECHOS-HDFS-BRONZE-CROSS-v1',
      'timestamp_utc':datetime.now(timezone.utc).isoformat(), 'source_generator':'NO_ACREDITADO',
      'gcs_access':False,'claims_consumed':0,'cloud_batches':0,'authorization_granted':False}
    try:
        raw=hdfs_cat(BASE+'/manifiesto_pdfs_hechos_importancia.csv')
        _,manifest_rows=csv_rows(raw)
        _,docs_rows=csv_rows(hdfs_cat(BASE+'/hechos_documentos_2025.csv'))
        pdfs=hdfs_list(PDF,'.pdf');txts=hdfs_list(TXT,'.txt')
        hist=historical_evidence(manifest_rows,docs_rows,pdfs,txts)
        report['historical_pdf_metadata_sha256']=digest(raw)
        report['historical_pdf_rows_validated']=len(hist)
        report['hdfs_pdf_count']=len(pdfs);report['hdfs_txt_count']=len(txts)
        report['historic_sha_claims_verified_against_pdf_bytes']=False
        bundle=locate_handoff(Path.home(),a.handoff)
        if bundle:
            if not bundle.is_file():raise ValueError('HANDOFF_NO_ENCONTRADO_EN_RUTA')
            bronze,manifest_sha=official_manifest(bundle)
            report['handoff_bundle_sha256']=PACKAGE_SHA
            report['bronze_official_manifest_generation']=GEN
            report['bronze_official_manifest_sha256']=manifest_sha
            report['cross']=cross_match(hist,pdfs,txts,bronze)
            report['status']='CRUCE_DE_METADATOS_COMPLETO_GENERADOR_PENDIENTE' if report['cross']['recorded_hashes_match'] else 'CRUCE_CON_DIFERENCIAS_NO_AUTORIZA'
        else:
            report['status']='PENDIENTE_HANDOFF_OFICIAL_NO_AUTORIZA'
    except Exception as exc:
        report['status']='AUDITORIA_INCOMPLETA_NO_AUTORIZA'
        report['error']=f'{type(exc).__name__}: {exc}'[:300]
    path=out/'BVL_F6_HECHOS_CRUCE_BRONZE.json'
    path.write_text(json.dumps(report,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf8')
    print('INFORME:',path)
    print('HDFS_PDF_METADATA:',report.get('historical_pdf_rows_validated','NO'))
    if report.get('cross'):
        x=report['cross']
        print('PDF_SHA_TAMANO_COINCIDENTES:',x['pdf_sha_size_matches'],'/990')
        print('RUTAS_HDFS_BRONZE_COINCIDENTES:',x['origin_paths_matched'],'/1979')
        print('DIFERENCIAS:',x['mismatches_count'])
    else: print('HANDOFF_OFICIAL:', 'PENDIENTE' if not a.handoff else 'ERROR')
    print('GENERADOR_ORIGINAL: NO_ACREDITADO')
    print('ESTADO:',report['status'])
    print('GCS:0 | CLAIM:0 | DATAPROC:0 | GIT:SIN_CAMBIOS')
    return 0 if report['status'] not in ('AUDITORIA_INCOMPLETA_NO_AUTORIZA',) else 1

if __name__=='__main__':raise SystemExit(main())
