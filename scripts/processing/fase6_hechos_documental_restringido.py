#!/usr/bin/env python3
"""Preparación local de Silver documental restringido, NO una ejecución/productiva.

Fuente exclusiva de identidad y linaje de objetos: manifest Bronze oficial fijado.
Verificación de bytes de los 990 PDF y 989 TXT desde HDFS original (solo lectura).
Nunca consulta portales, no lee metadatos históricos de expedientes, no escribe GCS,
no altera Bronze ni puertas protegidas, no consume claims ni se integra automáticamente.
"""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
import re
import subprocess
import tarfile
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse

HANDOFF_SHA = 'ecc5c1d44fcf529c0efab5652dedb488a6149e5f208e4b94ec247847c3898487'
MANIFEST_SHA = 'd2c5f5d5a363e271a770497b4af6feedde454f999bc2fe7696a5c0ab0784eab0'
MANIFEST_GENERATION = '1795680153321464'
RUN_ID = 'bvl-bronze-inicial-20261125-001'
DATASET = 'smv/hechos_importancia'
PREFIX_GCS = 'gs://bvl26-bronze-7f3a9c/smv/hechos_importancia/run_id=' + RUN_ID + '/'
PREFIX_HDFS = '/datalake/bvl/bronze/smv/hechos_importancia/'
NAME_RE = re.compile(r'^(\d{8})_([0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12})\.(pdf|txt)$')
SHA_RE = re.compile('[0-9a-f]{64}')
QUALITY_THRESHOLD = 100  # Propuesta: pendiente de ratificación Quality
MAX_BYTES = 64 * 1024 * 1024


def check(ok, message):
    if not ok:
        raise ValueError(message)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for buf in iter(lambda: f.read(1 << 20), b''):
            h.update(buf)
    return h.hexdigest()


def safe_tar_member(tar, name):
    m = [i for i in tar.getmembers() if i.isfile() and i.name == name]
    check(len(m) == 1 and m[0].size <= 16 * 1024 * 1024, 'HANDOFF_MIEMBRO_NO_UNICO_O_GRANDE:' + name)
    return tar.extractfile(m[0]).read()


def load_official_handoff(handoff: Path):
    check(handoff.is_file(), 'HANDOFF_NO_EXISTE')
    check(file_digest(handoff) == HANDOFF_SHA, 'HANDOFF_SHA_NO_COINCIDE_CON_OFICIAL')
    with tarfile.open(handoff, 'r:gz') as tar:
        handoff_meta = json.loads(safe_tar_member(tar, 'entrega_bronze_silver_preparada.json'))
        publication = json.loads(safe_tar_member(tar, 'resultado_publicacion_bronze.json'))
        raw = safe_tar_member(tar, 'manifests_bronze_oficiales/smv__hechos_importancia.json')
    check(sha256(raw) == MANIFEST_SHA, 'MANIFEST_SHA_NO_OFICIAL')
    manifest = json.loads(raw)
    check(all(x.get('run_id') == RUN_ID for x in (handoff_meta, publication, manifest)), 'RUN_ID_DIFERENTE')
    check(manifest.get('dataset') == DATASET and manifest.get('status') == 'BRONZE_COMPLETADO'
          and manifest.get('quality_approved') is True, 'MANIFEST_NO_APROBADO')
    h = [i for i in handoff_meta.get('datasets', []) if i.get('dataset') == DATASET]
    p = [i for i in publication.get('manifests', []) if i.get('dataset') == DATASET]
    check(len(h) == len(p) == 1, 'ENTRADA_PUBLICACION_AMBIGUA')
    for entry in h + p:
        check(str(entry.get('bronze_manifest_generation')) == MANIFEST_GENERATION,
              'GENERACION_MANIFEST_DISTINTA')
        check(entry.get('bronze_manifest_sha256') == MANIFEST_SHA, 'HASH_MANIFEST_DISTINTO')
    return manifest['files']


def parse_name(filename):
    m = NAME_RE.fullmatch(filename)
    check(bool(m), 'NOMBRE_PDF_TXT_NO_RECONOCIDO:' + filename)
    try:
        fecha = datetime.strptime(m.group(1), '%Y%m%d').date().isoformat()
    except ValueError as ex:
        raise ValueError('FECHA_NOMINAL_INVALIDA:' + filename) from ex
    return (fecha, m.group(2).upper(), m.group(3))


def classify_official(files, expect_counts=True):
    """Usa exclusivamente las entradas del manifest oficial, nunca CSV HDFS de eventos."""
    check(isinstance(files, list), 'MANIFEST_NO_LISTA')
    pdf, txt, origins, uris = {}, {}, set(), set()
    for x in files:
        check(isinstance(x, dict), 'ENTRADA_NO_OBJETO')
        uri, origin = x.get('uri'), x.get('source_hdfs')
        check(isinstance(uri, str) and uri.startswith(PREFIX_GCS), 'URI_GCS_FUERA_DE_RUN')
        check(isinstance(origin, str) and origin.startswith(PREFIX_HDFS), 'ORIGEN_HDFS_INVALIDO')
        check('..' not in PurePosixPath(origin).parts and '//' not in origin, 'RUTA_HDFS_INSEGURA')
        check(uri not in uris and origin not in origins, 'OBJETO_O_ORIGEN_REPETIDO')
        uris.add(uri); origins.add(origin)
        check(x.get('generation') is not None and str(x.get('generation')).isdigit(), 'GENERACION_FALTANTE')
        size = x.get('size_bytes')
        check(type(size) is int and 0 < size <= MAX_BYTES, 'TAMANO_INVALIDO')
        digest = x.get('sha256')
        check(isinstance(digest, str) and SHA_RE.fullmatch(digest), 'SHA256_INVALIDO')
        filename = PurePosixPath(origin).name
        check(uri.endswith('/' + filename), 'NOMBRE_BRONZE_HDFS_NO_COINCIDE')
        date, guid, ext = parse_name(filename)
        kind = 'pdf' if ext == 'pdf' else 'texto_extraido'
        check('/' + kind + '/ingestion_date=2026-10-04/' in origin, 'RUTA_FUENTE_INESPERADA')
        check('/' + kind + '/ingestion_date=2026-10-04/' in uri, 'RUTA_GCS_INESPERADA')
        item = {'archivo': filename, 'uri': uri, 'generation': str(x['generation']),
                'sha256': digest, 'tamano_bytes': size, 'source_hdfs': origin,
                'fecha_nominal_archivo': date, 'guid_documento': guid}
        target = pdf if ext == 'pdf' else txt
        check(filename not in target, 'NOMBRE_REPETIDO')
        target[filename] = item
    if expect_counts:
        check((len(pdf), len(txt), len(files)) == (990, 989, 1979), 'CONTEO_OFICIAL_NO_ESPERADO')
    check(len({x['guid_documento'] for x in pdf.values()}) == len(pdf), 'GUID_DOCUMENTAL_PDF_REPETIDO')
    return pdf, txt


def prepare_document_index(pdf, txt, require_complete=True):
    """Duplicidad solo por bytes: SHA-256 en manifest; origen canonical ordenado."""
    by_sha = defaultdict(list)
    for name, item in pdf.items():
        by_sha[item['sha256']].append(name)
    result=[]
    mapped_txt=set()
    for name in sorted(pdf):
        item=pdf[name]
        equal=sorted(by_sha[item['sha256']])
        canonical=equal[0]
        own_name=name[:-4] + '.txt'
        canonical_name=canonical[:-4] + '.txt'
        if own_name in txt:
            name_txt=own_name
            origin_text='DIRECTO'
        elif canonical_name in txt and len(equal)>1:
            name_txt=canonical_name
            origin_text='CANONICO_POR_SHA_DOCUMENTAL'
        else:
            # No inventar una fuente de texto, ni usar metadata histórica como prueba
            name_txt=None
            origin_text='SIN_TXT'
        if name_txt:
            mapped_txt.add(name_txt)
        result.append({'pdf':item, 'txt':txt.get(name_txt), 'duplicado_pdf_sha256': name != canonical,
                       'pdf_canonico':canonical, 'origen_texto':origin_text})
    if require_complete:
        check(len(result) == 990 and len(mapped_txt) == 989 and len(txt) == 989, 'TXTS_SIN_CONCILIAR')
        check(sum(i['origen_texto']=='SIN_TXT' for i in result)==0, 'TXT_SIN_RELACION')
        check(sum(i['origen_texto']=='CANONICO_POR_SHA_DOCUMENTAL' for i in result)==1, 'CANONICOS_NO_ESPERADOS')
        check(len(by_sha)==989, 'DUPLICIDAD_PDF_INESPERADA')
    return result


def hdfs_bytes_verified(item):
    """HDFS read-only, bytes completos con límites y hash contra Bronze oficial."""
    cmd=['hdfs', 'dfs', '-cat', item['source_hdfs']]
    p=subprocess.run(cmd, capture_output=True, timeout=90, check=False)
    check(p.returncode == 0, 'NO_SE_PUDO_LEER_HDFS:' + item['archivo'] + ':' + p.stderr.decode('utf-8','replace')[-120:])
    data=p.stdout
    check(len(data) == item['tamano_bytes'] and len(data)<=MAX_BYTES, 'BYTES_HDFS_NO_COINCIDEN:' + item['archivo'])
    check(sha256(data) == item['sha256'], 'SHA_HDFS_NO_COINCIDE:' + item['archivo'])
    return data


def quality_text(data):
    try: text=data.decode('utf-8-sig',errors='strict')
    except UnicodeDecodeError as exc:raise ValueError('TXT_NO_UTF8_VERIFICABLE') from exc
    char_count=len(text.strip())
    # Umbral ilustrativo. Registrar valores y solicitar ratificación de Quality.
    if char_count == 0: status='SIN_TEXTO'
    elif char_count < QUALITY_THRESHOLD: status='CORTO_REVISAR'
    else: status='CON_TEXTO'
    return text, char_count, status


def verify_and_build(index, read_hdfs=hdfs_bytes_verified):
    docs=[]; stats=Counter(); verified_pdf=set(); verified_txt={}
    for ix in index:
        p=ix['pdf']; t=ix['txt']
        if p['source_hdfs'] not in verified_pdf:
            read_hdfs(p)
            verified_pdf.add(p['source_hdfs'])
        stats['pdf_verificados'] += 1
        if t is None:
            text='';chars=0;status='SIN_TXT'
        else:
            if t['source_hdfs'] not in verified_txt:
                verified_txt[t['source_hdfs']]=quality_text(read_hdfs(t))
            text,chars,status=verified_txt[t['source_hdfs']]
            stats['asociaciones_txt'] += 1
        stats['documentos']+=1
        stats['duplicados_documentales_marcados']+=int(ix['duplicado_pdf_sha256'])
        stats['texto_'+status]+=1
        docs.append({
            'guid_documento_archivo':p['guid_documento'],
            'nombre_pdf':p['archivo'],
            'ruta_pdf_bronze':p['uri'],
            'generacion_pdf_bronze':p['generation'],
            'sha256_pdf':p['sha256'],
            'tamano_pdf_bytes':p['tamano_bytes'],
            'fecha_nominal_archivo':p['fecha_nominal_archivo'],
            'fecha_nominal_no_es_fecha_oficial_hecho':True,
            'duplicado_por_sha256':ix['duplicado_pdf_sha256'],
            'nombre_pdf_canonico':ix['pdf_canonico'],
            'nombre_txt': t['archivo'] if t else None,
            'ruta_txt_bronze':t['uri'] if t else None,
            'generacion_txt_bronze':t['generation'] if t else None,
            'sha256_txt':t['sha256'] if t else None,
            'tamano_txt_bytes':t['tamano_bytes'] if t else None,
            'origen_texto':ix['origen_texto'],
            'texto_extraido':text,
            'caracteres_txt_verificados':chars,
            'estado_calidad_txt_propuesto':status,
            'verificacion':'BYTES_HDFS_SHA256_IGUAL_MANIFEST_BRONZE_PINNED',
        })
    stats['txt_unicos_verificados']=len(verified_txt)
    return docs,dict(stats)


def schema_spark():
    from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
    types={'tamano_pdf_bytes':LongType,'tamano_txt_bytes':LongType,
           'caracteres_txt_verificados':LongType,
           'fecha_nominal_no_es_fecha_oficial_hecho':BooleanType,
           'duplicado_por_sha256':BooleanType}
    keys=[
     'guid_documento_archivo','nombre_pdf','ruta_pdf_bronze','generacion_pdf_bronze',
     'sha256_pdf','tamano_pdf_bytes','fecha_nominal_archivo','fecha_nominal_no_es_fecha_oficial_hecho',
     'duplicado_por_sha256','nombre_pdf_canonico','nombre_txt','ruta_txt_bronze',
     'generacion_txt_bronze','sha256_txt','tamano_txt_bytes','origen_texto','texto_extraido',
     'caracteres_txt_verificados','estado_calidad_txt_propuesto','verificacion']
    return StructType([StructField(k,types.get(k,StringType)(),nullable=(k not in {
      'guid_documento_archivo','nombre_pdf','ruta_pdf_bronze','sha256_pdf','generacion_pdf_bronze'
      })) for k in keys])


def write_parquet_local(rows, out):
    # Usar Spark únicamente para serializar a LOCAL. No usa GCS/Dataproc, ni publica Silver.
    from pyspark.sql import SparkSession
    spark=SparkSession.builder.master('local[1]').appName('bvl-hechos-documental-candidato-sin-publicar')\
        .config('spark.ui.enabled','false').getOrCreate()
    try:
        df=spark.createDataFrame(rows,schema_spark())
        check(df.count()==len(rows),'CONTEO_SPARK_NO_COINCIDE')
        df.write.mode('errorifexists').option('compression','snappy').parquet(out.resolve().as_uri())
        reloaded=spark.read.parquet(out.resolve().as_uri())
        check(reloaded.count()==len(rows),'PARQUET_RELECTURA_INCONSISTENTE')
        check(set(reloaded.columns)==set(rows[0]),'ESQUEMA_PARQUET_DIFERENTE')
        return sum(1 for p in out.glob('part-*.parquet'))
    finally:
        spark.stop()


def main(argv=None):
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--handoff',required=True,help='TAR oficial fijado por hash; NUNCA reemplazar por ZIP histórico')
    ap.add_argument('--salida',required=True,help='Carpeta local nueva, fuera del repo')
    ap.add_argument('--modo',required=True,choices=('preflight','procesar-hdfs'))
    args=ap.parse_args(argv)
    salida=Path(args.salida).expanduser().resolve()
    check(not salida.exists(), 'SALIDA_YA_EXISTE_NO_SOBRESCRIBIR')
    check('/bvl-analytics-platform/' not in str(salida) and not salida.name=='bvl-analytics-platform', 'NO_ESCRIBIR_EN_REPO')
    files=load_official_handoff(Path(args.handoff).expanduser())
    pdf,txt=classify_official(files)
    index=prepare_document_index(pdf,txt)
    summary={
      'schema_version':'BVL-HECHOS-DOCUMENTAL-RESTRINGIDO-CANDIDATO-v1',
      'estado':'NO_AUTORIZA_PUBLICACION', 'dataset':DATASET,
      'run_id_bronze':RUN_ID,'bronze_manifest_sha256':MANIFEST_SHA,
      'bronze_manifest_generation':MANIFEST_GENERATION,
      'pdf_esperados':len(pdf),'txt_esperados':len(txt),
      'pdf_duplicados_por_sha256':sum(v['duplicado_pdf_sha256'] for v in index),
      'relaciones_txt_canonicas':sum(v['origen_texto']=='CANONICO_POR_SHA_DOCUMENTAL' for v in index),
      'umbral_texto_caracteres_propuesto_no_ratificado':QUALITY_THRESHOLD,
      'no_campos_historicos_eventos':True,'cuarentena_eventos_sin_pdf':27,
      'origen_evidencia_eventos':'FUERA_DEL_PRODUCTO_OFICIAL',
      'gcs_leido':False,'gcs_escrito':False,'cloud_batches':0,'claims_consumidos':0,
      'parquet_local_partes':0,
    }
    if args.modo == 'procesar-hdfs':
        rows, stats=verify_and_build(index)
        check(len(rows)==990 and stats['txt_unicos_verificados']==989 and stats['pdf_verificados']==990,
              'CONCILIACION_FINAL_FALLIDA')
        check(sum(r['duplicado_por_sha256'] for r in rows)==1,
              'UN_PDF_NO_CANONICO_DUPLICADO_ESPERADO')
        salida.mkdir(parents=True)
        (salida/'reporte_antes_parquet.json').write_text(json.dumps({**summary,'estado':'BYTES_VERIFICADOS_PARQUET_PENDIENTE',**stats},indent=2,ensure_ascii=False)+'\n')
        try:
            partes=write_parquet_local(rows,salida/'hechos_documentales_restringidos.parquet')
        except Exception:
            # No se afirma salida satisfactoria, se conserva diagnóstico local separado.
            raise
        check(partes>0,'SIN_PARQUET')
        summary.update(stats)
        summary['parquet_local_partes']=partes
        summary['verificacion_hdfs_pdf_txt_sha256']='1979/1979'
        summary['estado']='CANDIDATO_LOCAL_COMPLETO_PENDIENTE_QUALITY'
    else:
        salida.mkdir(parents=True)
        summary['estado']='MANIFEST_OFICIAL_VALIDADO_NO_SE_HAN_LEIDO_BYTES_HDFS'
    (salida/'reporte_quality.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    print('DOCUMENTOS_PDF:',len(pdf),'TXT:',len(txt))
    print('DUPLICADO_SHA_MARCADO:',summary['pdf_duplicados_por_sha256'])
    print('ESTADO:',summary['estado'])
    print('INFORME:',salida/'reporte_quality.json')
    print('GCS:NO | CLAIMS:NO | PUBLICACION:NO | GIT:SIN_CAMBIOS')
    return 0

if __name__=='__main__':
    raise SystemExit(main())
