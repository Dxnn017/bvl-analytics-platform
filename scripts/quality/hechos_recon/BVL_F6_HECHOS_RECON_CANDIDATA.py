#!/usr/bin/env python3
"""Reconstruction of *candidate* Hechos metadata from individually linked inputs.

Reads HDFS metadata and a pinned Bronze official handoff; writes only local candidate files.
Does NOT establish independent provenance of the historical GUID/expediente mapping,
does NOT claim semantic equivalence with historical metadata, and never authorizes Silver.
"""
import argparse
from collections import defaultdict, Counter
from datetime import datetime, timezone
from pathlib import Path
import csv
import hashlib
import io
import json
import re

from BVL_F6_CRUCE_HECHOS_BRONZE import (
    BASE, PDF, TXT, GEN, hdfs_cat, hdfs_list, official_manifest,
    csv_rows, filename, canonical_path, historical_evidence, cross_match,
)

NAME='BVL-HECHOS-RECON-CANDIDATE-v1'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def normalized(v):
    return str(v or '').strip()


def unique_per_group(rows, field):
    values={normalized(row.get(field)) for row in rows if normalized(row.get(field))}
    return (next(iter(values)) if len(values)==1 else ''), len(values)>1


def csv_bytes(rows, cols):
    buf=io.StringIO(newline='')
    writer=csv.DictWriter(buf,fieldnames=cols,extrasaction='ignore',lineterminator='\n')
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buf.getvalue().encode('utf-8')


def reconstruct(manifest_rows,mapping_rows,bronze_files, pdf_paths,txt_paths, *, strict_counts=True):
    """Pure assembly. Returns candidate rows and review issues; never chooses a conflicting value."""
    pdf_by_name={}
    for m in manifest_rows:
        key=filename(m.get('archivo_pdf',''))
        if not key or key in pdf_by_name:
            raise ValueError('PDF_METADATA_NOMBRE_DUPLICADO_O_VACIO: '+key)
        try: size=int(m['tamano_bytes'])
        except (KeyError,TypeError,ValueError): raise ValueError('PDF_TAMANO_INVALIDO: '+key)
        hs=normalized(m.get('sha256')).lower()
        if size<0 or not re.fullmatch('[a-f0-9]{64}',hs):
            raise ValueError('PDF_HASH_INVALIDO: '+key)
        pdf_by_name[key]=m
    pset={canonical_path(x) for x in pdf_paths}
    tset={canonical_path(x) for x in txt_paths}
    official={}
    for obj in bronze_files:
        source=canonical_path(obj.get('source_hdfs'))
        if source in official or source not in pset|tset:
            raise ValueError('ORIGEN_BRONZE_DUPLICADO_O_NO_HDFS: '+source)
        official[source]=obj
    if set(official)!=(pset|tset):
        raise ValueError('INVENTARIO_BRONZE_HDFS_INCOMPLETO')
    pdf_bronze={filename(path): (path,official[path]) for path in pset}
    if len(pdf_bronze)!=len(pset) or set(pdf_by_name)!=set(pdf_bronze):
        raise ValueError('PDF_METADATA_Y_HDFS_NO_COINCIDEN')
    for name,(path,obj) in pdf_bronze.items():
        m=pdf_by_name[name]
        if (int(obj['size_bytes'])!=int(m['tamano_bytes']) or
            normalized(obj['sha256']).lower()!=normalized(m['sha256']).lower()):
            raise ValueError('SHA_TAMANO_PDF_NO_CONCUERDA_CON_BRONZE: '+name)
    seen_guid=set(); seen_pdf=set(); docs=[]; grouped=defaultdict(list); issues=[]
    txt_by_name={filename(p): p for p in tset}
    if len(txt_by_name)!=len(tset):raise ValueError('TXT_HDFS_NOMBRES_DUPLICADOS')
    for m in mapping_rows:
        guid=normalized(m.get('guid_documento'))
        exp=normalized(m.get('numero_expediente'))
        pdf=filename(m.get('archivo_pdf'))
        if not guid or not exp or not pdf:
            raise ValueError('MAPEO_CLAVES_OBLIGATORIAS_VACIAS')
        if guid in seen_guid:raise ValueError('GUID_DUPLICADO: '+guid)
        if pdf in seen_pdf:raise ValueError('MAPEO_PDF_DUPLICADO: '+pdf)
        if pdf not in pdf_by_name:raise ValueError('MAPEO_PDF_SIN_BRONZE: '+pdf)
        seen_guid.add(guid);seen_pdf.add(pdf)
        meta=pdf_by_name[pdf]; source, bron=pdf_bronze[pdf]
        canonical=filename(meta.get('archivo_canonico')) or pdf
        expected_txt=re.sub(r'(?i)\.pdf$', '.txt',pdf)
        canonical_txt=re.sub(r'(?i)\.pdf$', '.txt', canonical)
        if expected_txt in txt_by_name:
            text_source=txt_by_name[expected_txt]; text_relation='DIRECTO'
        elif canonical_txt in txt_by_name:
            text_source=txt_by_name[canonical_txt]; text_relation='CANONICO_DUPLICADO'
        else:
            raise ValueError('TXT_NO_RESUELTO_PARA_PDF: '+pdf)
        text_bron=official[text_source]
        row={
            'guid_documento':guid, 'numero_expediente':exp,
            'archivo_pdf':pdf,'archivo_canonico':canonical,
            'empresa':normalized(m.get('empresa')),
            'fecha_presentacion':normalized(m.get('fecha_presentacion')),
            'fecha_consulta_portal':normalized(m.get('fecha_consulta_portal')),
            'url_documento':normalized(m.get('url_documento')),
            'coherencia_temporal':normalized(m.get('coherencia_temporal')),
            'usar_silver_principal':normalized(m.get('usar_silver_principal')),
            'fecha_archivo':normalized(meta.get('fecha_archivo')),
            'tamano_bytes':normalized(meta.get('tamano_bytes')),
            'sha256':normalized(meta.get('sha256')).lower(),
            'duplicado':normalized(meta.get('duplicado')),
            'estado_extraccion':normalized(meta.get('estado_extraccion')),
            'caracteres_extraidos':normalized(meta.get('caracteres_extraidos')),
            'ruta_txt':normalized(meta.get('ruta_txt')),
            'bronze_pdf_uri':bron['uri'],
            'bronze_pdf_generation':str(bron['generation']),
            'bronze_pdf_sha256':str(bron['sha256']),
            'bronze_pdf_source_hdfs':source,
            'bronze_txt_uri':text_bron['uri'],
            'bronze_txt_generation':str(text_bron['generation']),
            'bronze_txt_sha256':str(text_bron['sha256']),
            'bronze_txt_source_hdfs':text_source,
            'fuente_texto':text_relation,
            'origen_campos_portal':'MAPEO_HDFS_HISTORICO_PENDIENTE_RATIFICACION',
            'origen_pdf_txt':'BRONZE_OFICIAL_MANIFEST_VERIFICADO',
        }
        docs.append(row);grouped[exp].append(row)
    if seen_pdf!=set(pdf_by_name):
        raise ValueError('MAPEO_NO_CUBRE_TODOS_LOS_PDF')
    events=[]
    for exp,rows in sorted(grouped.items()):
        event={'numero_expediente':exp,'cantidad_documentos_pdf':str(len(rows)),
               'cantidad_documentos_portal': '', 'fecha_acuerdo':'',
               'fuente_campos':'MAPEO_HDFS_HISTORICO_PENDIENTE_RATIFICACION'}
        for field in ('empresa','fecha_presentacion','fecha_consulta_portal','coherencia_temporal','usar_silver_principal'):
            value,conflict=unique_per_group(rows,field)
            event[field]=value
            if conflict:
                issues.append({'issue':'CAMPO_EVENTO_CONFLICTIVO','numero_expediente':exp,'campo':field})
        events.append(event)
    docs.sort(key=lambda x:(x['numero_expediente'],x['guid_documento'],x['archivo_pdf']))
    if strict_counts and (len(events)!=346 or len(docs)!=990 or len(pdf_paths)!=990 or len(txt_paths)!=989):
        raise ValueError(f'CONTEOS_NO_HISTORICOS: eventos={len(events)} documentos={len(docs)}')
    if strict_counts:
        for cls, expected in [('COHERENTE',338),('HISTORICO_REGULARIZACION',8)]:
            found=sum(x.get('coherencia_temporal')==cls for x in events)
            if found!=expected:
                issues.append({'issue':'DIFERENCIA_CLASIFICACION_HISTORICA','valor':cls,
                               'observado':found,'esperado':expected})
    if sum(x['fuente_texto']=='CANONICO_DUPLICADO' for x in docs)!=1:
        issues.append({'issue':'DIFERENCIA_TEXTO_CANONICO','valor':sum(x['fuente_texto']=='CANONICO_DUPLICADO' for x in docs)})
    return events,docs,issues


def compare_reference(events,docs,history_events,history_docs):
    olde={r.get('numero_expediente'):r for r in history_events}
    oldd={r.get('guid_documento'):r for r in history_docs}
    comparison={'eventos_candidato':len(events),'eventos_historicos':len(history_events),
                'documentos_candidato':len(docs),'documentos_historicos':len(history_docs),
                'expedientes_no_historicos':len(set(x['numero_expediente'] for x in events)-set(olde)),
                'guid_no_historicos':len(set(x['guid_documento'] for x in docs)-set(oldd)),
                'diferencias_campos_comparables':{},
                'advertencia':'NO ACREDITA EQUIVALENCIA SEMANTICA NI ORIGEN DEL MAPEO HISTORICO'}
    for field in ('empresa','fecha_presentacion','fecha_consulta_portal','coherencia_temporal','usar_silver_principal'):
        diff=sum(1 for r in events if (r['numero_expediente'] in olde and
                normalized(r.get(field))!=normalized(olde[r['numero_expediente']].get(field))))
        comparison['diferencias_campos_comparables'][field]=diff
    for field in ('numero_expediente','archivo_pdf','archivo_canonico','sha256'):
        diff=sum(1 for r in docs if (r['guid_documento'] in oldd and
                normalized(r.get(field))!=normalized(oldd[r['guid_documento']].get(field))))
        comparison['diferencias_campos_comparables']['docs_'+field]=diff
    return comparison


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--handoff',required=True,type=Path)
    ap.add_argument('--out',type=Path,default=Path.home()/'Descargas')
    args=ap.parse_args()
    args.out.mkdir(parents=True,exist_ok=True)
    bronze,manifest_sha=official_manifest(args.handoff.expanduser())
    inputs={}
    for name in ('manifiesto_pdfs_hechos_importancia.csv','mapeo_pdf_expediente.csv',
                 'hechos_eventos_2025.csv','hechos_documentos_2025.csv'):
        raw=hdfs_cat(BASE+'/'+name); cols,rows=csv_rows(raw)
        inputs[name]={'sha256_hdfs':sha(raw),'bytes':len(raw),'columns':cols,'rows':len(rows)}
        if name=='manifiesto_pdfs_hechos_importancia.csv': m_pdf=rows
        if name=='mapeo_pdf_expediente.csv': mapping=rows
        if name=='hechos_eventos_2025.csv': hist_event=rows
        if name=='hechos_documentos_2025.csv': hist_doc=rows
    pdfs=hdfs_list(PDF,'.pdf');txts=hdfs_list(TXT,'.txt')
    oldindex=historical_evidence(m_pdf,hist_doc,pdfs,txts)
    crossing=cross_match(oldindex,pdfs,txts,bronze)
    if not crossing['recorded_hashes_match']:
        raise SystemExit('ALTO: inventario no concuerda con Bronze oficial')
    events,docs,issues=reconstruct(m_pdf,mapping,bronze,pdfs,txts)
    comp=compare_reference(events,docs,hist_event,hist_doc)
    # Draft strictly local; do not claim historical identity of event or document fields.
    evcols=['numero_expediente','empresa','fecha_presentacion','fecha_consulta_portal',
            'coherencia_temporal','usar_silver_principal','cantidad_documentos_pdf',
            'cantidad_documentos_portal','fecha_acuerdo','fuente_campos']
    doccols=['guid_documento','numero_expediente','archivo_pdf','archivo_canonico',
             'empresa','fecha_presentacion','fecha_consulta_portal','url_documento',
             'coherencia_temporal','usar_silver_principal','fecha_archivo',
             'tamano_bytes','sha256','duplicado','estado_extraccion','caracteres_extraidos',
             'ruta_txt','bronze_pdf_uri','bronze_pdf_generation','bronze_pdf_sha256',
             'bronze_pdf_source_hdfs','bronze_txt_uri','bronze_txt_generation',
             'bronze_txt_sha256','bronze_txt_source_hdfs','fuente_texto',
             'origen_campos_portal','origen_pdf_txt']
    ebytes=csv_bytes(events,evcols);dbytes=csv_bytes(docs,doccols)
    prefix=args.out/'BVL_F6_HECHOS_RECONSTRUCCION_CANDIDATA'
    report={'schema_version':NAME,'estado':'BORRADOR_LOCAL_NO_AUTORIZA',
            'generated_at_utc':datetime.now(timezone.utc).isoformat(),
            'bronze_manifest_generation':GEN,'bronze_manifest_sha256':manifest_sha,
            'inputs':inputs,'hdfs_bronze_cross':crossing,
            'counts':{'events':len(events),'documents':len(docs),'pdf':len(pdfs),'txt':len(txts)},
            'reference_comparison':comp,'review_issues':issues,
            'known_provenance_gap':'GUID, numero_expediente y metadatos SMV provienen de mapeo HDFS que no forma parte del lote Bronze oficial; debe ratificarlo Quality',
            'known_missing_fields':'fecha_acuerdo y cantidad_documentos_portal sin fuente derivable inequívoca, vacíos a propósito',
            'legacy_generator_original':'NO_ACREDITADO','new_procedure':'DETERMINISTIC_REBUILD_FROM_VERIFIED_HDFS_MANIFEST_MAP_AND_PINNED_BRONZE',
            'output_sha256':{'events':sha(ebytes),'documents':sha(dbytes)},
            'writes_gcs':0,'claims_consumed':0,'batch_executed':False,
            'quality_approval':False}
    outputs={prefix.with_name(prefix.name+'_eventos.csv'):ebytes,
             prefix.with_name(prefix.name+'_documentos.csv'):dbytes,
             prefix.with_suffix('.json'):(json.dumps(report,ensure_ascii=False,sort_keys=True,indent=2)+'\n').encode()}
    if any(x.exists() for x in outputs):
        raise SystemExit('ALTO: salida ya existe, no sobrescribimos evidencia; use directorio --out diferente')
    for p,data in outputs.items():
        with p.open('xb') as f:f.write(data)
    print('ESTADO: BORRADOR_LOCAL_NO_AUTORIZA')
    print('EVENTOS_CANDIDATOS:',len(events),'DOCUMENTOS_CANDIDATOS:',len(docs))
    print('PDF_HASH_TAMANO_BRONZE:',crossing['pdf_sha_size_matches'],'/990')
    print('RUTAS_HDFS_BRONZE:',crossing['origin_paths_matched'],'/1979')
    print('DIFERENCIAS_DE_CAMPOS:',sum(comp['diferencias_campos_comparables'].values()))
    print('INCIDENCIAS_PARA_REVISAR:',len(issues))
    print('MAPEO_PORTAL_ORIGEN: PENDIENTE_RATIFICACION')
    print('INFORME:',prefix.with_suffix('.json'))
    print('GCS:0 CLAIM:0 DATAPROC:0 GIT:SIN_CAMBIOS')

if __name__=='__main__':main()
