#!/usr/bin/env python3
"""Auditoría candidata de 346 eventos (319 con PDF + 27 sin PDF).

NO declara que los 27 eventos estén verificados desde Bronze, NO autoriza ejecución
Silver, NO modifica Git/GCS/HDFS ni autoriza claims. Separa las referencias históricas
sin PDF como CUARENTENA para revisión independiente por Quality.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import json

from BVL_F6_CRUCE_HECHOS_BRONZE import (
    BASE, PDF, TXT, GEN, hdfs_cat, hdfs_list,
    official_manifest, csv_rows, historical_evidence, cross_match
)
from BVL_F6_HECHOS_RECON_CANDIDATA import (
    reconstruct, compare_reference, csv_bytes, sha, normalized
)

SCHEMA = 'BVL-HECHOS-RECON-AUDIT-v2'
EVENT_FIELDS = [
    'numero_expediente', 'empresa', 'fecha_presentacion',
    'fecha_consulta_portal', 'coherencia_temporal', 'usar_silver_principal',
    'cantidad_documentos_pdf', 'cantidad_documentos_portal', 'fecha_acuerdo',
    'fuente_campos', 'estado_trazabilidad'
]
DOC_FIELDS = [
    'guid_documento', 'numero_expediente', 'archivo_pdf', 'archivo_canonico',
    'empresa', 'fecha_presentacion', 'fecha_consulta_portal', 'url_documento',
    'coherencia_temporal', 'usar_silver_principal', 'fecha_archivo',
    'tamano_bytes', 'sha256', 'duplicado', 'estado_extraccion',
    'caracteres_extraidos', 'ruta_txt', 'bronze_pdf_uri',
    'bronze_pdf_generation', 'bronze_pdf_sha256', 'bronze_pdf_source_hdfs',
    'bronze_txt_uri', 'bronze_txt_generation', 'bronze_txt_sha256',
    'bronze_txt_source_hdfs', 'fuente_texto',
    'origen_campos_portal', 'origen_pdf_txt'
]


def reconcile_unlinked_events(linked_events, reference_events, expected_missing=27):
    """Reconciliación de IDENTIFICADORES; no reconstrucción de metadatos.

    Las filas históricas sin PDF sirven para identificar faltantes; NO se copian
    sus atributos empresariales, fechas, clasificaciones ni URLs a la salida.
    """
    keys = [normalized(r.get('numero_expediente')) for r in reference_events]
    if any(not x for x in keys) or len(keys) != len(set(keys)):
        raise ValueError('REFERENCIA_HISTORICA_CLAVES_DUPLICADAS_O_VACIAS')
    existing = [normalized(r.get('numero_expediente')) for r in linked_events]
    if any(not x for x in existing) or len(existing) != len(set(existing)):
        raise ValueError('DERIVADOS_CON_PDF_CLAVES_DUPLICADAS_O_VACIAS')
    extra = set(existing) - set(keys)
    if extra:
        raise ValueError('EXPEDIENTES_CON_PDF_AJENOS_A_REFERENCIA')
    missing = sorted(set(keys) - set(existing))
    if len(missing) != expected_missing:
        raise ValueError(f'CANTIDAD_SIN_PDF_NO_ESPERADA: {len(missing)} != {expected_missing}')
    linked=[]
    for r in linked_events:
        x=dict(r)
        x['estado_trazabilidad']='VINCULADO_PDF_BRONZE_MAPEO_HDFS_PENDIENTE_RATIFICACION'
        linked.append(x)
    isolated=[]
    for key in missing:
        x={name:'' for name in EVENT_FIELDS}
        x.update({
            'numero_expediente':key,
            'cantidad_documentos_pdf':'0',
            'fuente_campos':'SOLO_ID_REFERENCIA_HDFS_HISTORICA_NO_BRONZE',
            'estado_trazabilidad':'CUARENTENA_SIN_PDF_ORIGEN_EVENTO_NO_ACREDITADO',
        })
        isolated.append(x)
    return sorted(linked+isolated, key=lambda x:x['numero_expediente']), isolated


def reconcile_counts(events,docs,isolated,reference_events,reference_docs):
    if len(events)!=346 or len(docs)!=990 or len(isolated)!=27:
        raise ValueError('CONTEOS_FINALES_INESPERADOS')
    if len(reference_events)!=346 or len(reference_docs)!=990:
        raise ValueError('CONTEOS_REFERENCIA_INESPERADOS')
    isolated_keys={r['numero_expediente'] for r in isolated}
    if any(r['numero_expediente'] in isolated_keys for r in docs):
        raise ValueError('SIN_PDF_TIENE_DOCUMENTO')
    doc_counts=Counter(x['numero_expediente'] for x in docs)
    for r in events:
        if int(r['cantidad_documentos_pdf'])!=doc_counts.get(r['numero_expediente'],0):
            raise ValueError('CONTEO_PDF_POR_EXPEDIENTE_INCONSISTENTE')
    return True


def build(handoff, out):
    bronze, manifest_sha=official_manifest(handoff)
    data={}
    for name in ('manifiesto_pdfs_hechos_importancia.csv','mapeo_pdf_expediente.csv',
                 'hechos_eventos_2025.csv','hechos_documentos_2025.csv'):
        raw=hdfs_cat(BASE+'/'+name)
        cols,rows=csv_rows(raw)
        data[name]={'rows':rows,'sha256':sha(raw),'bytes':len(raw),'cols':cols}
    m_pdf=data['manifiesto_pdfs_hechos_importancia.csv']['rows']
    mapping=data['mapeo_pdf_expediente.csv']['rows']
    history_events=data['hechos_eventos_2025.csv']['rows']
    history_docs=data['hechos_documentos_2025.csv']['rows']
    pdfs=hdfs_list(PDF,'.pdf');txts=hdfs_list(TXT,'.txt')
    history_index=historical_evidence(m_pdf,history_docs,pdfs,txts)
    crossing=cross_match(history_index,pdfs,txts,bronze)
    if not crossing['recorded_hashes_match']:
        raise ValueError('BRONZE_HDFS_CROSS_NO_COINCIDE')
    linked, docs, issues = reconstruct(m_pdf,mapping,bronze,pdfs,txts,strict_counts=False)
    events, isolated = reconcile_unlinked_events(linked,history_events)
    reconcile_counts(events,docs,isolated,history_events,history_docs)
    comparison=compare_reference(events,docs,history_events,history_docs)
    ebytes=csv_bytes(events,EVENT_FIELDS)
    qbytes=csv_bytes(isolated,EVENT_FIELDS)
    dbytes=csv_bytes(docs,DOC_FIELDS)
    report={
        'schema_version':SCHEMA,'generated_at_utc':datetime.now(timezone.utc).isoformat(),
        'estado':'EVIDENCIA_LOCAL_EN_CUARENTENA_NO_AUTORIZA',
        'quality_approved':False,'cloud_execution_allowed':False,
        'authorizations_consumed':0,'gcs_writes':0,
        'bronze_manifest_generation':GEN,'bronze_manifest_sha256':manifest_sha,
        'input_sha256_hdfs':{k:v['sha256'] for k,v in data.items()},
        'hdfs_bronze_metadata_cross':crossing,
        'counts':{'events_audit_total':len(events),'events_linked_pdf':len(linked),
                  'events_isolated_no_pdf':len(isolated),'docs':len(docs),
                  'hdfs_pdf':len(pdfs),'hdfs_txt':len(txts)},
        'missing_expediente_keys': [r['numero_expediente'] for r in isolated],
        'historical_comparison':comparison,'issues':issues,
        'input_provenance_warning':'Los 27 ID proceden exclusivamente del CSV historico HDFS, NO de objetos Bronze. Su procedencia exige aprobacion separada de Quality.',
        'linked_provenance_warning':'El mapeo GUID↔expediente es un auxiliar HDFS pendiente de ratificacion.',
        'source_policy':'No copiamos campos historicos de los 27 eventos sin PDF: solo ID para conciliacion; los metadatos se dejan vacios.',
        'output_sha256':{'eventos_auditoria':sha(ebytes),'eventos_sin_pdf_cuarentena':sha(qbytes),'documentos':sha(dbytes)},
    }
    out.mkdir(parents=True,exist_ok=True)
    file_bytes={
        out/'BVL_F6_HECHOS_EVENTOS_AUDITORIA_346_NO_PUBLICAR.csv':ebytes,
        out/'BVL_F6_HECHOS_SIN_PDF_27_CUARENTENA.csv':qbytes,
        out/'BVL_F6_HECHOS_DOCUMENTOS_990_CANDIDATOS.csv':dbytes,
        out/'BVL_F6_HECHOS_RECONCILIACION_V2.json':
            (json.dumps(report,ensure_ascii=False,indent=2,sort_keys=True)+'\n').encode('utf8'),
    }
    # Evitar una escritura parcial si alguno de los destinos existe.
    collisions=[str(p) for p in file_bytes if p.exists()]
    if collisions: raise ValueError('DESTINO_EXISTENTE_NO_SOBRESCRIBIR: '+','.join(collisions))
    for file, content in file_bytes.items():
        with file.open('xb') as f:f.write(content)
    return report, list(file_bytes)


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--handoff', type=Path, required=True)
    ap.add_argument('--out',type=Path,required=True)
    args=ap.parse_args()
    report, outputs=build(args.handoff.expanduser(),args.out.expanduser())
    print('EVENTOS_AUDITORIA:',report['counts']['events_audit_total'])
    print('EVENTOS_CON_PDF:',report['counts']['events_linked_pdf'])
    print('EVENTOS_SIN_PDF_CUARENTENA:',report['counts']['events_isolated_no_pdf'])
    print('DOCUMENTOS:',report['counts']['docs'])
    print('BRONZE_METADATA_PDF_COINCIDE:',report['hdfs_bronze_metadata_cross']['pdf_sha_size_matches'],'/990')
    print('BRONZE_RUTAS_COINCIDEN:',report['hdfs_bronze_metadata_cross']['origin_paths_matched'],'/1979')
    print('INCIDENCIAS_CAMPO:',len(report['issues']))
    print('ARCHIVOS_GENERADOS:',len(outputs))
    print('ESTADO:',report['estado'])
    print('GCS:0 CLAIM:0 DATAPROC:0 GIT:SIN_CAMBIOS')

if __name__=='__main__':main()
