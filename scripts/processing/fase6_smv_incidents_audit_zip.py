#!/usr/bin/env python3
"""Analisis OFFLINE del ZIP aportado por Quality, SIN volver a leer GCS.
No autoriza publicar registros; solo calcula reparto y plan de cuarentena."""
import argparse,hashlib,json,zipfile
from pathlib import Path
from collections import Counter
from fase6_montos_sab_quarantine import OFFICIAL_EXPECTED

EXPECTED_ZIP='eabf85ec670e2661f08b016687556a0abf8d230aa54399961ca1abb02dc2eef9'


def audit_zip(path):
    raw=Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest()!=EXPECTED_ZIP:
        raise ValueError('ZIP_QUALITY_SHA256_NO_COINCIDE')
    with zipfile.ZipFile(path) as z:
        doc=json.loads(z.read('diagnostico_smv.json'))
        if doc.get('files_verified') !=1534 or len(doc.get('datasets',[]))!=3:
            raise ValueError('DIAGNOSTICO_NO_ESPERADO')
        d={i['dataset']:i for i in doc['datasets']}
        sab=d['smv/montos_sab']; vals=d['smv/valores_inscritos']; hechos=d['smv/hechos_valores_consulta']
        quarantine=[a for a in sab['objects'] if a.get('metadata',{}).get('ResponseCode')=='-2147024864']
        normal=[a for a in sab['objects'] if a.get('metadata',{}).get('ResponseCode')=='0']
        count=lambda items:sum(x['arrays']['Resultado'] for x in items)
        metrics=dict(files_bronze=len(sab['objects']),files_normal=len(normal),
                     files_quarantined=len(quarantine),bronze_rows=count(sab['objects']),
                     rows_normal=count(normal),rows_quarantined=count(quarantine))
        if any(metrics[k]!=v for k,v in OFFICIAL_EXPECTED.items()):
            raise ValueError('CONTEOS_SAB_DIFERENTES_DE_QUALITY')
        valid=lambda a: a.get('format')=='XML' and a.get('valor_obtenido_rows',0)==0 and a.get('codes')==['110'] and a.get('messages')==['Empresa no tiene valores inscritos en SMV']
        v110=[r for r in vals['xml_rejections_or_parse_errors'] if valid(r)]
        h110=[r for r in hechos['xml_rejections_or_parse_errors'] if valid(r)]
        if len(v110)!=94 or len(h110)!=1:raise ValueError('CODIGO_110_NO_COINCIDE')
        return {
           'sha256_zip_quality':EXPECTED_ZIP,
           'conclusiones':{
             'montos_sab':metrics,
             'valores_inscritos_110_no_registros':len(v110),
             'hechos_valores_consulta_110_no_registros':len(h110),
           },
           'cuarentena_montos_sab_14':[
               {'uri':x['uri'],'generation':x['generation'],'sha256':x['sha256'],
                'size_bytes':x['bytes'],'numero_registros':x['arrays']['Resultado'],
                'fecha_nominal':x['uri'].rsplit('/',1)[-1].replace('montos_sab_','').replace('.json','')}
                for x in quarantine],
           'advertencias':[
               'Este ZIP contiene reportes e inventarios; NO incluye los 1534 bytes originales Bronze',
               'Los 245 registros deben permanecer en cuarentena, sin homologar error como exito',
               'Publicacion de lote parcial o reprocesado requiere autorizacion expresa Quality',
           ]
        }


def main():
    p=argparse.ArgumentParser();p.add_argument('--zip',required=True);p.add_argument('--salida',required=True)
    args=p.parse_args()
    report=audit_zip(args.zip)
    output=Path(args.salida)
    if output.exists():raise SystemExit('ALTO: SALIDA_EXISTE_Y_NO_SE_SOBRESCRIBE')
    output.write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n',encoding='utf8')
    print('DIAGNOSTICO_ZIP_SHA256: CORRECTO')
    print('MONTOS_SAB: 1207 normales / 14 cuarentena; 21911 registros normales / 245 cuarentena')
    print('CODIGO_110: 94 Valores Inscritos / 1 Hechos Valores Consulta')
    print('ESTADO: CANDIDATO_PENDIENTE_QUALITY')
    print('INFORME:',output)

if __name__=='__main__':main()
