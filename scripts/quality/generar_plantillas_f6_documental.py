#!/usr/bin/env python3
"""Prepara JSON canónicos SIN FIRMA para Quality: NO autoriza ni accede a GCP.

Las fuentes son el contrato oficial y el anexo/acta historicos con firmas
oficiales. El programa NO contiene claves privadas, NO emite firmas,
NO consume claims y NO puede ejecutar el launcher.
"""
from __future__ import annotations
import argparse
from datetime import timedelta
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'/'pipeline'))
import puerta_silver_operativa_v1_candidata as gate


def build_templates(contract_raw:bytes, history_ann_raw:bytes, history_ann_sig:bytes,
                    history_close_raw:bytes, history_close_sig:bytes,
                    public_pem:bytes, *, attempt_id:str, not_before_utc:str,
                    expires_at_utc:str, closed_at_utc:str):
    c=gate.check_base(contract_raw)  # SHA historico fijado, no se sustituye
    historical_annex=gate.validate_annex(
        gate.signed(history_ann_raw,history_ann_sig,public_pem), c)
    historical_closure=gate.validate_closure(
        gate.signed(history_close_raw,history_close_sig,public_pem),c,history_ann_raw)
    gate.must(historical_annex['schema_version']==gate.ANNEX_SCHEMA and
              historical_closure['schema_version']==gate.CLOSURE_SCHEMA,
              'Requiere anexos historicos originales firmados')
    gate.must(gate.RUN_PATTERN.fullmatch(attempt_id) is not None,
              'attempt_id invalido')
    start=gate.core.utc(not_before_utc)
    end=gate.core.utc(expires_at_utc)
    gate.must(timedelta(0)<end-start<=timedelta(hours=4),
              'Vigencia mayor a cuatro horas')
    gate.must(gate.core.utc(closed_at_utc)>=gate.core.utc(historical_closure['closed_at_utc']),
              'Acta documental anterior al cierre historico')
    ah=gate.digest(history_ann_raw); ch=gate.digest(history_close_raw)
    a=gate.expected_documental_annex(historical_annex,ah,ch)
    annex_raw=gate.canonical(a)
    x=gate.expected_documental_closure(historical_closure,annex_raw,ah,ch,closed_at_utc)
    closure_raw=gate.canonical(x)
    d=next((d for d in c['datasets'] if d['dataset']==gate.DOC_DATASET),None)
    gate.must(d is not None,'Manifest Hechos no existe en contrato base')
    y={
        'schema_version':gate.AUTH_SCHEMA,
        'status':'SILVER_AUTORIZADO',
        'mode':'SINGLE_SOURCE',
        'dataset':gate.DOC_DATASET,
        'silver_product':None,
        'documental_scope':gate.DOC_SCOPE,
        'run_id':c['run_id'],
        'project_id':c['project_id'],
        'region':c['region'],
        'base_contract_sha256':gate.CONTRACT_SHA256,
        'operation':'BUILD_SILVER_STAGING',
        'service_account':c['consumer']['service_account'],
        'attempt_id':attempt_id,
        'not_before_utc':not_before_utc,
        'expires_at_utc':expires_at_utc,
        'overwrite_allowed':False,
        'official_publication_allowed':False,
        'automatic_retry_allowed':False,
        'bronze_dependencies':[{'dataset':gate.DOC_DATASET,
             'manifest_uri':d['bronze_manifest_uri'],
             'manifest_generation':d['bronze_manifest_generation'],
             'manifest_sha256':d['bronze_manifest_sha256']}],
        'silver_dependencies':[],
        'silver_outputs':[{'name':gate.DOC_PRODUCT,
            'staging_prefix':gate.stage(gate.DOC_PRODUCT,c['run_id'],attempt_id)}],
        'operational_annex_sha256':gate.digest(annex_raw),
        'phase5_closure_sha256':gate.digest(closure_raw),
    }
    gate.validate_annex(a,c)
    gate.validate_closure(x,c,annex_raw)
    # Valida estructura con instante dentro de la ventana, no afirma vigencia real
    gate.validate_auth(y,c,a,annex_raw,closure_raw,now=start+timedelta(seconds=1))
    return {'operational_annex_documental.json':annex_raw,
            'closure_documental.json':closure_raw,
            'authorization.json':gate.canonical(y)}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for flag in ('contract','historical-annex','historical-annex-sig',
                 'historical-closure','historical-closure-sig',
                 'quality-public-pem','attempt-id','not-before-utc',
                 'expires-at-utc','closed-at-utc','output-dir'):
        parser.add_argument('--'+flag,required=True)
    a=parser.parse_args()
    files=build_templates(Path(a.contract).read_bytes(),
        Path(a.historical_annex).read_bytes(),
        Path(a.historical_annex_sig).read_bytes(),
        Path(a.historical_closure).read_bytes(),
        Path(a.historical_closure_sig).read_bytes(),
        Path(a.quality_public_pem).read_bytes(),
        attempt_id=a.attempt_id,not_before_utc=a.not_before_utc,
        expires_at_utc=a.expires_at_utc,closed_at_utc=a.closed_at_utc)
    target=Path(a.output_dir).expanduser().resolve()
    gate.must(not target.exists(), 'No sobrescribir entregas candidatas existentes')
    target.mkdir(parents=True)
    for name,data in files.items():
        (target/name).write_bytes(data)
        print(name,'SHA256='+gate.digest(data))
    print('TEMPLATES_NO_FIRMADOS_NO_AUTORIZAN',target)

if __name__=='__main__':main()
