#!/usr/bin/env python3
"""Integra 2 verificadores ya existentes en una PREPARACIÓN OFFLINE de Hechos.

No autentica publicaciones GCS, firmas, generaciones reales ni habilita Spark.
"""
import argparse
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DS = 'smv/hechos_valores_consulta'
JSON_FILE = 'hechos_empresas_valores_smv.json'
EXPECTED_FILES = 23
EXPECTED_BYTES = 19917


def _load_script(filename, module_name):
    full = ROOT / 'scripts' / 'pipeline' / filename
    spec = importlib.util.spec_from_file_location(module_name, full)
    if spec is None or spec.loader is None:
        raise ValueError('Falta verificador existente: ' + filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _in_root(path, root):
    p = Path(path)
    if not p.is_absolute():
        raise ValueError('Las rutas del mapa deben ser absolutas')
    if '..' in p.parts or p.is_symlink():
        raise ValueError('Ruta no permitida en snapshot')
    if not p.resolve().is_relative_to(root.resolve()):
        raise ValueError('Snapshot fuera de staging local')
    return p


def validar_preparacion(contrato, manifest, mapa, staging_root, bronze_json):
    """Valida SOLO borrador y bytes locales; nunca devuelve autorización."""
    preflight = _load_script('preflight_bronze_silver_offline.py', 'bvl_preflight_integrado')
    snapshot = _load_script('verificar_snapshot_bronze_silver_offline.py', 'bvl_snapshot_integrado')
    borrador = preflight.check(contrato)  # exige flags BORRADOR/NO AUTORIZADO
    if borrador['autoriza_silver'] is not False:
        raise ValueError('Estado de borrador inesperado')
    if manifest.get('dataset') != DS:
        raise ValueError('Dataset no corresponde a hechos_valores_consulta')
    records = manifest.get('files')
    if not isinstance(records, list) or len(records) != EXPECTED_FILES:
        raise ValueError('La preparación requiere 23 archivos')
    if sum(r.get('size_bytes', -1) for r in records if isinstance(r, dict)) != EXPECTED_BYTES:
        raise ValueError('Bytes del dataset incompatibles')
    json_records = [r for r in records if isinstance(r,dict) and r.get('uri','').endswith('/'+JSON_FILE)]
    xml_records = [r for r in records if isinstance(r,dict) and r.get('uri','').lower().endswith('.xml')]
    if len(json_records) != 1 or len(xml_records) != 22:
        raise ValueError('Se esperan 1 JSON consolidado y 22 XML')
    root=Path(staging_root)
    if not root.is_dir() or root.is_symlink():
        raise ValueError('Staging local invalido')
    json_path=_in_root(bronze_json, root)
    if not json_path.is_file() or json_path.name!=JSON_FILE:
        raise ValueError('JSON consolidado de staging ausente')
    if not isinstance(mapa,list) or len(mapa)!=EXPECTED_FILES:
        raise ValueError('Mapa incompleto')
    for entry in mapa:
        if not isinstance(entry,dict):
            raise ValueError('Entrada del mapa invalida')
        _in_root(entry.get('local_path',''),root)
    selected = [r for r in mapa if r.get('uri') == json_records[0]['uri']]
    if len(selected)!=1 or Path(selected[0]['local_path']).resolve()!=json_path.resolve():
        raise ValueError('JSON no coincide con mapeo')
    checked = snapshot.check(manifest,mapa)  # compara bytes contra manifest SUMINISTRADO
    try:
        with json_path.open('r',encoding='utf-8') as f:
            json.load(f)
    except (UnicodeError,json.JSONDecodeError) as e:
        raise ValueError('JSON consolidado inválido') from e
    return {
        'estado': 'ENTRADA_LOCAL_CONCUERDA_CON_MANIFEST_SUMINISTRADO',
        'dataset': DS,
        'archivos': checked['archivos'],
        'bytes_verificados': checked['bytes_verificados'],
        'json_consolidado': JSON_FILE,
        'estructura_negocio_json_validada': False,
        'conciliacion_semantica_xml_json': False,
        'contrato_oficial_autenticado': False,
        'generaciones_gcs_verificadas': False,
        'manifest_oficial_autenticado': False,
        'autoriza_silver': False,
        'ejecuta_spark': False,
        'operaciones_gcs': 0,
    }


def main(argv=None):
    p=argparse.ArgumentParser(description='PREPARACION SOLO LOCAL - NUNCA habilita Silver')
    p.add_argument('--contrato-borrador',required=True,type=Path)
    p.add_argument('--manifest-suministrado',required=True,type=Path)
    p.add_argument('--mapa-local',required=True,type=Path)
    p.add_argument('--staging-root',required=True,type=Path)
    p.add_argument('--bronze-json-local',required=True,type=Path)
    a=p.parse_args(argv)
    resultado=validar_preparacion(
        json.loads(a.contrato_borrador.read_text(encoding='utf-8')),
        json.loads(a.manifest_suministrado.read_text(encoding='utf-8')),
        json.loads(a.mapa_local.read_text(encoding='utf-8')),
        a.staging_root,a.bronze_json_local)
    print(json.dumps(resultado,ensure_ascii=False,indent=2))
    print('RESULTADO: PREFLIGHT_HECHOS_SIN_AUTORIZACION_SILVER')
    return 0

if __name__=='__main__':
    raise SystemExit(main())
