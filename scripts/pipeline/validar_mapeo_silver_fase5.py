#!/usr/bin/env python3
"""Validacion offline, SOLO DOCUMENTAL del mapeo de Fase 5 BVL.

No valida autorizaciones, no consulta GCS, no ejecuta Spark ni edita contratos.
La salida consistente nunca equivale a aprobacion de Quality.
"""
import argparse
import json
import re
from pathlib import Path

import puerta_silver_v1_candidata as puerta


class MappingError(ValueError):
    pass


def require(ok, message):
    if not ok:
        raise MappingError(message)


def validate_mapping(contract, mapping):
    require(isinstance(mapping, dict), 'Mapeo debe ser objeto JSON')
    require(mapping.get('document_type') == 'PROPUESTA_MAPEO_NO_RATIFICADA', 'No debe simular un mapeo ratificado')
    require(mapping.get('contract_schema') == contract['schema_version'], 'Esquema contrato incorrecto')
    require(mapping.get('run_id') == contract['run_id'], 'run_id inconsistente')
    require(mapping.get('status') == 'PENDIENTE_DE_RATIFICACION_QUALITY', 'Estado documental incorrecto')
    require(mapping.get('derived_auth_route_ratified') is False, 'Ruta derivada no ratificada por Quality')
    require(contract.get('phase_5_closed') is False, 'Este verificador es SOLO Fase 5')
    require(contract.get('consumer', {}).get('spark_execution_allowed') is False, 'Contrato operativo no permitido aqui')
    official = {d['dataset']: d for d in contract['datasets']}
    sources = mapping.get('source_datasets')
    require(isinstance(sources, list) and len(sources) == len(official) == 12, 'Se esperan 12 fuentes')
    names = [x.get('bronze_dataset') for x in sources]
    require(len(set(names)) == len(names) and set(names) == set(official), 'Fuentes incompletas/repetidas')
    silver_bucket = contract['consumer']['silver_bucket']
    run_id = contract['run_id']
    products = []
    source_map = {}
    for row in sources:
        name = row['bronze_dataset']
        base = official[name]
        for field, expected in (
            ('files', base['files_count']),
            ('bronze_manifest_uri', base['bronze_manifest_uri']),
            ('bronze_manifest_generation', base['bronze_manifest_generation']),
            ('bronze_manifest_sha256', base['bronze_manifest_sha256']),
        ):
            require(row.get(field) == expected, f'{name}: {field} diferente de Quality')
        product = row.get('silver_product_proposed')
        require(isinstance(product, str) and re.fullmatch(r'[a-z_]+/[a-z_]+', product), f'{name}: producto invalido')
        if base.get('proposed_silver_product'):
            require(product == base['proposed_silver_product'], f'{name}: producto distinto al del contrato')
        expected_stage = f'gs://{silver_bucket}/staging/{product}/run_id={run_id}/attempt_id=<attempt_id>/'
        require(row.get('staging_prefix_proposed') == expected_stage, f'{name}: staging no exclusivo o incorrecto')
        require(row.get('mapping_status') == 'PENDIENTE_DE_RATIFICACION_QUALITY', f'{name}: falso estado ratificado')
        products.append(product)
        source_map[name] = product
    require(len(set(products)) == len(products), 'Productos Silver directos repetidos')
    require(sum(s['files'] for s in sources) == contract['bronze']['files_count'], 'Total archivos Bronze diferente')

    derived = mapping.get('derived_products')
    require(isinstance(derived, list) and len(derived) == 2, 'Se requieren los dos derivados documentados')
    dmap = {d.get('silver_product'): d for d in derived}
    require(len(dmap) == 2 and set(dmap) == {'referencias_smv/puente_ruc_nemonico', 'hechos_importancia/identidad_empresarial'}, 'Derivados desconocidos o repetidos')
    require(not set(dmap).intersection(products), 'Un derivado colisiona con Silver directo')
    bridge = dmap['referencias_smv/puente_ruc_nemonico']
    require(bridge.get('depends_on_bronze') == ['smv/valores_inscritos'], 'Puente: fuente Bronze distinta')
    require(bridge.get('depends_on_silver') == [], 'Puente no depende de Silver ya materializado')
    require(bridge.get('produced_together_with') == source_map['smv/valores_inscritos'], 'Puente: producto hermano incorrecto')
    require(bridge.get('potential_additional_bronze') == [], 'Puente: dependencia potencial sin resolver')
    require(bridge.get('status') == 'PROPUESTA_PENDIENTE_DE_RATIFICACION_QUALITY', 'Puente: estado incorrecto')
    identity = dmap['hechos_importancia/identidad_empresarial']
    required = {
        'hechos_importancia/eventos',
        'finanzas_empresariales/principales_cuentas',
        'referencias_smv/puente_ruc_nemonico',
        'mercado_diario/cotizaciones',
        'referencias_smv/hechos_valores_consulta',
    }
    deps = identity.get('depends_on_silver')
    require(isinstance(deps, list) and set(deps) == required and len(deps) == 5, 'Identidad: cinco dependencias incorrectas')
    require(identity.get('status') == 'PROPUESTA_PENDIENTE_DE_RATIFICACION_QUALITY', 'Identidad: estado incorrecto')

    # Validar el grafo entero (ausencia de referencias desconocidas/ciclos).
    graph = {p: [] for p in products}
    for name, d in dmap.items():
        graph[name] = d.get('depends_on_silver', [])
    visiting, visited = set(), set()
    def visit(node):
        require(node in graph, f'Dependencia Silver inexistente: {node}')
        require(node not in visiting, f'Ciclo en dependencias: {node}')
        if node in visited:
            return
        visiting.add(node)
        for dep in graph[node]:
            visit(dep)
        visiting.remove(node)
        visited.add(node)
    for product in graph:
        visit(product)

    return {'fuentes_bronze': len(sources), 'productos_silver_directos': len(products),
            'productos_derivados': len(derived), 'total_productos': len(graph),
            'archivos_bronze': sum(s['files'] for s in sources),
            'quality_ratificado': False, 'ejecucion_autorizada': False}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--contrato', type=Path, required=True)
    ap.add_argument('--sha256', required=True)
    ap.add_argument('--mapeo', type=Path, required=True)
    args = ap.parse_args()
    raw = args.contrato.read_bytes()
    contract = puerta.validate_contract(raw, args.sha256)
    data = puerta.load_strict(args.mapeo.read_bytes())
    require(data.get('contract_sha256') == args.sha256, 'Mapeo anclado a otro contrato')
    result = validate_mapping(contract, data)
    for k, v in result.items():
        print(f'{k.upper()}: {v}')
    print('RESULTADO: MAPEO_FASE5_COHERENTE_SIN_RATIFICACION')
    print('OPERACIONES_GCS: 0')
    print('SPARK_EJECUTADO: NO')

if __name__ == '__main__':
    main()
