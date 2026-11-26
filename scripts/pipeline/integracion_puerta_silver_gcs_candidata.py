#!/usr/bin/env python3
"""BVL Fase 5: integra validaciones y lectura GCS SIN consumo ni Spark.

ATENCION: solo componente de preparacion/read-only. No contiene publicacion
Silver ni funcion de consumo. El contrato original bloquea toda autorizacion.
No ejecutar operaciones reales antes de ratificacion y visto bueno de Quality.
"""
from __future__ import annotations

import re
from datetime import datetime

import adaptador_gcs_silver_v1 as gcs
import puerta_silver_v1_candidata as puerta


class IntegrationError(ValueError):
    pass


def require(ok: bool, message: str) -> None:
    if not ok:
        raise IntegrationError(message)


def validate_context(contract: dict, dataset: str, run_id: str) -> dict:
    """Vincula valores externos a dataset/run del contrato, sin permisos."""
    require(isinstance(dataset, str) and dataset, 'Dataset requerido')
    require(isinstance(run_id, str) and run_id == contract['run_id'], 'Run ID no coincide')
    ds = next((d for d in contract['datasets'] if d['dataset'] == dataset), None)
    require(ds is not None, 'Dataset fuera del contrato')
    return ds


def inspect_bronze_manifest_readonly(client, raw_contract: bytes, trusted_contract_sha256: str,
                                     dataset: str, run_id: str) -> dict:
    """Comprueba un manifest oficial en GCS, sin verificar autorizacion Silver.

    Es solo una herramienta de auditoria. La consulta GCS requiere un cliente
    expresamente inyectado; nunca crea claims ni modifica staging. La verificacion
    individual de los objetos Bronze no forma parte de esta llamada.
    """
    contract = puerta.validate_contract(raw_contract, trusted_contract_sha256)
    ds = validate_context(contract, dataset, run_id)
    raw, entries = gcs.read_official_manifest(client, ds, puerta.verify_manifest)
    return {
        'estado': 'MANIFEST_BRONZE_CONSISTENTE_SOLO_LECTURA',
        'dataset': dataset,
        'run_id': run_id,
        'manifest_uri': ds['bronze_manifest_uri'],
        'manifest_sha256': puerta.sha256(raw),
        'files_count': len(entries),
        'total_bytes': sum(x['size_bytes'] for x in entries),
        'inputs_reales_verificados': False,
        'silver_autorizado': False,
    }


def inspect_signed_authorization_readonly(
        client, raw_contract: bytes, trusted_contract_sha256: str,
        dataset: str, run_id: str, *, auth_uri: str,
        auth_generation: int | str, auth_sha256: str,
        signature_uri: str, signature_generation: int | str, signature_sha256: str,
        trusted_public_key_pem: bytes, attempt_id: str, now: datetime | None = None) -> dict:
    """Inspecciona una autorizacion simple; no consume permisos ni inicia Spark.

    Falla ANTES de acceder a GCS mientras el contrato tenga Fase 5 abierta,
    autorizaciones no emitidas o mapeo no ratificado. Formato multiple NO soportado.
    La version del contrato Fase 5 recibido no habilita esta funcion.
    """
    c = puerta.validate_contract(raw_contract, trusted_contract_sha256)
    ds = validate_context(c, dataset, run_id)
    require(c['phase_5_closed'] is True, 'Fase 5 continua abierta')
    require(ds.get('mapping_ratified') is True, 'Mapeo no ratificado')
    require(c['authorization_protocol'].get('issued_authorizations'), 'Sin autorizacion emitida')
    # Proteccion adicional: se exige un nuevo contrato Quality que permita ejecucion.
    puerta.enforce_contract_execution_block(c)
    require(re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}', attempt_id) is not None,
            'ID de intento no valido')
    root = f"gs://{gcs.CONTROL_BUCKET}/authorizations/silver/{dataset}/run_id={run_id}/"
    require(auth_uri == root + 'authorization.json', 'URI de autorizacion fuera de alcance')
    require(signature_uri == root + 'authorization.sig', 'URI de firma fuera de alcance')
    # Todos los SHA y generaciones deben provenir de un contexto de lanzamiento
    # independiente y de confianza, NO de campos del JSON remoto.
    auth = gcs.read_signed_authorization_versioned(
        client, auth_uri=auth_uri, auth_generation=auth_generation,
        sig_uri=signature_uri, sig_generation=signature_generation,
        expected_auth_sha256=auth_sha256, expected_sig_sha256=signature_sha256,
        trusted_public_pem=trusted_public_key_pem,
        signature_validator=puerta.verify_signed_authorization)
    puerta.validate_simple_scope(auth, c, now=now)
    require(auth['dataset'] == dataset and auth['attempt_id'] == attempt_id,
            'Dataset/intento distinto al contexto confiable')
    _, entries = gcs.read_official_manifest(client, ds, puerta.verify_manifest)
    # Solo lectura: la comprobacion completa de los objetos requeriria una llamada
    # posterior a verify_official_inputs() con toda la autorizacion validada.
    return {
        'estado': 'AUTORIZACION_FIRMADA_Y_MANIFEST_LEIDOS_SIN_CONSUMO',
        'dataset': dataset, 'run_id': run_id, 'attempt_id': attempt_id,
        'manifest_files_count': len(entries),
        'inputs_reales_verificados': False,
        'claim_creado': False, 'spark_ejecutado': False,
    }
