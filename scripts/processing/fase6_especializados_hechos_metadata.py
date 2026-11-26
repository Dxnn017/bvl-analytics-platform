#!/usr/bin/env python3
"""BVL F6: bloqueo explicito de Hechos si falta fuente de metadata oficial.

Los CSV `hechos_eventos_2025.csv` y `hechos_documentos_2025.csv`
son dependencias HISTORICAS ubicadas en /datalake/bvl/metadata/,
fuera del inventario firmado de 1979 objetos Bronze.
No reconstruir 346 eventos a partir de PDFs/TXT sin fuente aprobada.
"""

class MetadataApprovalRequired(RuntimeError):
    pass

REQUIRED = ('hechos_eventos_2025.csv', 'hechos_documentos_2025.csv')


def require_official_metadata(metadata_refs):
    """No concede autorizacion: comprueba estructura para futura revision Quality."""
    if not isinstance(metadata_refs, dict) or set(metadata_refs) != set(REQUIRED):
        raise MetadataApprovalRequired('Metadatos historicos no acreditados por Quality')
    for filename, ref in metadata_refs.items():
        if not isinstance(ref, dict) or not all(ref.get(k) for k in (
                'uri','generation','sha256','approved_quality_reference')):
            raise MetadataApprovalRequired('Metadatos sin lineage y aprobacion independiente: '+filename)
        if not str(ref['uri']).startswith('gs://') or '/staging/' in ref['uri']:
            raise MetadataApprovalRequired('Metadatos sin fuente oficial: '+filename)
    return True


def execute_hechos_events(*args, **kwargs):
    raise MetadataApprovalRequired('BLOQUEADO: faltan fuentes oficiales/versionadas de metadata de Hechos')
