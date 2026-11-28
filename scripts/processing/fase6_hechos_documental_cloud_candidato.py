#!/usr/bin/env python3
"""Adapter for verified GCS Bronze objects -> restricted documentary rows only.

NOT a launcher. No credentials, no GCS writes, no claim, no permission to publish.
Caller must supply the official manifest files AFTER independent F5 KMS/GCS
preflight and Quality authorization of the NEW restricted documentary product.
Never sources company/expediente/official event date from historical CSV.
"""
from __future__ import annotations
from hashlib import sha256
from fase6_hechos_documental_restringido import (
    classify_official, prepare_document_index, verify_and_build, schema_spark, check,
    MAX_BYTES
)

BRONZE_BUCKET='bvl26-bronze-7f3a9c'


def validate_input(files):
    pdf,txt=classify_official(files)
    index=prepare_document_index(pdf,txt)
    return index


def build_verified_document_rows(files, *, read_exact):
    """read_exact(uri,generation,sha256,size_bytes) -> bytes; fail closed.

    For runtime use, bind read_exact to an exact GCS-generation reader AFTER
    the F5 verified plan; never bind it to an untrusted raw URI fetcher.
    """
    check(callable(read_exact),'LECTOR_EXACTO_OBLIGATORIO')
    index=validate_input(files)
    def _read(item):
        raw=read_exact(item['uri'],item['generation'],item['sha256'],item['tamano_bytes'])
        check(type(raw) is bytes and 0 < len(raw)<=MAX_BYTES,'BRONZE_GCS_BYTES_INVALIDOS')
        check(len(raw)==item['tamano_bytes'] and sha256(raw).hexdigest()==item['sha256'],
              'BRONZE_GCS_SHA_O_TAMANO_INCORRECTO')
        return raw
    rows,metrics=verify_and_build(index,read_hdfs=_read)
    check(len(rows)==990 and metrics['pdf_verificados']==990 and
          metrics['txt_unicos_verificados']==989 and
          metrics['duplicados_documentales_marcados']==1,
          'CONCILIACION_DOCUMENTAL_GCS_INVALIDA')
    check(all('empresa' not in k.lower() and 'expediente' not in k.lower()
              for x in rows for k in x),
          'ATRIBUTO_HISTORICO_NO_ACREDITADO')
    return rows,{
        **metrics,'scope':'HECHOS_DOCUMENTAL_RESTRINGIDO',
        'input_verified':'GCS_GENERATION_SHA256_SIZE_BY_CALLER',
        'official_event_identity_accredited':False,
        'quality_publication_approved':False,
    }


def make_dataframe(spark,rows):
    """Produce Spark dataframe; no write, no staging, no publishing."""
    check(type(rows) is list and len(rows)==990,'SILVER_DOCUMENTAL_NUMERO_INVALIDO')
    return spark.createDataFrame(rows,schema_spark())


def exact_reader_from_gcs(client):
    """Provider to use ONLY after signed F5 preflight and adapted authorizations.

    This helper performs GCS reads (never writes); not invoked by default.
    """
    import adaptador_gcs_silver_v1 as gcs
    def reader(uri,generation,sha,size):
        return gcs.read_exact_current_bytes(client,uri,generation,sha,
                   expected_size=size,allowed_bucket=BRONZE_BUCKET)
    return reader
