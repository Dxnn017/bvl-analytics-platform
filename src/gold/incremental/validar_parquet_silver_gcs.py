"""Validación física de Parquet Silver con cliente GCS inyectado.

Solo lectura. No habilita consumo productivo.
"""

import os
import tempfile
from pathlib import Path

from adaptador_gcs_quality import leer_objeto_gcs
from validar_parquet_fisico import validar_parquet_fisico
from contrato_silver_oficial import validar_referencia


class ParquetSilverGCSError(ValueError):
    pass


def validar_parquet_desde_gcs(
    cliente,
    referencia,
    tipos_esperados,
    claves,
    particiones,
):
    """Verifica integridad y contenido físico antes de aceptarlo."""

    import hashlib

    validar_referencia(referencia)

    if not referencia["uri"].startswith(
        "gs://bvl26-silver-7f3a9c/versions/"
    ) or not referencia["uri"].endswith(".parquet"):
        raise ParquetSilverGCSError("Ruta Parquet no autorizada")

    contenido = leer_objeto_gcs(
        cliente,
        referencia["uri"],
        referencia["generation"],
    )

    if len(contenido) != referencia["size_bytes"]:
        raise ParquetSilverGCSError("Tamaño incompatible")

    if hashlib.sha256(contenido).hexdigest() != referencia["sha256"]:
        raise ParquetSilverGCSError("SHA-256 incompatible")

    ruta = None

    try:
        with tempfile.NamedTemporaryFile(
            suffix=".parquet", delete=False
        ) as temporal:
            ruta = Path(temporal.name)
            temporal.write(contenido)

        resultado = validar_parquet_fisico(
            ruta,
            tipos_esperados,
            claves,
            particiones,
        )

        return {
            **resultado,
            "uri": referencia["uri"],
            "generation": referencia["generation"],
            "integridad_verificada": True,
            "consumo_productivo_autorizado": False,
        }

    finally:
        if ruta is not None:
            ruta.unlink(missing_ok=True)
