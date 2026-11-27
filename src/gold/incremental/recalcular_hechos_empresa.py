"""Recálculo incremental local de hechos_empresa.

No escribe archivos ni publica versiones oficiales.
"""

import sys
from pathlib import Path

from pyspark.sql import functions as F

DIRECTORIO_EVENTOS = Path(__file__).resolve().parent.parent / "eventos"
sys.path.insert(0, str(DIRECTORIO_EVENTOS))

from build_hechos_empresa import construir_gold, validar_clave


def recalcular_hechos_empresa(
    silver_eventos,
    silver_identidad,
    silver_documentos,
    silver_textos,
    gold_base,
    expedientes_afectados,
):
    """Reconstruye expedientes completos desde las cuatro fuentes Silver."""

    expedientes = (
        expedientes_afectados
        .select("numero_expediente")
        .distinct()
    )

    condicion_invalida = (
        F.col("numero_expediente").isNull()
        | (
            F.trim(
                F.col("numero_expediente").cast("string")
            ) == ""
        )
    )

    if expedientes.filter(condicion_invalida).limit(1).count():
        raise ValueError("Existen expedientes afectados invalidos.")

    eventos = silver_eventos.join(
        expedientes, "numero_expediente", "left_semi"
    )

    identidad = silver_identidad.join(
        expedientes, "numero_expediente", "left_semi"
    )

    documentos = silver_documentos.join(
        expedientes, "numero_expediente", "left_semi"
    )

    guids = documentos.select("guid_documento").distinct()

    textos = silver_textos.join(
        guids, "guid_documento", "left_semi"
    )

    validar_clave(eventos, ["numero_expediente"], "eventos afectados")
    validar_clave(identidad, ["numero_expediente"], "identidad afectada")
    validar_clave(documentos, ["guid_documento"], "documentos afectados")
    validar_clave(textos, ["guid_documento"], "textos afectados")

    gold_recalculado = construir_gold(
        eventos,
        identidad,
        documentos,
        textos,
    )

    gold_conservado = gold_base.join(
        expedientes,
        "numero_expediente",
        "left_anti",
    )

    gold_candidato = gold_conservado.unionByName(
        gold_recalculado
    )

    validar_clave(
        gold_candidato,
        ["numero_expediente"],
        "Gold hechos_empresa candidato",
    )

    return gold_candidato
