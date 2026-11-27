"""Escritura local protegida de candidatas Gold SAB.

No publica datos oficiales ni modifica Gold historico.
"""

from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from storage_config import (
    crear_configuracion_local,
    unir_uri,
)
from storage_backend import LocalStorageBackend


def escribir_candidata_gold(gold_candidato, producto):
    """Escribe una candidata nueva, sin sobrescribir versiones."""

    config = crear_configuracion_local()
    backend = LocalStorageBackend(config)

    particiones = {
        "actividad_sab_diaria": ["anio", "mes"],
        "empresa_trimestre": ["anio", "trimestre"],
        "hechos_empresa": ["anio", "mes"],
        "impacto_hechos_valor": ["anio", "mes"],
    }

    if producto not in particiones:
        raise ValueError("Producto Gold no soportado.")

    intento = uuid4().hex

    destino_uri = unir_uri(
        config.gold_candidatas,
        f"{producto}_local",
        f"attempt_id={intento}",
    )

    # Aplica las protecciones del backend antes de usar Spark.
    destino = backend.resolver_ruta(
        destino_uri,
        escritura=True,
    )

    if destino.exists() or destino.is_symlink():
        raise FileExistsError(
            f"La candidata ya existe: {destino}"
        )

    gold_candidato.write.mode(
        "errorifexists"
    ).partitionBy(
        *particiones[producto]
    ).parquet(str(destino))

    # Verificar que Spark finalizo la escritura.
    if not (destino / "_SUCCESS").is_file():
        raise RuntimeError(
            "La candidata no tiene marcador _SUCCESS."
        )

    return destino_uri



def escribir_candidata_sab(gold_candidato):
    """Mantiene compatibilidad con el ejecutor SAB."""
    return escribir_candidata_gold(
        gold_candidato, "actividad_sab_diaria"
    )
