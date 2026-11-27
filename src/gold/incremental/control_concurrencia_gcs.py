"""Verifica que los current.json mantengan su generación vigente.

Cliente inyectado para pruebas locales. Sin escrituras GCS.
"""

import re


class ConcurrenciaGCSError(ValueError):
    pass


def verificar_generaciones_vigentes(cliente, referencias):
    if not isinstance(referencias, list) or not referencias:
        raise ConcurrenciaGCSError("Referencias ausentes")

    for ref in referencias:
        if not isinstance(ref, dict):
            raise ConcurrenciaGCSError("Referencia inválida")

        uri = ref.get("uri")
        generation = ref.get("generation")

        if not isinstance(uri, str) or not re.fullmatch(
            r"gs://bvl26-control-7f3a9c/"
            r"version_registry/silver/.+/current\.json",
            uri,
        ):
            raise ConcurrenciaGCSError("Ruta current.json inválida")

        if not isinstance(generation, str) or not re.fullmatch(
            r"[1-9][0-9]*", generation
        ):
            raise ConcurrenciaGCSError("Generación inválida")

        objeto = uri.removeprefix("gs://bvl26-control-7f3a9c/")

        try:
            blob = cliente.bucket(
                "bvl26-control-7f3a9c"
            ).blob(objeto)

            # Consulta la generación vigente, no la histórica.
            blob.reload()
            vigente = blob.generation
        except Exception as error:
            raise ConcurrenciaGCSError(
                "No se pudo comprobar el registry vigente"
            ) from error

        if vigente is None or int(vigente) != int(generation):
            raise ConcurrenciaGCSError(
                f"Registry modificado concurrentemente: {uri}"
            )

    return {
        "generaciones_vigentes": True,
        "registros_comprobados": len(referencias),
        "consumo_productivo_autorizado": False,
    }
