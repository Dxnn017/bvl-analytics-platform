"""Validacion local del registro Silver. Sin acceso a GCS."""

from scripts.pipeline.rutas_bvl import ruta

CAMPOS = (
    "schema_version",
    "dataset",
    "status",
    "current_version",
    "previous_version",
    "published_gcs_uri",
    "run_id",
    "approved_manifest_uri",
    "approved_at",
)


def validar_registro(datos, dataset):
    if not isinstance(datos, dict):
        raise ValueError("Registro JSON invalido")

    faltantes = [c for c in CAMPOS if c not in datos]
    if faltantes:
        raise ValueError(f"Campos faltantes: {faltantes}")

    if datos["dataset"] != dataset:
        raise ValueError("Dataset diferente al solicitado")

    # Estado provisional para las pruebas locales.
    # Quality debe confirmar el literal definitivo.
    if datos["status"] != "APROBADO":
        raise ValueError("Registro no aprobado")

    version = datos["current_version"]
    if (
        not isinstance(version, str)
        or not version
        or "/" in version
        or version in (".", "..")
    ):
        raise ValueError("Version invalida")

    for campo in ("schema_version", "run_id", "approved_at"):
        valor = datos[campo]
        if not isinstance(valor, str) or not valor.strip():
            raise ValueError(f"Campo invalido: {campo}")

    esperado = ruta(
        "silver",
        f"versions/{dataset}/version={version}",
        entorno="gcs"
    )

    publicada = datos["published_gcs_uri"]
    if not isinstance(publicada, str):
        raise ValueError("URI Silver invalida")

    if publicada.rstrip("/") != esperado:
        raise ValueError("La URI no coincide con la version oficial")

    prefijo_manifest = ruta(
        "control",
        "manifests/silver/approved",
        entorno="gcs"
    ) + "/"

    manifest = datos["approved_manifest_uri"]
    if (
        not isinstance(manifest, str)
        or not manifest.startswith(prefijo_manifest)
        or not manifest.endswith(".json")
    ):
        raise ValueError("Manifest aprobado invalido")

    return publicada


def obtener_registro_vigente_gcs(dataset):
    """Lee el puntero oficial usando la identidad de Dataproc."""
    import json
    from urllib.parse import urlsplit
    from google.cloud import storage

    uri = ruta(
        "control",
        f"version_registry/silver/{dataset}/current.json",
        entorno="gcs"
    )

    partes = urlsplit(uri)
    cliente = storage.Client()
    objeto = cliente.bucket(partes.netloc).blob(
        partes.path.lstrip("/")
    )

    objeto.reload()
    generacion = objeto.generation

    if generacion is None:
        raise ValueError("Registro sin generacion GCS")

    contenido = objeto.download_as_bytes(
        if_generation_match=int(generacion)
    )

    datos = json.loads(contenido.decode("utf-8"))
    version_aprobada = validar_registro(datos, dataset)

    return version_aprobada, generacion
