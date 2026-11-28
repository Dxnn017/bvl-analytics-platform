"""Launcher candidato de correccion Gold.

No concede permisos Quality.
No publica resultados.
La ejecucion real requiere autorizacion independiente.
"""

import argparse
import hashlib
import json
import sys
import zipfile

from pathlib import Path


def sha(datos):

    return hashlib.sha256(datos).hexdigest()


def exigir(condicion, mensaje):

    if not condicion:
        raise ValueError(mensaje)


def argumentos():

    parser = argparse.ArgumentParser(
        description="Launcher correctivo Gold"
    )

    modos = parser.add_mutually_exclusive_group(
        required=True
    )

    modos.add_argument(
        "--check-only",
        action="store_true"
    )

    modos.add_argument(
        "--execute-supervised",
        action="store_true"
    )

    parser.add_argument("--package")
    parser.add_argument("--inputs")
    parser.add_argument("--batch-id")
    parser.add_argument("--run-id")
    parser.add_argument("--attempt-id")

    return parser.parse_args()


def leer_local(raiz, nombre):

    ruta = raiz / nombre

    exigir(
        ruta.is_file()
        and not ruta.is_symlink()
        and ruta.resolve().is_relative_to(
            raiz.resolve()
        ),
        "Insumo local incompatible: " + nombre
    )

    return ruta.read_bytes()


def cargar_paquete(ruta):

    import zipimport

    exigir(
        ruta.is_file()
        and ruta.suffix == ".zip",
        "Paquete candidato ausente"
    )

    contenido = ruta.read_bytes()

    with zipfile.ZipFile(ruta) as z:

        nombres = z.namelist()

        exigir(
            len(nombres) == len(set(nombres)),
            "Entradas duplicadas en paquete"
        )

        exigir(
            z.testzip() is None,
            "Paquete corrupto"
        )

        metadata = json.loads(
            z.read("release_metadata.json")
        )

    import importlib.util

    ruta = ruta.resolve()

    sys.path.insert(0, str(ruta))

    cargador = zipimport.zipimporter(
        str(ruta)
    )

    # Todos los modulos planos del paquete deben
    # resolverse desde el artefacto acreditado.

    with zipfile.ZipFile(ruta) as z:

        modulos = [
            nombre[:-3]
            for nombre in z.namelist()
            if nombre.endswith(".py")
            and "/" not in nombre
        ]

    for modulo in modulos:

        exigir(
            modulo.isidentifier(),
            "Nombre de modulo incompatible"
        )

        esperado = cargador.find_spec(modulo)

        exigir(
            esperado is not None,
            "Modulo no resoluble desde ZIP"
        )

        efectivo = importlib.util.find_spec(modulo)

        exigir(
            efectivo is not None
            and efectivo.origin == esperado.origin,
            "Origen de modulo no acreditado: " + modulo
        )

    exigir(
        sha(ruta.read_bytes()) == sha(contenido),
        "Paquete modificado durante comprobacion"
    )

    for modulo in (
        "autorizacion_correctiva_quality",
        "preparar_candidatos_correctivos",
        "escritura_correctiva_staging",
        "ejecutor_portable",
        "integracion_quality_v03",
        "flujo_silver_gold_v03",
        "build_mercado_valor_diario",
        "build_actividad_sab_diaria",
        "build_empresa_trimestre",
    ):

        exigir(
            cargador.find_spec(modulo) is not None,
            "Modulo ausente del paquete: " + modulo
        )

    return {
        "package_sha256": sha(contenido),
        "processor_commit": metadata[
            "processor_commit"
        ],
        "launcher_sha256": sha(
            Path(__file__).read_bytes()
        ),
    }


def metadata_google(campo):

    from urllib.request import (
        Request,
        build_opener,
        ProxyHandler,
    )

    url = (
        "http://metadata.google.internal/"
        "computeMetadata/v1/" + campo
    )

    solicitud = Request(
        url,
        headers={
            "Metadata-Flavor": "Google"
        }
    )

    cliente = build_opener(
        ProxyHandler({})
    )

    with cliente.open(
        solicitud,
        timeout=5
    ) as respuesta:

        exigir(
            respuesta.headers.get(
                "Metadata-Flavor"
            ) == "Google",
            "Respuesta metadata incompatible"
        )

        return respuesta.read().decode().strip()


def verificar_contexto_cloud(auth):

    from google.auth.compute_engine import Credentials
    from google.auth.transport.requests import (
        AuthorizedSession,
    )

    from autorizacion_correctiva_quality import (
        PROYECTO,
        REGION,
        CUENTA,
    )

    proyecto = metadata_google(
        "project/project-id"
    )

    cuenta = metadata_google(
        "instance/service-accounts/default/email"
    )

    exigir(
        proyecto == PROYECTO
        and cuenta == CUENTA,
        "Identidad metadata incompatible"
    )

    credenciales = Credentials(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform"
        ]
    )

    nombre = (
        f"projects/{PROYECTO}/"
        f"locations/{REGION}/"
        f"batches/{auth['batch_id']}"
    )

    with AuthorizedSession(
        credenciales,
        max_refresh_attempts=0,
    ) as sesion:

        respuesta = sesion.get(
            "https://dataproc.googleapis.com/v1/"
            + nombre,
            timeout=15,
        )

        respuesta.raise_for_status()

        batch = respuesta.json()

    exigir(
        batch["name"] == nombre
        and batch["state"] == "RUNNING"
        and batch["environmentConfig"][
            "executionConfig"
        ]["serviceAccount"] == CUENTA,
        "Batch autorizado incompatible"
    )

    exigir(
        batch["pysparkBatch"].get(
            "args", []
        ) == sys.argv[1:],
        "Argumentos del batch incompatibles"
    )

    propiedades = batch.get(
        "runtimeConfig", {}
    ).get("properties", {})

    requeridas = {
        "spark.task.maxFailures": "1",
        "spark.stage.maxConsecutiveAttempts": "1",
        "spark.speculation": "false",
        "spark.dynamicAllocation.enabled": "false",
        "spark.driver.cores": "4",
        "spark.executor.cores": "4",
        "spark.executor.instances": "2",
        "spark.sql.session.timeZone": "UTC",
    }

    for clave, valor in requeridas.items():

        exigir(
            propiedades.get(clave) == valor,
            "Propiedad batch incompatible: " + clave
        )

    return credenciales, {
        "project_id": proyecto,
        "region": REGION,
        "consumer_service_account": cuenta,
        "batch_name": nombre,
        "batch_uuid": batch["uuid"],
        "runtime_properties": requeridas,
    }


def cargar_insumos(raiz):

    from autorizacion_correctiva_quality import FUENTES

    from preparar_candidatos_correctivos import ORDEN

    contenido = leer_local(
        raiz, "authorization.json"
    )

    firma = leer_local(
        raiz, "authorization.sig"
    )

    pem = leer_local(
        raiz, "quality_public.pem"
    )

    evidencias = {}

    for fuente in sorted(FUENTES):

        prefijo = (
            "sources/" +
            fuente.replace("/", "__")
        )

        evidencias[fuente] = {
            "autorizacion_bytes": leer_local(
                raiz,
                prefijo + "/authorization.json"
            ),
            "firma_autorizacion_der": leer_local(
                raiz,
                prefijo + "/authorization.sig"
            ),
            "contrato_fisico_bytes": leer_local(
                raiz,
                prefijo + "/physical_contract.json"
            ),
        }

    contratos = {

        producto: leer_local(
            raiz,
            "contracts/" + producto + ".json"
        )

        for producto in ORDEN
    }

    return contenido, firma, pem, evidencias, contratos


def main():

    args = argumentos()

    if args.check_only:

        exigir(
            not any((
                args.package,
                args.inputs,
                args.batch_id,
                args.run_id,
                args.attempt_id,
            )),
            "Check local no admite ejecucion"
        )

        print(json.dumps({
            "status": "LOCAL_CHECK_ONLY",
            "gcp_execution": False,
            "spark_execution": False,
            "publication_allowed": False,
        }))

        return

    exigir(
        all((
            args.package,
            args.inputs,
            args.batch_id,
            args.run_id,
            args.attempt_id,
        )),
        "Parametros supervisados incompletos"
    )

    artefactos = cargar_paquete(
        Path(args.package).resolve()
    )

    from autorizacion_correctiva_quality import (
        validar_autorizacion_correctiva,
        PROYECTO,
        REGION,
        CUENTA,
    )

    from preparar_candidatos_correctivos import (
        preparar_candidatos,
    )

    from escritura_correctiva_staging import (
        escribir_correccion_supervisada,
    )

    raiz = Path(args.inputs).resolve()

    (
        contenido,
        firma,
        pem,
        evidencias,
        contratos,
    ) = cargar_insumos(raiz)

    identidad = {
        **artefactos,
        "batch_id": args.batch_id,
        "run_id": args.run_id,
        "attempt_id": args.attempt_id,
        "project_id": PROYECTO,
        "region": REGION,
        "consumer_service_account": CUENTA,
    }

    auth = validar_autorizacion_correctiva(
        contenido,
        firma,
        pem,
        identidad,
    )

    credenciales, contexto = (
        verificar_contexto_cloud(auth)
    )

    identidad.update({
        campo: contexto[campo]
        for campo in (
            "project_id",
            "region",
            "consumer_service_account",
        )
    })

    validar_autorizacion_correctiva(
        contenido,
        firma,
        pem,
        identidad,
    )

    # A partir de este punto existe una autorizacion
    # correctiva firmada y un contexto cloud comprobado.
    # Quality debe acreditar adicionalmente la asociacion
    # efectiva del proceso con el batch supervisado.

    from google.cloud import storage
    from pyspark.sql import SparkSession

    cliente = storage.Client(
        project=PROYECTO,
        credentials=credenciales,
    )

    spark = None

    try:

        spark = (
            SparkSession.builder
            .appName("BVL_Gold_Corrective_Supervised")
            .config(
                "spark.sql.session.timeZone",
                "UTC"
            )
            .getOrCreate()
        )

        conf = spark.sparkContext.getConf()

        for clave, valor in contexto[
            "runtime_properties"
        ].items():

            observado = (
                spark.conf.get(clave)
                if clave.startswith("spark.sql.")
                else conf.get(clave, "")
            )

            exigir(
                observado == valor,
                "Configuracion efectiva incompatible: "
                + clave
            )

        preparado = preparar_candidatos(
            spark,
            cliente,
            contenido,
            firma,
            pem,
            identidad,
            evidencias,
            contratos,
        )

        preparado["runtime_context"] = contexto

        preparado["spark_application_id"] = (
            spark.sparkContext.applicationId
        )

        resultado = escribir_correccion_supervisada(
            spark,
            cliente,
            preparado,
        )

        print(json.dumps(
            resultado,
            sort_keys=True,
            indent=2,
        ))

    finally:

        if spark is not None:
            spark.stop()

        cliente.close()


if __name__ == "__main__":

    main()
