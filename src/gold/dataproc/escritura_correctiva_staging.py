"""Escritura candidata supervisada en staging Gold.

No publica versiones, no modifica registros oficiales,
no elimina objetos y no reintenta operaciones GCS.
"""

import hashlib
import json
import re
import tempfile

from datetime import datetime, timezone
from pathlib import Path

from autorizacion_correctiva_quality import (
    BUCKET,
    exigir,
)

from preparar_candidatos_correctivos import (
    ORDEN,
    PARTICIONES,
    CONTEOS,
)

from ejecutor_portable import validar_claves

from adaptador_numerico_integral import (
    REGLAS,
    POLITICA,
)

from autorizacion_correctiva_quality import (
    validar_decision_numerica,
)


EXCEPCIONES = {
    "finanzas_ruc_no_conciliado": 85,
    "finanzas_ruc_valido_ambiguo": 6,
}

MAX_BYTES_SALIDA = 512 * 1024 * 1024


def ahora_utc():

    return datetime.now(timezone.utc).isoformat()


def json_bytes(documento):

    return (
        json.dumps(
            documento,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        ) + "\n"
    ).encode("utf-8")


def sha_archivo(ruta):

    digest = hashlib.sha256()

    with ruta.open("rb") as archivo:

        for bloque in iter(
            lambda: archivo.read(1024 * 1024),
            b"",
        ):
            digest.update(bloque)

    return digest.hexdigest()


def validar_destino(auth):

    for campo in (
        "batch_id",
        "run_id",
        "attempt_id",
    ):

        exigir(
            isinstance(auth[campo], str)
            and re.fullmatch(
                r"[a-z0-9][a-z0-9_-]{0,127}",
                auth[campo]
            ),
            "Identificador de destino invalido"
        )

    prefijo = (
        "staging/correccion_fase6/"
        f"batch_id={auth['batch_id']}/"
        f"run_id={auth['run_id']}/"
        f"attempt_id={auth['attempt_id']}/"
    )

    exigir(
        auth["staging_uri"]
        == f"gs://{BUCKET}/{prefijo}",
        "Destino de escritura incompatible"
    )

    exigir(
        auth["staging_write_allowed"] is True
        and auth["quality_approved"] is True
        and auth["publication_allowed"] is False,
        "Alcance de escritura incompatible"
    )

    return prefijo


def validar_nombre_objeto(nombre):

    exigir(
        isinstance(nombre, str)
        and not nombre.startswith("/")
        and "\\" not in nombre
        and all(
            segmento not in {"", ".", ".."}
            for segmento in nombre.split("/")
        ),
        "Nombre de objeto inseguro"
    )


class EscritorStagingExclusivo:

    def __init__(self, cliente, auth, comprobar):

        self.cliente = cliente
        self.auth = auth
        self.comprobar = comprobar

        self.prefijo = validar_destino(auth)

        self.bucket = cliente.bucket(BUCKET)

        self.inventario = []

    def comprobar_prefijo_nuevo(self):

        self.comprobar()

        existentes = self.cliente.list_blobs(
            BUCKET,
            prefix=self.prefijo,
            max_results=1,
            retry=None,
        )

        exigir(
            next(iter(existentes), None) is None,
            "STAGING_EXISTENTE_NO_REUTILIZABLE"
        )

    def _verificar_objeto(
        self, nombre, generation,
        sha_esperado, bytes_esperados,
    ):

        exigir(
            generation is not None
            and int(generation) > 0,
            "Generation de salida ausente"
        )

        exacto = self.bucket.blob(
            nombre,
            generation=int(generation)
        )

        contenido = exacto.download_as_bytes(
            if_generation_match=int(generation),
            checksum="crc32c",
            retry=None,
        )

        exigir(
            len(contenido) == bytes_esperados
            and hashlib.sha256(
                contenido
            ).hexdigest() == sha_esperado,
            "Integridad remota incompatible"
        )

        actual = self.bucket.blob(nombre)

        actual.reload(retry=None)

        exigir(
            str(actual.generation) == str(generation),
            "Generation vigente de salida incompatible"
        )

        referencia = {
            "uri": f"gs://{BUCKET}/{nombre}",
            "generation": str(generation),
            "sha256": sha_esperado,
            "size_bytes": bytes_esperados,
        }

        self.inventario.append(referencia)

        return referencia

    def escribir_bytes(self, relativo, contenido):

        validar_nombre_objeto(relativo)

        self.comprobar()

        nombre = self.prefijo + relativo

        blob = self.bucket.blob(nombre)

        blob.upload_from_string(
            contenido,
            content_type="application/json",
            if_generation_match=0,
            checksum=None,
            retry=None,
        )

        return self._verificar_objeto(
            nombre,
            blob.generation,
            hashlib.sha256(contenido).hexdigest(),
            len(contenido),
        )

    def escribir_archivo(self, relativo, ruta):

        validar_nombre_objeto(relativo)

        self.comprobar()

        nombre = self.prefijo + relativo

        huella = sha_archivo(ruta)

        tamano = ruta.stat().st_size

        blob = self.bucket.blob(nombre)

        blob.upload_from_filename(
            str(ruta),
            content_type="application/octet-stream",
            if_generation_match=0,
            checksum=None,
            retry=None,
        )

        return self._verificar_objeto(
            nombre,
            blob.generation,
            huella,
            tamano,
        )

    def comprobar_inventario(self):

        self.comprobar()

        registrados = {
            entrada["uri"]: entrada["generation"]
            for entrada in self.inventario
        }

        exigir(
            len(registrados) == len(self.inventario),
            "Objetos duplicados en inventario"
        )

        observados = {}

        for blob in self.cliente.list_blobs(
            BUCKET,
            prefix=self.prefijo,
            retry=None,
        ):

            uri = f"gs://{BUCKET}/{blob.name}"

            observados[uri] = str(blob.generation)

        exigir(
            registrados == observados,
            "Inventario staging incompatible"
        )


def tipos_dataframe(df):

    return {
        campo.name: campo.dataType.simpleString()
        for campo in df.schema.fields
    }


def materializar_parquet_local(
    spark, df, producto, destino,
    particiones, cantidad_esperada,
):
    """Materializacion acotada por lotes en el driver."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    from collections import defaultdict
    from itertools import islice
    from pyspark.sql.pandas.types import to_arrow_schema

    exigir(
        not destino.exists(),
        "Destino temporal existente"
    )

    validar_claves(df, producto)

    cantidad = df.count()

    exigir(
        cantidad == cantidad_esperada,
        "Conteo previo incompatible"
    )

    esquema = to_arrow_schema(df.schema)

    columnas_fisicas = [
        nombre for nombre in df.columns
        if nombre not in particiones
    ]

    esquema_fisico = pa.schema([
        esquema.field(nombre)
        for nombre in columnas_fisicas
    ])

    destino.mkdir(parents=True, exist_ok=False)

    ordenado = (
        df.orderBy(*particiones)
        if particiones else df
    )

    iterador = ordenado.toLocalIterator(
        prefetchPartitions=False
    )

    inventario = []
    total_bytes = 0
    total_filas = 0
    secuencia = 0

    while True:

        lote = list(islice(iterador, 2048))

        if not lote:
            break

        grupos = defaultdict(list)

        for fila in lote:

            datos = fila.asDict(recursive=True)

            clave = tuple(
                datos[nombre]
                for nombre in particiones
            )

            exigir(
                all(
                    type(valor) is int
                    for valor in clave
                ),
                "Particion temporal invalida"
            )

            grupos[clave].append(datos)

        for clave, filas in grupos.items():

            carpeta = destino

            for nombre, valor in zip(
                particiones, clave
            ):

                carpeta = carpeta / (
                    f"{nombre}={valor}"
                )

            carpeta.mkdir(
                parents=True,
                exist_ok=True
            )

            ruta = carpeta / (
                f"part-{secuencia:06d}.parquet"
            )

            exigir(
                not ruta.exists(),
                "Archivo temporal existente"
            )

            original = pa.Table.from_pylist(
                filas,
                schema=esquema
            )

            fisico = original.select(
                columnas_fisicas
            )

            pq.write_table(
                fisico,
                str(ruta),
                compression="snappy",
            )

            restaurado = pq.ParquetFile(
                ruta
            ).read()

            exigir(
                restaurado.schema.equals(
                    esquema_fisico,
                    check_metadata=False
                ),
                "Esquema Parquet fisico incompatible"
            )

            exigir(
                fisico.equals(
                    restaurado,
                    check_metadata=False
                ),
                "Contenido Parquet incompatible"
            )

            for nombre, valor in zip(
                particiones, clave
            ):

                restaurado = restaurado.append_column(
                    esquema.field(nombre),
                    pa.array(
                        [valor] * restaurado.num_rows,
                        type=esquema.field(nombre).type,
                    )
                )

            restaurado = restaurado.select(
                df.columns
            )

            exigir(
                original.equals(
                    restaurado,
                    check_metadata=False
                ),
                "Contenido reconstruido incompatible"
            )

            tamano = ruta.stat().st_size

            total_bytes += tamano
            total_filas += restaurado.num_rows

            exigir(
                total_bytes <= MAX_BYTES_SALIDA,
                "Limite temporal excedido"
            )

            inventario.append({
                "ruta": ruta,
                "relativo": ruta.relative_to(
                    destino
                ).as_posix(),
                "records_count":
                    restaurado.num_rows,
                "size_bytes": tamano,
            })

            secuencia += 1

    exigir(
        bool(inventario)
        and total_filas == cantidad,
        "Materializacion temporal incompleta"
    )

    return {
        "records_count": cantidad,
        "column_count": len(df.columns),
        "schema_spark": df.schema.jsonValue(),
        "partition_columns": particiones,
        "content_roundtrip_verified": True,
        "materialization": "DRIVER_PYARROW",
        "files": inventario,
    }


def validar_evidencia_integral(preparado):

    auth = preparado["autorizacion"]

    decision = validar_decision_numerica(auth)

    evidencias = preparado.get("numeric_adaptations")

    exigir(
        isinstance(evidencias, dict)
        and set(evidencias) == set(REGLAS),
        "Evidencia numerica incompleta"
    )

    conteos_fuentes = {
        "mercado_diario/cotizaciones": 83130,
        "mercado_diario/montos_sab": 21911,
    }

    for fuente, reglas in REGLAS.items():

        evidencia = evidencias[fuente]
        referencia = decision["sources"][fuente]

        exigir(
            evidencia["status"] == "APPLIED"
            and evidencia["product"] == fuente
            and evidencia["policy"] == POLITICA
            and evidencia["authorization_id"]
            == auth["authorization_id"]
            and evidencia["decision_id"]
            == decision["decision_id"]
            and evidencia["version_id"]
            == referencia["version_id"]
            and evidencia["physical_contract_sha256"]
            == referencia["physical_contract_sha256"]
            and evidencia["input_records"]
            == conteos_fuentes[fuente]
            and set(evidencia["columns"]) == set(reglas)
            and evidencia["silver_modified"] is False
            and evidencia["constructor_modified"] is False
            and evidencia["publication_allowed"] is False
            and evidencia["rounding_monetary_allowed"] is False
            and evidencia["invalid_replacement_allowed"] is False,
            "Evidencia numerica incompatible"
        )

        for columna, tipo in reglas.items():

            campo = evidencia["columns"][columna]

            exigir(
                campo["input_type"] == "string"
                and campo["output_type"] == tipo
                and type(campo["converted_records"]) is int
                and campo["converted_records"] >= 0
                and type(campo["original_nulls"]) is int
                and campo["original_nulls"] >= 0
                and type(campo["invalid_records"]) is int
                and campo["invalid_records"] == 0
                and campo["converted_records"]
                + campo["original_nulls"]
                == evidencia["input_records"],
                "Conteo de conversion incompatible"
            )

    cobertura = preparado.get("sab_coverage_evidence")

    referencia = auth["sab_coverage"]

    if referencia is None:

        exigir(
            cobertura is None
            and preparado["cobertura_acreditada"] is False,
            "Cobertura no autorizada"
        )

    else:

        exigir(
            isinstance(cobertura, dict)
            and preparado["cobertura_acreditada"] is True
            and cobertura["policy"] == referencia["policy"]
            and cobertura["status"]
            == "DOCUMENTARY_RATIFICATION_VERIFIED"
            and cobertura["product"]
            == "mercado_diario/montos_sab"
            and cobertura["version_id"]
            == referencia["version_id"]
            and cobertura["sidecar_sha256"]
            == referencia["payload"]["sha256"]
            and cobertura["sidecar_signature_sha256"]
            == referencia["signature"]["sha256"]
            and cobertura["ratification_sha256"]
            == referencia["ratification"]["sha256"]
            and cobertura["ratification_signature_sha256"]
            == referencia["ratification_signature"]["sha256"]
            and cobertura["eligible_records"] == 21911
            and cobertura["coverage"] == "DESCONOCIDA"
            and cobertura["full_coverage_attested"] is False
            and cobertura[
                "daily_counts_verified_against_dataframe"
            ] is True
            and cobertura["production_authorized"] is False
            and cobertura["publication_allowed"] is False,
            "Evidencia integral SAB incompatible"
        )

    return evidencias, cobertura


def escribir_correccion_supervisada(
    spark, cliente, preparado,
):

    auth = preparado["autorizacion"]

    comprobar = preparado["verificar_vigencia"]

    exigir(
        preparado["publication_allowed"] is False,
        "Preparacion con alcance incompatible"
    )

    candidatos = preparado["candidatos"]
    excepciones = preparado["excepciones"]

    evidencias_numericas, evidencia_cobertura = (
        validar_evidencia_integral(preparado)
    )

    adaptacion = preparado.get(
        "cotizaciones_adaptation"
    )

    exigir(
        isinstance(adaptacion, dict)
        and adaptacion.get("status") == "APPLIED"
        and adaptacion.get("decision")
        == auth.get("cotizaciones_adaptation")
        and adaptacion.get("version_id")
        == auth["sources"][
            "mercado_diario/cotizaciones"
        ]["version_id"]
        and adaptacion.get("authorization_id")
        == auth["authorization_id"],
        "Evidencia de adaptacion no acreditada"
    )

    exigir(
        set(candidatos) == set(ORDEN)
        and set(excepciones) == set(EXCEPCIONES),
        "Conjunto de salidas incompleto"
    )

    comprobar()

    escritor = EscritorStagingExclusivo(
        cliente,
        auth,
        comprobar,
    )

    salidas = {}

    with tempfile.TemporaryDirectory(
        prefix="bvl-gold-corrective-"
    ) as temporal:

        raiz = Path(temporal)

        # Preparar y validar todas las salidas localmente
        # antes de crear el primer objeto en GCS.

        for producto in ORDEN:

            comprobar()

            relativo = "candidatos/" + producto

            salidas[relativo] = (
                materializar_parquet_local(
                    spark,
                    candidatos[producto],
                    producto,
                    raiz / relativo,
                    PARTICIONES[producto],
                    CONTEOS[producto],
                )
            )

        for nombre, df in excepciones.items():

            comprobar()

            relativo = "calidad/" + nombre

            salidas[relativo] = (
                materializar_parquet_local(
                    spark,
                    df,
                    "empresa_trimestre",
                    raiz / relativo,
                    PARTICIONES["empresa_trimestre"],
                    EXCEPCIONES[nombre],
                )
            )

        total_bytes = sum(
            archivo["size_bytes"]
            for salida in salidas.values()
            for archivo in salida["files"]
        )

        exigir(
            total_bytes <= MAX_BYTES_SALIDA,
            "Limite de salida candidata excedido"
        )

        comprobar()

        escritor.comprobar_prefijo_nuevo()

        inicio = {
            "status": "STARTED",
            "authorization_id":
                auth["authorization_id"],
            "batch_id": auth["batch_id"],
            "run_id": auth["run_id"],
            "attempt_id": auth["attempt_id"],
            "processor_commit":
                auth["processor_commit"],
            "package_sha256":
                auth["package_sha256"],
            "launcher_sha256":
                auth["launcher_sha256"],
            "started_at_utc": ahora_utc(),
            "staging_uri": auth["staging_uri"],
            "publication_allowed": False,
            "automatic_retry": False,
        }

        # Creacion exclusiva del marcador.
        # Una segunda ejecucion con el mismo intento
        # no puede adquirir esta reserva.

        escritor.escribir_bytes(
            "_control/STARTED.json",
            json_bytes(inicio),
        )

        inventarios = {}

        try:

            for relativo, salida in salidas.items():

                comprobar()

                archivos = []

                for archivo in salida["files"]:

                    referencia = escritor.escribir_archivo(
                        relativo + "/" + archivo["relativo"],
                        archivo["ruta"],
                    )

                    archivos.append({
                        **referencia,
                        "records_count":
                            archivo["records_count"],
                    })

                inventarios[relativo] = {

                    clave: valor

                    for clave, valor in salida.items()

                    if clave != "files"
                }

                inventarios[relativo]["files"] = archivos

                inventarios[relativo][
                    "quality_only"
                ] = relativo.startswith("calidad/")

                inventarios[relativo][
                    "production_authorized"
                ] = False

            inventario = {
                "status": "CANDIDATE_INVENTORY",
                "authorization_id":
                    auth["authorization_id"],
                "sources": auth["sources"],
                "output_contracts":
                    auth["output_contracts"],
                "outputs": inventarios,
                "publication_allowed": False,
            }

            referencia_inventario = (
                escritor.escribir_bytes(
                    "_control/INVENTORY.json",
                    json_bytes(inventario),
                )
            )

            auditoria = {
                **inicio,
                "status": "STAGING_VALIDATED",
                "validated_at_utc": ahora_utc(),
                "candidate_counts": CONTEOS,
                "financial_exclusions": EXCEPCIONES,
                "financial_decision":
                    auth["financial_decision"],
                "cotizaciones_adaptation":
                    adaptacion,
                "numeric_adaptations":
                    evidencias_numericas,
                "sab_coverage_evidence":
                    evidencia_cobertura,
                "sab_coverage_accredited":
                    preparado["cobertura_acreditada"],
                "sab_coverage_reference":
                    auth["sab_coverage"],
                "inventory": referencia_inventario,
                "runtime_context":
                    preparado.get("runtime_context"),
                "spark_application_id":
                    preparado.get("spark_application_id"),
                "remote_integrity_verified": True,
                "production_authorized": False,
                "publication_allowed": False,
                "blocked_products": [
                    "hechos_empresa",
                    "impacto_hechos_valor",
                ],
            }

            referencia_auditoria = (
                escritor.escribir_bytes(
                    "_control/AUDIT.json",
                    json_bytes(auditoria),
                )
            )

            escritor.comprobar_inventario()

            comprobar()

            finalizacion = {
                "status": "STAGING_COMPLETE",
                "authorization_id":
                    auth["authorization_id"],
                "batch_id": auth["batch_id"],
                "run_id": auth["run_id"],
                "attempt_id": auth["attempt_id"],
                "inventory": referencia_inventario,
                "audit": referencia_auditoria,
                "completed_at_utc": ahora_utc(),
                "publication_allowed": False,
                "production_authorized": False,
            }

            # Ultima escritura del intento.

            referencia_final = (
                escritor.escribir_bytes(
                    "_control/COMPLETE.json",
                    json_bytes(finalizacion),
                )
            )

            return {
                **finalizacion,
                "completion_reference":
                    referencia_final,
            }

        except Exception as error:

            # No reintentar escrituras, no borrar objetos
            # ni continuar el intento fallido.
            # Sin COMPLETE verificable, el staging
            # no se considera apto para aprobacion.

            print(
                json.dumps({
                    "status": "STAGING_REQUIRES_REVIEW",
                    "authorization_id":
                        auth["authorization_id"],
                    "staging_uri": auth["staging_uri"],
                    "error_type": type(error).__name__,
                    "automatic_retry": False,
                    "publication_allowed": False,
                })
            )

            raise
