"""Integracion nueva con detector Gold original.

DataFrames sinteticos con contratos reales.
No ejecuta constructores Gold ni servicios cloud.
"""

from datetime import date
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from preparador_incremental import (
    cargar_contratos_incrementales,
    normalizar_fila,
)

from adaptador_detector_gold import (
    extraer_delta_local,
    preparar_desde_detector,
)

from transferencia_incremental_local import (
    materializar_parquet,
)

from src.gold.bigquery.verificar_payload_local import (
    calcular_sha256,
)


MERCADO = "mercado_valor_diario"
SAB = "actividad_sab_diaria"
EMPRESA = "empresa_trimestre"

contratos = cargar_contratos_incrementales(
    Path.cwd()
)

aprobados = []


def aprobar(nombre):

    aprobados.append(nombre)
    print(nombre + "_OK", flush=True)


def rechazar(nombre, funcion, mensaje):

    try:

        funcion()

    except ValueError as error:

        assert mensaje in str(error), str(error)

        aprobar(nombre)

    else:

        raise AssertionError(
            "Debio rechazarse: " + nombre
        )


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("BVL-Detector-BigQuery-Local")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("SPARK_VERSION:", spark.version)


def tipo_spark(tipo):

    directos = {
        "string": T.StringType(),
        "boolean": T.BooleanType(),
        "date": T.DateType(),
        "double": T.DoubleType(),
        "int": T.IntegerType(),
        "integer": T.IntegerType(),
        "bigint": T.LongType(),
        "long": T.LongType(),
    }

    if tipo in directos:

        return directos[tipo]

    if tipo.startswith("decimal("):

        precision, escala = map(
            int,
            tipo[8:-1].split(","),
        )

        return T.DecimalType(
            precision, escala
        )

    raise AssertionError(tipo)


def esquema(producto):

    return T.StructType([
        T.StructField(
            campo["name"],
            tipo_spark(campo["spark_type"]),
            True,
        )
        for campo in contratos[
            producto
        ]["gold"]["columns"]
    ])


def fixture(producto, numero):

    fila = {}

    for campo in contratos[producto]["gold"]["columns"]:

        nombre = campo["name"]
        tipo = campo["spark_type"]

        if tipo == "string":

            valor = "FIXTURE"

        elif tipo == "boolean":

            valor = True

        elif tipo == "date":

            valor = date(2025, 1, 2)

        elif tipo == "double":

            valor = 100.25

        elif tipo.startswith("decimal("):

            valor = Decimal("100.25")

        elif tipo in (
            "int", "integer", "bigint", "long"
        ):

            valor = 1

        else:

            raise AssertionError(tipo)

        fila[nombre] = valor

    fila["anio"] = 2025
    fila["trimestre"] = 1

    if "mes" in fila:

        fila["mes"] = 1

    if producto == MERCADO:

        fila["nemonico"] = f"FIX{numero}"
        fila["nemonico_original"] = f" fix{numero} "

    elif producto == SAB:

        fila["codigo_sab"] = f"SAB{numero}"
        fila["estado_cobertura"] = "DESCONOCIDA"
        fila["cobertura_completa"] = False
        fila["trazabilidad_verificada"] = False
        fila["cantidad_sab_diaria"] = 3
        fila["registros_elegibles"] = 3
        fila["registros_cuarentena"] = None
        fila["archivos_excluidos"] = None
        fila["scope_quality_id"] = None

    else:

        fila["id_registro"] = f"{numero:064x}"
        fila["fecha_fin_trimestre"] = date(
            2025, 3, 31
        )
        fila["ejercicio"] = 2025
        fila["trimestre_num"] = 1

    return fila


def dataframe(producto, filas):

    return spark.createDataFrame(
        filas,
        schema=esquema(producto),
    )


def delta(producto, anterior, vigente, **kwargs):

    return extraer_delta_local(
        producto,
        anterior,
        vigente,
        contratos[producto],
        **kwargs,
    )


def preparar(
    producto,
    anterior,
    vigente,
    esperadas,
    hash_esperado,
    **cambios,
):

    parametros = {
        "batch_id": "batch-detector-001",
        "run_id": "run-detector-001",
        "source_version": "silver-fixture-v2",
        "previous_gold_version": "gold-fixture-v1",
        "new_gold_version": "gold-fixture-v2",
        "current_gold_version": "gold-fixture-v1",
        "source_manifest_sha256": "a" * 64,
        "expected_rows": esperadas,
        "payload_sha256_declarado": hash_esperado,
    }

    parametros.update(cambios)

    return preparar_desde_detector(
        producto,
        anterior,
        vigente,
        contratos[producto],
        **parametros,
    )


def normalizar(producto, fila):

    return normalizar_fila(
        producto,
        {
            **fila,
            "operation": "UPSERT",
        },
        contratos[producto],
    )[1]


CAMPO_CAMBIO = {
    MERCADO: "precio_cierre",
    SAB: "monto_compras",
    EMPRESA: "activo_total",
}

try:

    with TemporaryDirectory(
        prefix="bvl-detector-bigquery-"
    ) as temporal:

        for producto in contratos:

            fila1 = fixture(producto, 1)
            fila2 = fixture(producto, 2)
            fila3 = fixture(producto, 3)

            modificada = dict(fila2)

            modificada[
                CAMPO_CAMBIO[producto]
            ] = (
                Decimal("120.50")
                if producto == SAB
                else 120.50
            )

            anterior = dataframe(
                producto,
                [fila1, fila2],
            )

            vigente = dataframe(
                producto,
                [fila3, modificada, fila1],
            )

            payload_esperado = [
                normalizar(producto, modificada),
                normalizar(producto, fila3),
            ]

            hash_esperado = calcular_sha256(
                payload_esperado
            )

            resultado = preparar(
                producto,
                anterior,
                vigente,
                2,
                hash_esperado,
            )

            cambios = resultado["delta"]

            assert cambios["inserted_keys"] == 1
            assert cambios["modified_keys"] == 1
            assert cambios["deleted_keys"] == 0

            aprobar(
                "DETECTOR_INSERT_UPDATE_"
                + producto.upper()
            )

            assert cambios["rows"] == (
                payload_esperado
            )

            assert cambios["payload_sha256"] == (
                hash_esperado
            )

            aprobar(
                "FILAS_COMPLETAS_"
                + producto.upper()
            )

            paquete = resultado["batch"]

            assert paquete["payload"] == (
                payload_esperado
            )

            assert paquete["evidence"][
                "execution_authorized"
            ] is False

            aprobar(
                "PUENTE_DETECTOR_"
                + producto.upper()
            )

            informe = materializar_parquet(
                Path(temporal),
                producto,
                paquete,
                contratos[producto],
            )

            assert informe["transfer"][
                "records_count"
            ] == 2

            assert informe["transfer"][
                "roundtrip_verified"
            ] is True

            aprobar(
                "DETECTOR_A_PARQUET_"
                + producto.upper()
            )

            # Delta vacio: no crear staging.
            sin_cambios = preparar(
                producto,
                anterior,
                anterior,
                0,
                None,
            )

            assert sin_cambios["batch"] is None

            assert sin_cambios["evidence"][
                "version_advance_authorized"
            ] is False

            aprobar(
                "SIN_CAMBIOS_"
                + producto.upper()
            )

            # Desaparicion real de una clave.
            eliminado = dataframe(
                producto, [fila1]
            )

            rechazar(
                "ELIMINACION_BLOQUEADA_"
                + producto.upper(),
                lambda: delta(
                    producto,
                    anterior,
                    eliminado,
                ),
                "DELETE detectado",
            )

        # Controles adicionales con Mercado.
        producto = MERCADO

        fila1 = fixture(producto, 1)
        fila2 = fixture(producto, 2)

        anterior = dataframe(
            producto, [fila1]
        )

        vigente = dataframe(
            producto, [fila2, fila1]
        )

        esperado = [
            normalizar(producto, fila2)
        ]

        hash_esperado = calcular_sha256(
            esperado
        )

        rechazar(
            "HASH_DECLARADO_ALTERADO",
            lambda: preparar(
                producto,
                anterior,
                vigente,
                1,
                "0" * 64,
            ),
            "Hash del detector incompatible",
        )

        rechazar(
            "CONTEO_DECLARADO_INCORRECTO",
            lambda: preparar(
                producto,
                anterior,
                vigente,
                2,
                hash_esperado,
            ),
            "Conteo declarado incompatible",
        )

        duplicado = dataframe(
            producto,
            [fila1, fila2, fila2],
        )

        rechazar(
            "DUPLICADO_DETECTOR_ORIGINAL",
            lambda: delta(
                producto,
                anterior,
                duplicado,
            ),
            "claves duplicadas",
        )

        rechazar(
            "ESQUEMA_INCOMPATIBLE",
            lambda: delta(
                producto,
                anterior,
                vigente.drop("precio_cierre"),
            ),
            "Esquema Gold incompatible",
        )

        nula = dict(fila2)
        nula["nemonico"] = None

        rechazar(
            "CLAVE_NULA_EN_SNAPSHOT",
            lambda: delta(
                producto,
                anterior,
                dataframe(
                    producto, [fila1, nula]
                ),
            ),
            "Clave Gold nula o vacia",
        )

        # El detector compara tambien fecha_ingesta.
        metadata = dict(fila1)
        metadata["fecha_ingesta"] = date(
            2025, 1, 3
        )

        cambios_metadata = delta(
            producto,
            anterior,
            dataframe(producto, [metadata]),
        )

        assert cambios_metadata[
            "modified_keys"
        ] == 1

        assert cambios_metadata["rows"][0][
            "fecha_ingesta"
        ] == "2025-01-03"

        aprobar(
            "CAMBIO_METADATA_CONSERVADO"
        )

        tercera = fixture(producto, 3)

        rechazar(
            "LIMITE_MEMORIA_RECHAZADO",
            lambda: delta(
                producto,
                anterior,
                dataframe(
                    producto,
                    [fila1, fila2, tercera],
                ),
                max_rows=1,
            ),
            "Delta excede limite local",
        )

        # El orden de entrada no debe alterar
        # el orden canonico elegido para el delta.
        delta_a = delta(
            producto,
            anterior,
            dataframe(
                producto,
                [tercera,
                 fila2, fila1],
            ),
        )

        delta_b = delta(
            producto,
            anterior,
            dataframe(
                producto,
                [fila1, fila2, tercera],
            ),
        )

        assert delta_a["rows"] == delta_b["rows"]

        assert delta_a["payload_sha256"] == (
            delta_b["payload_sha256"]
        )

        aprobar("ORDEN_DETERMINISTA_DELTA")

finally:

    spark.stop()


assert len(aprobados) == 26, aprobados

print("\n=== RESUMEN INTEGRACION DETECTOR GOLD ===")
print("CONTROLES_DETECTOR_INCREMENTAL: 26/26")
print("PRODUCTOS_DETECTOR_INTEGRADOS: 3/3")
print("DETECTOR_A_PARQUET: 3/3")
print("DETECTOR_GOLD_ORIGINAL_UTILIZADO: True")
print("INSERT_UPDATE_IDENTIFICADOS: 3/3")
print("DELTA_VACIO_CONTROLADO: 3/3")
print("DELETE_BLOQUEADO: 3/3")
print("ORDEN_DELTA_DETERMINISTA: True")
print("CAMBIO_METADATA_CONSERVADO: True")
print("CONSTRUCTORES_GOLD_EJECUTADOS: 0")
print("DATOS_SINTETICOS: True")
print("AUTORIZACION_OPERATIVA_REAL: False")
print("SPARK_LOCAL_EJECUTADO: True")
print("GCP_EJECUTADO: False")
print("BIGQUERY_EJECUTADO: False")
print("INTEGRACION_DETECTOR_BIGQUERY_LOCAL_OK")
