"""Pruebas nuevas de transferencia fisica incremental."""

import pyarrow as pa
import pyarrow.parquet as pq

from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

from preparador_incremental import (
    cargar_contratos_incrementales,
    preparar_batch_incremental,
    normalizar_fila,
)

from transferencia_incremental_local import (
    materializar_parquet,
    validar_parquet,
    validar_paquete,
    esquema_staging,
)

from trazabilidad_carga_historica import (
    huella_documento,
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


def rechazar(
    nombre,
    funcion,
    excepcion=ValueError,
):

    try:

        funcion()

    except excepcion:

        aprobar(nombre)

    else:

        raise AssertionError(
            "Debio rechazarse: " + nombre
        )


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

            valor = "2025-01-02"

        elif tipo == "double":

            valor = 10.25

        elif tipo.startswith("decimal("):

            valor = "100.25"

        elif tipo in (
            "int", "integer", "long", "bigint"
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
        fila["precio_anterior"] = None

    elif producto == SAB:

        fila["codigo_sab"] = f"SAB{numero}"
        fila["estado_cobertura"] = "DESCONOCIDA"
        fila["cobertura_completa"] = False
        fila["trazabilidad_verificada"] = False
        fila["cantidad_sab_diaria"] = 2
        fila["registros_elegibles"] = 2
        fila["registros_cuarentena"] = None
        fila["archivos_excluidos"] = None
        fila["scope_quality_id"] = None

        maximo = "9" * 29 + ".99"

        fila["total_compras_diario"] = (
            maximo if numero == 1
            else "-" + maximo
        )

        fila["total_ventas_diario"] = None

    else:

        fila["id_registro"] = f"{numero:064x}"
        fila["fecha_fin_trimestre"] = "2025-03-31"
        fila["ejercicio"] = 2025
        fila["trimestre_num"] = 1
        fila["roa_aproximado_pct"] = None

    fila["operation"] = "UPSERT"

    return fila


def preparar(producto):

    contrato = contratos[producto]

    filas = [
        fixture(producto, 1),
        fixture(producto, 2),
    ]

    normalizadas = [
        normalizar_fila(
            producto, fila, contrato
        )[1]
        for fila in filas
    ]

    return preparar_batch_incremental(
        producto,
        filas,
        contrato,
        batch_id="batch-parquet-001",
        run_id="run-parquet-001",
        source_version="silver-fixture-v2",
        previous_gold_version="gold-fixture-v1",
        current_gold_version="gold-fixture-v1",
        new_gold_version="gold-fixture-v2",
        source_manifest_sha256="a" * 64,
        expected_rows=2,
        payload_sha256_declarado=(
            calcular_sha256(normalizadas)
        ),
    )


with TemporaryDirectory(
    prefix="bvl-incremental-parquet-"
) as temporal:

    destino = Path(temporal)

    paquetes = {}
    informes = {}

    for producto, contrato in contratos.items():

        paquete = preparar(producto)

        paquetes[producto] = paquete

        informe = materializar_parquet(
            destino,
            producto,
            paquete,
            contrato,
        )

        informes[producto] = informe

        ruta = destino / (
            producto + ".parquet"
        )

        fisico = pq.ParquetFile(ruta).read()

        assert fisico.num_rows == 2

        assert fisico.schema.names == (
            contrato["staging_columns"]
        )

        assert informe["transfer_sha256"] == (
            huella_documento(
                informe["transfer"]
            )
        )

        assert informe["transfer"][
            "cloud_write_authorized"
        ] is False

        aprobar(
            "PARQUET_" + producto.upper()
        )

        validar_parquet(
            ruta,
            producto,
            paquete,
            contrato,
        )

        aprobar(
            "ROUNDTRIP_" + producto.upper()
        )

        for nombre, _ in (
            ("operation", None),
            ("delete_authorized", None),
            ("batch_id", None),
            ("payload_sha256", None),
            ("run_id", None),
            ("source_version", None),
        ):

            assert not fisico.schema.field(
                nombre
            ).nullable

        aprobar(
            "CONTROLES_FISICOS_" + producto.upper()
        )

        rechazar(
            "SOBRESCRITURA_BLOQUEADA_"
            + producto.upper(),
            lambda: materializar_parquet(
                destino,
                producto,
                paquete,
                contrato,
            ),
            excepcion=FileExistsError,
        )

    assert sum(
        i["transfer"]["columns_count"]
        for i in informes.values()
    ) == 158

    aprobar("COLUMNAS_FISICAS_158")

    sab = pq.ParquetFile(
        destino / (SAB + ".parquet")
    ).read()

    for nombre in (
        "total_compras_diario",
        "total_ventas_diario",
        "total_actividad_diaria",
    ):

        assert sab.schema.field(nombre).type == (
            pa.decimal128(31, 2)
        )

    aprobar("DECIMALES_FISICOS_SAB_3_DE_3")

    assert sab["total_ventas_diario"].null_count == 2

    aprobar("NULL_DECIMAL_CONSERVADO")

    assert str(
        sab["total_compras_diario"][0].as_py()
    ) == "9" * 29 + ".99"

    assert str(
        sab["total_compras_diario"][1].as_py()
    ) == "-" + "9" * 29 + ".99"

    aprobar("LIMITES_DECIMALES_CONSERVADOS")

    assert sab[
        "registros_cuarentena"
    ].null_count == 2

    assert sab[
        "archivos_excluidos"
    ].null_count == 2

    aprobar("COBERTURA_DESCONOCIDA_CONSERVADA")

    alterado = deepcopy(paquetes[MERCADO])

    alterado["payload"][0][
        "precio_cierre"
    ] = 99.0

    rechazar(
        "PAYLOAD_ALTERADO_RECHAZADO",
        lambda: validar_paquete(
            MERCADO,
            alterado,
            contratos[MERCADO],
        ),
    )

    alterado = deepcopy(paquetes[MERCADO])

    alterado["staging"][0][
        "precio_cierre"
    ] = 99.0

    rechazar(
        "STAGING_ALTERADO_RECHAZADO",
        lambda: validar_paquete(
            MERCADO,
            alterado,
            contratos[MERCADO],
        ),
    )

    alterado = deepcopy(paquetes[MERCADO])

    alterado["staging"][0][
        "delete_authorized"
    ] = True

    alterado["evidence"][
        "staging_content_sha256"
    ] = huella_documento(
        alterado["staging"]
    )

    alterado["evidence_sha256"] = (
        huella_documento(
            alterado["evidence"]
        )
    )

    rechazar(
        "DELETE_REHASH_RECHAZADO",
        lambda: validar_paquete(
            MERCADO,
            alterado,
            contratos[MERCADO],
        ),
    )

    alterado = deepcopy(paquetes[MERCADO])

    alterado["evidence"][
        "execution_authorized"
    ] = True

    alterado["evidence_sha256"] = (
        huella_documento(
            alterado["evidence"]
        )
    )

    rechazar(
        "AUTORIZACION_FALSA_RECHAZADA",
        lambda: validar_paquete(
            MERCADO,
            alterado,
            contratos[MERCADO],
        ),
    )

    ruta = destino / (
        MERCADO + ".parquet"
    )

    tabla = pq.ParquetFile(ruta).read()

    columna = tabla.schema.get_field_index(
        "precio_cierre"
    )

    modificada = tabla.set_column(
        columna,
        tabla.schema.field(columna),
        pa.array(
            [999.0, 999.0],
            type=pa.float64(),
        ),
    )

    ruta_alterada = (
        destino / "alterada.parquet"
    )

    pq.write_table(
        modificada,
        ruta_alterada,
    )

    rechazar(
        "PARQUET_CONTENIDO_ALTERADO_RECHAZADO",
        lambda: validar_parquet(
            ruta_alterada,
            MERCADO,
            paquetes[MERCADO],
            contratos[MERCADO],
        ),
    )

    ruta_incompleta = (
        destino / "incompleta.parquet"
    )

    pq.write_table(
        tabla.drop(["run_id"]),
        ruta_incompleta,
    )

    rechazar(
        "PARQUET_ESQUEMA_ALTERADO_RECHAZADO",
        lambda: validar_parquet(
            ruta_incompleta,
            MERCADO,
            paquetes[MERCADO],
            contratos[MERCADO],
        ),
    )

assert len(aprobados) == 23, aprobados

print("\n=== RESUMEN TRANSFERENCIA FISICA ===")
print("CONTROLES_TRANSFERENCIA_FISICA: 23/23")
print("PRODUCTOS_PARQUET: 3/3")
print("ROUNDTRIP_INCREMENTAL: 3/3")
print("COLUMNAS_STAGING_FISICAS: 158/158")
print("REGISTROS_SINTETICOS_MATERIALIZADOS: 6")
print("DECIMALES_SAB_FISICOS: 3/3")
print("PRECISION_MONETARIA_CONSERVADA: True")
print("SOBRESCRITURA_BLOQUEADA: True")
print("DETECTOR_GOLD_INTEGRADO: False")
print("AUTORIZACION_OPERATIVA_REAL: False")
print("GCP_EJECUTADO: False")
print("BIGQUERY_EJECUTADO: False")
print("TRANSFERENCIA_INCREMENTAL_LOCAL_OK")
