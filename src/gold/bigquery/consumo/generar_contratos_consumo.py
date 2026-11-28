"""Contratos candidatos de consumo para Matthew.

Generacion local desde contratos Gold existentes.
Sin clientes cloud ni ejecucion GoogleSQL.
"""

import csv
import json
import hashlib

from pathlib import Path

from preparador_carga_historica import (
    cargar_contratos,
    PRODUCTOS,
    exigir,
)

from trazabilidad_carga_historica import (
    HASHES_CONTRATOS,
)


BASE_COMMIT = (
    "0b68dac7a26ff43ed09b9218958185cd79514407"
)

GOLD_COMMIT = (
    "62e0f0f76743822bc19859459e4307b2fb9b6ef2"
)

PROYECTO = "bvl-analytics-2026"

DATASET = "gold_analytics"

ESTADO = "LOCAL_CONSUMPTION_CANDIDATE_ONLY"

NUMERICOS = {
    "INT64",
    "NUMERIC",
    "FLOAT64",
}

GRANULARIDAD = {
    "mercado_valor_diario":
        "Una fila por nemonico y fecha",
    "actividad_sab_diaria":
        "Una fila por fecha y codigo_sab",
    "empresa_trimestre":
        "Una fila por id_registro financiero",
}

RESTRICCIONES = {

    "mercado_valor_diario": [

        "Conservar nemonico canonico.",
        "No sumar precios de cierre entre valores.",
        "No interpretar ausencia como precio cero.",
        "Validar comparabilidad temporal.",
        "No asumir ajustes por eventos corporativos.",
    ],

    "actividad_sab_diaria": [

        "Conservar precision monetaria NUMERIC.",
        "No convertir valores monetarios a FLOAT64.",
        "No reemplazar NULL por cero automaticamente.",
        "Conservar estado_cobertura.",
        "Distinguir COMPLETA, PARCIAL y DESCONOCIDA.",
        "No declarar cobertura total sin acreditacion.",
        "No sumar totales diarios repetidos por SAB.",
    ],

    "empresa_trimestre": [

        "Utilizar id_registro como clave contractual.",
        "No asumir unicidad por RUC y trimestre.",
        "No mezclar registros financieros ambiguos.",
        "No reincorporar excepciones sin autorizacion.",
        "No sumar ratios financieros.",
        "Validar comparabilidad de periodos contables.",
    ],
}


def escribir_json(ruta, documento):

    with ruta.open(
        "x",
        encoding="utf-8",
    ) as archivo:

        json.dump(
            documento,
            archivo,
            indent=2,
            ensure_ascii=False,
            allow_nan=False,
        )

        archivo.write("\n")


def huella(ruta, raiz):

    contenido = ruta.read_bytes()

    return {
        "path": ruta.relative_to(
            raiz
        ).as_posix(),
        "sha256": hashlib.sha256(
            contenido
        ).hexdigest(),
        "size_bytes": len(contenido),
    }


def rol_campo(nombre, tipo, claves, particiones):

    if nombre in claves:

        return "CLAVE_NEGOCIO"

    if nombre in particiones:

        return "DIMENSION_PARTICION"

    if nombre == "fecha_ingesta":

        return "METADATO_TECNICO"

    if nombre == "fuente" or (
        nombre.startswith("version_")
    ):

        return "TRAZABILIDAD"

    if nombre in {
        "estado_cobertura",
        "cobertura_completa",
        "trazabilidad_verificada",
        "registros_elegibles",
        "registros_cuarentena",
        "archivos_excluidos",
        "scope_quality_id",
    }:

        return "CONTROL_CALIDAD"

    if tipo == "DATE":

        return "DIMENSION_TEMPORAL"

    if tipo in NUMERICOS:

        return "NUMERICO_CANDIDATO"

    return "ATRIBUTO"


def generar(raiz):

    raiz = Path(raiz).resolve()

    _, contratos = cargar_contratos(raiz)

    destino = (
        raiz / "docs/bigquery/consumo_matthew"
    )

    destino.mkdir(
        parents=True,
        exist_ok=True,
    )

    productos = {}

    diccionario = []

    catalogo = {}

    for producto, contrato in sorted(
        contratos.items()
    ):

        exigir(
            contrato["sha256"]
            == HASHES_CONTRATOS[producto],
            "Contrato Gold incompatible"
        )

        gold = contrato["gold"]

        config = contrato["configuracion"]

        claves = gold["business_keys"]

        particiones = gold[
            "source_partition_columns"
        ]

        columnas = gold["columns"]

        productos[producto] = {

            "product": producto,

            "table_proposed": (
                f"{PROYECTO}.{DATASET}.{producto}"
            ),

            "grain": GRANULARIDAD[producto],

            "business_keys": claves,

            "source_partition_columns":
                particiones,

            "bigquery_partition":
                config["bigquery_partition"],

            "clustering":
                config["clustering"],

            "columns_count": len(columnas),

            "candidate_historical_rows":
                PRODUCTOS[producto],

            "gold_contract_sha256":
                contrato["sha256"],

            "columns": columnas,

            "consumption_restrictions":
                RESTRICCIONES[producto],

            "official_source_verified": False,

            "table_availability_verified": False,

            "production_consumption_authorized":
                False,
        }

        numericos = []

        for posicion, campo in enumerate(
            columnas,
            start=1,
        ):

            nombre = campo["name"]

            tipo = campo["bigquery_type"]

            rol = rol_campo(
                nombre,
                tipo,
                claves,
                particiones,
            )

            descripcion = campo.get(
                "description"
            )

            descripcion_documentada = (
                isinstance(descripcion, str)
                and bool(descripcion.strip())
            )

            diccionario.append({

                "producto": producto,

                "posicion": posicion,

                "columna": nombre,

                "spark_type":
                    campo["spark_type"],

                "bigquery_type": tipo,

                "nullable":
                    campo["nullable"],

                "clave_negocio":
                    nombre in claves,

                "particion_origen":
                    nombre in particiones,

                "particion_bigquery":
                    nombre == config[
                        "bigquery_partition"
                    ],

                "orden_clustering": (
                    config["clustering"].index(
                        nombre
                    ) + 1
                    if nombre in config["clustering"]
                    else None
                ),

                "rol_candidato": rol,

                "descripcion": (
                    descripcion
                    if descripcion_documentada
                    else None
                ),

                "descripcion_pendiente":
                    not descripcion_documentada,

                "agregacion_automatica_autorizada":
                    False,

                "consumo_productivo_autorizado":
                    False,
            })

            if rol == "NUMERICO_CANDIDATO":

                numericos.append({

                    "column": nombre,

                    "type": tipo,

                    "nullable":
                        campo["nullable"],

                    "status":
                        "AGGREGATION_REVIEW_REQUIRED",

                    "default_aggregation": None,

                    "unit": None,

                    "unit_verified": False,

                    "kpi_approved": False,
                })

        catalogo[producto] = {

            "product": producto,

            "numeric_candidates": numericos,

            "restrictions":
                RESTRICCIONES[producto],
        }

    exigir(
        len(diccionario) == 140,
        "Diccionario Gold incompleto"
    )

    exigir(
        len({
            (f["producto"], f["columna"])
            for f in diccionario
        }) == 140,
        "Campos de diccionario duplicados"
    )

    contrato_consumo = {

        "schema_version":
            "BVL-CONSUMPTION-CANDIDATE-v0.1",

        "status": ESTADO,

        "recipient_role":
            "Matthew - Analytics BI ML",

        "base_commit": BASE_COMMIT,

        "gold_base_commit": GOLD_COMMIT,

        "project_id": PROYECTO,

        "dataset_proposed": DATASET,

        "products": productos,

        "total_columns": 140,

        "candidate_historical_rows":
            sum(PRODUCTOS.values()),

        "data_access_mode_proposed":
            "READ_ONLY",

        "official_source_verified": False,

        "cloud_execution_authorized": False,

        "production_consumption_authorized":
            False,

        "publication_authorized": False,
    }

    archivos = []

    ruta = destino / "contratos_consumo.json"

    escribir_json(
        ruta,
        contrato_consumo,
    )

    archivos.append(ruta)

    ruta = destino / "diccionario_datos.json"

    escribir_json(
        ruta,
        {
            "status": ESTADO,
            "columns": diccionario,
            "total_columns": len(diccionario),
        },
    )

    archivos.append(ruta)

    ruta = destino / "diccionario_datos.csv"

    with ruta.open(
        "x",
        encoding="utf-8-sig",
        newline="",
    ) as archivo:

        escritor = csv.DictWriter(
            archivo,
            fieldnames=list(
                diccionario[0]
            ),
        )

        escritor.writeheader()

        escritor.writerows(diccionario)

    archivos.append(ruta)

    ruta = (
        destino
        / "catalogo_indicadores_candidatos.json"
    )

    escribir_json(
        ruta,
        {
            "status": ESTADO,
            "products": catalogo,
            "kpis_operatively_approved": False,
            "automatic_aggregation_authorized":
                False,
        },
    )

    archivos.append(ruta)

    inventario = {

        "status": ESTADO,

        "base_commit": BASE_COMMIT,

        "products": sorted(productos),

        "files": [
            huella(ruta, raiz)
            for ruta in archivos
        ],

        "production_consumption_authorized":
            False,

        "publication_authorized": False,
    }

    escribir_json(
        destino / "inventario_consumo.json",
        inventario,
    )

    return inventario


if __name__ == "__main__":

    inventario = generar(Path.cwd())

    for archivo in inventario["files"]:

        print(
            archivo["sha256"],
            archivo["path"],
        )

    print("CONTRATOS_CONSUMO_GENERADOS: 3/3")
    print("DICCIONARIO_COLUMNAS: 140/140")
    print("CONSUMO_PRODUCTIVO_AUTORIZADO: False")
