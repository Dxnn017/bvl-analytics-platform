"""Generador local de consultas GoogleSQL.

No contiene clientes BigQuery ni GCS.
No ejecuta las consultas generadas.
"""

import json
import re

from pathlib import Path

from preparador_carga_historica import (
    cargar_contratos,
    exigir,
    PRODUCTOS,
)

from trazabilidad_carga_historica import (
    HASHES_CONTRATOS,
    huella_archivo,
)


PROYECTO = "bvl-analytics-2026"

DATASET = "gold_analytics"


def identificador(nombre):

    exigir(
        isinstance(nombre, str)
        and re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*",
            nombre,
        ) is not None,
        "Identificador SQL incompatible"
    )

    return f"`{nombre}`"


def tabla(producto):

    exigir(
        producto in PRODUCTOS,
        "Producto no admitido"
    )

    return (
        f"`{PROYECTO}.{DATASET}."
        f"{producto}`"
    )


def literal(valor):

    exigir(
        isinstance(valor, str)
        and re.fullmatch(
            r"[A-Za-z0-9_(),.-]+",
            valor,
        ) is not None,
        "Literal SQL incompatible"
    )

    return "'" + valor + "'"


def encabezado(producto, finalidad):

    return (
        "-- BVL Analytics Platform\n"
        "-- ESTADO: CANDIDATO LOCAL NO AUTORIZADO\n"
        f"-- Producto: {producto}\n"
        f"-- Finalidad: {finalidad}\n"
        "-- No ejecutar sin autorizacion expresa.\n\n"
    )


def consulta_calidad(producto, contrato):

    gold = contrato["gold"]

    config = contrato["configuracion"]

    columnas = gold["columns"]

    tipos = {
        c["name"]: c["bigquery_type"]
        for c in columnas
    }

    claves = gold["business_keys"]

    claves_sql = ", ".join(
        identificador(c)
        for c in claves
    )

    fecha = identificador(
        config["bigquery_partition"]
    )

    metricas = []

    def agregar(nombre, expresion):

        metricas.append(
            (nombre, expresion)
        )

    for nombre in claves:

        columna = identificador(nombre)

        condicion = f"{columna} IS NULL"

        if tipos[nombre] == "STRING":

            condicion += (
                f" OR TRIM({columna}) = ''"
            )

        agregar(
            f"clave_invalida__{nombre}",
            f"COUNTIF({condicion})",
        )

    for campo in columnas:

        nombre = campo["name"]

        columna = identificador(nombre)

        if campo["nullable"] is False:

            agregar(
                f"nulo_obligatorio__{nombre}",
                f"COUNTIF({columna} IS NULL)",
            )

        if campo["bigquery_type"] == "FLOAT64":

            agregar(
                f"no_finito__{nombre}",
                f"COUNTIF(IS_NAN({columna}) "
                f"OR IS_INF({columna}))",
            )

        if campo["bigquery_type"] == "NUMERIC":

            agregar(
                f"escala_invalida__{nombre}",
                f"COUNTIF({columna} != "
                f"TRUNC({columna}, 2))",
            )

    condicion = (
        f"{fecha} IS NULL "
        "OR `anio` IS DISTINCT FROM "
        f"EXTRACT(YEAR FROM {fecha})"
    )

    if producto == "empresa_trimestre":

        condicion += (
            " OR `trimestre` IS DISTINCT FROM "
            f"EXTRACT(QUARTER FROM {fecha})"
        )

    else:

        condicion += (
            " OR `mes` IS DISTINCT FROM "
            f"EXTRACT(MONTH FROM {fecha})"
        )

    agregar(
        "particiones_invalidas",
        f"COUNTIF({condicion})",
    )

    expresiones = [
        "COUNT(*) AS registros_observados"
    ]

    expresiones.extend(
        f"{expresion} AS {identificador(nombre)}"
        for nombre, expresion in metricas
    )

    select_metricas = ",\n    ".join(
        expresiones
    )

    condiciones = [
        "@expected_rows > 0",
        "m.registros_observados = @expected_rows",
        "d.claves_duplicadas = 0",
    ]

    condiciones.extend(
        f"m.{identificador(nombre)} = 0"
        for nombre, _ in metricas
    )

    criterio = "\n    AND ".join(
        condiciones
    )

    return (
        encabezado(
            producto,
            "Calidad historica",
        )
        + "-- Parametro obligatorio:\n"
        "-- expected_rows INT64, obtenido del\n"
        "-- manifest Gold oficialmente aprobado.\n"
        f"-- Referencia candidata: "
        f"{PRODUCTOS[producto]} filas.\n\n"
        "WITH metricas AS (\n"
        "  SELECT\n"
        f"    {select_metricas}\n"
        f"  FROM {tabla(producto)}\n"
        "), duplicados AS (\n"
        "  SELECT COUNT(*) AS claves_duplicadas\n"
        "  FROM (\n"
        f"    SELECT {claves_sql}\n"
        f"    FROM {tabla(producto)}\n"
        f"    GROUP BY {claves_sql}\n"
        "    HAVING COUNT(*) > 1\n"
        "  )\n"
        ")\n"
        "SELECT\n"
        f"  {literal(producto)} AS producto,\n"
        "  @expected_rows AS registros_esperados,\n"
        "  m.*,\n"
        "  d.claves_duplicadas,\n"
        f"  ({criterio}) AS controles_aprobados\n"
        "FROM metricas AS m\n"
        "CROSS JOIN duplicados AS d;\n"
    )


def consulta_particiones(producto, contrato):

    particiones = contrato[
        "gold"
    ]["source_partition_columns"]

    columnas = ", ".join(
        identificador(c)
        for c in particiones
    )

    fecha = identificador(
        contrato["configuracion"][
            "bigquery_partition"
        ]
    )

    return (
        encabezado(
            producto,
            "Distribucion por particiones",
        )
        + "SELECT\n"
        f"  {columnas},\n"
        "  COUNT(*) AS registros,\n"
        f"  MIN({fecha}) AS fecha_minima,\n"
        f"  MAX({fecha}) AS fecha_maxima\n"
        f"FROM {tabla(producto)}\n"
        f"GROUP BY {columnas}\n"
        f"ORDER BY {columnas};\n"
    )


def consulta_esquema(producto, contrato):

    elementos = []

    for posicion, campo in enumerate(
        contrato["gold"]["columns"],
        start=1,
    ):

        nombre = literal(
            campo["name"]
        )

        tipo = literal(
            campo["bigquery_type"]
        )

        nullable = literal(
            "YES"
            if campo["nullable"]
            else "NO"
        )

        elementos.append(
            f"STRUCT({posicion} AS posicion, "
            f"{nombre} AS nombre, "
            f"{tipo} AS tipo, "
            f"{nullable} AS nullable)"
        )

    esquema = ",\n    ".join(
        elementos
    )

    info = (
        f"`{PROYECTO}.{DATASET}."
        "INFORMATION_SCHEMA.COLUMNS`"
    )

    return (
        encabezado(
            producto,
            "Comparacion de esquema",
        )
        + "WITH esperado AS (\n"
        "  SELECT * FROM UNNEST([\n"
        f"    {esquema}\n"
        "  ])\n"
        "), observado AS (\n"
        "  SELECT\n"
        "    ordinal_position AS posicion,\n"
        "    column_name AS nombre,\n"
        "    data_type AS tipo,\n"
        "    is_nullable AS nullable\n"
        f"  FROM {info}\n"
        f"  WHERE table_name = {literal(producto)}\n"
        "    AND is_system_defined = 'NO'\n"
        ")\n"
        "SELECT\n"
        "  COALESCE(e.nombre, o.nombre) AS columna,\n"
        "  e.posicion AS posicion_esperada,\n"
        "  o.posicion AS posicion_observada,\n"
        "  e.tipo AS tipo_esperado,\n"
        "  o.tipo AS tipo_observado,\n"
        "  e.nullable AS nullable_esperado,\n"
        "  o.nullable AS nullable_observado,\n"
        "  CASE\n"
        "    WHEN e.nombre IS NULL THEN 'ADICIONAL'\n"
        "    WHEN o.nombre IS NULL THEN 'FALTANTE'\n"
        "    WHEN e.posicion != o.posicion "
        "THEN 'POSICION_INCORRECTA'\n"
        "    WHEN e.tipo != o.tipo "
        "THEN 'TIPO_INCORRECTO'\n"
        "    WHEN e.nullable != o.nullable "
        "THEN 'NULABILIDAD_INCORRECTA'\n"
        "    ELSE 'OK'\n"
        "  END AS estado\n"
        "FROM esperado AS e\n"
        "FULL OUTER JOIN observado AS o\n"
        "ON e.nombre = o.nombre\n"
        "ORDER BY COALESCE(e.posicion, o.posicion);\n"
    )


def consulta_indicadores(producto, contrato):

    expresiones = [
        "COUNT(*) AS registros"
    ]

    for campo in contrato["gold"]["columns"]:

        nombre = campo["name"]

        tipo = campo["bigquery_type"]

        if tipo not in (
            "FLOAT64",
            "NUMERIC",
            "INT64",
        ):

            continue

        columna = identificador(nombre)

        expresiones.append(
            f"COUNT({columna}) AS "
            f"{identificador('presentes__' + nombre)}"
        )

        expresiones.append(
            f"COUNTIF({columna} IS NULL) AS "
            f"{identificador('nulos__' + nombre)}"
        )

        expresiones.append(
            f"MIN({columna}) AS "
            f"{identificador('min__' + nombre)}"
        )

        expresiones.append(
            f"MAX({columna}) AS "
            f"{identificador('max__' + nombre)}"
        )

        # BIGNUMERIC reduce el riesgo de
        # desbordamiento en agregaciones NUMERIC.

        if tipo in ("NUMERIC", "INT64"):

            valor = (
                f"CAST({columna} AS BIGNUMERIC)"
            )

        else:

            valor = columna

        expresiones.append(
            f"AVG({valor}) AS "
            f"{identificador('avg__' + nombre)}"
        )

    select_sql = ",\n  ".join(
        expresiones
    )

    return (
        encabezado(
            producto,
            "Perfil numerico para conciliacion",
        )
        + "SELECT\n"
        f"  {select_sql}\n"
        f"FROM {tabla(producto)};\n"
    )


GENERADORES = {
    "01_calidad.sql": consulta_calidad,
    "02_particiones.sql": consulta_particiones,
    "03_esquema.sql": consulta_esquema,
    "04_indicadores.sql": consulta_indicadores,
}


def generar_consultas(raiz):

    raiz = Path(raiz).resolve()

    _, contratos = cargar_contratos(raiz)

    destino = (
        raiz / "docs/bigquery/validacion_historica"
    )

    inventario = []

    for producto in sorted(PRODUCTOS):

        contrato = contratos[producto]

        exigir(
            contrato["sha256"]
            == HASHES_CONTRATOS[producto],
            "Contrato Gold no ratificado"
        )

        carpeta = destino / producto

        carpeta.mkdir(
            parents=True,
            exist_ok=True,
        )

        for nombre, generador in (
            GENERADORES.items()
        ):

            consulta = generador(
                producto,
                contrato,
            )

            ruta = carpeta / nombre

            exigir(
                not ruta.exists(),
                "Consulta existente: " + str(ruta)
            )

            ruta.write_text(
                consulta,
                encoding="utf-8",
            )

            inventario.append({
                "product": producto,
                "path":
                    ruta.relative_to(raiz).as_posix(),
                "gold_contract_sha256":
                    contrato["sha256"],
                **huella_archivo(ruta),
            })

            print(
                "SQL_GENERADO:",
                producto,
                nombre,
            )

    exigir(
        len(inventario) == 12,
        "Inventario SQL incompleto"
    )

    manifiesto = {
        "status":
            "LOCAL_CANDIDATE_NOT_AUTHORIZED",
        "query_count": len(inventario),
        "queries": inventario,
        "sql_execution_authorized": False,
        "cloud_execution_authorized": False,
        "publication_authorized": False,
    }

    ruta = destino / "inventario_consultas.json"

    ruta.write_text(
        json.dumps(
            manifiesto,
            indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )

    return manifiesto


if __name__ == "__main__":

    generar_consultas(Path.cwd())
