"""Controles historicos complementarios candidatos.

Generacion local exclusivamente.
No ejecuta consultas GoogleSQL.
"""

import hashlib
import json
import re

from pathlib import Path

from preparador_carga_historica import (
    cargar_contratos,
    exigir,
)


PROYECTO = "bvl-analytics-2026"

DATASET = "gold_analytics"

STAGING = "gold_analytics_staging"

CABECERA = """-- CANDIDATO LOCAL NO AUTORIZADO.
-- Consulta de validacion exclusivamente.
-- No ejecutar sin acreditacion operativa.
"""


def identificador(nombre):

    exigir(
        isinstance(nombre, str)
        and re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*",
            nombre,
        ) is not None,
        "Identificador incompatible"
    )

    return f"`{nombre}`"


def nombres_tablas(producto, batch_id):

    identificador(producto)

    exigir(
        isinstance(batch_id, str)
        and re.fullmatch(
            r"[A-Za-z0-9_]{1,80}",
            batch_id,
        ) is not None,
        "Identificador historico incompatible"
    )

    destino = (
        f"`{PROYECTO}.{DATASET}.{producto}`"
    )

    origen = (
        f"`{PROYECTO}.{STAGING}."
        f"{producto}_hist_{batch_id}`"
    )

    return origen, destino


def predicado_claves_invalidas(
    claves,
    tipos,
):

    condiciones = []

    for clave in claves:

        nombre = identificador(clave)

        condiciones.append(
            f"{nombre} IS NULL"
        )

        if tipos[clave] == "STRING":

            condiciones.append(
                f"TRIM({nombre}) = ''"
            )

    return " OR ".join(condiciones)


def generar_igualdad(
    producto,
    contrato,
    batch_id,
):

    origen, destino = nombres_tablas(
        producto,
        batch_id,
    )

    campos = contrato["gold"]["columns"]

    columnas = [
        campo["name"]
        for campo in campos
    ]

    tipos = {
        campo["name"]:
            campo["bigquery_type"]
        for campo in campos
    }

    claves = contrato["gold"]["business_keys"]

    exigir(
        bool(claves)
        and set(claves).issubset(columnas),
        "Claves incompatibles"
    )

    nombres = ", ".join(
        identificador(c)
        for c in columnas
    )

    grupo_claves = ", ".join(
        identificador(c)
        for c in claves
    )

    union = " AND ".join(
        f"S.{identificador(c)} = "
        f"T.{identificador(c)}"
        for c in claves
    )

    diferencias = "\n          OR ".join(
        f"S.{identificador(c)} "
        "IS DISTINCT FROM "
        f"T.{identificador(c)}"
        for c in columnas
    )

    invalidas = predicado_claves_invalidas(
        claves,
        tipos,
    )

    return CABECERA + f"""
-- Comparacion de una version historica completa.
-- No usar despues de aplicar deltas posteriores.
-- El staging debe corresponder a la misma version.
-- Los duplicados impiden aprobar la igualdad.

WITH origen AS (
  SELECT {nombres},
         TRUE AS presente_origen
  FROM {origen}
),
destino AS (
  SELECT {nombres},
         TRUE AS presente_destino
  FROM {destino}
),
calidad_origen AS (
  SELECT
    COUNT(*) AS filas_origen,
    COUNTIF({invalidas}) AS claves_invalidas_origen
  FROM origen
),
calidad_destino AS (
  SELECT
    COUNT(*) AS filas_destino,
    COUNTIF({invalidas}) AS claves_invalidas_destino
  FROM destino
),
duplicados_origen AS (
  SELECT COUNT(*) AS claves_duplicadas_origen
  FROM (
    SELECT {grupo_claves}
    FROM origen
    GROUP BY {grupo_claves}
    HAVING COUNT(*) > 1
  )
),
duplicados_destino AS (
  SELECT COUNT(*) AS claves_duplicadas_destino
  FROM (
    SELECT {grupo_claves}
    FROM destino
    GROUP BY {grupo_claves}
    HAVING COUNT(*) > 1
  )
),
comparacion AS (
  SELECT
    CASE
      WHEN S.presente_origen IS NULL
        THEN 'ADICIONAL_DESTINO'
      WHEN T.presente_destino IS NULL
        THEN 'FALTANTE_DESTINO'
      WHEN (
          {diferencias}
      ) THEN 'MODIFICADA'
      ELSE 'IGUAL'
    END AS estado
  FROM origen S
  FULL OUTER JOIN destino T
    ON {union}
),
metricas AS (
  SELECT
    COUNTIF(estado = 'ADICIONAL_DESTINO')
      AS filas_adicionales,
    COUNTIF(estado = 'FALTANTE_DESTINO')
      AS filas_faltantes,
    COUNTIF(estado = 'MODIFICADA')
      AS filas_modificadas,
    COUNTIF(estado = 'IGUAL')
      AS filas_iguales
  FROM comparacion
),
resultado AS (
  SELECT
    '{producto}' AS producto,
    @expected_rows AS filas_esperadas,
    {len(columnas)} AS columnas_comparadas,
    O.*,
    T.*,
    D1.*,
    D2.*,
    M.*
  FROM calidad_origen O
  CROSS JOIN calidad_destino T
  CROSS JOIN duplicados_origen D1
  CROSS JOIN duplicados_destino D2
  CROSS JOIN metricas M
)
SELECT *,
  COALESCE(
    filas_esperadas > 0
    AND filas_origen = filas_esperadas
    AND filas_destino = filas_esperadas
    AND filas_iguales = filas_esperadas
    AND claves_invalidas_origen = 0
    AND claves_invalidas_destino = 0
    AND claves_duplicadas_origen = 0
    AND claves_duplicadas_destino = 0
    AND filas_adicionales = 0
    AND filas_faltantes = 0
    AND filas_modificadas = 0,
    FALSE
  ) AS igualdad_aprobada
FROM resultado;
"""


def generar_layout(producto, contrato):

    identificador(producto)

    campos = contrato["gold"]["columns"]

    columnas = [
        campo["name"]
        for campo in campos
    ]

    configuracion = contrato["configuracion"]

    particion = configuracion[
        "bigquery_partition"
    ]

    clustering = configuracion[
        "clustering"
    ]

    exigir(
        particion in columnas,
        "Particion fuera del contrato"
    )

    exigir(
        isinstance(clustering, list)
        and len(clustering) <= 4
        and len(set(clustering)) == len(clustering)
        and set(clustering).issubset(columnas),
        "Clustering incompatible"
    )

    esperadas = []

    for columna in columnas:

        indicador = (
            "YES"
            if columna == particion
            else "NO"
        )

        posicion = (
            str(clustering.index(columna) + 1)
            if columna in clustering
            else "CAST(NULL AS INT64)"
        )

        esperadas.append(
            "STRUCT("
            f"'{columna}' AS columna, "
            f"'{indicador}' AS particion, "
            f"{posicion} AS clustering_orden)"
        )

    estructuras = ",\n    ".join(
        esperadas
    )

    return CABECERA + f"""
-- Valida columna de particion y orden clustering.
-- No acredita granularidad ni todas las opciones.
-- Esos controles requieren metadata de tabla.

WITH esperado AS (
  SELECT *
  FROM UNNEST([
    {estructuras}
  ])
),
observado AS (
  SELECT
    column_name AS columna,
    is_partitioning_column AS particion,
    clustering_ordinal_position AS clustering_orden
  FROM `{PROYECTO}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = '{producto}'
),
comparacion AS (
  SELECT
    COALESCE(E.columna, O.columna) AS columna,
    E.particion AS particion_esperada,
    O.particion AS particion_observada,
    E.clustering_orden AS clustering_esperado,
    O.clustering_orden AS clustering_observado,
    CASE
      WHEN E.columna IS NULL THEN 'ADICIONAL'
      WHEN O.columna IS NULL THEN 'FALTANTE'
      WHEN E.particion IS DISTINCT FROM O.particion
        THEN 'PARTICION_INCORRECTA'
      WHEN E.clustering_orden
           IS DISTINCT FROM O.clustering_orden
        THEN 'CLUSTERING_INCORRECTO'
      ELSE 'OK'
    END AS estado
  FROM esperado E
  FULL OUTER JOIN observado O
    ON E.columna = O.columna
)
SELECT
  '{producto}' AS producto,
  *
FROM comparacion
ORDER BY columna;
"""


def generar(raiz):

    raiz = Path(raiz).resolve()

    _, contratos = cargar_contratos(raiz)

    destino = (
        raiz / "docs/bigquery"
        / "integracion_transaccional"
        / "sql_historico_complementario"
    )

    # Nombre de staging de ejemplo.
    # No corresponde a un batch oficial.

    batch_id = "BATCH_CANDIDATO"

    documentos = {}

    for producto, contrato in contratos.items():

        documentos[
            f"{producto}_igualdad.sql"
        ] = generar_igualdad(
            producto,
            contrato,
            batch_id,
        )

        documentos[
            f"{producto}_layout.sql"
        ] = generar_layout(
            producto,
            contrato,
        )

    destino.mkdir(
        parents=True,
        exist_ok=False,
    )

    referencias = []

    for nombre, sql in sorted(
        documentos.items()
    ):

        ruta = destino / nombre

        with ruta.open(
            "x",
            encoding="utf-8",
        ) as archivo:

            archivo.write(sql)

        contenido = ruta.read_bytes()

        referencias.append({
            "path": str(
                ruta.relative_to(raiz)
            ),
            "sha256": hashlib.sha256(
                contenido
            ).hexdigest(),
            "size_bytes": len(contenido),
        })

    inventario = {
        "status":
            "LOCAL_HISTORICAL_COMPLEMENT_ONLY",
        "batch_id_example": batch_id,
        "products": sorted(contratos),
        "files": referencias,
        "official_source_verified": False,
        "google_sql_engine_validated": False,
        "execution_authorized": False,
        "production_write_authorized": False,
        "publication_authorized": False,
    }

    with (
        destino / "inventario_sql.json"
    ).open(
        "x",
        encoding="utf-8",
    ) as archivo:

        json.dump(
            inventario,
            archivo,
            indent=2,
            ensure_ascii=False,
        )

        archivo.write("\n")

    return inventario


if __name__ == "__main__":

    resultado = generar(Path.cwd())

    for referencia in resultado["files"]:

        print(
            referencia["sha256"],
            referencia["path"],
        )

    print(
        "SQL_HISTORICOS_COMPLEMENTARIOS:",
        len(resultado["files"]),
    )
