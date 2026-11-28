"""Comprobaciones estaticas de SQL candidato.

No constituye validacion del motor GoogleSQL.
"""

import ast
import hashlib
import json
import re

from pathlib import Path

from preparador_incremental import (
    cargar_contratos_incrementales,
)

from generar_sql_transaccional import (
    generar_producto,
    generar_control,
)

from src.gold.bigquery.comparador_negocio_local import (
    METADATOS_TECNICOS,
)


raiz = Path.cwd()

directorio = (
    raiz / "docs/bigquery"
    / "integracion_transaccional"
    / "sql_candidato"
)

contratos = cargar_contratos_incrementales(
    raiz
)

aprobados = []


def aprobar(nombre):

    assert nombre not in aprobados

    aprobados.append(nombre)

    print(nombre + "_OK")


inventario = json.loads(
    (directorio / "inventario_sql.json").read_text()
)

assert len(inventario["files"]) == 4

for referencia in inventario["files"]:

    ruta = raiz / referencia["path"]

    contenido = ruta.read_bytes()

    assert len(contenido) == referencia["size_bytes"]

    assert hashlib.sha256(
        contenido
    ).hexdigest() == referencia["sha256"]

aprobar("INTEGRIDAD_SQL_4_DE_4")

for bandera in (
    "execution_authorized",
    "delete_authorized",
    "production_write_authorized",
    "publication_authorized",
    "google_sql_engine_validated",
    "distributed_lock_verified",
):

    assert inventario[bandera] is False

aprobar("AUTORIZACIONES_DESACTIVADAS")

control = (
    directorio / "control_transaccional_v2.sql"
).read_text()

assert control == generar_control()

assert control.count(
    "CREATE TABLE IF NOT EXISTS"
) == 3

assert control.index(
    "ASSERT FALSE"
) < control.index(
    "CREATE TABLE"
)

aprobar("CONTROL_TRANSACCIONAL_BLOQUEADO")

for producto, contrato in contratos.items():

    sufijo = producto.upper()

    ruta = (
        directorio
        / f"{producto}_transaccional_v2.sql"
    )

    sql = ruta.read_text()

    assert sql == generar_producto(
        raiz,
        producto,
        contrato,
    )

    aprobar("GENERACION_DETERMINISTA_" + sufijo)

    assert sql.index(
        "ASSERT FALSE"
    ) < sql.index(
        "BEGIN TRANSACTION"
    )

    aprobar("BLOQUEO_SQL_" + sufijo)

    assert sql.count(
        "BEGIN TRANSACTION;"
    ) == 1

    assert sql.count(
        "COMMIT TRANSACTION;"
    ) == 1

    assert sql.count(
        "ROLLBACK TRANSACTION;"
    ) == 1

    aprobar("ESTRUCTURA_TRANSACCIONAL_" + sufijo)

    assert not re.search(
        r"\bTHEN\s+DELETE\b|\bDELETE\s+FROM\b",
        sql,
        re.IGNORECASE,
    )

    aprobar("SIN_DML_DELETE_" + sufijo)

    columnas = [
        c["name"]
        for c in contrato["gold"]["columns"]
    ]

    claves = contrato["gold"]["business_keys"]

    merge = sql.split(
        "\n    MERGE ", 1
    )[1].split(
        "\n    -- Conciliacion", 1
    )[0]

    for nombre in columnas:

        assert f"S.`{nombre}`" in merge

        assert (
            f"T.`{nombre}` IS DISTINCT FROM "
            f"S.`{nombre}`"
        ) in merge

    for clave in claves:

        assert (
            f"T.`{clave}` = S.`{clave}`"
        ) in merge

    aprobar("COLUMNAS_Y_CLAVES_MERGE_" + sufijo)

    comparacion = sql.split(
        "WITH clasificacion AS", 1
    )[1].split(
        "END AS accion", 1
    )[0]

    assert "'UPDATE_BUSINESS'" in comparacion

    assert "'UPDATE_TECHNICAL'" in comparacion

    assert "'NOOP'" in comparacion

    assert "fecha_ingesta" in METADATOS_TECNICOS

    assert (
        "T.`fecha_ingesta` IS DISTINCT FROM "
        "S.`fecha_ingesta`"
    ) in comparacion

    aprobar("SEMANTICA_COMPLETA_" + sufijo)

    assert sql.index(
        "'REPLAY_NOOP'"
    ) < sql.index(
        "'Continuidad incompatible'"
    )

    assert "'Version previamente aplicada'" in sql

    aprobar("REPLAY_Y_CONTINUIDAD_" + sufijo)

    assert sql.index(
        "CREATE TEMP TABLE metricas_batch"
    ) < sql.index(
        "\n    MERGE "
    )

    assert sql.index(
        "'Conciliacion posterior incompatible'"
    ) < sql.index(
        "\n    INSERT INTO "
    )

    assert sql.index(
        "'Avance de version incompatible'"
    ) < sql.index(
        "COMMIT TRANSACTION;"
    )

    aprobar("AUDITORIA_Y_CONCILIACION_" + sufijo)

for producto in contratos:

    for sufijo in (
        "_merge.sql",
        "_transaccional.sql",
    ):

        original = (
            raiz / "docs/bigquery"
            / f"{producto}{sufijo}"
        ).read_text()

        assert "ASSERT FALSE AS" in original

aprobar("SEIS_BLOQUEOS_ORIGINALES_CONSERVADOS")

ast.parse(
    (
        raiz / "src/gold/bigquery"
        / "transaccional"
        / "generar_sql_transaccional.py"
    ).read_text()
)

aprobar("SINTAXIS_GENERADOR_PYTHON")

assert len(aprobados) == 29

print("\n=== RESUMEN SQL CANDIDATO ===")
print("CONTROLES_ESTATICOS_SQL: 29/29")
print("SQL_CANDIDATOS: 4/4")
print("PRODUCTOS_MERGE: 3/3")
print("SEMANTICA_METADATA_IMPLEMENTADA: True")
print("METRICAS_AUDITORIA_IMPLEMENTADAS: True")
print("CONCILIACION_CLAVES_AFECTADAS_IMPLEMENTADA: True")
print("DELETE_DML_PRESENTE: False")
print("BLOQUEOS_SQL_ORIGINALES: 6/6")
print("GOOGLESQL_ENGINE_VALIDADO: False")
print("CONCURRENCIA_DISTRIBUIDA_VALIDADA: False")
print("EJECUCION_OPERATIVA_AUTORIZADA: False")
print("VALIDACION_ESTATICA_SQL_TRANSACCIONAL_OK")
