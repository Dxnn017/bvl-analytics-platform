"""Controles estaticos de consultas historicas adicionales."""

import hashlib
import json
import re

from pathlib import Path

from preparador_carga_historica import (
    cargar_contratos,
)

from generar_controles_historicos_complementarios import (
    generar_igualdad,
    generar_layout,
    nombres_tablas,
)


raiz = Path.cwd()

_, contratos = cargar_contratos(raiz)

directorio = (
    raiz / "docs/bigquery"
    / "integracion_transaccional"
    / "sql_historico_complementario"
)

inventario = json.loads(
    (directorio / "inventario_sql.json").read_text()
)

aprobados = []


def aprobar(nombre):

    assert nombre not in aprobados

    aprobados.append(nombre)

    print(nombre + "_OK")


assert len(inventario["files"]) == 6

for referencia in inventario["files"]:

    ruta = raiz / referencia["path"]

    contenido = ruta.read_bytes()

    assert len(contenido) == referencia["size_bytes"]

    assert hashlib.sha256(
        contenido
    ).hexdigest() == referencia["sha256"]

aprobar("INTEGRIDAD_SQL_HISTORICO")

for bandera in (
    "official_source_verified",
    "google_sql_engine_validated",
    "execution_authorized",
    "production_write_authorized",
    "publication_authorized",
):

    assert inventario[bandera] is False

aprobar("AUTORIZACIONES_DESACTIVADAS")

total_columnas = 0

for producto, contrato in contratos.items():

    sufijo = producto.upper()

    igualdad = (
        directorio
        / f"{producto}_igualdad.sql"
    ).read_text()

    layout = (
        directorio
        / f"{producto}_layout.sql"
    ).read_text()

    assert igualdad == generar_igualdad(
        producto,
        contrato,
        "BATCH_CANDIDATO",
    )

    assert layout == generar_layout(
        producto,
        contrato,
    )

    aprobar("GENERACION_DETERMINISTA_" + sufijo)

    columnas = [
        c["name"]
        for c in contrato["gold"]["columns"]
    ]

    total_columnas += len(columnas)

    for columna in columnas:

        assert (
            f"S.`{columna}` IS DISTINCT FROM "
            f"T.`{columna}`"
        ) in igualdad

    aprobar("IGUALDAD_COLUMNAS_" + sufijo)

    for metrica in (
        "filas_adicionales",
        "filas_faltantes",
        "filas_modificadas",
        "filas_iguales",
        "claves_invalidas_origen",
        "claves_invalidas_destino",
        "claves_duplicadas_origen",
        "claves_duplicadas_destino",
    ):

        assert f"{metrica} = " in igualdad

    assert "FULL OUTER JOIN" in igualdad

    assert igualdad.count(
        "HAVING COUNT(*) > 1"
    ) == 2

    aprobar("CARDINALIDAD_Y_CLAVES_" + sufijo)

    assert (
        "filas_esperadas > 0"
    ) in igualdad

    assert (
        "filas_iguales = filas_esperadas"
    ) in igualdad

    aprobar("CONTEO_ESPERADO_" + sufijo)

    config = contrato["configuracion"]

    particion = config[
        "bigquery_partition"
    ]

    assert (
        f"'{particion}' AS columna, "
        "'YES' AS particion"
    ) in layout

    assert "is_partitioning_column" in layout

    aprobar("PARTICION_" + sufijo)

    for posicion, columna in enumerate(
        config["clustering"],
        start=1,
    ):

        indicador = (
            "YES"
            if columna == particion
            else "NO"
        )

        assert (
            f"'{columna}' AS columna, "
            f"'{indicador}' AS particion, "
            f"{posicion} AS clustering_orden"
        ) in layout

    assert (
        "clustering_ordinal_position"
    ) in layout

    aprobar("CLUSTERING_" + sufijo)

    for sql in (igualdad, layout):

        # Retirar comentarios exclusivamente
        # para la comprobacion estatica.

        contenido = re.sub(
            r"(?m)^--.*$",
            "",
            sql,
        ).strip()

        assert contenido.startswith("WITH ")

        assert not re.search(
            r"\b(INSERT|UPDATE|DELETE|MERGE|"
            r"DROP|ALTER|TRUNCATE|CREATE)\s",
            contenido,
            re.IGNORECASE,
        )

    aprobar("SOLO_LECTURA_" + sufijo)

assert total_columnas == 140

aprobar("COLUMNAS_GOLD_140")

try:

    nombres_tablas(
        "mercado_valor_diario",
        "batch`; DROP TABLE prueba",
    )

except ValueError:

    aprobar("IDENTIFICADOR_INSEGURO_RECHAZADO")

else:

    raise AssertionError(
        "Identificador inseguro admitido"
    )

esperados = 2 + 7 * len(contratos) + 2

assert esperados == 25

assert len(aprobados) == esperados

print("\n=== RESUMEN HISTORICO COMPLEMENTARIO ===")
print("CONTROLES_ESTATICOS_COMPLEMENTARIOS: 25/25")
print("CONSULTAS_COMPLEMENTARIAS: 6/6")
print("PRODUCTOS: 3/3")
print("COLUMNAS_COMPARADAS: 140/140")
print("IGUALDAD_FILA_A_FILA_SQL_GENERADA: True")
print("PARTICION_Y_CLUSTERING_SQL_GENERADOS: True")
print("GOOGLESQL_ENGINE_VALIDADO: False")
print("IGUALDAD_REAL_BIGQUERY_VERIFICADA: False")
print("LAYOUT_REAL_BIGQUERY_VERIFICADO: False")
print("VALIDACION_HISTORICA_COMPLEMENTARIA_OK")
