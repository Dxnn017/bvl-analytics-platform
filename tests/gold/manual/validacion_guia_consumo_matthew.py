"""Controles estaticos de recursos de consumo."""

import hashlib
import json
import re

from pathlib import Path

from preparador_carga_historica import (
    cargar_contratos,
)

from generar_guia_consumo import (
    INDICADORES,
    COLUMNAS_CONSUMO,
    consulta_detalle,
    consulta_sab_diaria,
    SAB,
)


raiz = Path.cwd()

doc = raiz / "docs/bigquery/consumo_matthew"

_, contratos = cargar_contratos(raiz)

aprobados = []


def aprobar(nombre):

    assert nombre not in aprobados

    aprobados.append(nombre)

    print(nombre + "_OK")


inventario = json.loads(
    (
        doc / "inventario_guia_consumo.json"
    ).read_text()
)

catalogo = json.loads(
    (
        doc / "indicadores_gold_documentados.json"
    ).read_text()
)

assert len(inventario["files"]) == 5

for referencia in inventario["files"]:

    contenido = (
        raiz / referencia["path"]
    ).read_bytes()

    assert len(contenido) == referencia["size_bytes"]

    assert hashlib.sha256(
        contenido
    ).hexdigest() == referencia["sha256"]

aprobar("INTEGRIDAD_GUIA_CONSUMO")

indicadores = catalogo["indicators"]

assert len(indicadores) == len(INDICADORES) == 13

assert len({
    (i["product"], i["column"])
    for i in indicadores
}) == 13

aprobar("TRECE_INDICADORES_EXISTENTES")

for indicador in indicadores:

    producto = indicador["product"]

    campos = {
        c["name"]: c
        for c in contratos[producto]["gold"]["columns"]
    }

    nombre = indicador["column"]

    assert nombre in campos

    assert indicador["bigquery_type"] == (
        campos[nombre]["bigquery_type"]
    )

    for bandera in indicador["quality_flags"]:

        assert campos[bandera]["bigquery_type"] == "BOOL"

aprobar("INDICADORES_Y_FLAGS_COMPATIBLES")

assert all(
    i["automatic_aggregation"] is None
    and i["operatively_approved"] is False
    for i in indicadores
)

aprobar("SIN_AGREGACION_AUTOMATICA")

for producto, contrato in contratos.items():

    sufijo = producto.upper()

    sql = (
        doc / "sql_consumo"
        / f"{producto}_detalle.sql"
    ).read_text()

    assert sql == consulta_detalle(
        producto, contrato
    )

    aprobar("CONSULTA_DETERMINISTA_" + sufijo)

    columnas = COLUMNAS_CONSUMO[producto]

    assert set(
        contrato["gold"]["business_keys"]
    ).issubset(columnas)

    assert len(columnas) == len(set(columnas))

    aprobar("CLAVES_CONSERVADAS_" + sufijo)

    fecha = contrato["configuracion"][
        "bigquery_partition"
    ]

    assert (
        f"WHERE `{fecha}` BETWEEN"
    ) in sql

    assert "@fecha_inicio AND @fecha_fin" in sql

    aprobar("FILTRO_TEMPORAL_" + sufijo)

    seleccion = sql.split(
        "\nSELECT\n", 1
    )[1].split("\nFROM ", 1)[0]

    assert set(re.findall(
        r"`([A-Za-z0-9_]+)`",
        seleccion,
    )) == set(columnas)

    aprobar("PROYECCION_CONTRACTUAL_" + sufijo)


sab_diaria = (
    doc / "sql_consumo"
    / "actividad_sab_resumen_diario.sql"
).read_text()

assert sab_diaria == consulta_sab_diaria(
    contratos[SAB]
)

aprobar("RESUMEN_SAB_DETERMINISTA")

assert "GROUP BY fecha" in sab_diaria

assert "SUM(" not in sab_diaria

assert "MAX(total_actividad_diaria)" in sab_diaria

aprobar("EVITA_SUMA_TOTAL_DIARIO_REPETIDO")

assert "variantes_actividad = 1" in sab_diaria

assert "actividad_presente = filas_sab" in sab_diaria

assert "filas_sab = sab_distintos" in sab_diaria

assert "claves_invalidas = 0" in sab_diaria

aprobar("CONTROLES_CONSISTENCIA_SAB")

assert "filas_cobertura_completa = filas_sab" in sab_diaria

assert "MIXTA_O_INCONSISTENTE" in sab_diaria

assert "'PARCIAL'" in sab_diaria

assert "'DESCONOCIDA'" in sab_diaria

aprobar("COBERTURA_SAB_CONSERVADA")

assert "hhi_presente = filas_sab" in sab_diaria

assert "variantes_hhi = 1" in sab_diaria

assert "actividad_candidata > 0" in sab_diaria

aprobar("HHI_SIN_DOBLE_CONTEO")

consultas = sorted(
    (doc / "sql_consumo").glob("*.sql")
)

assert len(consultas) == 4

for ruta in consultas:

    sql = ruta.read_text()

    assert "ASSERT FALSE AS" in sql

    limpio = re.sub(
        r"(?m)^--.*$",
        "",
        sql,
    )

    assert not re.search(
        r"\b(INSERT|UPDATE|DELETE|MERGE|"
        r"CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE)\s",
        limpio,
        re.IGNORECASE,
    )

aprobar("CUATRO_SQL_BLOQUEADOS_SIN_DML_DDL")

for bandera in (
    "google_sql_engine_validated",
    "cloud_execution_authorized",
    "production_consumption_authorized",
    "publication_authorized",
):

    assert inventario[bandera] is False

aprobar("AUTORIZACIONES_DESACTIVADAS")

esperados = 4 + 4 * len(contratos) + 7

assert len(aprobados) == esperados == 23, aprobados

print("\n=== RESUMEN GUIA DE CONSUMO ===")
print("CONTROLES_GUIA_CONSUMO: 23/23")
print("INDICADORES_DOCUMENTADOS: 13/13")
print("SQL_CONSUMO: 4/4")
print("PRODUCTOS: 3/3")
print("CONTROLES_PREVIOS_REPETIDOS: False")
print("GOOGLESQL_ENGINE_VALIDADO: False")
print("CONSUMO_PRODUCTIVO_AUTORIZADO: False")
print("VALIDACION_GUIA_CONSUMO_OK")
