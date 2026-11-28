"""Validacion sintetica de compatibilidad SQL-Python."""

import re

from pathlib import Path
from datetime import date, datetime
from decimal import Decimal
from copy import deepcopy

from preparador_carga_historica import (
    cargar_contratos,
    PRODUCTOS,
)

from generar_validaciones_historicas import (
    consulta_calidad,
    consulta_particiones,
    consulta_esquema,
    consulta_indicadores,
)

from adaptador_conciliacion_historica import (
    esquema_calidad,
    esquema_particiones,
    esquema_columnas,
    esquema_indicadores,
    esquema_igualdad,
    esquema_layout,
    normalizar_escalar,
    conciliar_historico_completo_local,
)

from generar_controles_historicos_complementarios import (
    generar_igualdad,
    generar_layout,
)

from trazabilidad_carga_historica import (
    huella_documento,
)


raiz = Path.cwd()

_, contratos = cargar_contratos(raiz)

aprobados = []

MERCADO = "mercado_valor_diario"
SAB = "actividad_sab_diaria"


def aprobar(nombre):

    assert nombre not in aprobados

    aprobados.append(nombre)

    print(nombre + "_OK", flush=True)


def rechazar(nombre, funcion):

    try:

        funcion()

    except ValueError:

        aprobar(nombre)

    else:

        raise AssertionError(
            "Debio rechazarse: " + nombre
        )


def fixture():

    origenes = {}
    resultados = {}
    complementarios = {}

    for producto, cantidad in PRODUCTOS.items():

        contrato = contratos[producto]

        calidad = {
            nombre: 0
            for nombre in esquema_calidad(
                contrato
            )
        }

        calidad.update(
            producto=producto,
            registros_esperados=cantidad,
            registros_observados=cantidad,
            controles_aprobados=True,
        )

        esquema = []

        layout = []

        perfil = {
            "registros": cantidad
        }

        config = contrato["configuracion"]

        for posicion, campo in enumerate(
            contrato["gold"]["columns"],
            start=1,
        ):

            nombre = campo["name"]

            tipo = campo["bigquery_type"]

            nullable = (
                "YES" if campo["nullable"]
                else "NO"
            )

            esquema.append({
                "columna": nombre,
                "posicion_esperada": posicion,
                "posicion_observada": posicion,
                "tipo_esperado": tipo,
                "tipo_observado": tipo,
                "nullable_esperado": nullable,
                "nullable_observado": nullable,
                "estado": "OK",
            })

            particion = (
                "YES"
                if nombre == config[
                    "bigquery_partition"
                ]
                else "NO"
            )

            orden = (
                config["clustering"].index(
                    nombre
                ) + 1
                if nombre in config["clustering"]
                else None
            )

            layout.append({
                "producto": producto,
                "columna": nombre,
                "particion_esperada": particion,
                "particion_observada": particion,
                "clustering_esperado": orden,
                "clustering_observado": orden,
                "estado": "OK",
            })

            if tipo not in (
                "INT64", "NUMERIC", "FLOAT64"
            ):

                continue

            perfil[
                "presentes__" + nombre
            ] = cantidad

            perfil[
                "nulos__" + nombre
            ] = 0

            if tipo == "INT64":

                valor = 100
                promedio = Decimal("100.00")

            elif tipo == "NUMERIC":

                valor = Decimal("100.25")
                promedio = valor

            else:

                valor = 100.25
                promedio = valor

            perfil["min__" + nombre] = valor
            perfil["max__" + nombre] = valor
            perfil["avg__" + nombre] = promedio

        empresa = (
            producto == "empresa_trimestre"
        )

        periodo = (
            "trimestre" if empresa
            else "mes"
        )

        fecha = (
            date(2025, 3, 31)
            if empresa
            else date(2025, 1, 2)
        )

        particiones = [{
            "anio": 2025,
            periodo: 1,
            "registros": cantidad,
            "fecha_minima": fecha,
            "fecha_maxima": fecha,
        }]

        igualdad = {
            nombre: 0
            for nombre in esquema_igualdad()
        }

        igualdad.update(
            producto=producto,
            filas_esperadas=cantidad,
            filas_origen=cantidad,
            filas_destino=cantidad,
            filas_iguales=cantidad,
            columnas_comparadas=len(
                contrato["gold"]["columns"]
            ),
            igualdad_aprobada=True,
        )

        origenes[producto] = {
            "product": producto,
            "version_id": "fixture-local-v1",
            "manifest_sha256": "a" * 64,
            "gold_contract_sha256":
                contrato["sha256"],
            "records_count": cantidad,
            "particiones": deepcopy(
                particiones
            ),
            "indicadores": deepcopy(perfil),
        }

        resultados[producto] = {
            "calidad": calidad,
            "esquema": esquema,
            "particiones": particiones,
            "indicadores": perfil,
        }

        complementarios[producto] = {
            "igualdad": igualdad,
            "layout": layout,
        }

    return (
        origenes,
        resultados,
        complementarios,
    )


# Compatibilidad estatica con SQL real generado.

for producto, contrato in contratos.items():

    sql_calidad = consulta_calidad(
        producto, contrato
    )

    sql_particiones = consulta_particiones(
        producto, contrato
    )

    sql_esquema = consulta_esquema(
        producto, contrato
    )

    sql_indicadores = consulta_indicadores(
        producto, contrato
    )

    sql_igualdad = generar_igualdad(
        producto,
        contrato,
        "BATCH_CANDIDATO",
    )

    sql_layout = generar_layout(
        producto, contrato
    )

    interfaces = (
        (
            sql_calidad,
            esquema_calidad(contrato),
        ),
        (
            sql_particiones,
            esquema_particiones(contrato),
        ),
        (
            sql_esquema,
            esquema_columnas(),
        ),
        (
            sql_indicadores,
            esquema_indicadores(contrato),
        ),
        (
            sql_igualdad,
            esquema_igualdad(),
        ),
        (
            sql_layout,
            esquema_layout(),
        ),
    )

    for sql, campos in interfaces:

        for campo in campos:

            assert re.search(
                r"\b" + re.escape(campo) + r"\b",
                sql,
            ), (producto, campo)

    aprobar(
        "ALIASES_SEIS_CONSULTAS_"
        + producto.upper()
    )


origenes, resultados, complementarios = (
    fixture()
)


def conciliar():

    return conciliar_historico_completo_local(
        origenes,
        resultados,
        complementarios,
        contratos,
    )


informe = conciliar()

assert informe["report"][
    "products_reconciled"
] == 3

assert informe["report"][
    "total_records"
] == 110199

aprobar("CONCILIACION_COMPLETA_TRES_PRODUCTOS")

assert informe["report_sha256"] == (
    huella_documento(informe["report"])
)

aprobar("HUELLA_INFORME_CORRECTA")

assert conciliar() == informe

aprobar("INFORME_DETERMINISTA")

for bandera in (
    "official_sources_verified",
    "google_sql_engine_validated",
    "row_level_equality_verified",
    "physical_table_layout_verified",
    "production_authorized",
    "publication_authorized",
):

    assert informe["report"][bandera] is False

aprobar("INFORME_NO_OPERATIVO")


# Normalizacion exacta de tipos.

assert normalizar_escalar(
    date(2025, 1, 2),
    "DATE",
) == "2025-01-02"

aprobar("DATE_ISO_CONSERVADO")

assert normalizar_escalar(
    Decimal(
        "99999999999999999999999999999.99"
    ),
    "NUMERIC",
) == "99999999999999999999999999999.99"

aprobar("NUMERIC_LIMITE_EXACTO")

assert normalizar_escalar(
    Decimal("1E-38"),
    "BIGNUMERIC",
) == "0." + "0" * 37 + "1"

aprobar("BIGNUMERIC_SIN_NOTACION_CIENTIFICA")

assert normalizar_escalar(
    None,
    "NUMERIC",
) is None

aprobar("NULL_CONSERVADO")

rechazar(
    "DECIMAL_FLOAT_RECHAZADO",
    lambda: normalizar_escalar(
        100.25, "NUMERIC"
    ),
)

rechazar(
    "DECIMAL_NAN_RECHAZADO",
    lambda: normalizar_escalar(
        Decimal("NaN"), "NUMERIC"
    ),
)

rechazar(
    "DATE_DATETIME_RECHAZADO",
    lambda: normalizar_escalar(
        datetime(2025, 1, 2),
        "DATE",
    ),
)

rechazar(
    "BOOL_COMO_ENTERO_RECHAZADO",
    lambda: normalizar_escalar(
        True, "INT64"
    ),
)

rechazar(
    "INT64_DESBORDADO_RECHAZADO",
    lambda: normalizar_escalar(
        2**63, "INT64"
    ),
)

rechazar(
    "FLOAT_INFINITO_RECHAZADO",
    lambda: normalizar_escalar(
        float("inf"), "FLOAT64"
    ),
)


# Alteraciones de resultados.

def rechazar_modificacion(
    nombre,
    modificar,
):

    o, r, c = fixture()

    modificar(o, r, c)

    rechazar(
        nombre,
        lambda: conciliar_historico_completo_local(
            o, r, c, contratos
        ),
    )


rechazar_modificacion(
    "ALIAS_FALTANTE_RECHAZADO",
    lambda o, r, c:
        r[MERCADO]["calidad"].pop(
            "registros_observados"
        ),
)

rechazar_modificacion(
    "ALIAS_ADICIONAL_RECHAZADO",
    lambda o, r, c:
        r[MERCADO]["calidad"].update(
            inesperado=0
        ),
)

rechazar_modificacion(
    "PROMEDIO_FLOAT_INCOHERENTE",
    lambda o, r, c:
        r[MERCADO]["indicadores"].update(
            avg__precio_cierre=500.0
        ),
)

rechazar_modificacion(
    "PROMEDIO_DECIMAL_INCOHERENTE",
    lambda o, r, c:
        r[SAB]["indicadores"].update(
            avg__total_compras_diario=Decimal(
                "999.99"
            )
        ),
)

for metrica in (
    "filas_faltantes",
    "filas_adicionales",
    "filas_modificadas",
    "claves_invalidas_origen",
    "claves_duplicadas_destino",
):

    rechazar_modificacion(
        "RECHAZO_" + metrica.upper(),
        lambda o, r, c, m=metrica:
            c[MERCADO]["igualdad"].update(
                {m: 1}
            ),
    )

rechazar_modificacion(
    "IGUALDAD_FALSE_RECHAZADA",
    lambda o, r, c:
        c[MERCADO]["igualdad"].update(
            igualdad_aprobada=False
        ),
)

rechazar_modificacion(
    "CONTEO_IGUALDAD_RECHAZADO",
    lambda o, r, c:
        c[MERCADO]["igualdad"].update(
            filas_destino=1
        ),
)

rechazar_modificacion(
    "LAYOUT_PARTICION_RECHAZADA",
    lambda o, r, c:
        next(
            fila for fila
            in c[MERCADO]["layout"]
            if fila["columna"] == "fecha"
        ).update(
            particion_observada="NO"
        ),
)

rechazar_modificacion(
    "LAYOUT_CLUSTERING_RECHAZADO",
    lambda o, r, c:
        next(
            fila for fila
            in c[MERCADO]["layout"]
            if fila["columna"] == "nemonico"
        ).update(
            clustering_observado=2
        ),
)

rechazar_modificacion(
    "LAYOUT_COLUMNA_REPETIDA",
    lambda o, r, c:
        c[MERCADO]["layout"].__setitem__(
            1,
            deepcopy(
                c[MERCADO]["layout"][0]
            ),
        ),
)

rechazar_modificacion(
    "LAYOUT_COLUMNA_FALTANTE",
    lambda o, r, c:
        c[MERCADO]["layout"].pop(),
)

rechazar_modificacion(
    "PRODUCTO_COMPLEMENTARIO_FALTANTE",
    lambda o, r, c: c.pop(SAB),
)

# 3 aliases + 4 conciliacion + 10 tipos
# + 16 rechazos de alteraciones.

esperados = 3 + 4 + 10 + 16

assert len(aprobados) == esperados, (
    len(aprobados),
    aprobados,
)

print("\n=== RESUMEN COMPATIBILIDAD HISTORICA ===")
print(
    "CONTROLES_ADAPTADOR_HISTORICO:",
    f"{len(aprobados)}/{esperados}",
)
print("INTERFACES_SQL_REVISADAS: 18/18")
print("PRODUCTOS_CONCILIADOS_SINTETICOS: 3/3")
print("REGISTROS_REFERENCIA_SINTETICOS: 110199")
print("PRECISION_DECIMAL_CONSERVADA: True")
print("TIPOS_SQL_PYTHON_COMPATIBLES_LOCALMENTE: True")
print("RESULTADOS_IGUALDAD_SINTETICOS_VALIDADOS: True")
print("RESULTADOS_LAYOUT_SINTETICOS_VALIDADOS: True")
print("GOOGLESQL_ENGINE_VALIDADO: False")
print("FUENTES_OFICIALES_ACREDITADAS: False")
print("IGUALDAD_REAL_BIGQUERY_VERIFICADA: False")
print("LAYOUT_REAL_BIGQUERY_VERIFICADO: False")
print("GCP_EJECUTADO: False")
print("BIGQUERY_EJECUTADO: False")
print("PUBLICACION_AUTORIZADA: False")
print("COMPATIBILIDAD_CONCILIADOR_HISTORICO_OK")
