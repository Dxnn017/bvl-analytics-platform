"""Pruebas locales de conciliacion de perfiles.

Resultados SQL sinteticos, contratos reales.
Sin Spark ni clientes cloud.
"""

from copy import deepcopy
from pathlib import Path

from preparador_carga_historica import (
    cargar_contratos,
    PRODUCTOS,
)

from trazabilidad_carga_historica import (
    huella_documento,
)

from conciliar_validacion_historica import (
    conciliar_lote,
    metricas_calidad,
    comparar_numero,
)


_, contratos = cargar_contratos(Path.cwd())

MERCADO = "mercado_valor_diario"
SAB = "actividad_sab_diaria"
EMPRESA = "empresa_trimestre"

aprobados = []


def aprobar(nombre):

    aprobados.append(nombre)

    print(nombre + "_OK")


def fixture():

    origenes = {}
    resultados = {}

    for producto, cantidad in PRODUCTOS.items():

        contrato = contratos[producto]

        esquema = []

        perfil = {
            "registros": cantidad
        }

        for posicion, campo in enumerate(
            contrato["gold"]["columns"],
            start=1,
        ):

            nombre = campo["name"]

            tipo = campo["bigquery_type"]

            nullable = (
                "YES"
                if campo["nullable"]
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

            if tipo not in (
                "FLOAT64", "NUMERIC", "INT64"
            ):

                continue

            perfil["presentes__" + nombre] = (
                cantidad
            )

            perfil["nulos__" + nombre] = 0

            if tipo == "FLOAT64":

                valor = 100.25
                promedio = valor

            elif tipo == "INT64":

                valor = 100
                promedio = "100.00"

            else:

                valor = "100.25"
                promedio = valor

            perfil["min__" + nombre] = valor
            perfil["max__" + nombre] = valor
            perfil["avg__" + nombre] = promedio

        periodo = (
            "trimestre"
            if producto == EMPRESA
            else "mes"
        )

        fecha = (
            "2025-03-31"
            if producto == EMPRESA
            else "2025-01-02"
        )

        particiones = [{
            "anio": 2025,
            periodo: 1,
            "registros": cantidad,
            "fecha_minima": fecha,
            "fecha_maxima": fecha,
        }]

        calidad = {
            "producto": producto,
            "registros_esperados": cantidad,
            "registros_observados": cantidad,
            "controles_aprobados": True,
            **{
                metrica: 0
                for metrica in metricas_calidad(
                    contrato
                )
            },
        }

        origenes[producto] = {
            "product": producto,
            "version_id": "fixture-local-v1",
            "manifest_sha256": "a" * 64,
            "gold_contract_sha256":
                contrato["sha256"],
            "records_count": cantidad,
            "particiones": deepcopy(particiones),
            "indicadores": deepcopy(perfil),
        }

        resultados[producto] = {
            "calidad": calidad,
            "esquema": esquema,
            "particiones": particiones,
            "indicadores": perfil,
        }

    return origenes, resultados


def rechazar(nombre, modificar, mensaje):

    origenes, resultados = fixture()

    modificar(origenes, resultados)

    try:

        conciliar_lote(
            origenes,
            resultados,
            contratos,
        )

    except ValueError as error:

        assert mensaje in str(error), str(error)

        aprobar(nombre)

    else:

        raise AssertionError(
            "Debio rechazarse: " + nombre
        )


origenes, resultados = fixture()

informe = conciliar_lote(
    origenes,
    resultados,
    contratos,
)

assert informe["report"][
    "products_reconciled"
] == 3

assert informe["report"][
    "total_records"
] == 110199

assert informe["report_sha256"] == (
    huella_documento(informe["report"])
)

aprobar("CONCILIACION_TRES_PRODUCTOS")

assert conciliar_lote(
    origenes, resultados, contratos
) == informe

aprobar("INFORME_DETERMINISTA")

assert informe["report"][
    "production_authorized"
] is False

assert informe["report"][
    "official_sources_verified"
] is False

aprobar("INFORME_NO_OPERATIVO")

rechazar(
    "PRODUCTO_FALTANTE_RECHAZADO",
    lambda o, r: r.pop(SAB),
    "Lote historico incompleto",
)

rechazar(
    "CONTEO_INCORRECTO_RECHAZADO",
    lambda o, r: r[MERCADO]["calidad"].update(
        registros_observados=1
    ),
    "Conteo historico no conciliado",
)

rechazar(
    "DUPLICADOS_RECHAZADOS",
    lambda o, r: r[MERCADO]["calidad"].update(
        claves_duplicadas=1
    ),
    "Control de calidad rechazado",
)

rechazar(
    "CLAVES_NULAS_RECHAZADAS",
    lambda o, r: r[MERCADO]["calidad"].update(
        clave_invalida__nemonico=1
    ),
    "Control de calidad rechazado",
)

rechazar(
    "METRICA_FALTANTE_RECHAZADA",
    lambda o, r: r[MERCADO]["calidad"].pop(
        "particiones_invalidas"
    ),
    "Resultado de calidad incompleto",
)

rechazar(
    "ESQUEMA_INCORRECTO_RECHAZADO",
    lambda o, r: r[MERCADO]["esquema"][0].update(
        tipo_observado="INT64"
    ),
    "Diferencia de esquema",
)

rechazar(
    "COLUMNA_FALTANTE_RECHAZADA",
    lambda o, r: r[SAB]["esquema"].pop(),
    "Cantidad de columnas incompatible",
)

rechazar(
    "PARTICION_INVALIDA_RECHAZADA",
    lambda o, r:
        r[EMPRESA]["particiones"][0].update(
            trimestre=2
        ),
    "Fecha fuera de particion",
)

rechazar(
    "CONTEO_PARTICION_RECHAZADO",
    lambda o, r:
        r[MERCADO]["particiones"][0].update(
            registros=1
        ),
    "Total por particiones incompatible",
)

rechazar(
    "PERIODO_DIFERENTE_RECHAZADO",
    lambda o, r:
        r[MERCADO]["particiones"][0].update(
            mes=2,
            fecha_minima="2025-02-03",
            fecha_maxima="2025-02-03",
        ),
    "Particiones no conciliadas",
)

rechazar(
    "PERFIL_MONETARIO_ALTERADO_RECHAZADO",
    lambda o, r:
        r[SAB]["indicadores"].update(
            avg__total_compras_diario="100.26"
        ),
    "Perfil numerico no conciliado",
)

rechazar(
    "PERFIL_FLOAT_ALTERADO_RECHAZADO",
    lambda o, r:
        r[MERCADO]["indicadores"].update(
            avg__precio_cierre=999.0
        ),
    "Perfil numerico no conciliado",
)

rechazar(
    "PERFIL_NAN_RECHAZADO",
    lambda o, r:
        r[MERCADO]["indicadores"].update(
            avg__precio_cierre=float("nan")
        ),
    "FLOAT64 no finito",
)

rechazar(
    "PERFIL_NULO_INCONSISTENTE_RECHAZADO",
    lambda o, r:
        r[MERCADO]["indicadores"].update(
            avg__precio_cierre=None
        ),
    "Nulabilidad de perfil inconsistente",
)

rechazar(
    "CONTRATO_ALTERADO_RECHAZADO",
    lambda o, r: o[SAB].update(
        gold_contract_sha256="0" * 64
    ),
    "Contrato Gold incompatible",
)

assert comparar_numero(
    100.25,
    100.250000001,
    "FLOAT64",
)

aprobar("TOLERANCIA_FLOAT_ACEPTADA")

assert not comparar_numero(
    "99999999999999999999999999999.98",
    "99999999999999999999999999999.99",
    "NUMERIC",
)

aprobar("PRECISION_MONETARIA_CONSERVADA")

assert len(aprobados) == 20, aprobados

print("\n=== RESUMEN DE CONCILIACION ===")
print("CONTROLES_CONCILIACION: 20/20")
print("PRODUCTOS_CONCILIADOS_SINTETICOS: 3/3")
print("REGISTROS_REFERENCIA_SINTETICOS: 110199")
print("RESULTADOS_SQL_SINTETICOS: True")
print("COMPARACION_MONETARIA_EXACTA: True")
print("TOLERANCIA_FLOAT_CANDIDATA: True")
print("IGUALDAD_FILA_A_FILA_ACREDITADA: False")
print("FUENTES_OFICIALES_ACREDITADAS: False")
print("AUTORIZACION_OPERATIVA_REAL: False")
print("GCP_EJECUTADO: False")
print("BIGQUERY_EJECUTADO: False")
print("CONCILIACION_HISTORICA_LOCAL_OK")
