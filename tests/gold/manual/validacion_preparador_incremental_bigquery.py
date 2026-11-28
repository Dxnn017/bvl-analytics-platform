"""Validacion del nuevo puente incremental.

Contratos reales y datos sinteticos.
Integracion con SQLite local existente.
Sin ejecucion GCP ni BigQuery.
"""

import sqlite3

from copy import deepcopy
from decimal import Decimal
from pathlib import Path

from preparador_incremental import (
    cargar_contratos_incrementales,
    preparar_batch_incremental,
    normalizar_fila,
    normalizar_decimal,
    exigir_autorizacion_operativa,
    COLUMNAS_STAGING,
)

from trazabilidad_carga_historica import (
    huella_documento,
)

from src.gold.bigquery.verificar_payload_local import (
    calcular_sha256,
)

from src.gold.bigquery.batch_atomico_local import (
    procesar_batch,
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
    mensaje=None,
):

    try:

        funcion()

    except excepcion as error:

        if mensaje is not None:

            assert mensaje in str(error), (
                nombre, str(error)
            )

        aprobar(nombre)

    else:

        raise AssertionError(
            "Debio rechazarse: " + nombre
        )


def fixture(producto, numero=1):

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

            valor = 100.25

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

    else:

        fila["id_registro"] = (
            f"{numero:064x}"
        )

        fila["fecha_fin_trimestre"] = "2025-03-31"
        fila["ejercicio"] = 2025
        fila["trimestre_num"] = 1
        fila["ruc_valido"] = True
        fila["colision_clave_negocio"] = False
        fila["colision_financiera"] = False
        fila["cantidad_clave_negocio"] = 1

    fila["operation"] = "UPSERT"

    return fila


def preparar(
    producto,
    filas=None,
    **cambios,
):

    if filas is None:

        filas = [
            fixture(producto, 1),
            fixture(producto, 2),
        ]

    contrato = contratos[producto]

    argumentos = {
        "batch_id": "batch-fixture-001",
        "run_id": "run-fixture-001",
        "source_version": "silver-fixture-v2",
        "previous_gold_version": "gold-fixture-v1",
        "new_gold_version": "gold-fixture-v2",
        "current_gold_version": "gold-fixture-v1",
        "source_manifest_sha256": "a" * 64,
        "expected_rows": len(filas),
        "payload_sha256_declarado": "0" * 64,
    }

    if "payload_sha256_declarado" not in cambios:

        # Solo fixtures validos requieren una
        # huella candidata calculada previamente.

        try:

            normalizadas = [
                normalizar_fila(
                    producto,
                    fila,
                    contrato,
                )[1]
                for fila in filas
            ]

        except ValueError:

            pass

        else:

            argumentos[
                "payload_sha256_declarado"
            ] = calcular_sha256(normalizadas)

    argumentos.update(cambios)

    return preparar_batch_incremental(
        producto,
        filas,
        contrato,
        **argumentos,
    )


def fila_modificada(producto, **cambios):

    fila = fixture(producto)

    fila.update(cambios)

    return [fila]


# ==========================================
# 1. TRES PRODUCTOS Y ESQUEMAS STAGING
# ==========================================

paquetes = {}

for producto in contratos:

    paquete = preparar(producto)

    paquetes[producto] = paquete

    evidencia = paquete["evidence"]

    assert evidencia["records_count"] == 2

    assert evidencia["staging_columns"] == (
        COLUMNAS_STAGING[producto]
    )

    assert evidencia["operations"] == {
        "UPSERT": 2,
        "DELETE": 0,
    }

    assert evidencia["execution_authorized"] is False

    for fila in paquete["staging"]:

        assert list(fila) == (
            contratos[producto]["staging_columns"]
        )

        assert fila["operation"] == "UPSERT"
        assert fila["delete_authorized"] is False

        assert fila["payload_sha256"] == (
            evidencia["payload_sha256"]
        )

    assert calcular_sha256(
        paquete["payload"]
    ) == evidencia["payload_sha256"]

    assert huella_documento(
        paquete["staging"]
    ) == evidencia["staging_content_sha256"]

    assert huella_documento(
        evidencia
    ) == paquete["evidence_sha256"]

    assert preparar(producto) == paquete

    aprobar("STAGING_" + producto.upper())


# ==========================================
# 2. OPERACIONES, CLAVES Y CALIDAD
# ==========================================

rechazar(
    "DELETE_RECHAZADO",
    lambda: preparar(
        MERCADO,
        fila_modificada(
            MERCADO, operation="DELETE"
        ),
    ),
)

rechazar(
    "OPERACION_DESCONOCIDA_RECHAZADA",
    lambda: preparar(
        MERCADO,
        fila_modificada(
            MERCADO, operation="INSERT"
        ),
    ),
)

rechazar(
    "OPERACION_NULA_RECHAZADA",
    lambda: preparar(
        MERCADO,
        fila_modificada(
            MERCADO, operation=None
        ),
    ),
)

rechazar(
    "BATCH_VACIO_RECHAZADO",
    lambda: preparar(MERCADO, []),
)

rechazar(
    "CONTEO_INCORRECTO_RECHAZADO",
    lambda: preparar(
        MERCADO, expected_rows=3
    ),
)

rechazar(
    "CLAVE_DUPLICADA_RECHAZADA",
    lambda: preparar(
        MERCADO,
        [fixture(MERCADO), fixture(MERCADO)],
    ),
    mensaje="Clave incremental duplicada",
)

rechazar(
    "CLAVE_NULA_RECHAZADA",
    lambda: preparar(
        MERCADO,
        fila_modificada(
            MERCADO, nemonico=None
        ),
    ),
)

rechazar(
    "CLAVE_VACIA_RECHAZADA",
    lambda: preparar(
        MERCADO,
        fila_modificada(
            MERCADO, nemonico="   "
        ),
    ),
)

rechazar(
    "NEMONICO_NO_CANONICO_RECHAZADO",
    lambda: preparar(
        MERCADO,
        fila_modificada(
            MERCADO, nemonico="fix1"
        ),
    ),
)

rechazar(
    "PARTICION_INCORRECTA_RECHAZADA",
    lambda: preparar(
        MERCADO,
        fila_modificada(
            MERCADO, mes=12
        ),
    ),
)

rechazar(
    "TRIMESTRE_INCORRECTO_RECHAZADO",
    lambda: preparar(
        EMPRESA,
        fila_modificada(
            EMPRESA, trimestre=2
        ),
    ),
)

rechazar(
    "FECHA_INVALIDA_RECHAZADA",
    lambda: preparar(
        SAB,
        fila_modificada(
            SAB, fecha="2025-02-30"
        ),
    ),
)

incompleta = fixture(MERCADO)

incompleta.pop("precio_cierre")

rechazar(
    "COLUMNA_FALTANTE_RECHAZADA",
    lambda: preparar(
        MERCADO, [incompleta]
    ),
)

rechazar(
    "COLUMNA_ADICIONAL_RECHAZADA",
    lambda: preparar(
        MERCADO,
        fila_modificada(
            MERCADO, campo_extra=1
        ),
    ),
)

rechazar(
    "OBLIGATORIO_NULO_RECHAZADO",
    lambda: preparar(
        SAB,
        fila_modificada(
            SAB, montos_validos=None
        ),
    ),
)

rechazar(
    "TIPO_INVALIDO_RECHAZADO",
    lambda: preparar(
        MERCADO,
        fila_modificada(
            MERCADO, precio_cierre="100"
        ),
    ),
)

rechazar(
    "NAN_RECHAZADO",
    lambda: preparar(
        MERCADO,
        fila_modificada(
            MERCADO,
            precio_cierre=float("nan"),
        ),
    ),
)

rechazar(
    "INFINITO_RECHAZADO",
    lambda: preparar(
        MERCADO,
        fila_modificada(
            MERCADO,
            precio_cierre=float("inf"),
        ),
    ),
)

rechazar(
    "ENTERO_DESBORDADO_RECHAZADO",
    lambda: preparar(
        MERCADO,
        fila_modificada(
            MERCADO,
            dias_desde_cierre_anterior=2**31,
        ),
    ),
)

# ==========================================
# 3. PRECISION NUMERICA
# ==========================================

maximo = "9" * 29 + ".99"

assert normalizar_decimal(
    Decimal(maximo),
    "decimal(34,2)",
) == maximo

aprobar("DECIMAL_LIMITE_POSITIVO")

assert normalizar_decimal(
    "-" + maximo,
    "decimal(34,2)",
) == "-" + maximo

aprobar("DECIMAL_LIMITE_NEGATIVO")

assert normalizar_decimal(
    "100.2500",
    "decimal(24,2)",
) == "100.25"

aprobar("DECIMAL_ESCALA_EXACTA")

rechazar(
    "DECIMAL_DESBORDADO_RECHAZADO",
    lambda: preparar(
        SAB,
        fila_modificada(
            SAB,
            total_compras_diario=(
                "1" + "0" * 29 + ".00"
            ),
        ),
    ),
)

rechazar(
    "REDONDEO_RECHAZADO",
    lambda: preparar(
        SAB,
        fila_modificada(
            SAB, monto_compras="100.251"
        ),
    ),
)

rechazar(
    "DECIMAL_FLOAT_RECHAZADO",
    lambda: preparar(
        SAB,
        fila_modificada(
            SAB, monto_compras=100.25
        ),
    ),
)

paquete_nulo = preparar(
    SAB,
    fila_modificada(
        SAB, total_compras_diario=None
    ),
)

assert paquete_nulo["staging"][0][
    "total_compras_diario"
] is None

aprobar("DECIMAL_NULL_CONSERVADO")


# ==========================================
# 4. COBERTURA SAB
# ==========================================

fila_sab = paquetes[SAB]["staging"][0]

assert fila_sab["estado_cobertura"] == "DESCONOCIDA"
assert fila_sab["cobertura_completa"] is False
assert fila_sab["registros_cuarentena"] is None
assert fila_sab["archivos_excluidos"] is None

aprobar("COBERTURA_DESCONOCIDA_CONSERVADA")

rechazar(
    "COBERTURA_INVENTADA_RECHAZADA",
    lambda: preparar(
        SAB,
        fila_modificada(
            SAB, cobertura_completa=True
        ),
    ),
)

rechazar(
    "COBERTURA_SIN_SUSTENTO_RECHAZADA",
    lambda: preparar(
        SAB,
        fila_modificada(
            SAB,
            estado_cobertura="COMPLETA",
            cobertura_completa=True,
        ),
    ),
)

rechazar(
    "ELEGIBLES_INCONSISTENTES_RECHAZADOS",
    lambda: preparar(
        SAB,
        fila_modificada(
            SAB, registros_elegibles=99
        ),
    ),
)

rechazar(
    "CUARENTENA_NEGATIVA_RECHAZADA",
    lambda: preparar(
        SAB,
        fila_modificada(
            SAB, registros_cuarentena=-1
        ),
    ),
)

# ==========================================
# 5. VERSIONES, HASH Y TRAZABILIDAD
# ==========================================

rechazar(
    "VERSION_ANTERIOR_INCOMPATIBLE",
    lambda: preparar(
        MERCADO,
        current_gold_version="gold-fixture-v0",
    ),
)

rechazar(
    "VERSION_SIN_AVANCE_RECHAZADA",
    lambda: preparar(
        MERCADO,
        new_gold_version="gold-fixture-v1",
    ),
)

rechazar(
    "VERSION_INVALIDA_RECHAZADA",
    lambda: preparar(
        MERCADO,
        source_version="../insegura",
    ),
)

rechazar(
    "BATCH_ID_INVALIDO_RECHAZADO",
    lambda: preparar(
        MERCADO,
        batch_id="../inseguro",
    ),
)

rechazar(
    "MANIFEST_HASH_INVALIDO_RECHAZADO",
    lambda: preparar(
        MERCADO,
        source_manifest_sha256="incorrecto",
    ),
)

rechazar(
    "PAYLOAD_HASH_INCORRECTO_RECHAZADO",
    lambda: preparar(
        MERCADO,
        payload_sha256_declarado="0" * 64,
    ),
)

original = paquetes[MERCADO]

invertido = preparar(
    MERCADO,
    [fixture(MERCADO, 2), fixture(MERCADO, 1)],
)

assert original["evidence"]["payload_sha256"] != (
    invertido["evidence"]["payload_sha256"]
)

aprobar("ORDEN_PAYLOAD_PROTEGIDO")

otro_run = preparar(
    MERCADO,
    run_id="run-fixture-002",
)

assert original["evidence"]["payload_sha256"] == (
    otro_run["evidence"]["payload_sha256"]
)

assert original["evidence"]["staging_content_sha256"] != (
    otro_run["evidence"]["staging_content_sha256"]
)

aprobar("METADATOS_STAGING_VINCULADOS")


# ==========================================
# 6. INTEGRACION CON BATCH ATOMICO REAL LOCAL
# ==========================================

def aplicar_local(db, producto, paquete):

    evidencia = paquete["evidence"]

    batch = evidencia["batch_id"]
    run = evidencia["run_id"]
    version = evidencia["source_version"]

    # Estas referencias son fixtures exclusivos
    # del simulador SQLite. No son acreditaciones.

    return procesar_batch(
        db,
        producto,
        batch,
        run,
        version,
        paquete["payload"],
        hash_declarado=evidencia["payload_sha256"],
        versiones_oficiales={
            producto: version
        },
        autorizaciones_quality={
            (producto, batch): version
        },
    )


CAMPO_CAMBIO = {
    MERCADO: "precio_cierre",
    SAB: "monto_compras",
    EMPRESA: "activo_total",
}

for producto, paquete in paquetes.items():

    db = sqlite3.connect(":memory:")

    try:

        primero = aplicar_local(
            db, producto, paquete
        )

        assert primero["status"] == "COMPLETED"
        assert primero["inserted_rows"] == 2
        assert primero["updated_rows"] == 0
        assert primero["deleted_rows"] == 0

        aprobar(
            "PUENTE_INSERT_" + producto.upper()
        )

        replay = aplicar_local(
            db, producto, paquete
        )

        assert replay["status"] == "REPLAY_NOOP"

        assert db.execute(
            "SELECT COUNT(*) FROM batches"
        ).fetchone()[0] == 1

        assert db.execute(
            "SELECT COUNT(*) FROM auditoria"
        ).fetchone()[0] == 1

        aprobar(
            "PUENTE_REPLAY_" + producto.upper()
        )

        segundo = preparar(
            producto,
            batch_id="batch-fixture-002",
            run_id="run-fixture-002",
            source_version="silver-fixture-v3",
            previous_gold_version="gold-fixture-v2",
            current_gold_version="gold-fixture-v2",
            new_gold_version="gold-fixture-v3",
        )

        noop = aplicar_local(
            db, producto, segundo
        )

        assert noop["inserted_rows"] == 0
        assert noop["updated_rows"] == 0
        assert noop["unchanged_rows"] == 2
        assert noop["deleted_rows"] == 0

        aprobar(
            "PUENTE_NOOP_" + producto.upper()
        )

        filas = [
            fixture(producto, 1),
            fixture(producto, 2),
        ]

        filas[0][CAMPO_CAMBIO[producto]] = (
            "120.50"
            if producto == SAB
            else 120.50
        )

        tercero = preparar(
            producto,
            filas,
            batch_id="batch-fixture-003",
            run_id="run-fixture-003",
            source_version="silver-fixture-v4",
            previous_gold_version="gold-fixture-v3",
            current_gold_version="gold-fixture-v3",
            new_gold_version="gold-fixture-v4",
        )

        actualizado = aplicar_local(
            db, producto, tercero
        )

        assert actualizado["inserted_rows"] == 0
        assert actualizado["updated_rows"] == 1
        assert actualizado["unchanged_rows"] == 1
        assert actualizado["deleted_rows"] == 0

        assert db.execute(
            "SELECT COUNT(*) FROM datos"
        ).fetchone()[0] == 2

        aprobar(
            "PUENTE_UPDATE_" + producto.upper()
        )

        incompatible = preparar(
            producto, filas
        )

        rechazar(
            "PUENTE_HASH_REUTILIZADO_" + producto.upper(),
            lambda: aplicar_local(
                db, producto, incompatible
            ),
        )

        assert db.execute(
            "SELECT COUNT(*) FROM datos"
        ).fetchone()[0] == 2

        assert db.execute(
            "SELECT COUNT(*) FROM batches"
        ).fetchone()[0] == 3

        assert db.execute(
            "SELECT COUNT(*) FROM auditoria"
        ).fetchone()[0] == 3

    finally:

        db.close()


# ==========================================
# 7. BLOQUEO OPERATIVO
# ==========================================

rechazar(
    "EJECUCION_INCREMENTAL_BLOQUEADA",
    lambda: exigir_autorizacion_operativa(
        paquetes
    ),
    excepcion=PermissionError,
)

assert len(aprobados) == 58, aprobados

print("\n=== RESUMEN INCREMENTAL BIGQUERY ===")
print("CONTROLES_INCREMENTALES: 58/58")
print("PRODUCTOS_COMPATIBLES: 3/3")
print("COLUMNAS_STAGING: 158/158")
print("HASH_ORIGINAL_REUTILIZADO: True")
print("PREPARADOR_ATOMICO_LOCAL_INTEGRADO: True")
print("INSERT_LOCAL: 3/3")
print("UPDATE_LOCAL: 3/3")
print("NOOP_LOCAL: 3/3")
print("REPLAY_LOCAL: 3/3")
print("DUPLICACION_DE_DATOS: False")
print("DELETE_EJECUTADO: False")
print("COBERTURA_SAB_CONSERVADA: True")
print("PRECISION_MONETARIA_CONSERVADA: True")
print("DATOS_NEGOCIO_SINTETICOS: True")
print("AUTORIZACIONES_SQLITE_SIMULADAS: True")
print("DETECTOR_GOLD_INTEGRADO: False")
print("TRANSFERENCIA_PARQUET_INCREMENTAL_PROBADA: False")
print("AUTORIZACION_OPERATIVA_REAL: False")
print("GCP_EJECUTADO: False")
print("BIGQUERY_EJECUTADO: False")
print("PUBLICACION_AUTORIZADA: False")
print("VALIDACION_PREPARADOR_INCREMENTAL_OK")
