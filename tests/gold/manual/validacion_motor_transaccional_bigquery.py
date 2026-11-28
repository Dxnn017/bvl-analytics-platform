"""Validacion funcional del motor transaccional candidato.

SQLite local y contratos Gold reales.
Datos exclusivamente sinteticos.
Sin ejecucion GCP ni BigQuery.
"""

import json
import sqlite3

from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

from preparador_incremental import (
    cargar_contratos_incrementales,
    preparar_batch_incremental,
    normalizar_fila,
)

from motor_transaccional_local import (
    inicializar_control_local,
    inicializar_snapshot_sintetico,
    procesar_incremental_local,
    exigir_autorizacion_operativa,
    contenido_gold,
    clave_canonica,
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

    assert nombre not in aprobados

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
        fila["cantidad_sab_diaria"] = 4
        fila["registros_elegibles"] = 4
        fila["registros_cuarentena"] = None
        fila["archivos_excluidos"] = None
        fila["scope_quality_id"] = None

    else:

        fila["id_registro"] = f"{numero:064x}"
        fila["fecha_fin_trimestre"] = "2025-03-31"
        fila["ejercicio"] = 2025
        fila["trimestre_num"] = 1

    return fila


def preparar(
    producto,
    filas,
    batch_id,
    anterior,
    nueva,
    **cambios,
):

    contrato = contratos[producto]

    payload = [
        normalizar_fila(
            producto,
            {
                **fila,
                "operation": "UPSERT",
            },
            contrato,
        )[1]
        for fila in filas
    ]

    argumentos = {
        "batch_id": batch_id,
        "run_id": "run-" + batch_id,
        "source_version": "silver-fixture",
        "previous_gold_version": anterior,
        "new_gold_version": nueva,
        "current_gold_version": anterior,
        "source_manifest_sha256": "a" * 64,
        "expected_rows": len(payload),
        "payload_sha256_declarado":
            calcular_sha256(payload),
    }

    argumentos.update(cambios)

    return preparar_batch_incremental(
        producto,
        payload,
        contrato,
        **argumentos,
    )


def conectar(ruta=":memory:"):

    db = sqlite3.connect(
        ruta,
        timeout=0.1,
    )

    db.execute("PRAGMA foreign_keys = ON")

    return db


def estado_completo(db):

    return {
        tabla: db.execute(
            f"SELECT * FROM {tabla} ORDER BY 1, 2"
        ).fetchall()
        for tabla in (
            "bq_versiones",
            "bq_datos",
            "bq_batches",
            "bq_auditoria",
            "bq_version_history",
        )
    }


def version_actual(db, producto):

    return db.execute(
        """
        SELECT current_version
        FROM bq_versiones
        WHERE product = ?
        """,
        (producto,),
    ).fetchone()[0]


def estado_gold(db, producto):

    return {
        clave: json.loads(payload)
        for clave, payload in db.execute(
            """
            SELECT business_key, payload
            FROM bq_datos
            WHERE product = ?
            """,
            (producto,),
        )
    }


def esperado_gold(producto, filas):

    contrato = contratos[producto]

    resultado = {}

    for fila in filas:

        _, normalizada = normalizar_fila(
            producto,
            {
                **fila,
                "operation": "UPSERT",
            },
            contrato,
        )

        resultado[
            clave_canonica(
                normalizada, contrato
            )
        ] = contenido_gold(
            normalizada, contrato
        )

    return resultado


CAMPO_CAMBIO = {
    MERCADO: "precio_cierre",
    SAB: "monto_compras",
    EMPRESA: "activo_total",
}


# ==========================================
# 1. TRANSACCION COMPLETA POR PRODUCTO
# ==========================================

for producto, contrato in contratos.items():

    db = conectar()

    prefijo = producto.upper()

    try:

        inicializar_control_local(db)

        fila1 = fixture(producto, 1)
        fila2 = fixture(producto, 2)
        fila3 = fixture(producto, 3)
        fila4 = fixture(producto, 4)
        fila5 = fixture(producto, 5)

        inicializar_snapshot_sintetico(
            db,
            producto,
            "gold-v1",
            [fila1, fila2, fila3, fila5],
            contrato,
        )

        assert version_actual(
            db, producto
        ) == "gold-v1"

        assert len(
            estado_gold(db, producto)
        ) == 4

        aprobar("SNAPSHOT_INICIAL_" + prefijo)

        cambio_negocio = dict(fila1)

        cambio_negocio[
            CAMPO_CAMBIO[producto]
        ] = (
            "250.75"
            if producto == SAB
            else 250.75
        )

        cambio_tecnico = dict(fila2)

        cambio_tecnico[
            "fecha_ingesta"
        ] = "2025-01-03"

        filas_delta = [
            cambio_negocio,
            cambio_tecnico,
            fila3,
            fila4,
        ]

        paquete = preparar(
            producto,
            filas_delta,
            "batch-001",
            "gold-v1",
            "gold-v2",
        )

        resultado = procesar_incremental_local(
            db,
            producto,
            paquete,
            contrato,
        )

        informe = resultado["report"]

        assert informe["result"] == "COMPLETED"

        assert informe["inserted_rows"] == 1

        aprobar("INSERT_" + prefijo)

        assert informe["business_updates"] == 1

        aprobar("UPDATE_NEGOCIO_" + prefijo)

        assert informe["technical_updates"] == 1

        aprobar("UPDATE_TECNICO_" + prefijo)

        assert informe["unchanged_rows"] == 1

        aprobar("NOOP_" + prefijo)

        assert informe["updated_rows"] == 2
        assert informe["received_rows"] == 4
        assert informe["deleted_rows"] == 0

        assert resultado["report_sha256"] == (
            huella_documento(informe)
        )

        aprobar("METRICAS_AUDITORIA_" + prefijo)

        assert estado_gold(
            db, producto
        ) == esperado_gold(
            producto,
            [
                cambio_negocio,
                cambio_tecnico,
                fila3,
                fila4,
                fila5,
            ],
        )

        aprobar("IGUALDAD_FINAL_GOLD_" + prefijo)

        assert version_actual(
            db, producto
        ) == "gold-v2"

        aprobar("AVANCE_VERSION_" + prefijo)

        antes = estado_completo(db)

        replay = procesar_incremental_local(
            db,
            producto,
            paquete,
            contrato,
        )

        assert replay["result"] == "REPLAY_NOOP"

        assert replay["applied_rows"] == 0

        assert estado_completo(db) == antes

        aprobar("REPLAY_EXACTO_" + prefijo)

        incompatible = preparar(
            producto,
            [fila1],
            "batch-001",
            "gold-v1",
            "gold-v2",
        )

        rechazar(
            "BATCH_REUTILIZADO_" + prefijo,
            lambda: procesar_incremental_local(
                db,
                producto,
                incompatible,
                contrato,
            ),
            mensaje="Batch reutilizado",
        )

        assert estado_completo(db) == antes

        atrasado = preparar(
            producto,
            [fila1],
            "batch-atrasado",
            "gold-v1",
            "gold-v3",
        )

        rechazar(
            "VERSION_ATRASADA_" + prefijo,
            lambda: procesar_incremental_local(
                db,
                producto,
                atrasado,
                contrato,
            ),
            mensaje="Continuidad",
        )

        assert estado_completo(db) == antes

        retroceso = preparar(
            producto,
            [fila1],
            "batch-retroceso",
            "gold-v2",
            "gold-v1",
        )

        rechazar(
            "VERSION_REUTILIZADA_" + prefijo,
            lambda: procesar_incremental_local(
                db,
                producto,
                retroceso,
                contrato,
            ),
            mensaje="Version Gold previamente aplicada",
        )

        assert estado_completo(db) == antes

        siguiente = preparar(
            producto,
            [fila5],
            "batch-002",
            "gold-v2",
            "gold-v3",
        )

        segundo = procesar_incremental_local(
            db,
            producto,
            siguiente,
            contrato,
        )

        assert segundo["report"][
            "unchanged_rows"
        ] == 1

        assert version_actual(
            db, producto
        ) == "gold-v3"

        antes = estado_completo(db)

        replay_antiguo = procesar_incremental_local(
            db,
            producto,
            paquete,
            contrato,
        )

        assert replay_antiguo[
            "result"
        ] == "REPLAY_NOOP"

        assert estado_completo(db) == antes

        aprobar("REPLAY_VERSION_ANTIGUA_" + prefijo)

    finally:

        db.close()


# ==========================================
# 2. ROLLBACK EN TRES PUNTOS CRITICOS
# ==========================================

producto = MERCADO

contrato = contratos[producto]

db = conectar()

try:

    inicializar_control_local(db)

    fila1 = fixture(producto, 1)
    fila2 = fixture(producto, 2)

    inicializar_snapshot_sintetico(
        db,
        producto,
        "gold-v1",
        [fila1],
        contrato,
    )

    paquete = preparar(
        producto,
        [fila2],
        "batch-rollback",
        "gold-v1",
        "gold-v2",
    )

    inicial = estado_completo(db)

    for punto in (
        "AFTER_DATA",
        "AFTER_AUDIT",
        "AFTER_VERSION",
    ):

        try:

            procesar_incremental_local(
                db,
                producto,
                paquete,
                contrato,
                fallo_simulado=punto,
            )

        except RuntimeError as error:

            assert (
                "FALLO_SIMULADO_" + punto
            ) in str(error)

        else:

            raise AssertionError(
                "Fallo simulado no ejecutado"
            )

        assert not db.in_transaction

        assert estado_completo(db) == inicial

        aprobar("ROLLBACK_" + punto)

    recuperado = procesar_incremental_local(
        db,
        producto,
        paquete,
        contrato,
    )

    assert recuperado["report"][
        "inserted_rows"
    ] == 1

    assert version_actual(
        db, producto
    ) == "gold-v2"

    aprobar("REINTENTO_TRAS_ROLLBACK")

    # Auditoria alterada: impedir replay.
    db.execute(
        """
        UPDATE bq_auditoria
        SET report_sha256 = ?
        WHERE product = ?
        """,
        ("0" * 64, producto),
    )

    db.commit()

    rechazar(
        "AUDITORIA_ALTERADA_RECHAZADA",
        lambda: procesar_incremental_local(
            db,
            producto,
            paquete,
            contrato,
        ),
        mensaje="Auditoria de replay incompatible",
    )

finally:

    db.close()


# ==========================================
# 3. EXCLUSION ENTRE CONEXIONES SQLITE
# ==========================================

with TemporaryDirectory(
    prefix="bvl-transaccional-"
) as temporal:

    ruta = str(
        Path(temporal) / "control.sqlite"
    )

    escritor1 = conectar(ruta)
    escritor2 = conectar(ruta)

    try:

        inicializar_control_local(escritor1)

        inicializar_snapshot_sintetico(
            escritor1,
            producto,
            "gold-v1",
            [fila1],
            contrato,
        )

        inicial = estado_completo(escritor2)

        escritor1.execute("BEGIN IMMEDIATE")

        rechazar(
            "SEGUNDO_ESCRITOR_BLOQUEADO",
            lambda: procesar_incremental_local(
                escritor2,
                producto,
                paquete,
                contrato,
            ),
            excepcion=sqlite3.OperationalError,
            mensaje="locked",
        )

        assert not escritor2.in_transaction

        escritor1.rollback()

        assert estado_completo(
            escritor2
        ) == inicial

        resultado = procesar_incremental_local(
            escritor2,
            producto,
            paquete,
            contrato,
        )

        assert resultado["report"][
            "inserted_rows"
        ] == 1

        aprobar("ESCRITOR_RECUPERADO")

        replay = procesar_incremental_local(
            escritor1,
            producto,
            paquete,
            contrato,
        )

        assert replay["result"] == "REPLAY_NOOP"

        assert escritor1.execute(
            "SELECT COUNT(*) FROM bq_batches"
        ).fetchone()[0] == 1

        aprobar("REPLAY_ENTRE_CONEXIONES")

    finally:

        escritor1.close()
        escritor2.close()

    persistente = conectar(ruta)

    try:

        assert version_actual(
            persistente, producto
        ) == "gold-v2"

        replay = procesar_incremental_local(
            persistente,
            producto,
            paquete,
            contrato,
        )

        assert replay["result"] == "REPLAY_NOOP"

        aprobar("REPLAY_TRAS_REAPERTURA")

    finally:

        persistente.close()


# ==========================================
# 4. INTEGRIDAD Y BLOQUEOS
# ==========================================

db = conectar()

try:

    inicializar_control_local(db)

    rechazar(
        "HISTORICO_NO_INICIALIZADO",
        lambda: procesar_incremental_local(
            db,
            producto,
            paquete,
            contrato,
        ),
        mensaje="Carga historica local no inicializada",
    )

    inicializar_snapshot_sintetico(
        db,
        producto,
        "gold-v1",
        [fila1],
        contrato,
    )

    inicial = estado_completo(db)

    alterado = deepcopy(paquete)

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
        "DELETE_ALTERADO_RECHAZADO",
        lambda: procesar_incremental_local(
            db,
            producto,
            alterado,
            contrato,
        ),
    )

    assert estado_completo(db) == inicial

    alterado = deepcopy(paquete)

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
        lambda: procesar_incremental_local(
            db,
            producto,
            alterado,
            contrato,
        ),
    )

    assert estado_completo(db) == inicial

    db.execute("BEGIN IMMEDIATE")

    rechazar(
        "TRANSACCION_PREEXISTENTE_RECHAZADA",
        lambda: procesar_incremental_local(
            db,
            producto,
            paquete,
            contrato,
        ),
        mensaje="Conexion con transaccion activa",
    )

    assert db.in_transaction

    db.rollback()

    assert estado_completo(db) == inicial

finally:

    db.close()


rechazar(
    "EJECUCION_CLOUD_BLOQUEADA",
    exigir_autorizacion_operativa,
    excepcion=PermissionError,
)

assert len(aprobados) == 53, aprobados

print("\n=== RESUMEN TRANSACCIONAL ===")
print("CONTROLES_TRANSACCIONALES: 53/53")
print("PRODUCTOS_TRANSACCIONALES: 3/3")
print("INSERT_LOCAL: 3/3")
print("UPDATE_NEGOCIO: 3/3")
print("UPDATE_TECNICO: 3/3")
print("NOOP_LOCAL: 3/3")
print("IGUALDAD_FINAL_GOLD: 3/3")
print("REPLAY_EXACTO: 3/3")
print("REPLAY_VERSION_ANTIGUA: 3/3")
print("ROLLBACK_PUNTOS_CRITICOS: 3/3")
print("EXCLUSION_ESCRITORES_SQLITE: True")
print("REINTENTO_TRAS_ROLLBACK: True")
print("PERSISTENCIA_REPLAY: True")
print("AUDITORIA_ALTERADA_RECHAZADA: True")
print("DELETE_EJECUTADO: False")
print("DATOS_SINTETICOS: True")
print("AUTORIZACION_OPERATIVA_REAL: False")
print("GCP_EJECUTADO: False")
print("BIGQUERY_EJECUTADO: False")
print("VALIDACION_MOTOR_TRANSACCIONAL_OK")
