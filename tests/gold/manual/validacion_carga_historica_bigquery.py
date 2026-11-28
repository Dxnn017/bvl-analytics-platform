"""Validacion funcional local de carga historica BigQuery.

Datos sinteticos, contratos Gold reales.
No ejecuta constructores ni servicios cloud.
"""

import shutil

from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from preparador_carga_historica import (
    PRODUCTOS,
    DECIMALES_SAB,
    cargar_contratos,
    preparar_producto,
    preparar_lote_historico,
    expresion_decimal_exacta,
)

from trazabilidad_carga_historica import (
    construir_plan_local,
    verificar_integridad_plan,
    huella_documento,
    exigir_autorizacion_operativa,
)


MERCADO = "mercado_valor_diario"
SAB = "actividad_sab_diaria"
EMPRESA = "empresa_trimestre"

RAIZ = Path.cwd()

historico, contratos = cargar_contratos(RAIZ)

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("BVL_BigQuery_Historico_Local")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

cacheados = []
aprobados = []


def aprobado(nombre):

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

        aprobado(nombre)

    else:

        raise AssertionError(
            "Debio rechazarse: " + nombre
        )


def fecha_fixture(producto):

    if producto == EMPRESA:

        return F.when(
            F.col("id") % 2 == 0,
            F.lit("2025-03-31").cast("date"),
        ).otherwise(
            F.lit("2025-06-30").cast("date")
        )

    return F.when(
        F.col("id") % 2 == 0,
        F.lit("2025-01-02").cast("date"),
    ).otherwise(
        F.lit("2025-02-03").cast("date")
    )


def generar_producto(producto):

    contrato = contratos[producto]["gold"]

    base = spark.range(
        0,
        PRODUCTOS[producto],
        1,
        2,
    )

    fecha = fecha_fixture(producto)

    expresiones = []

    for campo in contrato["columns"]:

        nombre = campo["name"]

        tipo = campo["spark_type"]

        if nombre == "anio":

            valor = F.year(fecha)

        elif nombre == "mes":

            valor = F.month(fecha)

        elif nombre in (
            "trimestre",
            "trimestre_num",
        ) and tipo in ("int", "integer"):

            valor = F.quarter(fecha)

        elif nombre == "ejercicio":

            valor = F.year(fecha)

        elif nombre == "id_registro":

            valor = F.sha2(
                F.concat(
                    F.lit("FIXTURE-EMPRESA-"),
                    F.col("id").cast("string"),
                ),
                256,
            )

        elif nombre in (
            "nemonico",
            "codigo_sab",
        ):

            valor = F.concat(
                F.lit("FIXTURE-"),
                F.col("id").cast("string"),
            )

        elif nombre == "estado_cobertura":

            valor = F.lit("DESCONOCIDA")

        elif nombre in (
            "cobertura_completa",
            "trazabilidad_verificada",
        ):

            valor = F.lit(False)

        elif nombre in (
            "registros_cuarentena",
            "archivos_excluidos",
            "scope_quality_id",
        ) and campo["nullable"]:

            valor = F.lit(None)

        elif nombre in DECIMALES_SAB:

            valor = F.lit("123456789.25")

            if campo["nullable"]:

                valor = F.when(
                    F.col("id") == 0,
                    F.lit(None),
                ).otherwise(valor)

        elif tipo == "string":

            valor = F.lit("FIXTURE_SINTETICO")

        elif tipo == "date":

            valor = fecha

        elif tipo == "boolean":

            valor = F.lit(True)

        elif tipo == "double":

            valor = F.lit(125.25)

        elif tipo.startswith("decimal("):

            valor = F.lit("100.25")

        elif tipo in (
            "int",
            "integer",
            "long",
            "bigint",
        ):

            valor = F.lit(1)

        else:

            raise AssertionError(
                f"Tipo no contemplado: {tipo}"
            )

        expresiones.append(
            valor.cast(tipo).alias(nombre)
        )

    df = base.select(*expresiones).cache()

    cacheados.append(df)

    assert df.count() == PRODUCTOS[producto]

    print(
        "FIXTURE_GENERADO:",
        producto,
        PRODUCTOS[producto],
        len(df.columns),
        flush=True,
    )

    return df


def comprobar_decimales():

    maximo = (
        "9" * 29 + ".99"
    )

    desbordamiento = (
        "1" + "0" * 29 + ".00"
    )

    casos = [
        (0, "0.00", True),
        (1, "100.25", True),
        (2, "-100.25", True),
        (3, maximo, True),
        (4, "-" + maximo, True),
        (5, desbordamiento, False),
        (6, "-" + desbordamiento, False),
        (7, None, True),
    ]

    df = spark.createDataFrame(
        casos,
        "id int, texto string, valido boolean",
    ).withColumn(
        "valor",
        F.col("texto").cast("decimal(34,2)")
    )

    convertido, invalido = (
        expresion_decimal_exacta("valor")
    )

    filas = df.select(
        "*",
        convertido.alias("transferencia"),
        invalido.alias("invalido"),
    ).collect()

    for fila in filas:

        assert fila["invalido"] == (
            not fila["valido"]
        ), fila

        if fila["valido"]:

            if fila["texto"] is None:

                assert fila["transferencia"] is None

            else:

                assert fila["transferencia"] == (
                    Decimal(fila["texto"])
                )

        aprobado(
            f"DECIMAL_CASO_{fila['id']:02d}"
        )


def comparar_datos(original, resultado):

    esperado = original.select(
        *resultado.columns
    )

    assert not esperado.exceptAll(
        resultado
    ).limit(1).count()

    assert not resultado.exceptAll(
        esperado
    ).limit(1).count()


def transferencia_parquet(
    producto,
    df,
    temporal,
    destino,
):

    particiones = contratos[producto][
        "configuracion"
    ]["gcs_partitions"]

    salida = temporal / producto

    (
        df.write
        .mode("errorifexists")
        .partitionBy(*particiones)
        .parquet(str(salida))
    )

    carpeta = destino / producto

    carpeta.mkdir()

    cantidad = 0

    for ruta in sorted(
        salida.rglob("*.parquet")
    ):

        relativo = ruta.relative_to(salida)

        archivo = carpeta / relativo

        archivo.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        shutil.copyfile(ruta, archivo)

        cantidad += 1

    assert cantidad > 0

    reconstruido = spark.read.parquet(
        str(carpeta)
    ).select(*df.columns)

    assert {
        f.name: f.dataType.simpleString()
        for f in reconstruido.schema.fields
    } == {
        f.name: f.dataType.simpleString()
        for f in df.schema.fields
    }

    assert reconstruido.count() == (
        PRODUCTOS[producto]
    )

    comparar_datos(df, reconstruido)

    print(
        "PARQUET_ARCHIVOS:",
        producto,
        cantidad,
        flush=True,
    )

    aprobado(
        "ROUNDTRIP_" + producto.upper()
    )


def referencia_sintetica(producto, nombre):

    return {
        "uri": (
            "gs://bvl26-control-7f3a9c/"
            "fixtures-no-operativos/"
            f"{producto}/{nombre}"
        ),
        "generation": "1",
        "sha256": "a" * 64,
        "size_bytes": 100,
    }


try:

    # ==========================================
    # 1. PRECISION NUMERICA
    # ==========================================

    comprobar_decimales()

    # ==========================================
    # 2. LOTE SINTETICO COMPLETO
    # ==========================================

    originales = {
        producto: generar_producto(producto)
        for producto in PRODUCTOS
    }

    transferencias, resumen = (
        preparar_lote_historico(
            originales,
            contratos,
        )
    )

    assert resumen["total_rows"] == 110199

    assert resumen["total_columns"] == 140

    assert resumen["execution_authorized"] is False

    assert resumen["official_sources_verified"] is False

    for producto, df in transferencias.items():

        df.cache()

        cacheados.append(df)

        assert df.count() == PRODUCTOS[producto]

        original = originales[producto]

        if producto == SAB:

            reconstruido = df.select(*[
                F.col(c).cast(
                    "decimal(34,2)"
                ).alias(c)
                if c in DECIMALES_SAB
                else F.col(c)
                for c in df.columns
            ])

            comparar_datos(
                original,
                reconstruido,
            )

            for columna in DECIMALES_SAB:

                assert (
                    df.schema[columna]
                    .dataType.simpleString()
                ) == "decimal(31,2)"

                assert (
                    original.schema[columna]
                    .dataType.simpleString()
                ) == "decimal(34,2)"

        else:

            comparar_datos(
                original, df
            )

        aprobado(
            "LOTE_" + producto.upper()
        )

    # ==========================================
    # 3. CONTROLES NEGATIVOS
    # ==========================================

    mercado = originales[MERCADO]

    contrato = contratos[MERCADO]

    rechazar(
        "LOTE_INCOMPLETO_RECHAZADO",
        lambda: preparar_lote_historico(
            {MERCADO: mercado},
            contratos,
        ),
        mensaje="Lote historico incompleto",
    )

    rechazar(
        "COLUMNA_FALTANTE_RECHAZADA",
        lambda: preparar_producto(
            MERCADO,
            mercado.drop("nemonico"),
            contrato,
        ),
        mensaje="Columnas faltantes o adicionales",
    )

    rechazar(
        "COLUMNA_ADICIONAL_RECHAZADA",
        lambda: preparar_producto(
            MERCADO,
            mercado.withColumn(
                "campo_extra", F.lit(1)
            ),
            contrato,
        ),
        mensaje="Columnas faltantes o adicionales",
    )

    rechazar(
        "TIPO_INCORRECTO_RECHAZADO",
        lambda: preparar_producto(
            MERCADO,
            mercado.withColumn(
                "nemonico", F.lit(1)
            ),
            contrato,
        ),
        mensaje="Tipos Gold incompatibles",
    )

    rechazar(
        "CONTEO_INCORRECTO_RECHAZADO",
        lambda: preparar_producto(
            MERCADO,
            mercado.limit(1),
            contrato,
        ),
        mensaje="Conteo incompatible",
    )

    rechazar(
        "CLAVE_NULA_RECHAZADA",
        lambda: preparar_producto(
            MERCADO,
            mercado.withColumn(
                "nemonico",
                F.lit(None).cast("string"),
            ),
            contrato,
        ),
        mensaje="clave__nemonico",
    )

    rechazar(
        "CLAVE_VACIA_RECHAZADA",
        lambda: preparar_producto(
            MERCADO,
            mercado.withColumn(
                "nemonico", F.lit("   ")
            ),
            contrato,
        ),
        mensaje="clave__nemonico",
    )

    rechazar(
        "CLAVE_DUPLICADA_RECHAZADA",
        lambda: preparar_producto(
            MERCADO,
            mercado.withColumn(
                "nemonico", F.lit("REPETIDO")
            ),
            contrato,
        ),
        mensaje="Claves de negocio duplicadas",
    )

    rechazar(
        "PARTICION_INVALIDA_RECHAZADA",
        lambda: preparar_producto(
            MERCADO,
            mercado.withColumn(
                "mes", F.lit(13)
            ),
            contrato,
        ),
        mensaje="particiones_invalidas",
    )

    rechazar(
        "NAN_RECHAZADO",
        lambda: preparar_producto(
            MERCADO,
            mercado.withColumn(
                "precio_cierre",
                F.lit(float("nan")),
            ),
            contrato,
        ),
        mensaje="no_finito__precio_cierre",
    )

    rechazar(
        "INFINITO_RECHAZADO",
        lambda: preparar_producto(
            MERCADO,
            mercado.withColumn(
                "precio_cierre",
                F.lit(float("inf")),
            ),
            contrato,
        ),
        mensaje="no_finito__precio_cierre",
    )

    rechazar(
        "DECIMAL_DESBORDADO_RECHAZADO",
        lambda: preparar_producto(
            SAB,
            originales[SAB].withColumn(
                "total_compras_diario",
                F.lit(
                    "1" + "0" * 29 + ".00"
                ).cast("decimal(34,2)"),
            ),
            contratos[SAB],
        ),
        mensaje=(
            "decimal_invalido__"
            "total_compras_diario"
        ),
    )

    # ==========================================
    # 4. PARQUET LOCAL E INVENTARIO
    # ==========================================

    with TemporaryDirectory(
        prefix="bvl-bigquery-historico-"
    ) as carpeta:

        raiz = Path(carpeta)

        temporal = raiz / "spark"
        destino = raiz / "transferencia"

        temporal.mkdir()
        destino.mkdir()

        for producto, df in (
            transferencias.items()
        ):

            transferencia_parquet(
                producto,
                df,
                temporal,
                destino,
            )

        origenes = {
            producto: {
                "version_id": "fixture-local-v1",
                "manifest":
                    referencia_sintetica(
                        producto, "manifest.json"
                    ),
                "manifest_signature":
                    referencia_sintetica(
                        producto, "manifest.sig"
                    ),
                "current_registry":
                    referencia_sintetica(
                        producto, "current.json"
                    ),
            }
            for producto in PRODUCTOS
        }

        paquete = construir_plan_local(
            RAIZ,
            destino,
            "batch_fixture_001",
            "run_fixture_001",
            origenes,
            resumen["products"],
        )

        verificacion = verificar_integridad_plan(
            paquete, destino
        )

        assert verificacion["status"] == (
            "LOCAL_INTEGRITY_VERIFIED"
        )

        assert verificacion[
            "execution_authorized"
        ] is False

        aprobado("PLAN_LOCAL_VERIFICADO")

        repetido = construir_plan_local(
            RAIZ,
            destino,
            "batch_fixture_001",
            "run_fixture_001",
            origenes,
            resumen["products"],
        )

        assert paquete == repetido

        aprobado("PLAN_DETERMINISTA")

        modificado = deepcopy(paquete)

        modificado["plan"]["run_id"] = (
            "run_distinto"
        )

        rechazar(
            "PLAN_MODIFICADO_RECHAZADO",
            lambda: verificar_integridad_plan(
                modificado, destino
            ),
            mensaje="Plan historico modificado",
        )

        no_autorizado = deepcopy(paquete)

        no_autorizado["plan"][
            "execution_authorized"
        ] = True

        no_autorizado["plan_sha256"] = (
            huella_documento(
                no_autorizado["plan"]
            )
        )

        rechazar(
            "PLAN_CON_PERMISO_FALSO_RECHAZADO",
            lambda: verificar_integridad_plan(
                no_autorizado, destino
            ),
            mensaje="Alcance local incompatible",
        )

        archivo = next(
            (destino / MERCADO).rglob("*.parquet")
        )

        contenido = archivo.read_bytes()

        try:

            archivo.write_bytes(
                contenido + b"ALTERACION"
            )

            rechazar(
                "PARQUET_ALTERADO_RECHAZADO",
                lambda: verificar_integridad_plan(
                    paquete, destino
                ),
                mensaje=(
                    "Inventario de transferencia modificado"
                ),
            )

        finally:

            archivo.write_bytes(contenido)

        adicional = (
            archivo.parent / "no_acreditado.parquet"
        )

        try:

            adicional.write_bytes(contenido)

            rechazar(
                "ARCHIVO_ADICIONAL_RECHAZADO",
                lambda: verificar_integridad_plan(
                    paquete, destino
                ),
                mensaje=(
                    "Inventario de transferencia modificado"
                ),
            )

        finally:

            adicional.unlink()

        oculto = (
            archivo.parent / "fuera_del_contrato.txt"
        )

        try:

            oculto.write_text(
                "fixture", encoding="utf-8"
            )

            rechazar(
                "ARCHIVO_NO_PARQUET_RECHAZADO",
                lambda: verificar_integridad_plan(
                    paquete, destino
                ),
                mensaje=(
                    "Archivo de transferencia no admitido"
                ),
            )

        finally:

            oculto.unlink()

        verificar_integridad_plan(
            paquete, destino
        )

        aprobado("INVENTARIO_RESTAURADO")

        rechazar(
            "EJECUCION_OPERATIVA_BLOQUEADA",
            lambda: exigir_autorizacion_operativa(
                paquete
            ),
            excepcion=PermissionError,
        )

    # ==========================================
    # 5. RESUMEN
    # ==========================================

    assert len(aprobados) == 35, aprobados

    print("\n=== RESUMEN HISTORICO BIGQUERY ===")

    print("CONTROLES_FUNCIONALES: 35/35")
    print("PRODUCTOS_HISTORICOS: 3/3")
    print("REGISTROS_SINTETICOS: 110199")
    print("COLUMNAS_GOLD: 140/140")
    print("CASOS_DECIMALES: 8/8")
    print("DECIMALES_SAB_ADAPTADOS: 3/3")
    print("ROUNDTRIP_PARQUET: 3/3")
    print("CONSERVACION_DATOS: True")
    print("HUELLA_LOTE_VERIFICADA: True")
    print("ALTERACIONES_RECHAZADAS: True")
    print("BLOQUEO_OPERATIVO_ACTIVO: True")
    print("DATOS_OFICIALES_PROCESADOS: False")
    print("CONSTRUCTORES_GOLD_EJECUTADOS: 0")
    print("AUTORIZACION_QUALITY_REAL: False")
    print("GCP_EJECUTADO: False")
    print("BIGQUERY_EJECUTADO: False")
    print("PUBLICACION_AUTORIZADA: False")
    print("VALIDACION_LOCAL_BIGQUERY_HISTORICO_OK")

finally:

    for df in cacheados:

        df.unpersist()

    spark.stop()
