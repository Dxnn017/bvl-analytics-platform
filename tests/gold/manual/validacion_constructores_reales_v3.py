"""Validacion integral candidata v3.

Utiliza los seis esquemas Silver recibidos y
los tres constructores reales.

Los datos de negocio son sinteticos.
No utiliza GCP ni autoriza publicacion.
"""

import os
import json
import math
import hashlib
import zipfile

from copy import deepcopy
from datetime import date
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from adaptador_numerico_integral import (
    adaptar_numericos,
    expresiones_conversion,
    REGLAS,
    POLITICA,
)

from adaptador_cotizaciones_quality import (
    adaptar_cotizaciones,
)

from preparar_candidatos_correctivos import (
    adaptar_fuente_numerica,
    validar_contrato_salida,
    validar_claves_conservadas,
    separar_finanzas,
    digest_ids,
)

from ejecutor_portable import (
    construir_candidato,
    construir_mercado,
    construir_sab,
    construir_empresa,
)

from escritura_correctiva_staging import (
    validar_evidencia_integral,
    materializar_parquet_local,
)

from sidecar_sab_ratificado import (
    interpretar_sidecar_ratificado,
)


COT = "mercado_diario/cotizaciones"
TC = "macroeconomia/tipo_cambio"
EMBIG = "macroeconomia/embig"
PUENTE = "referencias_smv/puente_ruc_nemonico"
SAB = "mercado_diario/montos_sab"
FIN = "finanzas_empresariales/principales_cuentas"

FECHA = date(2025, 1, 2)

contratos = {}
contratos_gold = {}
manifests = {}

ruta = os.environ["ZIP_CONTRATOS"]


def sha(datos):

    return hashlib.sha256(datos).hexdigest()


with open(ruta, "rb") as archivo:

    assert sha(archivo.read()) == (
        "32a1b866cb13f135a33783678601699df"
        "b582ae3501280a899e42c91a933bf5f"
    )


with zipfile.ZipFile(ruta) as z:

    assert z.testzip() is None

    for nombre in z.namelist():

        if nombre.endswith("/physical_contract.json"):

            contenido = z.read(nombre)
            contrato = json.loads(contenido)

            fuente = contrato["product"]

            contratos[fuente] = {
                "documento": contrato,
                "sha256": sha(contenido),
            }

            prefijo = nombre.rsplit("/", 1)[0]

            manifest_bytes = z.read(
                prefijo + "/manifest.json"
            )

            manifests[fuente] = {
                "documento": json.loads(manifest_bytes),
                "sha256": sha(manifest_bytes),
                "signature_sha256": sha(z.read(
                    prefijo + "/manifest.sig"
                )),
            }

        elif nombre.startswith("gold/") \
                and nombre.endswith(".json"):

            contenido = z.read(nombre)
            contrato = json.loads(contenido)

            contratos_gold[
                contrato["product"]
            ] = contenido

    cobertura_real, evidencia_sidecar = (
        interpretar_sidecar_ratificado(
            contenido=z.read(
                "quality/sidecar_sab.json"
            ),
            firma=z.read(
                "quality/sidecar_sab.sig"
            ),
            ratificacion_bytes=z.read(
                "quality/ratificacion_quality.json"
            ),
            ratificacion_firma=z.read(
                "quality/ratificacion_quality.sig"
            ),
            pem=z.read(
                "quality/quality_public.pem"
            ),
            version_id=contratos[SAB][
                "documento"
            ]["version_id"],
            manifest_sha256=manifests[SAB]["sha256"],
            manifest_signature_sha256=manifests[SAB][
                "signature_sha256"
            ],
            manifest_generation="1795765197526161",
            registros_esperados=21911,
        )
    )


assert len(contratos) == 6
assert len(contratos_gold) == 3

assert len(cobertura_real) == 1207

assert sum(
    r["registros_elegibles"]
    for r in cobertura_real
) == 21911


# Autorizacion sintetica para pruebas unitarias
# de interfaces. NO es autorizacion firmada.

auth = {
    "authorization_id": "fixture-integral-v3",
    "sources": {
        fuente: {
            "version_id":
                c["documento"]["version_id"],
            "physical_contract_sha256":
                c["sha256"],
        }
        for fuente, c in contratos.items()
    },
    "output_contracts": {
        producto: sha(contenido)
        for producto, contenido
        in contratos_gold.items()
    },
    "cotizaciones_adaptation": {
        "decision_id": "fixture-null-v2",
        "decision": "ALLOW_TYPED_NULLS",
        "quality_approved": True,
        "product": COT,
        "version_id": contratos[COT][
            "documento"
        ]["version_id"],
        "reason":
            "INSUFFICIENT_ACCREDITED_ROW_METADATA",
        "columns": {
            "fuente": {
                "spark_type": "string",
                "value": None,
            },
            "fecha_ingesta": {
                "spark_type": "date",
                "value": None,
            },
        },
    },
    "numeric_adaptations": {
        "decision_id": "fixture-numerico-v3",
        "decision":
            "ALLOW_STRICT_NUMERIC_CONVERSIONS",
        "quality_approved": True,
        "policy": POLITICA,
        "sources": {
            fuente: {
                "version_id": contratos[fuente][
                    "documento"
                ]["version_id"],
                "physical_contract_sha256":
                    contratos[fuente]["sha256"],
                "columns": {
                    nombre: {
                        "input_type": "string",
                        "output_type": tipo,
                    }
                    for nombre, tipo
                    in reglas.items()
                },
            }
            for fuente, reglas in REGLAS.items()
        },
    },
}


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("BVL_Integracion_Constructores_V3")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

cacheados = []


def crear(fuente, filas):

    esquema = StructType.fromJson(
        contratos[fuente]["documento"][
            "schema_spark"
        ]
    )

    datos = []

    for fila in filas:

        assert set(fila) <= set(
            esquema.fieldNames()
        ), fuente

        datos.append({
            campo.name: fila.get(campo.name)
            for campo in esquema.fields
        })

    df = spark.createDataFrame(
        datos,
        schema=esquema,
        verifySchema=True,
    )

    assert df.schema == esquema

    return df


def fixture_preflight(fuente, cantidad):

    """Metadatos sinteticos para interfaces.

    No acredita Parquet ni generaciones GCS.
    """

    version = auth["sources"][fuente][
        "version_id"
    ]

    return {
        "auth": {
            "product": fuente,
            "version_id": version,
            "manifest": {
                "sha256":
                    manifests[fuente]["sha256"],
            },
        },
        "manifest": {
            "product": fuente,
            "version_id": version,
            "records_count": cantidad,
        },
        "preflight": {
            "product": fuente,
            "version_id": version,
            "esquema_fisico_verificado": True,
            "filas_validadas": cantidad,
        },
    }


def comprobar_salida(producto, df):

    df = df.cache()
    cacheados.append(df)

    resultado = validar_contrato_salida(
        producto,
        df,
        contratos_gold[producto],
        auth,
    )

    contrato = json.loads(
        contratos_gold[producto]
    )

    assert resultado.columns == [
        c["name"]
        for c in contrato["columns"]
    ]

    # Fuerza evaluacion de todas las columnas.
    filas = resultado.collect()

    assert filas

    print(
        "CONTRATO_GOLD_OK:",
        producto,
        len(resultado.columns),
        "FILAS:",
        len(filas),
        flush=True,
    )

    return resultado, filas


def rechazar(nombre, funcion):

    try:

        funcion()

    except ValueError:

        print(nombre + "_OK", flush=True)

    else:

        raise AssertionError(
            "Debio rechazarse: " + nombre
        )


def comprobar_conversiones():

    casos = [
        ("double", "10.25", True),
        ("double", "-0.25", True),
        ("double", "  2.5  ", True),
        ("double", "1e3", True),
        ("double", None, True),
        ("double", "NaN", False),
        ("double", "Infinity", False),
        ("double", "1e999", False),
        ("double", "1e-999", False),
        ("double", "", False),
        ("double", "1,25", False),
        ("double", "10%", False),
        ("double", "S/ 10", False),
        ("decimal(24,2)", "100.25", True),
        ("decimal(24,2)", "100.2500", True),
        ("decimal(24,2)", "-10.25", True),
        ("decimal(24,2)", ".50", True),
        ("decimal(24,2)", None, True),
        ("decimal(24,2)", "100.251", False),
        ("decimal(24,2)", "1,000.25", False),
        ("decimal(24,2)", "1e3", False),
        ("decimal(24,2)", "NaN", False),
        ("decimal(24,2)", "9" * 23, False),
        ("decimal(24,2)", "9" * 22 + ".99", True),
    ]

    df = spark.createDataFrame(
        [
            (i, tipo, valor, valido)
            for i, (tipo, valor, valido)
            in enumerate(casos)
        ],
        "id int, tipo string, valor string, valido boolean",
    )

    for tipo in ("double", "decimal(24,2)"):

        convertido, invalido = (
            expresiones_conversion(
                "valor", tipo
            )
        )

        filas = (
            df.filter(F.col("tipo") == tipo)
            .select(
                "*",
                convertido.alias("convertido"),
                invalido.alias("invalido"),
            )
            .collect()
        )

        for fila in filas:

            assert fila["invalido"] == (
                not fila["valido"]
            ), fila

            if fila["valor"] is None:

                assert fila["convertido"] is None

    print(
        "CASOS_NUMERICOS_APROBADOS:",
        len(casos),
        flush=True,
    )


try:

    comprobar_conversiones()

    base_cot = {
        "valor": " demo1 ",
        "descripcion": "FIXTURE_SINTETICO",
        "fecha_cotizacion": FECHA,
        "fecha_anterior": date(2025, 1, 1),
        "fecha_registro": FECHA,
        "cierre_anterior": "9.00",
        "cierre_actual": "10.00",
        "apertura_actual": "9.50",
        "maxima_actual": "10.50",
        "minima_actual": "9.00",
        "promedio_actual": "9.75",
        "monto_negociado": "1000.50",
        "anio": 2025,
        "mes": 1,
    }

    cot = crear(COT, [
        base_cot,
        {
            **base_cot,
            "valor": "DEMO2",
            "cierre_actual": None,
            "monto_negociado": "0",
        },
    ])

    tc = crear(TC, [{
        "fecha": FECHA,
        "tipo_cambio_venta": 3.75,
        "anio": 2025,
        "mes": 1,
    }])

    embig = crear(EMBIG, [{
        "fecha": FECHA,
        "embig_peru": 150.0,
        "anio": 2025,
        "mes": 1,
    }])

    puente = crear(PUENTE, [{
        "ruc": "RUC-FIXTURE",
        "nemonico_valor": "DEMO1",
        "razon_social": "FIXTURE",
        "codigo_isin": "ISIN-FIXTURE",
        "tipo_valor": "ACCION",
        "moneda": "PEN",
        "fecha_inscripcion": date(2020, 1, 1),
    }])

    preflight_cot = fixture_preflight(
        COT, 2
    )

    cot_adaptado, evidencia_null = (
        adaptar_cotizaciones(
            cot, auth, preflight_cot
        )
    )

    cot_adaptado, evidencia_cot = (
        adaptar_fuente_numerica(
            cot_adaptado,
            COT,
            auth,
            preflight_cot,
        )
    )

    assert len(evidencia_cot["columns"]) == 7

    assert evidencia_cot["columns"][
        "cierre_actual"
    ]["original_nulls"] == 1

    assert not cot_adaptado.filter(
        F.col("fuente").isNotNull()
        | F.col("fecha_ingesta").isNotNull()
    ).count()

    rechazar(
        "COTIZACION_INVALIDA_RECHAZADA",
        lambda: adaptar_numericos(
            crear(COT, [{
                **base_cot,
                "cierre_actual": "INVALIDO",
            }]),
            COT,
        ),
    )

    mercado, filas_mercado = comprobar_salida(
        "mercado_valor_diario",
        construir_candidato(
            "mercado_valor_diario",
            {
                "cotizaciones": cot_adaptado,
                "tipo_cambio": tc,
                "embig": embig,
                "puente_ruc_nemonico": puente,
            },
        ),
    )

    validar_claves_conservadas(
        "mercado_valor_diario",
        mercado,
        cot,
    )

    por_nemonico = {
        f["nemonico"]: f
        for f in filas_mercado
    }

    m = por_nemonico["DEMO1"]

    assert m["nemonico_original"] == " demo1 "

    assert m["precio_cierre"] == 10.0

    assert m["variacion_precio"] == 1.0

    assert math.isclose(
        m["retorno_diario_pct"],
        100 / 9,
        abs_tol=1e-9,
    )

    assert m["rango_intradia_valido"] is True
    assert m["rango_intradia"] == 1.5

    assert m["macro_disponible"] is True

    assert m["empresa_vinculada"] is True

    assert por_nemonico["DEMO2"][
        "retorno_diario_pct"
    ] is None

    print("MERCADO_INDICADORES_OK", flush=True)

    # Construir datos SAB sinteticos siguiendo
    # exclusivamente la cardinalidad documental.
    # No se presentan como registros oficiales.

    filas_sab = []

    for registro in cobertura_real:

        fecha = date.fromisoformat(
            registro["fecha_negociacion"]
        )

        cantidad = registro["registros_elegibles"]

        for i in range(cantidad):

            codigo = f"FIXTURE-SAB-{i:03d}"

            filas_sab.append({
                "CodigoSAB": codigo,
                "codigo_sab": codigo,
                "SocAgenteBolsa": "FIXTURE",
                "fecha_negociacion": fecha,
                "MontoCompras": "100.25",
                "MontoVentas": "50.00",
                "MontoTotal": "150.25",
                "PorcentajeCompras":
                    str(100 / cantidad),
                "PorcentajeVentas":
                    str(100 / cantidad),
                "PorcentajeTotal":
                    str(100 / cantidad),
                "fuente_datos": "FIXTURE_SINTETICO",
                "fecha_ingesta": FECHA,
                "anio": fecha.year,
                "mes": fecha.month,
            })

    assert len(filas_sab) == 21911

    sab = crear(SAB, filas_sab)

    sab_adaptado, evidencia_sab = (
        adaptar_fuente_numerica(
            sab,
            SAB,
            auth,
            fixture_preflight(SAB, 21911),
        )
    )

    assert len(evidencia_sab["columns"]) == 6

    rechazar(
        "SAB_REDONDEO_NO_AUTORIZADO_RECHAZADO",
        lambda: adaptar_numericos(
            crear(SAB, [{
                **filas_sab[0],
                "MontoCompras": "100.251",
            }]),
            SAB,
        ),
    )

    gold_sab, filas_gold_sab = comprobar_salida(
        "actividad_sab_diaria",
        construir_candidato(
            "actividad_sab_diaria",
            {
                "montos_sab": sab_adaptado,
            },
            cobertura=cobertura_real,
            version_silver=auth["sources"][SAB][
                "version_id"
            ],
        ),
    )

    validar_claves_conservadas(
        "actividad_sab_diaria",
        gold_sab,
        sab,
    )

    observados = {}

    for fila in filas_gold_sab:

        assert fila["monto_compras"] == Decimal(
            "100.25"
        )

        assert fila["monto_total"] == Decimal(
            "150.25"
        )

        assert fila["diferencia_monetaria"] == (
            Decimal("0.00")
        )

        assert fila["montos_validos"] is True
        assert fila["total_consistente"] is True

        assert fila["estado_cobertura"] == (
            "DESCONOCIDA"
        )

        assert fila["cobertura_completa"] is False
        assert fila["trazabilidad_verificada"] is False

        assert fila["registros_cuarentena"] is None
        assert fila["archivos_excluidos"] is None
        assert fila["scope_quality_id"] is None

        cantidad = fila["cantidad_sab_diaria"]

        assert fila["registros_elegibles"] == cantidad

        assert math.isclose(
            fila["participacion_total_pct"],
            100 / cantidad,
            abs_tol=1e-9,
        )

        assert math.isclose(
            fila["hhi_diario"],
            10000 / cantidad,
            abs_tol=1e-6,
        )

        assert fila["version_silver"] == (
            auth["sources"][SAB]["version_id"]
        )

        fecha = fila["fecha"].isoformat()

        observados[fecha] = (
            observados.get(fecha, 0) + 1
        )

    esperados = {
        r["fecha_negociacion"]:
            r["registros_elegibles"]
        for r in cobertura_real
    }

    assert observados == esperados

    print("SAB_INDICADORES_OK", flush=True)

    print(
        "SAB_FECHAS_CONTRASTADAS_SINTETICAS:",
        len(observados),
        flush=True,
    )

    print(
        "SAB_REGISTROS_SINTETICOS:",
        len(filas_gold_sab),
        flush=True,
    )

    # Construccion financiera real con casos
    # aceptados, identidad invalida y colisiones.

    base_fin = {
        "id_registro": "a" * 64,
        "clave_negocio": "FIXTURE-FIN",
        "rpj": "FIXTURE-RPJ",
        "ruc": "RUC-FIXTURE",
        "ruc_valido": True,
        "nombre_empresa": "FIXTURE",
        "ejercicio": 2025,
        "trimestre_num": 1,
        "trimestre": "1",
        "moneda": "PEN",
        "activo_total": 1000.0,
        "pasivo_total": 400.0,
        "patrimonio_total": 600.0,
        "total_ingreso": 500.0,
        "utilidad_neta": 100.0,
        "cantidad_clave_negocio": 1,
        "colision_clave_negocio": False,
        "fuente_datos": "FIXTURE_SINTETICO",
        "fecha_ingesta": FECHA,
    }

    fin = crear(FIN, [
        base_fin,
        {
            **base_fin,
            "id_registro": "b" * 64,
            "rpj": "FIXTURE-RPJ-2",
            "clave_negocio": "FIXTURE-FIN-2",
            "patrimonio_total": 500.0,
        },
        {
            **base_fin,
            "id_registro": "c" * 64,
            "rpj": "FIXTURE-COLISION",
            "clave_negocio": "FIXTURE-COLISION",
            "colision_clave_negocio": True,
            "cantidad_clave_negocio": 2,
        },
        {
            **base_fin,
            "id_registro": "d" * 64,
            "rpj": "FIXTURE-COLISION",
            "clave_negocio": "FIXTURE-COLISION",
            "colision_clave_negocio": True,
            "cantidad_clave_negocio": 2,
        },
        {
            **base_fin,
            "id_registro": "e" * 64,
            "rpj": "FIXTURE-RPJ-3",
            "clave_negocio": "FIXTURE-FIN-3",
            "utilidad_neta": None,
        },
    ])

    empresa, filas_empresa = comprobar_salida(
        "empresa_trimestre",
        construir_candidato(
            "empresa_trimestre",
            {
                "principales_cuentas": fin,
            },
        ),
    )

    assert digest_ids(fin) == digest_ids(empresa)

    por_id = {
        fila["id_registro"]: fila
        for fila in filas_empresa
    }

    f = por_id["a" * 64]

    assert f["apto_kpi_general"] is True

    assert math.isclose(
        f["roa_aproximado_pct"], 10.0
    )

    assert math.isclose(
        f["roe_aproximado_pct"], 100 / 6
    )

    assert math.isclose(
        f["endeudamiento_pct"], 40.0
    )

    assert math.isclose(
        f["margen_neto_pct"], 20.0
    )

    assert math.isclose(
        f["autonomia_financiera_pct"], 60.0
    )

    for id_ in ("b", "c", "d"):

        f = por_id[id_ * 64]

        assert f["apto_kpi_general"] is False

        for kpi in (
            "roa_aproximado_pct",
            "roe_aproximado_pct",
            "endeudamiento_pct",
            "margen_neto_pct",
            "autonomia_financiera_pct",
        ):

            assert f[kpi] is None

    assert por_id["e" * 64][
        "roa_aproximado_pct"
    ] is None

    assert por_id["e" * 64][
        "margen_neto_pct"
    ] is None

    print("EMPRESA_INDICADORES_OK", flush=True)

    # Validar escritura Parquet exclusivamente
    # local con el escritor real, sin cliente GCS.

    from tempfile import TemporaryDirectory
    from pathlib import Path

    with TemporaryDirectory(
        prefix="bvl-v3-prueba-local-"
    ) as temporal:

        raiz = Path(temporal)

        for producto, df, particiones, cantidad in (
            (
                "mercado_valor_diario",
                mercado,
                ["anio", "mes"],
                2,
            ),
            (
                "actividad_sab_diaria",
                gold_sab,
                ["anio", "mes"],
                21911,
            ),
            (
                "empresa_trimestre",
                empresa,
                ["anio", "trimestre"],
                5,
            ),
        ):

            salida = materializar_parquet_local(
                spark,
                df,
                producto,
                raiz / producto,
                particiones,
                cantidad,
            )

            assert salida[
                "content_roundtrip_verified"
            ] is True

            print(
                "PARQUET_LOCAL_ROUNDTRIP_OK:",
                producto,
                salida["records_count"],
                flush=True,
            )

    assert len(mercado.columns) == 41
    assert len(gold_sab.columns) == 41
    assert len(empresa.columns) == 58

    assert (
        len(mercado.columns)
        + len(gold_sab.columns)
        + len(empresa.columns)
    ) == 140

    print("\n=== RESUMEN FINAL V3 ===", flush=True)

    print("CONSTRUCTORES_REALES_COMPATIBLES: 3/3")
    print("CONSTRUCTORES_SIMULADOS: 0")
    print("ESQUEMAS_SILVER_UTILIZADOS: 6/6")
    print("COLUMNAS_GOLD_COMPATIBLES: 140/140")
    print("CONVERSIONES_NUMERICAS: 13/13")
    print("METADATOS_NULL_TIPADOS: 2/2")
    print("CASOS_NUMERICOS: 24/24")
    print("CLAVES_NEGOCIO_CONSERVADAS: True")
    print("INDICADORES_VERIFICADOS: 3/3")
    print("PARQUET_LOCAL_ROUNDTRIP: 3/3")
    print("SAB_FECHAS_SINTETICAS: 1207")
    print("SAB_REGISTROS_SINTETICOS: 21911")
    print("SAB_COBERTURA_COMPLETA: False")
    print("DATOS_NEGOCIO_SINTETICOS: True")
    print("PREFLIGHT_CLOUD_REAL: False")
    print("AUTORIZACION_QUALITY_REAL: False")
    print("CONCILIACION_HISTORICA_REEJECUTADA: False")
    print("AUDITORIA_WRITER_INTEGRAL_PROBADA: False")
    print("GCP_EJECUTADO: False")
    print("PUBLICACION_AUTORIZADA: False")
    print("VALIDACION_CONSTRUCTORES_REALES_V3_OK")

finally:

    for df in cacheados:

        df.unpersist()

    spark.stop()
