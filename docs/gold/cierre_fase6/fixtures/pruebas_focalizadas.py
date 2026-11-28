from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from detectar_alcance_mercado import detectar_claves_mercado
from build_mercado_valor_diario import validar_nemonico_canonico
from cobertura_sab import propagar_cobertura

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("BVL_Cierre_Fase6")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

def comprobar(condicion, mensaje):
    if not condicion:
        raise AssertionError(mensaje)
    print(mensaje + ": OK")

def comprobar_rechazo(funcion, mensaje):
    try:
        funcion()
    except ValueError as exc:
        print(mensaje + ": OK")
        print("MOTIVO:", str(exc))
    else:
        raise AssertionError(mensaje)

try:
    print("\n=== 1. NORMALIZACION CANONICA ===")

    d1 = date(2026, 1, 15)
    d2 = date(2026, 1, 16)
    d3 = date(2026, 1, 17)
    d4 = date(2026, 1, 18)

    esquema = "valor string, fecha_cotizacion date, cierre_actual double"

    antes = spark.createDataFrame(
        [("DEMO1", d1, 10.0)], esquema
    )

    despues = spark.createDataFrame(
        [(" demo1 ", d1, 12.0)], esquema
    )

    claves = detectar_claves_mercado(antes, despues).collect()

    comprobar(
        len(claves) == 1 and claves[0]["nemonico"] == "DEMO1",
        "CLAVE_CANONICA_UNICA"
    )

    colision = antes.unionByName(despues)

    comprobar_rechazo(
        lambda: validar_nemonico_canonico(
            colision, "valor", ["fecha_cotizacion"], "fixture"
        ),
        "COLISION_CANONICA_RECHAZADA"
    )

    for invalido in (None, "", "   "):
        df = spark.createDataFrame(
            [(invalido, d1, 10.0)], esquema
        )

        comprobar_rechazo(
            lambda: validar_nemonico_canonico(
                df, "valor", ["fecha_cotizacion"], "fixture"
            ),
            "NEMONICO_INVALIDO_RECHAZADO"
        )

    print("\n=== 2. COBERTURA SAB ===")

    gold = spark.createDataFrame([
        (d1, "A", 2, True, 100.0, 5000.0),
        (d1, "B", 2, True, 100.0, 5000.0),
        (d2, "A", 1, True, 100.0, 10000.0),
        (d3, "A", 1, True, 100.0, 10000.0),
        (d4, "A", 1, True, 100.0, 10000.0),
    ], """
        fecha date, codigo_sab string,
        cantidad_sab_diaria long,
        actividad_diaria_valida boolean,
        monto_total double, hhi_diario double
    """)

    # Reproduccion de los tres casos del fixture Daniela.
    # La cuarta fecha comprueba ausencia de metadatos.
    metadatos = []

    for fecha, elegibles, cuarentena, excluidos, estado, traza in [
        (d1, 2, 0, 0, "COMPLETA", True),
        (d2, 1, 1, 1, "PARCIAL", True),
        (d3, None, None, None, "DESCONOCIDA", False),
    ]:
        metadatos.append({
            "fecha_negociacion": fecha.isoformat(),
            "registros_elegibles": elegibles,
            "registros_cuarentena": cuarentena,
            "archivos_excluidos": excluidos,
            "estado_cobertura": estado,
            "version_silver": "SINTETICO-V1",
            "scope_quality_id": "TEST-SCOPE-001" if traza else None,
            "trazabilidad_verificada": traza,
        })

    candidato = propagar_cobertura(
        gold, metadatos, "SINTETICO-V1"
    ).cache()

    candidato.select(
        "fecha", "codigo_sab",
        "actividad_diaria_valida",
        "estado_cobertura", "cobertura_completa"
    ).orderBy("fecha", "codigo_sab").show(truncate=False)

    estados = {
        r["fecha"]: (
            r["estado_cobertura"], r["cobertura_completa"]
        )
        for r in candidato.select(
            "fecha", "estado_cobertura", "cobertura_completa"
        ).distinct().collect()
    }

    comprobar(estados[d1] == ("COMPLETA", True), "SAB_COMPLETA")
    comprobar(estados[d2] == ("PARCIAL", False), "SAB_PARCIAL")
    comprobar(estados[d3] == ("DESCONOCIDA", False), "SAB_DESCONOCIDA")
    comprobar(estados[d4] == ("DESCONOCIDA", False), "SAB_SIN_METADATOS")

    comprobar(candidato.count() == gold.count(), "SAB_CONSERVA_FILAS")

    comprobar(
        gold.exceptAll(candidato.select(*gold.columns)).count() == 0
        and candidato.select(*gold.columns).exceptAll(gold).count() == 0,
        "SAB_INDICADORES_ORIGINALES_CONSERVADOS"
    )

    comprobar_rechazo(
        lambda: propagar_cobertura(
            gold, metadatos + [metadatos[0]], "SINTETICO-V1"
        ),
        "SAB_FECHA_DUPLICADA_RECHAZADA"
    )

    comprobar_rechazo(
        lambda: propagar_cobertura(
            gold, metadatos, "OTRA-VERSION"
        ),
        "SAB_VERSION_INCOMPATIBLE_RECHAZADA"
    )

    incorrectos = [dict(metadatos[0], registros_elegibles=99)]

    comprobar_rechazo(
        lambda: propagar_cobertura(
            gold, incorrectos, "SINTETICO-V1"
        ),
        "SAB_CONTEO_INCOMPATIBLE_RECHAZADO"
    )

    candidato.unpersist()

    print("\n=== 3. CONCILIACION GOLD LOCAL ===")

    raiz = Path("outputs/integracion_daniela_local_20261127")

    productos = [
        ("mercado/mercado_valor_diario", ["nemonico", "fecha"], 83130),
        ("mercado/actividad_sab_diaria", ["codigo_sab", "fecha"], 21911),
        ("empresas/empresa_trimestre", ["id_registro"], 5158),
    ]

    total = 0

    for producto, claves, esperado_gcp in productos:
        df = spark.read.parquet(str(raiz / producto)).cache()

        registros = df.count()
        total += registros

        condicion = F.lit(False)

        for clave in claves:
            condicion = condicion | (
                F.col(clave).isNull()
                | (F.trim(F.col(clave).cast("string")) == "")
            )

        invalidos = df.filter(condicion).count()

        duplicados = (
            df.groupBy(*claves).count()
            .filter(F.col("count") > 1).count()
        )

        print("\nPRODUCTO:", producto)
        print("REGISTROS_LOCAL:", registros)
        print("REGISTROS_GCP_REPORTADOS:", esperado_gcp)
        print("DIFERENCIA_LOCAL_GCP:", registros - esperado_gcp)
        print("CLAVES_INVALIDAS:", invalidos)
        print("GRUPOS_DUPLICADOS:", duplicados)

        df.groupBy("anio").count().orderBy("anio").show()

        if producto.endswith("empresa_trimestre"):
            print("\n=== AUDITORIA FINANCIERA BNB ===")

            bnb = df.filter(F.col("rpj") == "SG0005")

            columnas = [
                c for c in [
                    "id_registro", "rpj", "ejercicio",
                    "trimestre_num", "activo_total",
                    "utilidad_neta", "colision_financiera",
                    "apto_kpi_financiero"
                ] if c in df.columns
            ]

            bnb.select(*columnas).orderBy(
                "ejercicio", "trimestre_num"
            ).show(100, truncate=False)

            if "colision_financiera" in df.columns:
                print(
                    "COLISIONES_FINANCIERAS_TOTAL:",
                    df.filter(F.col("colision_financiera") == True).count()
                )

                print(
                    "COLISIONES_BNB:",
                    bnb.filter(F.col("colision_financiera") == True).count()
                )

            print("COLUMNAS_FINANCIERAS:", df.columns)

        df.unpersist()

    print("\nTOTAL_GOLD_LOCAL:", total)
    print("TOTAL_GCP_REPORTADO:", 110199)
    print("DIFERENCIA_TOTAL:", total - 110199)

    print("PUBLICACION_AUTORIZADA: False")
    print("PRUEBAS_FOCALIZADAS_Y_AUDITORIA_FINALIZADAS_OK")

finally:
    spark.stop()
