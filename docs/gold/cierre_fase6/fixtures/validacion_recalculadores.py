from datetime import date

from pyspark.sql import SparkSession, functions as F

from build_mercado_valor_diario import construir_gold as construir_mercado
from build_actividad_sab_diaria import construir_gold as construir_sab
from recalcular_mercado_valor import recalcular_mercado_valor
from recalcular_actividad_sab import recalcular_actividad_sab
from detectar_alcance_mercado import detectar_claves_mercado

spark = (
    SparkSession.builder.master("local[2]")
    .appName("BVL_Fase6_Recalculadores")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

def comprobar(condicion, mensaje):
    assert condicion, mensaje
    print(mensaje + ": OK")

try:
    d1 = date(2026, 1, 15)
    d2 = date(2026, 1, 16)

    print("=== MERCADO INCREMENTAL ===")

    cotizaciones = spark.createDataFrame([
        (d1, "DEMO1", "Demo", d1,
         9., 10., 9., 11., 8., 10., 100., "TEST", "2026-01-15"),
        (d2, "DEMO2", "Demo 2", d1,
         9., 10., 9., 11., 8., 10., 100., "TEST", "2026-01-16"),
    ], """
        fecha_cotizacion date, valor string, descripcion string,
        fecha_anterior date, cierre_anterior double,
        cierre_actual double, apertura_actual double,
        maxima_actual double, minima_actual double,
        promedio_actual double, monto_negociado double,
        fuente string, fecha_ingesta string
    """)

    tc = spark.createDataFrame(
        [(d1, 3.7), (d2, 3.7)],
        "fecha date, tipo_cambio_venta double"
    )

    embig = spark.createDataFrame(
        [(d1, 150.), (d2, 150.)],
        "fecha date, embig_peru double"
    )

    puente = spark.createDataFrame([
        ("DEMO1", "TEST01", "Demo", "ISIN1", "ACCION", "PEN", d1),
        ("DEMO2", "TEST02", "Demo 2", "ISIN2", "ACCION", "PEN", d1),
    ], """
        nemonico_valor string, ruc string, razon_social string,
        codigo_isin string, tipo_valor string,
        moneda string, fecha_inscripcion date
    """)

    base = construir_mercado(
        cotizaciones, tc, embig, puente
    ).cache()

    nuevas = (
        cotizaciones.withColumn(
            "valor",
            F.when(F.col("valor") == "DEMO1", " demo1 ")
            .otherwise(F.col("valor"))
        )
        .withColumn(
            "cierre_actual",
            F.when(F.col("valor") == " demo1 ", 12.)
            .otherwise(F.col("cierre_actual"))
        )
    )

    claves = detectar_claves_mercado(cotizaciones, nuevas)

    candidato = recalcular_mercado_valor(
        nuevas, tc, embig, puente,
        base.drop("nemonico_original"), claves
    ).cache()

    fila = candidato.filter(
        F.col("nemonico") == "DEMO1"
    ).first()

    comprobar(candidato.count() == 2, "MERCADO_CONSERVA_FILAS")
    comprobar(fila.nemonico_original == " demo1 ", "MERCADO_TRAZABILIDAD")
    comprobar(fila.precio_cierre == 12., "MERCADO_PRECIO_ACTUALIZADO")
    comprobar(fila.ruc == "TEST01", "MERCADO_PUENTE_CANONICO")

    comprobar(
        base.filter("nemonico = 'DEMO2'")
        .select(*candidato.columns)
        .exceptAll(candidato.filter("nemonico = 'DEMO2'"))
        .count() == 0,
        "MERCADO_NO_AFECTADOS_CONSERVADOS"
    )

    replay = recalcular_mercado_valor(
        nuevas, tc, embig, puente, candidato, claves
    )

    comprobar(
        candidato.exceptAll(replay.select(*candidato.columns)).count() == 0
        and replay.select(*candidato.columns).exceptAll(candidato).count() == 0,
        "MERCADO_IDEMPOTENCIA"
    )

    print("=== SAB INCREMENTAL ===")

    sab = spark.createDataFrame([
        (d1, "A", "SAB A", 60., 40., 100., 50., 50., 50., "TEST", "2026-01-15"),
        (d1, "B", "SAB B", 60., 40., 100., 50., 50., 50., "TEST", "2026-01-15"),
        (d2, "A", "SAB A", 60., 40., 100., 100., 100., 100., "TEST", "2026-01-16"),
    ], """
        fecha_negociacion date, codigo_sab string,
        SocAgenteBolsa string, MontoCompras double,
        MontoVentas double, MontoTotal double,
        PorcentajeCompras double, PorcentajeVentas double,
        PorcentajeTotal double, fuente_datos string,
        fecha_ingesta string
    """)

    anterior = construir_sab(sab).cache()

    metadatos = [{
        "fecha_negociacion": d2.isoformat(),
        "registros_elegibles": 1,
        "registros_cuarentena": 1,
        "archivos_excluidos": 1,
        "estado_cobertura": "PARCIAL",
        "version_silver": "SINTETICO-V1",
        "scope_quality_id": "TEST-SCOPE-001",
        "trazabilidad_verificada": True,
    }]

    fechas_vacias = spark.createDataFrame([], "fecha date")

    actualizado = recalcular_actividad_sab(
        sab, anterior, fechas_vacias,
        metadatos, "SINTETICO-V1"
    ).cache()

    fila = actualizado.filter(F.col("fecha") == d2).first()

    comprobar(fila.estado_cobertura == "PARCIAL", "SAB_CAMBIO_SOLO_METADATA")
    comprobar(fila.actividad_diaria_valida, "SAB_VALIDEZ_ARITMETICA")
    comprobar(not fila.cobertura_completa, "SAB_COBERTURA_INCOMPLETA")
    comprobar(actualizado.count() == anterior.count(), "SAB_FILAS_CONSERVADAS")

    comprobar(
        anterior.filter(F.col("fecha") == d1)
        .exceptAll(actualizado.filter(F.col("fecha") == d1)
                   .select(*anterior.columns)).count() == 0,
        "SAB_NO_AFECTADOS_CONSERVADOS"
    )

    replay_sab = recalcular_actividad_sab(
        sab, actualizado, fechas_vacias,
        metadatos, "SINTETICO-V1"
    )

    comprobar(
        actualizado.exceptAll(replay_sab.select(*actualizado.columns)).count() == 0
        and replay_sab.select(*actualizado.columns).exceptAll(actualizado).count() == 0,
        "SAB_IDEMPOTENCIA"
    )

    print("=== ELEGIBILIDAD FINANCIERA BNB ===")

    financiero = spark.read.parquet(
        "outputs/integracion_daniela_local_20261127/empresas/empresa_trimestre"
    )

    bnb = financiero.filter(
        (F.col("rpj") == "SG0005")
        & F.col("colision_financiera")
    )

    bnb.select(
        "id_registro", "ejercicio", "trimestre_num",
        "colision_financiera", "apto_kpi_general",
        "roa_calculable", "roe_calculable",
        "endeudamiento_calculable", "margen_neto_calculable",
        "autonomia_calculable", "motivos_calidad"
    ).show(truncate=False)

    comprobar(bnb.count() == 6, "BNB_SEIS_FILAS_CONSERVADAS")

    campos = [
        "apto_kpi_general", "roa_calculable", "roe_calculable",
        "endeudamiento_calculable", "margen_neto_calculable",
        "autonomia_calculable",
    ]

    condicion = F.lit(False)

    for campo in campos:
        condicion = condicion | F.coalesce(F.col(campo), F.lit(True))

    comprobar(
        bnb.filter(condicion).count() == 0,
        "BNB_EXCLUIDOS_KPI"
    )

    print("PUBLICACION_AUTORIZADA: False")
    print("VALIDACION_RECALCULADORES_FASE6_OK")

finally:
    spark.stop()
