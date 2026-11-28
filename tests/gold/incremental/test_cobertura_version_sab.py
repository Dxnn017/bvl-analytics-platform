from datetime import date

from pyspark.sql import SparkSession, functions as F
from build_actividad_sab_diaria import construir_gold
from recalcular_actividad_sab import recalcular_actividad_sab

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("BVL_ControlVersionSAB")
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

    sab = spark.createDataFrame([
        (d1, "A", "SAB A", 60., 40., 100.,
         100., 100., 100., "TEST", "2026-01-15"),
        (d2, "A", "SAB A", 60., 40., 100.,
         100., 100., 100., "TEST", "2026-01-16"),
    ], """
        fecha_negociacion date, codigo_sab string,
        SocAgenteBolsa string, MontoCompras double,
        MontoVentas double, MontoTotal double,
        PorcentajeCompras double, PorcentajeVentas double,
        PorcentajeTotal double, fuente_datos string,
        fecha_ingesta string
    """)

    def metadata(fecha, version):
        return {
            "fecha_negociacion": fecha.isoformat(),
            "registros_elegibles": 1,
            "registros_cuarentena": 0,
            "archivos_excluidos": 0,
            "estado_cobertura": "COMPLETA",
            "version_silver": version,
            "scope_quality_id": "TEST-SCOPE-" + version,
            "trazabilidad_verificada": True,
        }

    cobertura_v1 = [
        metadata(d1, "V1"),
        metadata(d2, "V1"),
    ]

    gold_base = construir_gold(
        sab, cobertura_v1, "V1"
    ).cache()

    comprobar(
        gold_base.filter("cobertura_completa").count() == 2,
        "BASE_V1_COBERTURA_COMPLETA"
    )

    fechas_vacias = spark.createDataFrame([], "fecha date")

    print("\n=== MISMA VERSION SILVER ===")

    misma = recalcular_actividad_sab(
        sab, gold_base, fechas_vacias,
        None, "V1"
    ).cache()

    comprobar(
        misma.filter("cobertura_completa").count() == 2,
        "EVIDENCIA_MISMA_VERSION_CONSERVADA"
    )

    misma.unpersist()

    print("\n=== CAMBIO DE VERSION SIN EVIDENCIA ===")

    nueva = recalcular_actividad_sab(
        sab, gold_base, fechas_vacias,
        None, "V2"
    ).cache()

    comprobar(
        nueva.filter(
            "estado_cobertura = 'DESCONOCIDA'"
        ).count() == 2,
        "COBERTURA_ANTIGUA_INVALIDADA"
    )

    comprobar(
        nueva.filter(
            "cobertura_completa OR trazabilidad_verificada"
        ).count() == 0,
        "SIN_ACREDITACION_INDEBIDA"
    )

    comprobar(
        nueva.filter(
            "version_silver IS NOT NULL OR "
            "scope_quality_id IS NOT NULL OR "
            "registros_elegibles IS NOT NULL OR "
            "registros_cuarentena IS NOT NULL OR "
            "archivos_excluidos IS NOT NULL"
        ).count() == 0,
        "METADATOS_ANTIGUOS_RETIRADOS"
    )

    nueva.unpersist()

    print("\n=== CAMBIO DE VERSION CON EVIDENCIA PARCIAL ===")

    candidato = recalcular_actividad_sab(
        sab, gold_base, fechas_vacias,
        [metadata(d1, "V2")], "V2"
    ).cache()

    candidato.select(
        "fecha", "version_silver",
        "estado_cobertura", "cobertura_completa",
        "actividad_diaria_valida"
    ).orderBy("fecha").show(truncate=False)

    filas = {
        r.fecha: r
        for r in candidato.collect()
    }

    comprobar(
        filas[d1].estado_cobertura == "COMPLETA"
        and filas[d1].version_silver == "V2",
        "EVIDENCIA_NUEVA_ACREDITADA_EN_FIXTURE"
    )

    comprobar(
        filas[d2].estado_cobertura == "DESCONOCIDA"
        and filas[d2].version_silver is None,
        "JORNADA_SIN_EVIDENCIA_V2_DESCONOCIDA"
    )

    columnas = [
        "fecha", "codigo_sab",
        "monto_compras", "monto_ventas", "monto_total",
        "actividad_diaria_valida", "hhi_diario",
    ]

    comprobar(
        gold_base.select(*columnas)
        .exceptAll(candidato.select(*columnas)).count() == 0
        and candidato.select(*columnas)
        .exceptAll(gold_base.select(*columnas)).count() == 0,
        "INDICADORES_ORIGINALES_CONSERVADOS"
    )

    candidato.unpersist()

    print("\n=== VERSION NO INFORMADA ===")

    desconocida = recalcular_actividad_sab(
        sab, gold_base, fechas_vacias
    )

    comprobar(
        desconocida.filter(
            "estado_cobertura = 'DESCONOCIDA' "
            "AND cobertura_completa = false"
        ).count() == 2,
        "SIN_VERSION_NO_SE_HEREDA_EVIDENCIA"
    )

    print("CONTROL_VERSION_COBERTURA_SAB_VALIDADO_OK")
    print("PUBLICACION_AUTORIZADA: False")

finally:
    spark.stop()
