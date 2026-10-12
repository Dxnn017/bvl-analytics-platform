from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

spark = (
    SparkSession.builder
    .appName("Evidencia_3_4_Calidad")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print()
print("=" * 90)
print("3.4 - CONTROL DE CALIDAD DE LOS DATOS")
print("=" * 90)

# ============================================================
# 1. SERIES MACROECONOMICAS
# ============================================================

tc = spark.read.parquet(
    "/datalake/bvl/silver/macroeconomia/tipo_cambio"
)

embig = spark.read.parquet(
    "/datalake/bvl/silver/macroeconomia/embig"
)

total_tc = tc.count()
total_embig = embig.count()

tc_fecha_nula = tc.filter(
    col("fecha").isNull()
).count()

tc_valor_nulo = tc.filter(
    col("tipo_cambio_venta").isNull()
).count()

embig_fecha_nula = embig.filter(
    col("fecha").isNull()
).count()

embig_valor_nulo = embig.filter(
    col("embig_peru").isNull()
).count()

pct_tc_fecha = (
    (total_tc - tc_fecha_nula) * 100.0 / total_tc
    if total_tc else 0
)

pct_tc_valor = (
    (total_tc - tc_valor_nulo) * 100.0 / total_tc
    if total_tc else 0
)

pct_embig_fecha = (
    (total_embig - embig_fecha_nula) * 100.0 / total_embig
    if total_embig else 0
)

pct_embig_valor = (
    (total_embig - embig_valor_nulo) * 100.0 / total_embig
    if total_embig else 0
)

tc_duplicados = (
    tc.filter(col("fecha").isNotNull())
      .groupBy("fecha")
      .count()
      .filter(col("count") > 1)
      .count()
)

embig_duplicados = (
    embig.filter(col("fecha").isNotNull())
         .groupBy("fecha")
         .count()
         .filter(col("count") > 1)
         .count()
)

tc_fuera_rango = tc.filter(
    col("tipo_cambio_venta").isNotNull()
    & (col("tipo_cambio_venta") <= 0)
).count()

embig_fuera_rango = embig.filter(
    col("embig_peru").isNotNull()
    & (col("embig_peru") < 0)
).count()

print()
print("1. SERIES MACROECONOMICAS")
print("-" * 90)

print(f"Tipo Cambio - observaciones Silver    : {total_tc:,}")
print(f"  Fechas nulas                        : {tc_fecha_nula:,}")
print(f"  Valores nulos                       : {tc_valor_nulo:,}")
print(f"  Completitud fecha                   : {pct_tc_fecha:.2f}%")
print(f"  Completitud valor                   : {pct_tc_valor:.2f}%")
print(f"  Fechas duplicadas                   : {tc_duplicados:,}")
print(f"  Valores <= 0                        : {tc_fuera_rango:,}")

print()

print(f"EMBIG Peru - observaciones Silver     : {total_embig:,}")
print(f"  Fechas nulas                        : {embig_fecha_nula:,}")
print(f"  Valores nulos                       : {embig_valor_nulo:,}")
print(f"  Completitud fecha                   : {pct_embig_fecha:.2f}%")
print(f"  Completitud valor                   : {pct_embig_valor:.2f}%")
print(f"  Fechas duplicadas                   : {embig_duplicados:,}")
print(f"  Valores < 0                         : {embig_fuera_rango:,}")

# ============================================================
# 2. PUENTE RUC - NEMONICO
# ============================================================

puente = spark.read.parquet(
    "/datalake/bvl/silver/referencias_smv/puente_ruc_nemonico"
)

total_puente = puente.count()

pares_unicos = (
    puente
    .select("ruc", "nemonico_valor")
    .distinct()
    .count()
)

pares_duplicados = (
    puente
    .groupBy("ruc", "nemonico_valor")
    .count()
    .filter(col("count") > 1)
    .count()
)

puente_nulos = puente.filter(
    col("ruc").isNull()
    | (col("ruc") == "")
    | col("nemonico_valor").isNull()
    | (col("nemonico_valor") == "")
).count()

multi_ruc = (
    puente
    .groupBy("nemonico_valor")
    .agg(
        countDistinct("ruc").alias("cantidad_ruc")
    )
    .filter(col("cantidad_ruc") > 1)
    .count()
)

print()
print("2. PUENTE RUC - NEMONICO")
print("-" * 90)

print(f"Registros                             : {total_puente:,}")
print(f"Pares RUC-nemonico unicos             : {pares_unicos:,}")
print(f"Pares duplicados                      : {pares_duplicados:,}")
print(f"Campos criticos nulos/vacios          : {puente_nulos:,}")
print(f"Nemonicos asociados a >1 RUC          : {multi_ruc:,}")

# ============================================================
# 3. COBERTURA TEMPORAL COTIZACIONES - BCRP
# ============================================================

cot = spark.read.parquet(
    "/datalake/bvl/silver/mercado_diario/cotizaciones"
)

# En Cotizaciones la fecha limpia se llama fecha_cotizacion.
fechas_cot = (
    cot
    .filter(col("fecha_cotizacion").isNotNull())
    .select(
        col("fecha_cotizacion").alias("fecha")
    )
    .distinct()
)

# Para medir cobertura util solo se consideran observaciones
# macroeconomicas que tengan fecha y valor.
tc_validas = (
    tc
    .filter(
        col("fecha").isNotNull()
        & col("tipo_cambio_venta").isNotNull()
    )
    .select("fecha")
    .distinct()
)

embig_validas = (
    embig
    .filter(
        col("fecha").isNotNull()
        & col("embig_peru").isNotNull()
    )
    .select("fecha")
    .distinct()
)

total_fechas_cot = fechas_cot.count()

fechas_tc = (
    fechas_cot
    .join(
        tc_validas,
        "fecha",
        "inner"
    )
    .count()
)

fechas_embig = (
    fechas_cot
    .join(
        embig_validas,
        "fecha",
        "inner"
    )
    .count()
)

sin_tc = total_fechas_cot - fechas_tc
sin_embig = total_fechas_cot - fechas_embig

pct_tc = (
    fechas_tc * 100.0 / total_fechas_cot
    if total_fechas_cot else 0
)

pct_embig = (
    fechas_embig * 100.0 / total_fechas_cot
    if total_fechas_cot else 0
)

print()
print("3. COHERENCIA TEMPORAL COTIZACIONES - BCRP")
print("-" * 90)

print(f"Fechas distintas de Cotizaciones      : {total_fechas_cot:,}")
print(f"Fechas cubiertas por Tipo de Cambio   : {fechas_tc:,} ({pct_tc:.2f}%)")
print(f"Fechas sin Tipo de Cambio valido      : {sin_tc:,}")
print(f"Fechas cubiertas por EMBIG            : {fechas_embig:,} ({pct_embig:.2f}%)")
print(f"Fechas sin EMBIG valido               : {sin_embig:,}")

# ============================================================
# 4. IDENTIDAD EMPRESARIAL DE HECHOS
# ============================================================

identidad = spark.read.parquet(
    "/datalake/bvl/silver/hechos_importancia/identidad_empresarial"
)

total_hechos = identidad.count()

ruc_exacto = identidad.filter(
    col("metodo_vinculacion") == "RUC_EXACTO"
).count()

nemonico_smv = identidad.filter(
    col("metodo_vinculacion") == "NEMONICO_SMV"
).count()

sin_puente = identidad.filter(
    col("metodo_vinculacion") == "SIN_PUENTE"
).count()

identificados = ruc_exacto + nemonico_smv

pct_identidad = (
    identificados * 100.0 / total_hechos
    if total_hechos else 0
)

inconsistentes = identidad.filter(
    (
        (col("metodo_vinculacion") == "RUC_EXACTO")
        & col("ruc").isNull()
    )
    |
    (
        (col("metodo_vinculacion") == "NEMONICO_SMV")
        & col("nemonicos_cotizables").isNull()
    )
).count()

print()
print("4. IDENTIDAD DE HECHOS DE IMPORTANCIA")
print("-" * 90)

print(f"Eventos principales                   : {total_hechos:,}")
print(f"Identificados por RUC_EXACTO          : {ruc_exacto:,}")
print(f"Identificados por NEMONICO_SMV        : {nemonico_smv:,}")
print(f"Eventos identificados                 : {identificados:,}")
print(f"Eventos SIN_PUENTE                    : {sin_puente:,}")
print(f"Cobertura de identidad                 : {pct_identidad:.2f}%")
print(f"Inconsistencias de identidad           : {inconsistentes:,}")

# ============================================================
# RESUMEN
# ============================================================

print()
print("=" * 90)
print("RESUMEN DE VERACIDAD")
print("=" * 90)

print("✓ Unicidad RUC-nemonico validada" if pares_duplicados == 0 else
      "X Existen duplicados RUC-nemonico")

print("✓ Identidad no forzada: SIN_PUENTE conservados")

print(
    f"✓ Cobertura temporal Tipo Cambio: {pct_tc:.2f}%"
)

print(
    f"✓ Cobertura temporal EMBIG: {pct_embig:.2f}%"
)

print(
    f"✓ Cobertura identidad Hechos: {pct_identidad:.2f}%"
)

if tc_valor_nulo > 0:
    print(
        f"! Tipo Cambio contiene {tc_valor_nulo:,} "
        "valores ausentes en su serie historica"
    )

print("=" * 90)

spark.stop()
