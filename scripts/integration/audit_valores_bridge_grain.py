import csv
import json
from pathlib import Path
from collections import defaultdict, Counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# CONFIGURACION
# ============================================================

BRIDGE = Path(
    "/media/sf_bvl_shared/puentes_smv/"
    "valores_inscritos_smv.csv"
)

COTIZACIONES = (
    "/datalake/bvl/silver/"
    "mercado_diario/cotizaciones"
)

OUT = Path(
    "outputs/quality/"
    "valores_smv_bridge_grain_audit.json"
)


def norm(x):
    return str(x or "").strip().upper()


# ============================================================
# 1. LEER VALORES SMV
# ============================================================

with open(
    BRIDGE,
    "r",
    encoding="utf-8-sig",
    newline=""
) as f:
    rows = list(csv.DictReader(f))

for r in rows:
    r["_ruc_norm"] = norm(
        r.get("_ruc_consultado")
        or r.get("RUC")
    )
    r["_nem_norm"] = norm(
        r.get("NemonicoValor")
    )

validos = [
    r for r in rows
    if r["_ruc_norm"]
    and r["_nem_norm"]
]

# ============================================================
# 2. AUDITAR GRANULARIDAD RUC + NEMONICO
# ============================================================

grupos = defaultdict(list)

for r in validos:
    grupos[
        (
            r["_ruc_norm"],
            r["_nem_norm"]
        )
    ].append(r)

repetidos = {
    k: v
    for k, v in grupos.items()
    if len(v) > 1
}

registros_extra = sum(
    len(v) - 1
    for v in repetidos.values()
)

print("=" * 85)
print("AUDITORIA DE GRANULARIDAD - VALORES INSCRITOS SMV")
print("=" * 85)

print("Registros fuente            :", f"{len(rows):,}")
print("Registros con RUC+nemonico  :", f"{len(validos):,}")
print("Pares RUC+nemonico          :", f"{len(grupos):,}")
print("Grupos repetidos            :", f"{len(repetidos):,}")
print("Registros extra             :", f"{registros_extra:,}")

# ============================================================
# 3. ¿POR QUE SE REPITEN?
# ============================================================

campos_negocio = [
    "CodigoISIN",
    "DenominacionValor",
    "TipoValor",
    "FechaInscripcion",
    "ResolucionInscripcion",
    "Moneda",
    "MontoInscrito",
    "Cotizacion",
    "FechaUltCot"
]

causas = Counter()

detalle_repetidos = []

for (ruc, nem), registros in repetidos.items():

    variaciones = {}

    for campo in campos_negocio:

        vals = {
            norm(r.get(campo))
            for r in registros
            if norm(r.get(campo))
        }

        if len(vals) > 1:
            variaciones[campo] = sorted(vals)

    if variaciones:
        for campo in variaciones:
            causas[campo] += 1
    else:
        causas["SIN_DIFERENCIA_NEGOCIO"] += 1

    detalle_repetidos.append({
        "ruc": ruc,
        "nemonico": nem,
        "filas": len(registros),
        "variaciones": variaciones
    })

print()
print("=" * 85)
print("CAMPOS QUE EXPLICAN REPETICIONES")
print("=" * 85)

for campo, n in causas.most_common():
    print(f"{campo:<35} {n:>5}")

print()
print("=" * 85)
print("PRIMEROS PARES REPETIDOS")
print("=" * 85)

for d in sorted(
    detalle_repetidos,
    key=lambda x: x["filas"],
    reverse=True
)[:15]:

    print()
    print(
        d["ruc"],
        "|",
        d["nemonico"],
        "| filas:",
        d["filas"]
    )

    if d["variaciones"]:

        for campo, valores in d["variaciones"].items():

            vista = valores[:5]

            print(
                "   ",
                campo,
                "=>",
                vista
            )

    else:
        print(
            "    Sin diferencias en campos de negocio"
        )


# ============================================================
# 4. CONJUNTO DE NEMONICOS OFICIALES
# ============================================================

nemonicos_smv = {
    r["_nem_norm"]
    for r in validos
}

# ============================================================
# 5. LEER COTIZACIONES
# ============================================================

spark = (
    SparkSession.builder
    .appName("Auditoria-Cobertura-Puente-SMV")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

cot = spark.read.parquet(
    COTIZACIONES
)

cot = cot.withColumn(
    "valor_norm",
    F.upper(
        F.trim(
            F.col("valor")
        )
    )
)

total_cot = cot.count()

# ============================================================
# 6. MATCH A NIVEL DE FILAS
# ============================================================

lista_nemonicos = sorted(
    nemonicos_smv
)

cot_match = cot.filter(
    F.col("valor_norm").isin(
        lista_nemonicos
    )
)

cot_no_match = cot.filter(
    ~F.col("valor_norm").isin(
        lista_nemonicos
    )
)

filas_match = cot_match.count()
filas_no_match = cot_no_match.count()

cobertura_filas = (
    100.0 * filas_match / total_cot
    if total_cot
    else 0
)

print()
print("=" * 85)
print("COBERTURA SOBRE REGISTROS DE COTIZACIONES")
print("=" * 85)

print(
    "Filas totales Cotizaciones :",
    f"{total_cot:,}"
)

print(
    "Filas con puente SMV       :",
    f"{filas_match:,}"
)

print(
    "Filas sin puente SMV       :",
    f"{filas_no_match:,}"
)

print(
    "Cobertura por filas        :",
    f"{cobertura_filas:.2f}%"
)

# ============================================================
# 7. MATCH POR AÑO
# ============================================================

print()
print("=" * 85)
print("COBERTURA POR AÑO")
print("=" * 85)

por_anio_total = {
    int(r["anio"]): int(r["count"])
    for r in (
        cot
        .groupBy("anio")
        .count()
        .collect()
    )
}

por_anio_match = {
    int(r["anio"]): int(r["count"])
    for r in (
        cot_match
        .groupBy("anio")
        .count()
        .collect()
    )
}

cobertura_anual = {}

for anio in sorted(por_anio_total):

    total = por_anio_total[anio]
    match = por_anio_match.get(
        anio,
        0
    )

    pct = (
        100.0 * match / total
        if total
        else 0
    )

    cobertura_anual[anio] = {
        "total": total,
        "match": match,
        "pct": round(pct, 4)
    }

    print(
        f"{anio}: "
        f"{match:,} / {total:,} "
        f"= {pct:.2f}%"
    )

# ============================================================
# 8. TOP VALORES SIN PUENTE POR CANTIDAD DE FILAS
# ============================================================

print()
print("=" * 85)
print("TOP 30 VALORES SIN PUENTE POR NUMERO DE COTIZACIONES")
print("=" * 85)

cols = [
    F.col("valor_norm").alias("valor"),
    F.count("*").alias("filas")
]

if "descripcion" in cot.columns:
    cols.append(
        F.first(
            "descripcion",
            ignorenulls=True
        ).alias("descripcion")
    )

top_no_match = (
    cot_no_match
    .groupBy("valor_norm")
    .agg(
        F.count("*").alias("filas"),
        *(
            [
                F.first(
                    "descripcion",
                    ignorenulls=True
                ).alias("descripcion")
            ]
            if "descripcion" in cot.columns
            else []
        )
    )
    .orderBy(
        F.desc("filas")
    )
    .limit(30)
    .collect()
)

top_no_match_json = []

for r in top_no_match:

    valor = r["valor_norm"]
    filas = int(r["filas"])

    descripcion = (
        r["descripcion"]
        if "descripcion" in r.asDict()
        else None
    )

    print(
        f"{valor:<18} "
        f"{filas:>6} | "
        f"{descripcion or ''}"
    )

    top_no_match_json.append({
        "valor": valor,
        "filas": filas,
        "descripcion": descripcion
    })

# ============================================================
# 9. TOP VALORES CON PUENTE
# ============================================================

print()
print("=" * 85)
print("TOP 30 VALORES CON PUENTE")
print("=" * 85)

top_match = (
    cot_match
    .groupBy("valor_norm")
    .agg(
        F.count("*").alias("filas"),
        *(
            [
                F.first(
                    "descripcion",
                    ignorenulls=True
                ).alias("descripcion")
            ]
            if "descripcion" in cot.columns
            else []
        )
    )
    .orderBy(
        F.desc("filas")
    )
    .limit(30)
    .collect()
)

for r in top_match:

    descripcion = (
        r["descripcion"]
        if "descripcion" in r.asDict()
        else ""
    )

    print(
        f"{r['valor_norm']:<18} "
        f"{int(r['filas']):>6} | "
        f"{descripcion or ''}"
    )

# ============================================================
# 10. GUARDAR AUDITORIA
# ============================================================

resultado = {
    "valores_smv": {
        "registros_fuente": len(rows),
        "registros_con_ruc_nemonico": len(validos),
        "pares_ruc_nemonico": len(grupos),
        "grupos_repetidos": len(repetidos),
        "registros_extra": registros_extra,
        "causas_repeticion": dict(causas)
    },

    "cotizaciones": {
        "filas_totales": total_cot,
        "filas_con_puente": filas_match,
        "filas_sin_puente": filas_no_match,
        "cobertura_filas_pct": round(
            cobertura_filas,
            4
        ),
        "cobertura_por_anio": cobertura_anual
    },

    "top_valores_sin_puente": (
        top_no_match_json
    )
}

OUT.parent.mkdir(
    parents=True,
    exist_ok=True
)

OUT.write_text(
    json.dumps(
        resultado,
        ensure_ascii=False,
        indent=2
    ),
    encoding="utf-8"
)

print()
print("=" * 85)
print("ARCHIVO GENERADO")
print("=" * 85)
print(OUT)

spark.stop()
