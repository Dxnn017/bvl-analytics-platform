import csv
import json
from pathlib import Path
from collections import Counter, defaultdict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# CONFIGURACION
# ============================================================

BRIDGE_CSV = Path(
    "/media/sf_bvl_shared/puentes_smv/"
    "valores_inscritos_smv.csv"
)

COTIZACIONES = (
    "/datalake/bvl/silver/"
    "mercado_diario/cotizaciones"
)

OUT = Path(
    "outputs/quality/"
    "valores_smv_bridge_analysis.json"
)

# ============================================================
# UTILIDADES
# ============================================================

def norm(x):
    return str(x or "").strip().upper()


# ============================================================
# LEER PUENTE SMV
# ============================================================

with open(
    BRIDGE_CSV,
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
    r["_nemonico_norm"] = norm(
        r.get("NemonicoValor")
    )
    r["_isin_norm"] = norm(
        r.get("CodigoISIN")
    )
    r["_tipo_norm"] = norm(
        r.get("TipoValor")
    )

rows_nemonico = [
    r for r in rows
    if r["_nemonico_norm"]
]

pares = {
    (
        r["_ruc_norm"],
        r["_nemonico_norm"]
    )
    for r in rows_nemonico
}

nemonicos_smv = {
    r["_nemonico_norm"]
    for r in rows_nemonico
}

rucs_con_valores = {
    r["_ruc_norm"]
    for r in rows_nemonico
    if r["_ruc_norm"]
}

# ============================================================
# CARDINALIDAD DEL NEMONICO EN SMV
# ============================================================

nemonico_rucs = defaultdict(set)

for r in rows_nemonico:
    nemonico_rucs[
        r["_nemonico_norm"]
    ].add(
        r["_ruc_norm"]
    )

ambiguos = {
    nem: sorted(rucs)
    for nem, rucs in nemonico_rucs.items()
    if len(rucs) > 1
}

duplicados_par = (
    len(rows_nemonico)
    - len(pares)
)

# ============================================================
# LEER COTIZACIONES
# ============================================================

spark = (
    SparkSession.builder
    .appName("Analisis-Puente-Valores-SMV")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

cot = spark.read.parquet(
    COTIZACIONES
)

valores_cot = {
    norm(r["valor"])
    for r in (
        cot
        .select("valor")
        .filter(
            F.col("valor").isNotNull()
        )
        .distinct()
        .collect()
    )
    if norm(r["valor"])
}

# ============================================================
# MATCH EXACTO
# ============================================================

matches = (
    nemonicos_smv
    & valores_cot
)

sin_match_smv = (
    nemonicos_smv
    - valores_cot
)

sin_puente_cot = (
    valores_cot
    - nemonicos_smv
)

rows_match = [
    r
    for r in rows_nemonico
    if r["_nemonico_norm"] in matches
]

rucs_match = {
    r["_ruc_norm"]
    for r in rows_match
    if r["_ruc_norm"]
}

pares_match = {
    (
        r["_ruc_norm"],
        r["_nemonico_norm"]
    )
    for r in rows_match
}

# ============================================================
# TIPOS DE VALOR QUE SI HACEN MATCH
# ============================================================

tipos_match = Counter(
    r["_tipo_norm"] or "(VACIO)"
    for r in rows_match
)

# ============================================================
# EMPRESAS CON MAS NEMONICOS MATCH
# ============================================================

match_por_ruc = Counter(
    r["_ruc_norm"]
    for r in rows_match
    if r["_ruc_norm"]
)

top_rucs = (
    match_por_ruc
    .most_common(15)
)

# ============================================================
# SALIDA
# ============================================================

print("=" * 80)
print("ANALISIS PUENTE OFICIAL SMV -> COTIZACIONES")
print("=" * 80)

print()
print("FUENTE VALORES SMV")
print("Registros             :", f"{len(rows):,}")
print("RUC con valores       :", f"{len(rucs_con_valores):,}")
print("Nemonicos distintos   :", f"{len(nemonicos_smv):,}")
print("Pares RUC+nemonico    :", f"{len(pares):,}")
print("Duplicados del par    :", f"{duplicados_par:,}")
print(
    "Nemonicos con >1 RUC  :",
    f"{len(ambiguos):,}"
)

print()
print("COTIZACIONES")
print(
    "Valores distintos     :",
    f"{len(valores_cot):,}"
)

print()
print("=" * 80)
print("COBERTURA EXACTA")
print("=" * 80)

print(
    "Nemonicos SMV que hacen MATCH :",
    f"{len(matches):,}"
)

print(
    "Nemonicos SMV sin MATCH       :",
    f"{len(sin_match_smv):,}"
)

print(
    "Valores Cotizaciones sin puente:",
    f"{len(sin_puente_cot):,}"
)

cobertura_cot = (
    100 * len(matches) / len(valores_cot)
    if valores_cot
    else 0
)

cobertura_smv = (
    100 * len(matches) / len(nemonicos_smv)
    if nemonicos_smv
    else 0
)

print(
    "Cobertura sobre Cotizaciones  :",
    f"{cobertura_cot:.2f}%"
)

print(
    "Cobertura sobre nemonicos SMV :",
    f"{cobertura_smv:.2f}%"
)

print()
print(
    "RUC con al menos 1 MATCH      :",
    f"{len(rucs_match):,}"
)

print(
    "Cobertura de los 148 con valor:",
    f"{100 * len(rucs_match) / 148:.2f}%"
)

print(
    "Cobertura de los 289 RUC      :",
    f"{100 * len(rucs_match) / 289:.2f}%"
)

print()
print("=" * 80)
print("TIPOS DE VALOR CON MATCH")
print("=" * 80)

for tipo, n in tipos_match.most_common():
    print(f"{tipo:<45} {n:>5}")

print()
print("=" * 80)
print("TOP RUC POR NEMONICOS CON MATCH")
print("=" * 80)

for ruc, n in top_rucs:
    empresa = next(
        (
            r.get("_empresa_origen", "")
            for r in rows_match
            if r["_ruc_norm"] == ruc
        ),
        ""
    )

    print(
        f"{ruc:<12} | {n:>3} | {empresa}"
    )

print()
print("=" * 80)
print("PRIMEROS MATCH")
print("=" * 80)

for n in sorted(matches)[:30]:
    print("-", n)

print()
print("=" * 80)
print("PRIMEROS VALORES COTIZADOS SIN PUENTE")
print("=" * 80)

for n in sorted(sin_puente_cot)[:30]:
    print("-", n)

if ambiguos:
    print()
    print("=" * 80)
    print("ALERTA: NEMONICOS ASOCIADOS A MAS DE UN RUC")
    print("=" * 80)

    for nem, rucs in list(
        sorted(ambiguos.items())
    )[:20]:
        print(
            nem,
            "->",
            ", ".join(rucs)
        )

# ============================================================
# JSON DE CALIDAD
# ============================================================

resultado = {
    "registros_valores_smv": len(rows),
    "ruc_con_valores": len(rucs_con_valores),
    "nemonicos_smv_distintos": len(nemonicos_smv),
    "pares_ruc_nemonico": len(pares),
    "duplicados_par_ruc_nemonico": duplicados_par,
    "nemonicos_asociados_multiples_ruc": len(ambiguos),

    "valores_cotizaciones_distintos": len(valores_cot),

    "nemonicos_match": len(matches),
    "nemonicos_smv_sin_match": len(sin_match_smv),
    "valores_cotizaciones_sin_puente": len(sin_puente_cot),

    "cobertura_cotizaciones_pct": round(
        cobertura_cot,
        4
    ),

    "cobertura_nemonicos_smv_pct": round(
        cobertura_smv,
        4
    ),

    "ruc_con_al_menos_un_match": len(rucs_match),

    "pares_ruc_nemonico_match": len(
        pares_match
    ),

    "tipos_valor_match": dict(
        tipos_match
    ),

    "nemonicos_ambiguos": ambiguos
}

OUT.parent.mkdir(
    parents=True,
    exist_ok=True
)

OUT.write_text(
    json.dumps(
        resultado,
        indent=2,
        ensure_ascii=False
    ),
    encoding="utf-8"
)

print()
print("=" * 80)
print("ARCHIVO GENERADO")
print("=" * 80)
print(OUT)

spark.stop()
