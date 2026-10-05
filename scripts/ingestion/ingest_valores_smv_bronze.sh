#!/bin/bash
set -e

# ============================================================
# CONFIGURACION
# ============================================================

BASE_LOCAL="/media/sf_bvl_shared/puentes_smv"

RAW_XML="$BASE_LOCAL/raw_valores_smv"
JSON="$BASE_LOCAL/valores_inscritos_smv.json"
LOG="$BASE_LOCAL/extraccion_valores_smv_log.csv"

FECHA_INGESTA="2026-10-05"

HDFS_BASE="/datalake/bvl/bronze/smv/valores_inscritos"

HDFS_XML="$HDFS_BASE/xml/ingestion_date=$FECHA_INGESTA"
HDFS_JSON="$HDFS_BASE/json/ingestion_date=$FECHA_INGESTA"

HDFS_METADATA="/datalake/bvl/metadata/smv/valores_inscritos/ingestion_date=$FECHA_INGESTA"

echo "============================================================"
echo "CARGA BRONZE - VALORES INSCRITOS SMV"
echo "============================================================"

# ============================================================
# VALIDACION LOCAL
# ============================================================

echo
echo "=== VALIDACION FUENTE LOCAL ==="

XML_COUNT=$(find "$RAW_XML" \
    -maxdepth 1 \
    -type f \
    -name '*.xml' \
    | wc -l)

echo "XML originales : $XML_COUNT"

if [ "$XML_COUNT" -ne 289 ]; then
    echo "ERROR: se esperaban 289 respuestas XML."
    exit 1
fi

if [ ! -f "$JSON" ]; then
    echo "ERROR: no existe $JSON"
    exit 1
fi

if [ ! -f "$LOG" ]; then
    echo "ERROR: no existe $LOG"
    exit 1
fi

python3 - <<'PY'
import json
from pathlib import Path

p = Path(
    "/media/sf_bvl_shared/puentes_smv/"
    "valores_inscritos_smv.json"
)

data = json.loads(
    p.read_text(encoding="utf-8")
)

registros = data.get("Resultado", [])

print("Registros JSON :", len(registros))

if len(registros) != 1008:
    raise SystemExit(
        "ERROR: se esperaban exactamente 1,008 registros."
    )
PY

echo
echo "Checksum JSON local:"
sha256sum "$JSON"

# ============================================================
# LIMPIAR DESTINO SOLO DE ESTA FUENTE
# ============================================================

echo
echo "=== PREPARANDO BRONZE HDFS ==="

hdfs dfs -rm -r -f "$HDFS_BASE" 2>/dev/null || true
hdfs dfs -rm -r -f \
    "/datalake/bvl/metadata/smv/valores_inscritos" \
    2>/dev/null || true

hdfs dfs -mkdir -p "$HDFS_XML"
hdfs dfs -mkdir -p "$HDFS_JSON"
hdfs dfs -mkdir -p "$HDFS_METADATA"

# ============================================================
# CARGAR XML CRUDOS
# ============================================================

echo
echo "=== SUBIENDO 289 RESPUESTAS XML ==="

hdfs dfs -put \
    "$RAW_XML"/*.xml \
    "$HDFS_XML/"

# ============================================================
# CARGAR JSON CONSOLIDADO
# ============================================================

echo
echo "=== SUBIENDO JSON CONSOLIDADO ==="

hdfs dfs -put \
    "$JSON" \
    "$HDFS_JSON/"

# ============================================================
# CARGAR METADATA
# ============================================================

echo
echo "=== SUBIENDO LOG DE EXTRACCION ==="

hdfs dfs -put \
    "$LOG" \
    "$HDFS_METADATA/"

# ============================================================
# REPLICACION
# ============================================================

echo
echo "=== REPLICACION 1 ==="

hdfs dfs -setrep -R 1 "$HDFS_BASE" >/dev/null
hdfs dfs -setrep -R 1 \
    "/datalake/bvl/metadata/smv/valores_inscritos" \
    >/dev/null

# ============================================================
# VALIDACION
# ============================================================

echo
echo "============================================================"
echo "VALIDACION BRONZE"
echo "============================================================"

XML_HDFS=$(hdfs dfs -ls -R "$HDFS_XML" \
    | grep '\.xml$' \
    | wc -l)

JSON_HDFS=$(hdfs dfs -ls -R "$HDFS_JSON" \
    | grep '\.json$' \
    | wc -l)

echo "XML en HDFS  : $XML_HDFS"
echo "JSON en HDFS : $JSON_HDFS"

if [ "$XML_HDFS" -ne 289 ]; then
    echo "ERROR: cantidad XML incorrecta en HDFS."
    exit 1
fi

if [ "$JSON_HDFS" -ne 1 ]; then
    echo "ERROR: cantidad JSON incorrecta en HDFS."
    exit 1
fi

echo
echo "=== CHECKSUM LOCAL VS HDFS ==="

echo -n "LOCAL : "
sha256sum "$JSON" | awk '{print $1}'

echo -n "HDFS  : "
hdfs dfs -cat \
    "$HDFS_JSON/valores_inscritos_smv.json" \
    | sha256sum \
    | awk '{print $1}'

echo
echo "=== VOLUMEN BRONZE ==="

hdfs dfs -du -s -h "$HDFS_BASE"

echo
echo "=== ESTRUCTURA ==="

hdfs dfs -ls -R "$HDFS_BASE" \
    | head -40

echo
echo "============================================================"
echo "BRONZE VALORES INSCRITOS SMV COMPLETADO"
echo "============================================================"
