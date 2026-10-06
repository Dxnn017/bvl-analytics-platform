#!/bin/bash

set -e

LOCAL="/media/sf_bvl_shared/puentes_smv/hechos_valores_smv"

FECHA_INGESTA="2026-10-05"

BASE="/datalake/bvl/bronze/smv/hechos_valores_consulta"
META="/datalake/bvl/metadata/smv/hechos_valores_consulta"

XML_DEST="${BASE}/xml/ingestion_date=${FECHA_INGESTA}"
JSON_DEST="${BASE}/json/ingestion_date=${FECHA_INGESTA}"
META_DEST="${META}/ingestion_date=${FECHA_INGESTA}"

echo "============================================================"
echo "CARGA BRONZE - VALORES SMV CONSULTADOS POR RAZON SOCIAL"
echo "============================================================"

echo
echo "=== VALIDANDO FUENTE LOCAL ==="

XML_COUNT=$(find \
    "${LOCAL}/raw_xml" \
    -maxdepth 1 \
    -type f \
    -name '*.xml' \
    | wc -l)

echo "XML locales: ${XML_COUNT}"

if [ "${XML_COUNT}" -ne 22 ]; then
    echo "ERROR: se esperaban 22 respuestas XML."
    exit 1
fi

for archivo in \
    "${LOCAL}/hechos_empresas_valores_smv.json" \
    "${LOCAL}/hechos_empresas_valores_smv.csv" \
    "${LOCAL}/hechos_empresas_valores_smv_resumen.csv"
do
    if [ ! -f "$archivo" ]; then
        echo "ERROR: falta $archivo"
        exit 1
    fi
done

echo
echo "=== PREPARANDO HDFS ==="

hdfs dfs -rm -r -f "${BASE}" 2>/dev/null || true
hdfs dfs -rm -r -f "${META}" 2>/dev/null || true

hdfs dfs -mkdir -p "${XML_DEST}"
hdfs dfs -mkdir -p "${JSON_DEST}"
hdfs dfs -mkdir -p "${META_DEST}"

echo
echo "=== SUBIENDO 22 XML ORIGINALES ==="

hdfs dfs -put \
    "${LOCAL}/raw_xml/"*.xml \
    "${XML_DEST}/"

echo
echo "=== SUBIENDO JSON CONSOLIDADO ==="

hdfs dfs -put \
    "${LOCAL}/hechos_empresas_valores_smv.json" \
    "${JSON_DEST}/"

echo
echo "=== SUBIENDO METADATA DE EXTRACCION ==="

hdfs dfs -put \
    "${LOCAL}/hechos_empresas_valores_smv.csv" \
    "${META_DEST}/"

hdfs dfs -put \
    "${LOCAL}/hechos_empresas_valores_smv_resumen.csv" \
    "${META_DEST}/"

echo
echo "=== REPLICACION 1 ==="

hdfs dfs -setrep -R 1 "${BASE}" >/dev/null
hdfs dfs -setrep -R 1 "${META}" >/dev/null

echo
echo "============================================================"
echo "VALIDACION BRONZE"
echo "============================================================"

XML_HDFS=$(hdfs dfs -ls "${XML_DEST}" | grep '\.xml$' | wc -l)
JSON_HDFS=$(hdfs dfs -ls "${JSON_DEST}" | grep '\.json$' | wc -l)

echo "XML en HDFS : ${XML_HDFS}"
echo "JSON en HDFS: ${JSON_HDFS}"

if [ "${XML_HDFS}" -ne 22 ]; then
    echo "ERROR: cantidad XML HDFS incorrecta."
    exit 1
fi

if [ "${JSON_HDFS}" -ne 1 ]; then
    echo "ERROR: JSON consolidado no encontrado."
    exit 1
fi

echo
echo "=== CHECKSUM JSON LOCAL VS HDFS ==="

echo -n "LOCAL: "
sha256sum \
  "${LOCAL}/hechos_empresas_valores_smv.json"

echo -n "HDFS : "
hdfs dfs -cat \
  "${JSON_DEST}/hechos_empresas_valores_smv.json" \
  | sha256sum

echo
echo "=== VOLUMEN ==="

hdfs dfs -du -s -h "${BASE}"
hdfs dfs -du -s -h "${META}"

echo
echo "============================================================"
echo "BRONZE HECHOS -> VALORES SMV COMPLETADO"
echo "============================================================"
