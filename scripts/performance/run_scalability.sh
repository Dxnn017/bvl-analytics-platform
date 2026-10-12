#!/bin/bash

set -u

cd ~/bvl-analytics-platform

mkdir -p outputs/performance docs/evidencias

CSV="outputs/performance/escalabilidad_resultados.csv"

echo "factor,registros,tiempo_seg,tamano_mb" > "$CSV"

for FACTOR in 1 10 100
do

    echo
    echo "======================================================================"
    echo "EJECUTANDO PRUEBA ${FACTOR}x"
    echo "======================================================================"

    LOG="docs/evidencias/escalabilidad_${FACTOR}x.txt"

    spark-submit \
      --master yarn \
      --driver-memory 512m \
      --executor-memory 512m \
      --executor-cores 1 \
      --num-executors 1 \
      scripts/performance/benchmark_scalability.py "$FACTOR" \
      2>&1 | tee "$LOG"

    ESTADO=${PIPESTATUS[0]}

    if [ "$ESTADO" -ne 0 ]; then
        echo "ERROR: la prueba ${FACTOR}x no pudo completarse."
        echo "${FACTOR},ERROR,ERROR,ERROR" >> "$CSV"
        continue
    fi

    RESULTADO=$(grep "SCALABILITY_RESULT|" "$LOG" | tail -1)

    REGISTROS=$(echo "$RESULTADO" | cut -d'|' -f3)
    TIEMPO=$(echo "$RESULTADO" | cut -d'|' -f4)
    OUT=$(echo "$RESULTADO" | cut -d'|' -f5)

    BYTES=$(hdfs dfs -du -s "$OUT" 2>/dev/null | awk '{print $1}')

    if [ -n "${BYTES:-}" ]; then
        TAMANO_MB=$(awk -v b="$BYTES" 'BEGIN {printf "%.2f", b/1024/1024}')
    else
        TAMANO_MB="NA"
    fi

    echo "${FACTOR},${REGISTROS},${TIEMPO},${TAMANO_MB}" >> "$CSV"

    echo
    echo "RESUMEN ${FACTOR}x"
    echo "Registros : $REGISTROS"
    echo "Tiempo    : $TIEMPO s"
    echo "Tamaño    : $TAMANO_MB MB"

    # La réplica se elimina porque solo sirve para la prueba.
    hdfs dfs -rm -r -skipTrash "$OUT" >/dev/null 2>&1 || true

done

echo
echo "======================================================================"
echo "RESULTADOS FINALES DE ESCALABILIDAD"
echo "======================================================================"

column -s, -t "$CSV" 2>/dev/null || cat "$CSV"

echo
echo "======================================================================"
echo "FIN PRUEBA DE ESCALABILIDAD"
echo "======================================================================"
