#!/usr/bin/env bash
# Ejecuta el benchmark verificado 1x/10x/100x con los MISMOS recursos.
# No limpia sus resultados automáticamente: conservar para auditoría del docente.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
mkdir -p outputs/performance docs/evidencias
CSV="outputs/performance/escalabilidad_verificada.csv"
echo 'factor,esperado,real,escritura_s,auditoria_s,bytes_hdfs,filas_s,ruta_hdfs,app_id' > "$CSV"
for FACTOR in 1 10 100; do
    LOG="docs/evidencias/escalabilidad_verificada_${FACTOR}x.log"
    echo "===== Iniciando ${FACTOR}x con 1 executor/1 core ====="
    if ! spark-submit --master yarn --deploy-mode client \
          --driver-memory 512m --executor-memory 512m \
          --executor-cores 1 --num-executors 1 \
          scripts/performance/benchmark_escalabilidad_auditado.py "$FACTOR" \
          > "$LOG" 2>&1; then
        echo "ERROR en ${FACTOR}x. Revisa $LOG (no se borraron datos)." >&2
        exit 1
    fi
    LINE=$(grep '^SCALABILITY_RESULT_V2|' "$LOG" | tail -1 || true)
    APP=$(grep '^APPLICATION_ID|' "$LOG" | tail -1 | cut -d'|' -f2 || true)
    if [[ -z "$LINE" ]]; then
      echo "Sin evidencia de salida: $LOG" >&2
      exit 1
    fi
    IFS='|' read -r etiqueta factor esperado real escritura auditoria bytes tasa ruta <<< "$LINE"
    echo "${factor},${esperado},${real},${escritura},${auditoria},${bytes},${tasa},${ruta},${APP}" >> "$CSV"
    echo "RESULTADO ${factor}x: filas=$real tiempo=${escritura}s salida=${bytes} bytes (log $LOG)"
done
printf '\nRESUMEN: %s\n' "$CSV"
cat "$CSV"
