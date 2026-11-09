#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(dirname "$0")/../.."

MODO="${1:-verificar}"
shift || true

declare -A PROCESADORES=(
  [cotizaciones]="cotizaciones_incremental_desde_bronze.py"
  [montos_sab]="montos_sab_incremental.py"
  [bcrp]="bcrp_incremental.py"
  [finanzas]="finanzas_incremental.py"
  [principales]="principales_cuentas_incremental.py"
  [hechos]="hechos_incremental.py"
  [gold]="preparar_gold_seguro_v3.py"
)

verificar() {
  python3 - "${PROCESADORES[@]}" <<'PY'
import ast
import sys
from pathlib import Path

for nombre in sys.argv[1:]:
    ruta = Path("scripts/processing") / nombre
    if not ruta.is_file():
        raise SystemExit(f"FALTA|{ruta}")
    ast.parse(ruta.read_text(encoding="utf-8"))
    print(f"OK|{ruta}")

print("PREFLIGHT|APROBADO")
PY
}

if [[ "$MODO" == "verificar" ]]; then
  verificar
  exit 0
fi

if [[ "$MODO" != "plan" && "$MODO" != "ejecutar" ]]; then
  echo "Uso: $0 verificar|plan|ejecutar [dataset] [argumentos]"
  exit 2
fi

DATASET="${1:?Falta nombre del dataset}"
shift

if [[ -z "${PROCESADORES[$DATASET]:-}" ]]; then
  echo "ERROR: dataset desconocido: $DATASET"
  exit 2
fi

SCRIPT="scripts/processing/${PROCESADORES[$DATASET]}"

COMANDO=(spark-submit "$SCRIPT" "$@")

printf 'COMANDO|'
printf ' %q' "${COMANDO[@]}"
printf '\n'

if [[ "$MODO" == "plan" ]]; then
  echo "ESTADO|PLANIFICADO_NO_EJECUTADO"
  exit 0
fi

# Por ahora solo ejecutamos pruebas aisladas.
# No se permiten publicaciones oficiales.
case "$DATASET" in
  hechos)
    CANDIDATO=""
    while (($#)); do
      if [[ "$1" == "--candidate" && $# -ge 2 ]]; then
        CANDIDATO="$2"
        break
      fi
      shift
    done

    [[ "$CANDIDATO" == /datalake/bvl/staging/* ]] || {
      echo "ERROR: candidato fuera de staging"
      exit 2
    }
    hdfs dfs -test -d "$CANDIDATO" || exit 2
    ;;
  *)
    echo "EJECUCION_BLOQUEADA|$DATASET"
    echo "Solo habilitado PLAN hasta revisar rutas de escritura"
    exit 3
    ;;
esac

mkdir -p outputs/quality/incremental/logs

LOG="outputs/quality/incremental/logs/${DATASET}_$(date -u +%Y%m%dT%H%M%S).log"

echo "LOG|$LOG"

if "${COMANDO[@]}" > "$LOG" 2>&1; then
  echo "EJECUCION|FINALIZADA"
  grep -E '^HECHOS_CHECK|^HECHOS_INCREMENTAL|^SILVER_OFICIAL' "$LOG" || true
else
  rc=$?
  echo "EJECUCION|ERROR|$rc"
  tail -25 "$LOG"
  exit "$rc"
fi
