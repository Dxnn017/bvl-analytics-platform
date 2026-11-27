#!/usr/bin/env bash

set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$BASE/../../.." && pwd)"

PYTHON_BIN="${PYTHON_BIN:-python3}"

cd "$ROOT"

export PYTHONDONTWRITEBYTECODE=1
export PYSPARK_PYTHON="$PYTHON_BIN"
export PYTHONPATH="$ROOT:$ROOT/src/gold/mercado:$ROOT/src/gold/empresas:$ROOT/src/gold/incremental"

echo "=================================================="
echo "BVL - VALIDACION LOCAL SILVER GOLD"
echo "=================================================="

echo "=== CONTEXTO DE EJECUCION ==="
date -u +"%Y-%m-%dT%H:%M:%SZ"
git branch --show-current
git rev-parse HEAD
git status --short

"$PYTHON_BIN" --version

"$PYTHON_BIN" -c \
    'import pyspark; print("PYSPARK_VERSION:", pyspark.__version__)'

echo
echo "=== ESCENARIO 1: NORMALIZACION NEMONICOS ==="
"$PYTHON_BIN" "$BASE/fixtures/prueba_nemonicos.py"

echo
echo "=== ESCENARIO 2: CUARENTENA SAB ==="
"$PYTHON_BIN" "$BASE/fixtures/prueba_cuarentena_sab.py"

echo
echo "=== ESCENARIO 3: SUSTITUCION FINANCIERA ==="
"$PYTHON_BIN" "$BASE/fixtures/prueba_sustitucion_financiera.py"

echo
echo "=== ESCENARIO 4: INCREMENTALIDAD Y TRAZABILIDAD ==="
"$PYTHON_BIN" -m unittest discover \
    -s tests/bigquery \
    -p 'test_*.py' \
    -v

echo
echo "=================================================="
echo "ESCENARIOS_EVALUADOS: 4"
echo "LIMITACIONES_DIAGNOSTICADAS: NEMONICOS Y SAB"
echo "PUBLICACION_AUTORIZADA: False"
echo "VALIDACION_CONJUNTA_LOCAL_FINALIZADA_OK"
echo "=================================================="
