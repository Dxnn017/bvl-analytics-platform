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
  cotizaciones)
    # Solo permitir el lote Bronze y las rutas aprobadas.
    if [[ $# -ne 8 ||
          "$1" != "--bronze" ||
          "$3" != "--ingestion-date" ||
          "$5" != "--current" ||
          "$7" != "--stage-root" ]]; then
      echo "ERROR: argumentos de Cotizaciones no autorizados"
      exit 2
    fi

    if [[ "$2" != /datalake/bvl/bronze/smv/cotizaciones/ingestion_date=* ||
          ! "$4" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ||
          "$6" != "/datalake/bvl/silver/mercado_diario/cotizaciones" ||
          "$8" != /datalake/bvl/staging/* ]]; then
      echo "ERROR: rutas o fecha no autorizadas"
      exit 2
    fi

    if ! hdfs dfs -test -d "$2"; then
      echo "ERROR: lote Bronze inexistente"
      exit 2
    fi

    echo "COTIZACIONES|STAGING_SEGURO"
    ;;


  bcrp)
    if [[ $# -ne 6 ||
          "$1" != "--dataset" ||
          "$3" != "--bronze" ||
          "$5" != "--current" ]]; then
      echo "ERROR: argumentos BCRP no autorizados"
      exit 2
    fi

    TIPO="$2"
    RUTA="$4"
    ACTUAL="$6"

    if [[ "$TIPO" != "tipo_cambio" &&
          "$TIPO" != "embig" ]]; then
      echo "ERROR: dataset BCRP no autorizado"
      exit 2
    fi

    if [[ ! "$RUTA" =~ ^/datalake/bvl/bronze/bcrp/${TIPO}/ingestion_date=[0-9]{4}-[0-9]{2}-[0-9]{2}/[^/]+[.]csv$ ]]; then
      echo "ERROR: ruta Bronze no autorizada"
      exit 2
    fi

    if [[ "$ACTUAL" != "/datalake/bvl/silver/macroeconomia/$TIPO" ]]; then
      echo "ERROR: ruta Silver no autorizada"
      exit 2
    fi

    if ! hdfs dfs -test -f "$RUTA"; then
      echo "ERROR: archivo Bronze inexistente"
      exit 2
    fi

    if ! hdfs dfs -test -d "$ACTUAL"; then
      echo "ERROR: Silver inexistente"
      exit 2
    fi

    echo "BCRP|RUTAS_VALIDADAS|$TIPO"
    ;;


  finanzas)
    if [[ $# -ne 6 ||
          "$1" != "--dataset" ||
          "$3" != "--bronze" ||
          "$5" != "--current" ]]; then
      echo "ERROR: argumentos Finanzas no autorizados"
      exit 2
    fi

    TIPO="$2"
    RUTA="$4"
    ACTUAL="$6"

    case "$TIPO" in
      estado_resultados)
        ARCHIVO="EstadoResultadosSMV.json" ;;
      flujo_efectivo)
        ARCHIVO="FlujoEfectivoSMV.json" ;;
      situacion_financiera)
        ARCHIVO="SituacionFinancieraSMV.json" ;;
      cambios_patrimonio)
        ARCHIVO="CambiosPatrimonioSMV.json" ;;
      *)
        echo "ERROR: dataset Finanzas no autorizado"
        exit 2 ;;
    esac

    PATRON="^/datalake/bvl/bronze/smv/${TIPO}/ingestion_date=[0-9]{4}-[0-9]{2}-[0-9]{2}/${ARCHIVO}$"

    if [[ ! "$RUTA" =~ $PATRON ]]; then
      echo "ERROR: ruta Bronze no autorizada"
      exit 2
    fi

    if [[ "$ACTUAL" != "/datalake/bvl/silver/finanzas_empresariales/$TIPO" ]]; then
      echo "ERROR: ruta Silver no autorizada"
      exit 2
    fi

    if ! hdfs dfs -test -f "$RUTA"; then
      echo "ERROR: archivo Bronze inexistente"
      exit 2
    fi

    if ! hdfs dfs -test -d "$ACTUAL"; then
      echo "ERROR: Silver inexistente"
      exit 2
    fi

    echo "FINANZAS|RUTAS_VALIDADAS|$TIPO"
    ;;


  principales)
    if [[ $# -ne 6 ||
          "$1" != "--bronze" ||
          "$3" != "--current" ||
          "$5" != "--ingestion-date" ]]; then
      echo "ERROR: argumentos no autorizados"
      exit 2
    fi

    RUTA="$2"
    ACTUAL="$4"
    FECHA="$6"

    if [[ ! "$FECHA" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      echo "ERROR: fecha invalida"
      exit 2
    fi

    ESPERADA="/datalake/bvl/bronze/smv/principales_cuentas/ingestion_date=${FECHA}/principales_*.json"

    if [[ "$RUTA" != "$ESPERADA" ]]; then
      echo "ERROR: ruta Bronze no autorizada"
      exit 2
    fi

    if [[ "$ACTUAL" != "/datalake/bvl/silver/finanzas_empresariales/principales_cuentas" ]]; then
      echo "ERROR: ruta Silver no autorizada"
      exit 2
    fi

    if ! hdfs dfs -ls "$RUTA" >/dev/null 2>&1; then
      echo "ERROR: archivos Bronze inexistentes"
      exit 2
    fi

    if ! hdfs dfs -test -d "$ACTUAL"; then
      echo "ERROR: Silver inexistente"
      exit 2
    fi

    echo "PRINCIPALES|RUTAS_VALIDADAS"
    ;;


  montos_sab)
    if [[ $# -ne 6 ||
          "$1" != "--bronze" ||
          "$3" != "--ingestion-date" ||
          "$5" != "--current" ]]; then
      echo "ERROR: argumentos Montos SAB invalidos"
      exit 2
    fi

    RUTA="$2"
    FECHA="$4"
    ACTUAL="$6"

    if [[ ! "$FECHA" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      echo "ERROR: fecha invalida"
      exit 2
    fi

    ESPERADA="/datalake/bvl/bronze/smv/montos_sab/ingestion_date=$FECHA"

    if [[ "$RUTA" != "$ESPERADA" ||
          "$ACTUAL" != "/datalake/bvl/silver/mercado_diario/montos_sab" ]]; then
      echo "ERROR: rutas no autorizadas"
      exit 2
    fi

    if ! hdfs dfs -test -d "$RUTA"; then
      echo "ERROR: Bronze inexistente"
      exit 2
    fi

    if ! hdfs dfs -ls "$RUTA/*.json" >/dev/null 2>&1; then
      echo "ERROR: no hay archivos JSON"
      exit 2
    fi

    if ! hdfs dfs -test -d "$ACTUAL"; then
      echo "ERROR: Silver inexistente"
      exit 2
    fi

    echo "MONTOS_SAB|RUTAS_VALIDADAS"
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
  grep -E '^HECHOS_CHECK|^HECHOS_INCREMENTAL|^SILVER_OFICIAL|^STAGING_VALIDADO|^SIN_CAMBIOS|^PARTICION_STAGE|^MANIFEST|^PARTICIONES_AFECTADAS' "$LOG" || true
else
  rc=$?
  echo "EJECUCION|ERROR|$rc"
  tail -25 "$LOG"
  exit "$rc"
fi
