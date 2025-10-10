#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# fires-risk-monitor | pipeline runner
# - ETL INPE (fires)
# - ETL Weather (Open-Meteo)
# - REF Municipios (CSV bruto ou padronizado)
# - COORDS Municipios (CSV com lat/lon)  <-- NOVO
# - GOLD (Parquet: partitioned + single)
#
# Usage:
#   ./scripts/run_update.sh
#   ./scripts/run_update.sh --days 7 --coords-csv data/ref/coords_municipios.csv --overwrite
#
# Flags:
#   --days N               Lookback dos focos (INPE); default: 7
#   --coords-csv PATH      Caminho do CSV com codigo_ibge, lat, lon (nomes flexíveis)
#   --overwrite            Sobrescreve lat/lon existentes em ref_municipios
#   --skip-fires           Pula etapa INPE
#   --skip-weather         Pula etapa Weather
#   --skip-ref             Pula carga de municipios (nome/uf/população)
#   --skip-coords          Pula atualização de coordenadas
#   --skip-gold            Pula export Gold
#   -h | --help            Ajuda
# ------------------------------------------------------------------------------

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# -------- logs --------
mkdir -p logs
STAMP="$(date +'%Y%m%d_%H%M%S')"
LOG="logs/update_${STAMP}.log"

# -------- defaults --------
DAYS="${DAYS:-7}"
COORDS_CSV_DEFAULT="data/ref/coords_municipios.csv"
COORDS_CSV=""
OVERWRITE=false
SKIP_FIRES=false
SKIP_WEATHER=false
SKIP_REF=false
SKIP_COORDS=false
SKIP_GOLD=false

# -------- parse args --------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --days)           DAYS="${2:-7}"; shift 2 ;;
    --coords-csv)     COORDS_CSV="${2:-}"; shift 2 ;;
    --overwrite)      OVERWRITE=true; shift ;;
    --skip-fires)     SKIP_FIRES=true; shift ;;
    --skip-weather)   SKIP_WEATHER=true; shift ;;
    --skip-ref)       SKIP_REF=true; shift ;;
    --skip-coords)    SKIP_COORDS=true; shift ;;
    --skip-gold)      SKIP_GOLD=true; shift ;;
    -h|--help)
      cat <<EOF
Usage: $0 [options]

Options:
  --days N               Lookback dos focos (INPE); default: 7
  --coords-csv PATH      CSV com codigo_ibge, lat, lon (nomes flexíveis)
  --overwrite            Sobrescreve lat/lon existentes
  --skip-fires           Pula INPE
  --skip-weather         Pula Weather
  --skip-ref             Pula REF municipios (nome/uf/população)
  --skip-coords          Pula COORDS (CSV lat/lon)
  --skip-gold            Pula GOLD (Parquet)
  -h, --help             Esta ajuda

Exemplos:
  $0
  $0 --days 10 --coords-csv data/ref/coords_municipios.csv
  $0 --coords-csv data/ref/coords.csv --overwrite
EOF
      exit 0
      ;;
    *)
      echo "[warn] argumento desconhecido: $1" | tee -a "$LOG"
      shift ;;
  esac
done

# -------- venv --------
if [ ! -d ".venv" ]; then
  echo "[setup] Creating venv..." | tee -a "$LOG"
  python3 -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate

# -------- deps --------
if [ -f "requirements.txt" ]; then
  echo "[setup] Installing requirements..." | tee -a "$LOG"
  pip install -r requirements.txt >>"$LOG" 2>&1
fi

# -------- env --------
if [ -f "configs/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . configs/.env
  set +a
else
  echo "[info] configs/.env não encontrado — usando defaults do código." | tee -a "$LOG"
fi

# -------- INPE --------
if ! $SKIP_FIRES; then
  echo "[run] INPE fires (days=${DAYS})..." | tee -a "$LOG"
  python -m etl.inpe.fetch_fires --days "${DAYS}" 2>&1 | tee -a "$LOG"
else
  echo "[skip] INPE fires" | tee -a "$LOG"
fi

# -------- WEATHER --------
if ! $SKIP_WEATHER; then
  echo "[run] Weather (Open-Meteo)..." | tee -a "$LOG"
  python -m etl.weather.fetch_weather 2>&1 | tee -a "$LOG"
else
  echo "[skip] Weather" | tee -a "$LOG"
fi

# -------- REF MUNICIPIOS (nome/uf/população) --------
if ! $SKIP_REF; then
  if [ -f "data/ref/municipios_raw.csv" ]; then
    echo "[run] Ref municipios (data/ref/municipios_raw.csv)..." | tee -a "$LOG"
    python -m etl.ibge.load_ref_municipios --csv data/ref/municipios_raw.csv 2>&1 | tee -a "$LOG"
  elif [ -f "data/ref/municipios.csv" ]; then
    echo "[run] Ref municipios (data/ref/municipios.csv)..." | tee -a "$LOG"
    python -m etl.ibge.load_ref_municipios --csv data/ref/municipios.csv 2>&1 | tee -a "$LOG"
  else
    echo "[info] Sem CSV de municipios (nome/uf/populacao). Pulando." | tee -a "$LOG"
  fi
else
  echo "[skip] REF municipios" | tee -a "$LOG"
fi

# -------- COORDENADAS MUNICIPIOS (lat/lon via CSV) --------
if ! $SKIP_COORDS; then
  # prioridade: flag --coords-csv; senão, arquivo default se existir
  if [ -z "${COORDS_CSV}" ] && [ -f "${COORDS_CSV_DEFAULT}" ]; then
    COORDS_CSV="${COORDS_CSV_DEFAULT}"
  fi

  if [ -n "${COORDS_CSV}" ] && [ -f "${COORDS_CSV}" ]; then
    echo "[run] Coords municipios (${COORDS_CSV}) overwrite=${OVERWRITE} ..." | tee -a "$LOG"
    if $OVERWRITE; then
      python -m etl.ibge.load_coords_csv --csv "${COORDS_CSV}" --overwrite 2>&1 | tee -a "$LOG"
    else
      python -m etl.ibge.load_coords_csv --csv "${COORDS_CSV}" 2>&1 | tee -a "$LOG"
    fi
  else
    echo "[info] Sem CSV de coordenadas (lat/lon). Pulando." | tee -a "$LOG"
  fi
else
  echo "[skip] COORDS municipios" | tee -a "$LOG"
fi

# -------- GOLD --------
if ! $SKIP_GOLD; then
  echo "[run] Export Gold (Parquet)..." | tee -a "$LOG"
  python -m etl.gold.export_parquet 2>&1 | tee -a "$LOG"
else
  echo "[skip] GOLD" | tee -a "$LOG"
fi

echo "[done] Pipeline concluída. Log: $LOG"
