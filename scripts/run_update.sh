#!/usr/bin/env bash
set -euo pipefail

# Diretório do projeto (ajuste se necessário)
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Logs
mkdir -p logs
STAMP="$(date +'%Y%m%d_%H%M%S')"
LOG="logs/update_${STAMP}.log"

# Venv
if [ ! -d ".venv" ]; then
  echo "[setup] Creating venv..." | tee -a "$LOG"
  python3 -m venv .venv
fi
source .venv/bin/activate

# Dependências (idempotente)
if [ -f "requirements.txt" ]; then
  pip install -r requirements.txt >>"$LOG" 2>&1
fi

# .env (opcional)
if [ -f "configs/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . configs/.env
  set +a
else
  echo "[warn] configs/.env não encontrado — usando defaults do código." | tee -a "$LOG"
fi

echo "[run] INPE fires..." | tee -a "$LOG"
python -m etl.inpe.fetch_fires --days 7        2>&1 | tee -a "$LOG"

echo "[run] Weather (Open-Meteo)..." | tee -a "$LOG"
python -m etl.weather.fetch_weather            2>&1 | tee -a "$LOG"

# Se você mantiver um CSV de municípios para atualizar
if [ -f "data/ref/municipios.csv" ] || [ -f "data/ref/municipios_raw.csv" ]; then
  CSV="data/ref/municipios.csv"
  [ -f "data/ref/municipios_raw.csv" ] && CSV="data/ref/municipios_raw.csv"
  echo "[run] Ref municipios (${CSV})..." | tee -a "$LOG"
  python -m etl.ibge.load_ref_municipios --csv "$CSV" 2>&1 | tee -a "$LOG"
else
  echo "[info] Sem CSV de municípios novo — pulando ref_municipios." | tee -a "$LOG"
fi

echo "[run] Export Gold (Parquet)..." | tee -a "$LOG"
python -m etl.gold.export_parquet              2>&1 | tee -a "$LOG"

echo "[done] Pipeline concluída. Log: $LOG"
