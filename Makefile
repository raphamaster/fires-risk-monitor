PY=python
VENV=.venv

.PHONY: help install venv lint format fires weather ref gold update clean

help:
	@echo "make install   -> instala deps no venv"
	@echo "make lint      -> ruff lint"
	@echo "make format    -> ruff format"
	@echo "make fires     -> etl inpe (7 dias)"
	@echo "make weather   -> open-meteo"
	@echo "make ref       -> carrega municipios (se CSV existir)"
	@echo "make gold      -> exporta parquet (partitioned + single)"
	@echo "make update    -> roda pipeline completa"
	@echo "make clean     -> remove caches"

venv:
	@test -d $(VENV) || python3 -m venv $(VENV)
	@. $(VENV)/bin/activate

install: venv
	@. $(VENV)/bin/activate && pip install -U pip && \
		(test -f requirements.txt && pip install -r requirements.txt || echo "sem requirements.txt")

lint: venv
	@. $(VENV)/bin/activate && ruff check .

format: venv
	@. $(VENV)/bin/activate && ruff format .

fires: venv
	@. $(VENV)/bin/activate && $(PY) -m etl.inpe.fetch_fires --days 7

weather: venv
	@. $(VENV)/bin/activate && $(PY) -m etl.weather.fetch_weather

ref: venv
	@if [ -f data/ref/municipios_raw.csv ]; then \
	  . $(VENV)/bin/activate && $(PY) -m etl.ibge.load_ref_municipios --csv data/ref/municipios_raw.csv ; \
	elif [ -f data/ref/municipios.csv ]; then \
	  . $(VENV)/bin/activate && $(PY) -m etl.ibge.load_ref_municipios --csv data/ref/municipios.csv ; \
	else \
	  echo "Nenhum CSV de municipios encontrado em data/ref/."; \
	fi

gold: venv
	@. $(VENV)/bin/activate && $(PY) -m etl.gold.export_parquet

update: venv
	@./scripts/run_update.sh

clean:
	@find . -name "__pycache__" -type d -prune -exec rm -rf {} +
	@find . -name "*.pyc" -delete
