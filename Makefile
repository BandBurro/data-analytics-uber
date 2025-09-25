SHELL := /bin/bash

.PHONY: help venv install env run-csv run-pg etl test clean-venv reinstall kernel

help:
	@echo "Targets:"
	@echo "  venv      - create virtualenv .venv"
	@echo "  install   - install requirements into .venv"
	@echo "  env       - copy ENV.sample to .env if missing"
	@echo "  clean-venv- remove .venv (use if venv points to old path)"
	@echo "  reinstall - clean venv and install deps fresh"
	@echo "  kernel    - register ipykernel named 'venv' for notebooks"
	@echo "  run-csv   - run CSV-backed API on :8000"
	@echo "  run-pg    - run Postgres-backed API on :8001"
	@echo "  etl       - load CSV into Postgres using CSV_PATH"
	@echo "  test      - run tests"

venv:
	python3 -m venv .venv

install: venv
	. .venv/bin/activate && pip install -U pip && pip install -r requirements.txt

clean-venv:
	rm -rf .venv

reinstall: clean-venv install

kernel: venv
	. .venv/bin/activate && python -m ipykernel install --user --name venv --display-name "venv"

env:
	@[ -f .env ] || cp ENV.sample .env

run-csv:
	. .venv/bin/activate && uvicorn src.api.main:app --reload --port 8000

run-pg:
	. .venv/bin/activate && uvicorn src.api.main_db:app --reload --port 8001

etl:
	. .venv/bin/activate && python -m src.etl.load_csv_to_postgres

test:
	. .venv/bin/activate && python -m pytest -q


