# Requires Java 17+ for Spark 4 (set JAVA_HOME) and GNU make.
PYTHON ?= python

install:
	$(PYTHON) -m pip install -r requirements-dev.txt

test:
	$(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m ruff check src tests scripts

generate-data:
	$(PYTHON) scripts/generate_sample_data.py --rows 100 --out sample_data

validate:
	$(PYTHON) scripts/validate_project.py --out evidence/local_validation_results.txt

.PHONY: install test lint generate-data validate
