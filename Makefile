.PHONY: help setup run bronze silver gold test test-unit test-integracion propuesta clean

VENV := .venv
PY   := $(VENV)/bin/python
PIP  := $(VENV)/bin/pip

help:
	@echo "make setup            crea el entorno virtual e instala dependencias"
	@echo "make run              corre el pipeline completo (bronze -> silver -> gold)"
	@echo "make bronze|silver|gold  corre hasta la capa indicada"
	@echo "make test             corre toda la suite"
	@echo "make test-unit        sólo tests de lógica (no requiere el lake)"
	@echo "make test-integracion verificación cruzada contra los archivos fuente"
	@echo "make propuesta        regenera docs/propuesta_tecnica.pdf"
	@echo "make clean            borra outputs/ y artefactos de Spark"

setup:
	python3 -m venv $(VENV)
	$(PIP) install --quiet --upgrade pip setuptools wheel
	$(PIP) install --quiet -r requirements.txt

run:
	PYTHONPATH=src $(PY) -m cafenorte.pipeline

bronze silver gold:
	PYTHONPATH=src $(PY) -m cafenorte.pipeline --capa $@

test:
	$(PY) -m pytest -q

test-unit:
	$(PY) -m pytest -q tests/test_product_keys.py tests/test_transformaciones.py tests/test_preguntas_negocio.py

test-integracion:
	$(PY) -m pytest -q tests/test_integracion.py

propuesta:
	$(PY) scripts/build_propuesta_pdf.py

clean:
	rm -rf outputs/lake outputs/reportes spark-warehouse metastore_db derby.log .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
