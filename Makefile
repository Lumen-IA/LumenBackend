VENV=.venv
PY=$(VENV)/bin/python

all: etl rank build

venv:
	python3 -m venv $(VENV)
	. $(VENV)/bin/activate; pip install --upgrade pip
	. $(VENV)/bin/activate; pip install geopandas shapely pyproj rtree pandas numpy requests openpyxl odfpy

etl:
	$(PY) etl_sp_capital.py

rank:
	$(PY) rank_llm.py

build:
	$(PY) build_featurecollection.py

run: etl rank build

clean:
	rm -rf out/*
