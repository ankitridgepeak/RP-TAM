# Simple helpers for IEF Paving Pilot
# Usage examples:
#   make venv
#   make install
#   make run        # CLI pipeline
#   make demo       # Streamlit UI

PY?=python
PIP?=pip
VENV=.venv

STATES?=TX MI CO
DOT_DIR?=data/dot
OUT?=out/paving_tx_mi_co.csv
EVIDENCE?=out/evidence.parquet
OSM?=yes
WEB?=yes

venv:
	$(PY) -m venv $(VENV)
	@echo 'Activate with: source $(VENV)/bin/activate  # Windows: $(VENV)\Scripts\activate'

install:
	. $(VENV)/bin/activate && $(PIP) install -r requirements.txt -e .

run:
	. $(VENV)/bin/activate && $(PY) -m ief.flows.paving_run --states $(STATES) --dot-dir $(DOT_DIR) --osm $(OSM) --web-discovery $(WEB) --out $(OUT) --save-evidence $(EVIDENCE)

demo:
	. $(VENV)/bin/activate && streamlit run app_streamlit.py

clean:
	rm -rf out __pycache__ .pytest_cache .mypy_cache

.PHONY: venv install run demo clean
