# Industry Entity Finder (IEF) — Paving Pilot (TX, MI, CO)

Free/low-cost pipeline to enumerate **paving contractors** using:
- **OpenStreetMap (Overpass API)** for name-matched POIs
- **State DOT prequalified lists** (TX, MI, CO) — import from local files for now
- Optional **license/association lists** (add as CSVs)

> No paid APIs required. Internet access is required when you actually run Overpass queries.

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e .
python -m ief.flows.paving_run --states TX MI CO --out out/paving_tx_mi_co.csv
```

### Options
```
python -m ief.flows.paving_run --states TX MI --dot-dir ./data/dot --osm yes --save-evidence out/evidence.parquet
```

- `--dot-dir`: directory with DOT prequalification files (CSV/XLSX/HTML you downloaded).
- `--osm`: turn OSM Overpass on/off (default: on). If running air-gapped, set `--osm no`.
- `--save-evidence`: path to write raw evidence table (Parquet).

## Adding DOT files
Place your files here (examples):
```
data/dot/
  txdot_prequalified_2025.csv
  mdot_prequalified_2025.xlsx
  cdot_prequalified_2025.html
```

Then run with `--dot-dir data/dot`.

## Layout
```
ief/
  config/markets/paving_us_v1.yaml    # market definition
  ingestion/osm_overpass.py           # OSM collector
  ingestion/dot_tx.py dot_mi.py dot_co.py
  normalize/cleaning.py               # standardizers
  classify/rules.py                   # rule-based market fit
  resolve/matching.py                 # light entity resolution
  storage/db.py                       # write CSV/Parquet
  flows/paving_run.py                 # orchestrator CLI
```

## Outputs
- **CSV**: deduped, classified entities with confidence scores
- **Parquet** (optional): evidence rows for audit

## Roadmap
- Add association/license adapters
- Improve entity resolution model & capture–recapture
- Add Streamlit review UI