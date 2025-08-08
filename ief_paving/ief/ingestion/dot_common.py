from __future__ import annotations
import pandas as pd
from pathlib import Path

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower().replace('\n', ' ') for c in df.columns]
    return df

def load_any(path: Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() in [".csv", ".txt"]:
        return pd.read_csv(path)
    if path.suffix.lower() in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    if path.suffix.lower() in [".parquet"]:
        return pd.read_parquet(path)
    # naive HTML table read
    tables = pd.read_html(path.read_text())
    return tables[0]