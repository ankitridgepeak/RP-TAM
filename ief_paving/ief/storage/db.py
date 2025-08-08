from __future__ import annotations
import pandas as pd
from pathlib import Path

def write_csv(df: pd.DataFrame, path: str|Path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def write_parquet(df: pd.DataFrame, path: str|Path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)