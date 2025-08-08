from __future__ import annotations
from pathlib import Path
import pandas as pd
from .dot_common import _normalize_cols, load_any

def parse_txdot(path: Path) -> pd.DataFrame:
    df = load_any(path)
    df = _normalize_cols(df)
    mapping = {}
    for c in df.columns:
        cl = c.lower()
        if any(k in cl for k in ["firm","vendor","company","name"]):
            mapping["name"] = c
        elif "address" in cl or "street" in cl:
            mapping["address"] = c
        elif "city" in cl:
            mapping["city"] = c
        elif cl == "state":
            mapping["state"] = c
        elif "zip" in cl or "postal" in cl:
            mapping["postal_code"] = c
        elif "phone" in cl or "telephone" in cl:
            mapping["phone"] = c
        elif "web" in cl or "url" in cl:
            mapping["website"] = c
        elif any(k in cl for k in ["work","code","class","category"]):
            mapping["work_types"] = c
    out = pd.DataFrame()
    for k, v in mapping.items():
        out[k] = df[v]
    out["source_name"] = "txdot"
    return out