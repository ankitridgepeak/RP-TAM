from __future__ import annotations
import pandas as pd

def simple_dedupe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['__key'] = (
        (df.get('phone', pd.Series(['']*len(df))).fillna('')) + '|' +
        (df.get('website_root', pd.Series(['']*len(df))).fillna('')) + '|' +
        (df.get('postal_code', pd.Series(['']*len(df))).fillna(''))
    )
    df = df.sort_values(by=['phone', 'website_root'], ascending=False)
    deduped = df.drop_duplicates(subset=['__key', 'name'], keep='first')
    deduped = deduped.drop(columns='__key', errors='ignore')
    return deduped