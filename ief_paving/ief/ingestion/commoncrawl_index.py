from __future__ import annotations
from typing import Iterable, List, Dict
import re
import tldextract

def _clean_domain(url: str) -> str:
    try:
        ext = tldextract.extract(url if '://' in url else f'https://{url}')
        return ext.registered_domain.lower()
    except Exception:
        return ""

def query_commoncrawl_keywords(keywords: List[str], collections: List[str]|None=None, limit: int=2000) -> List[str]:
    """Return a list of candidate domains whose URLs/text likely match keywords.
    Requires `cdx_toolkit` if you want real results. If unavailable at runtime,
    this function returns an empty list (safe fallback).
    """
    try:
        import cdx_toolkit  # type: ignore
    except Exception:
        return []
    collections = collections or ["CC-MAIN-2025-10","CC-MAIN-2025-06","CC-MAIN-2024-50"]
    q = " ".join(keywords)
    domains = set()
    for coll in collections:
        try:
            cc = cdx_toolkit.CDXFetcher(source='cc', index=coll)
            for hit in cc.iter(q=q, limit=limit//len(collections), filter=['status:200']):
                url = hit.get('url')
                if not url: continue
                dom = _clean_domain(url)
                if dom:
                    domains.add(dom)
        except Exception:
            continue
    return sorted(domains)