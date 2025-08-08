from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict

@dataclass
class MarketConfig:
    include_terms: List[str]
    exclude_terms: List[str]

def score_record(rec: Dict, cfg: MarketConfig) -> float:
    name = (rec.get('name') or '') + ' ' + (rec.get('work_types') or '') + ' ' + (rec.get('source_name') or '')
    name_low = name.lower()
    score = 0.0
    if any(t in name_low for t in cfg.include_terms):
        score += 0.6
    if any(t in name_low for t in cfg.exclude_terms):
        score -= 0.9
    if rec.get('has_dot_flag'):
        score += 0.4
    return max(min(score, 1.0), -1.0)

def label_from_score(s: float) -> str:
    if s > 0.5: return 'include'
    if s < 0.2: return 'exclude'
    return 'review'