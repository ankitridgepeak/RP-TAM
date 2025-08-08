from __future__ import annotations
import time, logging
from typing import Iterable, List, Tuple
import requests

OSM_TIMEOUT_S = 60
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

STATE_BBOX = {
    "TX": (25.83, -106.65, 36.50, -93.51),
    "MI": (41.68, -90.42, 48.31, -82.41),
    "CO": (36.99, -109.06, 41.00, -102.04),
}

def grid(bbox: Tuple[float,float,float,float], step: float=2.0) -> Iterable[Tuple[float,float,float,float]]:
    south, west, north, east = bbox
    lat = south
    while lat < north:
        lon = west
        lat2 = min(lat + step, north)
        while lon < east:
            lon2 = min(lon + step, east)
            yield (lat, lon, lat2, lon2)
            lon += step
        lat += step

def build_query(name_regex: str, bbox: Tuple[float,float,float,float]) -> str:
    s, w, n, e = bbox
    return f"""    [out:json][timeout:{OSM_TIMEOUT_S}];
    (
      node["name"~"{name_regex}", i]({s},{w},{n},{e});
      way["name"~"{name_regex}", i]({s},{w},{n},{e});
      relation["name"~"{name_regex}", i]({s},{w},{n},{e});
    );
    out center tags;
    """

def fetch_overpass(name_regex: str, bbox: Tuple[float,float,float,float]) -> dict:
    q = build_query(name_regex, bbox)
    r = requests.post(OVERPASS_URL, data={"data": q}, timeout=OSM_TIMEOUT_S+10)
    r.raise_for_status()
    return r.json()

def collect_state(state: str, name_regex: str) -> List[dict]:
    assert state in STATE_BBOX, f"Unsupported state {state}"
    out = []
    for i, tile in enumerate(grid(STATE_BBOX[state], step=2.0)):
        backoff = 1.0
        while True:
            try:
                data = fetch_overpass(name_regex, tile)
                out.extend(data.get("elements", []))
                break
            except Exception as e:
                logging.warning("Overpass error on tile %s: %s; backing off %.1fs", i, e, backoff)
                time.sleep(backoff)
                backoff = min(backoff*1.8, 30)
    return out