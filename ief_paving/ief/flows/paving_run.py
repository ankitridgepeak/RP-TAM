from __future__ import annotations
import argparse, yaml
from pathlib import Path
import pandas as pd

from ief.ingestion.osm_overpass import collect_state
from ief.ingestion.commoncrawl_index import query_commoncrawl_keywords
from ief.ingestion.web_discovery import crawl_domains
from ief.ingestion.dot_tx import parse_txdot
from ief.ingestion.dot_mi import parse_mdot
from ief.ingestion.dot_co import parse_cdot
from ief.normalize.cleaning import normalize_name, to_e164, root_domain
from ief.classify.rules import MarketConfig, score_record, label_from_score
from ief.resolve.matching import simple_dedupe
from ief.storage.db import write_csv, write_parquet

def load_cfg() -> dict:
    cfgp = Path(__file__).parents[1] / "config" / "markets" / "paving_us_v1.yaml"
    with open(cfgp, "r") as f:
        return yaml.safe_load(f)

def ingest_osm(states, name_regex) -> pd.DataFrame:
    rows = []
    for st in states:
        elements = collect_state(st, name_regex)
        for el in elements:
            tags = el.get("tags", {}) or {}
            name = tags.get("name") or ""
            addr = " ".join([tags.get(k, "") for k in ["addr:housenumber","addr:street","addr:unit","addr:city","addr:state","addr:postcode"]]).strip()
            rows.append({
                "source_name": "osm",
                "name": name,
                "address": addr,
                "city": tags.get("addr:city"),
                "state": tags.get("addr:state"),
                "postal_code": tags.get("addr:postcode"),
                "phone": tags.get("phone") or tags.get("contact:phone"),
                "website": tags.get("website") or tags.get("contact:website"),
                "work_types": "",
            })
    return pd.DataFrame(rows)

def ingest_dot(states, dot_dir: Path) -> pd.DataFrame:
    frames = []
    # Web discovery (domains via CommonCrawl + focused crawl)
    if args.web_discovery.lower().startswith('y'):
        keywords = cfg['include_terms']
        domains = query_commoncrawl_keywords(keywords, limit=1500)
        if domains:
            import asyncio
            web_rows = asyncio.run(crawl_domains(domains, limit=800))
            import pandas as pd
            if web_rows:
                frames.append(pd.DataFrame(web_rows))
        else:
            print('CommonCrawl query returned 0 domains (library unavailable or no matches). Skipping web discovery.')
    if "TX" in states:
        for p in dot_dir.glob("*tx*.*"):
            frames.append(parse_txdot(p))
    if "MI" in states:
        for p in dot_dir.glob("*mi*.*"):
            frames.append(parse_mdot(p))
    if "CO" in states:
        for p in dot_dir.glob("*co*.*"):
            frames.append(parse_cdot(p))
    if frames:
        df = pd.concat(frames, ignore_index=True)
        df["has_dot_flag"] = True
        return df
    return pd.DataFrame(columns=["name","address","city","state","postal_code","phone","website","work_types","source_name","has_dot_flag"])

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["name","phone","website","address","city","state","postal_code","work_types","source_name"]:
        if col not in df.columns:
            df[col] = ""
    df["name"] = df["name"].map(normalize_name)
    df["phone"] = df["phone"].map(to_e164)
    df["website_root"] = df["website"].map(root_domain)
    return df

def classify_df(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    mc = MarketConfig(include_terms=cfg["include_terms"], exclude_terms=cfg["exclude_terms"])
    scores = []
    labels = []
    for _, r in df.iterrows():
        s = score_record(r.to_dict(), mc)
        scores.append(s)
        labels.append(label_from_score(s))
    df["market_fit_score"] = scores
    df["fit_label"] = labels
    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--states", nargs="+", default=["TX","MI","CO"])
    parser.add_argument("--dot-dir", type=str, default="data/dot")
    parser.add_argument("--osm", type=str, default="yes", help="yes/no to use Overpass")
    parser.add_argument("--out", type=str, default="out/paving_entities.csv")
    parser.add_argument("--save-evidence", type=str, default="")
    parser.add_argument("--web-discovery", type=str, default="no", help="yes/no to use CommonCrawl + focused crawl")
    args = parser.parse_args()

    cfg = load_cfg()
    frames = []
    # Web discovery (domains via CommonCrawl + focused crawl)
    if args.web_discovery.lower().startswith('y'):
        keywords = cfg['include_terms']
        domains = query_commoncrawl_keywords(keywords, limit=1500)
        if domains:
            import asyncio
            web_rows = asyncio.run(crawl_domains(domains, limit=800))
            import pandas as pd
            if web_rows:
                frames.append(pd.DataFrame(web_rows))
        else:
            print('CommonCrawl query returned 0 domains (library unavailable or no matches). Skipping web discovery.')

    dot_dir = Path(args.dot_dir)
    if dot_dir.exists():
        frames.append(ingest_dot(args.states, dot_dir))

    if args.osm.lower().startswith("y"):
        frames.append(ingest_osm(args.states, cfg["platform_categories"]["osm_name_regex"]))

    evidence = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if evidence.empty:
        print("No evidence rows produced. Provide DOT files or enable OSM with internet.")
        return

    norm = normalize_df(evidence)
    clf = classify_df(norm, cfg)
    pruned = clf[clf["fit_label"] != "exclude"].copy()
    dedup = simple_dedupe(pruned)

    write_csv(dedup, args.out)
    if args.save_evidence:
        write_parquet(evidence, args.save_evidence)
    print(f"Wrote {len(dedup)} entities to {args.out}")

if __name__ == "__main__":
    main()