import os
import sys
import subprocess
from pathlib import Path
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Industry Entity Finder — Demo", layout="wide")
st.title("Industry Entity Finder — Paving Pilot Demo")
st.write("Select options, then click **Run pipeline** to discover paving contractors in TX/MI/CO using free/low-cost sources.")

# --- Controls ---
col1, col2, col3 = st.columns(3)
with col1:
    states = st.multiselect("States", ["TX", "MI", "CO"], default=["TX", "MI", "CO"])
with col2:
    use_osm = st.checkbox("Use OpenStreetMap (Overpass)", value=True)
with col3:
    use_web = st.checkbox("Use Web Discovery (Common Crawl + crawler)", value=True)

col4, col5 = st.columns([2,1])
with col4:
    dot_dir = st.text_input("DOT files folder (optional)", value=str(Path("data/dot").resolve()))
with col5:
    out_path = st.text_input("Output CSV", value=str(Path("out/paving_demo.csv").resolve()))

save_evidence = st.text_input("Evidence Parquet (optional)", value=str(Path("out/evidence_demo.parquet").resolve()))

st.divider()

# --- Run button ---
run_clicked = st.button("Run pipeline", type="primary")

# Ensure output dirs exist
Path(out_path).parent.mkdir(parents=True, exist_ok=True)
if save_evidence:
    Path(save_evidence).parent.mkdir(parents=True, exist_ok=True)

if run_clicked:
    if not states:
        st.error("Please select at least one state.")
        st.stop()

    cmd = [
        sys.executable, "-m", "ief.flows.paving_run",
        "--states", *states,
        "--osm", "yes" if use_osm else "no",
        "--web-discovery", "yes" if use_web else "no",
        "--out", out_path,
    ]
    if dot_dir:
        cmd += ["--dot-dir", dot_dir]
    if save_evidence:
        cmd += ["--save-evidence", save_evidence]

    st.info("Running: ``{}``".format(" ".join(cmd)))

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        st.code(proc.stdout or "(no stdout)")
        if proc.returncode != 0:
            st.error("Pipeline exited with code {}".format(proc.returncode))
            st.code(proc.stderr or "(no stderr)")
        else:
            st.success("Pipeline completed.")
    except Exception as e:
        st.exception(e)

# --- Results preview ---
if Path(out_path).exists():
    try:
        df = pd.read_csv(out_path)
        st.subheader("Results Preview")
        st.caption(f"{len(df):,} rows • {out_path}")
        st.dataframe(df.head(500), use_container_width=True)
        st.download_button("Download CSV", data=df.to_csv(index=False), file_name=Path(out_path).name, mime="text/csv")
    except Exception as e:
        st.warning(f"Couldn't read output yet: {e}")

if save_evidence and Path(save_evidence).exists():
    st.caption(f"Evidence saved at: {save_evidence}")
    if st.button("Preview first 1,000 evidence rows"):
        try:
            import pyarrow.parquet as pq
            table = pq.read_table(save_evidence)
            df_e = table.to_pandas().head(1000)
            st.dataframe(df_e, use_container_width=True)
        except Exception as e:
            st.warning(f"Couldn't read evidence parquet: {e}")

st.markdown("""
**Notes**
- Web discovery uses Common Crawl via `cdx_toolkit` if available; otherwise it skips gracefully.
- Crawling is **robots.txt–aware** and polite; first run may take time.
- For best results, download DOT lists for TX/MI/CO into the folder above.
""")
