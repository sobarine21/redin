import streamlit as st
import pandas as pd
import io
import requests
import zipfile
from tempfile import NamedTemporaryFile

SUPABASE_URL = st.secrets["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

def fetch_table_data(table):
    """
    Fetch ALL data from a given table.
    """
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    params = {"select": "*"}
    resp = requests.get(url, headers=headers, params=params, timeout=300)
    resp.raise_for_status()
    rows = resp.json()
    return pd.DataFrame(rows)

st.title("Supabase: Download All Tables as CSV (Read-Only, Automatic or Manual)")

# Try to auto-detect tables (most likely won't work, so fallback to manual)
tables = []
try:
    url = f"{SUPABASE_URL}/rest/v1/"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    resp = requests.options(url, headers=headers, timeout=30)
    resp.raise_for_status()
    try:
        tables = resp.json()
        tables = [t for t in tables if not t.startswith("rpc/")]
    except Exception:
        tables = []
except Exception:
    tables = []

if not tables:
    st.warning("Could not auto-detect tables from Supabase. Please paste your table names below (comma-separated, no spaces):")
    manual_tables = st.text_area("Enter table names (comma-separated):", "")
    tables = [t.strip() for t in manual_tables.split(",") if t.strip()]

if not tables:
    st.info("No tables specified. Please paste your table names above to continue.")
    st.stop()

st.info(f"{len(tables)} tables will be downloaded. Click below to download all as CSV (one file per table, zipped).")

if st.button("Download ALL tables as ZIP of CSVs"):
    with NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
        with zipfile.ZipFile(tmp_zip, "w") as zf:
            for t in tables:
                try:
                    st.write(f"Downloading `{t}` ...")
                    df = fetch_table_data(t)
                    csv_bytes = df.to_csv(index=False).encode("utf-8")
                    zf.writestr(f"{t}.csv", csv_bytes)
                except Exception as e:
                    st.warning(f"Failed to fetch {t}: {e}")
        tmp_zip.flush()
        tmp_zip.seek(0)
        st.success("All tables downloaded. Click below to get the ZIP.")
        with open(tmp_zip.name, "rb") as f:
            st.download_button(
                label="Download ZIP of all tables",
                data=f,
                file_name="supabase_all_tables.zip",
                mime="application/zip"
            )

st.info("""
**Database access is strictly read-only.**
- The app uses Supabase REST API with your key, but only performs GET/HEAD/OPTIONS.
- No write/update/delete/DDL statements are possible through this UI.
""")
