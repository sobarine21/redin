import streamlit as st
import pandas as pd
import requests
import zipfile
import io
from tempfile import NamedTemporaryFile

SUPABASE_URL = st.secrets["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

BATCH_SIZE = 10000  # Number of rows per request; safe and efficient for most setups

def fetch_table_data_all(table):
    """
    Fetch ALL rows from a given table in batches using the Range header.
    Returns a DataFrame with all rows.
    """
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Range-Unit": "items"
    }
    params = {"select": "*"}
    all_rows = []
    start = 0

    # First batch: get total via Content-Range
    while True:
        end = start + BATCH_SIZE - 1
        batch_headers = {**headers, "Range": f"{start}-{end}"}
        resp = requests.get(url, headers=batch_headers, params=params, timeout=300)
        resp.raise_for_status()
        batch = resp.json()
        if batch:
            all_rows.extend(batch)
            st.info(f"Fetched rows {start + 1} to {start + len(batch)} from `{table}`")
        else:
            break
        # Stop if this is the last batch
        content_range = resp.headers.get("Content-Range", None)
        if content_range:
            # Format: items start-end/total
            try:
                total = int(content_range.split("/")[-1])
            except Exception:
                total = None
        else:
            total = None
        start += len(batch)
        if total is not None and start >= total:
            break
        if len(batch) < BATCH_SIZE:
            break  # No more rows
    return pd.DataFrame(all_rows)

st.title("Supabase: Download All Tables as Complete CSVs (No Row Skipping)")

st.markdown("""
- Paste your comma-separated table names below (required).
- All data from each table will be fetched and included in a ZIP file of CSVs.
- No data is skipped. **This fetches all rows, no matter the table size.**
- Only GET requests are used (strictly read-only).
""")

manual_tables = st.text_area("Enter table names (comma-separated, no spaces):", "")
tables = [t.strip() for t in manual_tables.split(",") if t.strip()]

if not tables:
    st.info("No tables specified. Please paste your table names above to continue.")
    st.stop()

if st.button("Download ALL tables as ZIP of CSVs"):
    with NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
        with zipfile.ZipFile(tmp_zip, "w") as zf:
            for t in tables:
                try:
                    st.write(f"Downloading **{t}** ...")
                    df = fetch_table_data_all(t)
                    csv_bytes = df.to_csv(index=False).encode("utf-8")
                    zf.writestr(f"{t}.csv", csv_bytes)
                    st.success(f"Added `{t}` ({len(df)} rows) to ZIP.")
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
