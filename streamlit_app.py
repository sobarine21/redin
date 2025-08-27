import streamlit as st
import pandas as pd
import requests
import zipfile
from tempfile import NamedTemporaryFile

# --------------------------------------------------------------
# ✨ Secrets – add these in .streamlit/secrets.toml or via UI
# --------------------------------------------------------------
SUPABASE_URL = st.secrets["SUPABASE_URL"].rstrip("/")
SUPABASE_ANON_KEY = st.secrets["SUPABASE_ANON_KEY"]
# Optional – needed only for the full‑SQL‑dump feature
SUPABASE_SERVICE_ROLE_KEY = st.secrets.get("SUPABASE_SERVICE_ROLE_KEY")

# --------------------------------------------------------------
# Helper – build the auth headers
# --------------------------------------------------------------
def _auth_headers(use_service_role: bool = False) -> dict:
    """Return the proper Authorization headers."""
    key = SUPABASE_SERVICE_ROLE_KEY if use_service_role else SUPABASE_ANON_KEY
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
    }

# --------------------------------------------------------------
# 1️⃣ Discover all user tables (public schema)
# --------------------------------------------------------------
@st.cache_data(ttl=600)    # cache for 10 min to avoid repeated queries
def list_user_tables() -> list[str]:
    """Return a list of table names in the public schema."""
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE';
    """
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc",
        json={"query": sql, "params": []},
        headers=_auth_headers(),
        timeout=30,
    )
    resp.raise_for_status()
    rows = resp.json()
    return [r["table_name"] for r in rows]

# --------------------------------------------------------------
# 2️⃣ Paginated fetch for a single table (Range header)
# --------------------------------------------------------------
def fetch_table_paginated(table: str, chunk: int = 10_000) -> pd.DataFrame:
    """
    Pull *all* rows from `table` using Supabase’s Range header.
    The function loops until the server returns the final page.
    """
    all_rows = []
    start = 0

    while True:
        end = start + chunk - 1
        headers = _auth_headers()
        headers["Range"] = f"0-{end}"   # “0‑end” is the range Supabase expects
        url = f"{SUPABASE_URL}/rest/v1/{table}"
        resp = requests.get(url, headers=headers, timeout=300)

        if resp.status_code not in (200, 206):
            resp.raise_for_status()

        batch = resp.json()
        if not batch:
            break

        all_rows.extend(batch)
        # 206 = more rows available, 200 = final page (or empty)
        if resp.status_code == 200:
            break
        start += chunk

    return pd.DataFrame(all_rows)

# --------------------------------------------------------------
# 3️⃣ OPTIONAL: Full SQL dump (service‑role only)
# --------------------------------------------------------------
def download_sql_dump() -> bytes:
    """
    Calls Supabase’s hidden `pg_dump` RPC. It returns a base‑64 string;
    we decode it to raw SQL bytes.
    """
    # Extract project reference from the Supabase URL (e.g. xyz.supabase.co → xyz)
    ref = SUPABASE_URL.split("/")[-1].split(".")[0]
    dump_url = f"https://{ref}.supabase.co/rest/v1/rpc/pg_dump"

    resp = requests.post(
        dump_url,
        json={},
        headers=_auth_headers(use_service_role=True),
        timeout=600,
    )
    resp.raise_for_status()
    import base64

    payload = resp.json()
    # Some deployments wrap the string in `dump` or `data`
    b64 = payload.get("dump") or payload.get("data")
    return base64.b64decode(b64)

# --------------------------------------------------------------
# UI
# --------------------------------------------------------------
st.title("🗂️ Supabase – Export Every Table (Full Data)")

# -----------------------------------------------------------------
# Step 1 – list tables
# -----------------------------------------------------------------
with st.spinner("Fetching list of tables…"):
    tables = list_user_tables()

if not tables:
    st.error("❌ No tables found in the `public` schema.")
    st.stop()

st.success(f"✅ Found **{len(tables)}** tables.")
st.caption(", ".join(tables))

# -----------------------------------------------------------------
# Step 2 – download CSV + SQL ZIP
# -----------------------------------------------------------------
if st.button("Export ALL tables as CSV + SQL ZIP"):
    with st.spinner("Downloading tables – this can take a few minutes…"):
        with NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
            with zipfile.ZipFile(tmp_zip, "w") as zf:
                for tbl in tables:
                    st.info(f"📥 Fetching **{tbl}** …")
                    df = fetch_table_paginated(tbl)
                    if df.empty:
                        st.warning(f"⚠️ `{tbl}` appears empty.")
                    csv_bytes = df.to_csv(index=False).encode("utf‑8")
                    zf.writestr(f"{tbl}.csv", csv_bytes)
                    st.success(f"✅ `{tbl}` ({len(df)} rows) added.")

                # -------------------------------------------------
                # Optional: add full SQL dump to same archive
                # -------------------------------------------------
                if SUPABASE_SERVICE_ROLE_KEY:
                    try:
                        st.info("Generating full SQL dump …")
                        dump_bytes = download_sql_dump()
                        zf.writestr("database.sql", dump_bytes)
                        st.success("✅ SQL dump added to ZIP.")
                    except Exception as exc:
                        st.warning(f"⚠️ Could not create SQL dump: {exc}")

            tmp_zip.flush()
            tmp_zip.seek(0)

            with open(tmp_zip.name, "rb") as f:
                st.download_button(
                    label="⬇️ Download ZIP (CSV + SQL dump)",
                    data=f,
                    file_name="supabase_export.zip",
                    mime="application/zip",
                )
    st.success("✅ Export ready!")

# -----------------------------------------------------------------
# Step 3 – pure‑SQL dump (service‑role only)
# -----------------------------------------------------------------
if SUPABASE_SERVICE_ROLE_KEY:
    if st.button("Download ONLY full SQL dump (service‑role)"):
        with st.spinner("Creating SQL dump…"):
            try:
                dump_bytes = download_sql_dump()
                st.download_button(
                    label="⬇️ Download database.sql",
                    data=dump_bytes,
                    file_name="database.sql",
                    mime="application/sql",
                )
                st.success("✅ SQL dump ready!")
            except Exception as exc:
                st.error(f"❌ Failed to generate dump: {exc}")

# -----------------------------------------------------------------
# Info panel
# -----------------------------------------------------------------
st.info(
    """
- **Read‑only**: The app only issues GET requests (or the service‑role RPC for dumps).  
- **RLS**: When using the anon key you only receive rows the client can read.  
- **Full dump**: Requires the `SUPABASE_SERVICE_ROLE_KEY`; it bypasses RLS and returns the exact DB state.  
- **No external services** – everything runs locally in the Streamlit process.  
"""
)
