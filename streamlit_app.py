import streamlit as st
import pandas as pd
import requests
import zipfile
from tempfile import NamedTemporaryFile

# --------------------------------------------------------------
# 🔑 Secrets – add these in .streamlit/secrets.toml or via UI
# --------------------------------------------------------------
SUPABASE_URL = st.secrets["SUPABASE_URL"].rstrip("/")
SUPABASE_ANON_KEY = st.secrets["SUPABASE_ANON_KEY"]
# Optional – needed only for the full‑SQL‑dump feature
SUPABASE_SERVICE_ROLE_KEY = st.secrets.get("SUPABASE_SERVICE_ROLE_KEY")

# --------------------------------------------------------------
# 📦 Helper – build auth headers
# --------------------------------------------------------------
def _auth_headers(use_service_role: bool = False) -> dict:
    """Return headers for the anon key or the service‑role key."""
    key = SUPABASE_SERVICE_ROLE_KEY if use_service_role else SUPABASE_ANON_KEY
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
    }

# --------------------------------------------------------------
# 1️⃣ List every user table in the public schema
# --------------------------------------------------------------
@st.cache_data(ttl=600)  # cache for 10 min
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
    Pull *all* rows from `table` using Supabase’s Range-Unit header.
    The loop ends when the server returns HTTP 200 (final page) or
    when the declared total row count is reached.
    """
    all_rows = []
    start = 0

    while True:
        end = start + chunk - 1
        headers = _auth_headers()
        headers.update(
            {
                "Range-Unit": "items",      # crucial for correct pagination
                "Range": f"{start}-{end}",  # fetch the slice we need
            }
        )
        url = f"{SUPABASE_URL}/rest/v1/{table}"
        resp = requests.get(url, headers=headers, timeout=300)

        # 206 = partial content (more pages); 200 = final page
        if resp.status_code not in (200, 206):
            resp.raise_for_status()

        batch = resp.json()
        if not batch:
            break   # empty table

        all_rows.extend(batch)

        if resp.status_code == 200:
            # Last page – we’re done
            break

        # Optional: stop early if we know the total size
        content_range = resp.headers.get("Content-Range")
        if content_range:
            try:
                _, range_part = content_range.split(" ")
                _, total = range_part.split("/")
                total = int(total)
                if start + chunk >= total:
                    break
            except Exception:
                pass  # parsing failure – just continue

        start += chunk

    return pd.DataFrame(all_rows)

# --------------------------------------------------------------
# 3️⃣ OPTIONAL: Full SQL dump (service‑role only)
# --------------------------------------------------------------
def download_sql_dump() -> bytes:
    """
    Calls Supabase’s hidden pg_dump RPC.
    Returns the raw SQL (base‑64 decoded).
    """
    # Extract the project reference from the URL (e.g. xyz.supabase.co → xyz)
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
    b64 = payload.get("dump") or payload.get("data")
    return base64.b64decode(b64)

# --------------------------------------------------------------
# 🎨 UI
# --------------------------------------------------------------
st.title("🗂️ Supabase – Export Every Table (Full Data)")

# -----------------------------------------------------------------
# Step 1 – discover tables (or keep manual list if you prefer)
# -----------------------------------------------------------------
with st.spinner("Fetching list of tables…"):
    discovered_tables = list_user_tables()

if not discovered_tables:
    st.error("❌ No tables found in the `public` schema.")
    st.stop()

st.success(f"✅ Found **{len(discovered_tables)}** tables.")
st.caption(", ".join(discovered_tables))

# -----------------------------------------------------------------
# Step 2 – download CSV + SQL ZIP
# -----------------------------------------------------------------
if st.button("Export ALL tables as CSV + SQL ZIP"):
    with st.spinner("Downloading tables – this can take a while…"):
        with NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
            with zipfile.ZipFile(tmp_zip, "w") as zf:
                for tbl in discovered_tables:
                    st.info(f"📥 Fetching **{tbl}** …")
                    try:
                        df = fetch_table_paginated(tbl)
                        if df.empty:
                            st.warning(f"⚠️ `{tbl}` appears empty.")
                        csv_bytes = df.to_csv(index=False).encode("utf‑8")
                        zf.writestr(f"{tbl}.csv", csv_bytes)
                        st.success(f"✅ `{tbl}` ({len(df)} rows) added to ZIP.")
                    except Exception as e:
                        st.warning(f"❌ Failed to fetch `{tbl}`: {e}")

                # -------------------------------------------------
                # Optional: full‑SQL dump (requires service‑role key)
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
- **Read‑only**: Only GET requests (or the optional service‑role RPC) are made.  
- **RLS**: When using the anon key you only receive rows the client is allowed to see.  
- **Full dump**: Requires `SUPABASE_SERVICE_ROLE_KEY`; it bypasses RLS and returns the exact DB state.  
- **No external services** – everything runs locally in the Streamlit process.  
"""
)
