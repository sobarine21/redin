import streamlit as st
import pandas as pd
import io
import requests

# --- CONFIGURATION: Use Streamlit secrets ---
SUPABASE_URL = st.secrets["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

def fetch_user_tables():
    """
    Fetch user tables in the public schema by requesting each table individually
    via the REST API root endpoint, since system tables are not exposed.
    """
    # We need to manually provide or fetch table names.
    # Option 1: If you know your table names, list them here as a fallback:
    # Example: return ["companies", "holdings", "transactions"]
    # Option 2: Try to fetch from a known 'metadata' table or other means
    # For now, scan the API root for available tables (public REST endpoints)
    url = f"{SUPABASE_URL}/rest/v1/"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    resp = requests.options(url, headers=headers)
    resp.raise_for_status()
    # The returned JSON will have available endpoints
    table_list = []
    if "Allow" in resp.headers:
        # Not standard, but fallback for CORS preflight
        return []
    # Try to parse the table list from text/html (if PostgREST root)
    try:
        # Response should be a JSON list of table endpoints, but sometimes is HTML
        # PostgREST root returns a JSON array of available tables
        table_list = resp.json()
        table_list = [t for t in table_list if not t.startswith("rpc/")]
    except Exception:
        pass
    return table_list

def fetch_table_columns(table):
    # Fetch just 1 row to get the column names
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    params = {"select": "*", "limit": 1}
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()
    if data:
        return list(data[0].keys())
    else:
        # If table is empty, ask the user for columns
        return []

def count_occurrences(table, column, values):
    """
    Count how many times each value appears in a given column of a table.
    """
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "count=exact"
    }
    total = 0
    for value in values:
        params = {f"{column}.ilike": value}
        try:
            resp = requests.get(url, headers=headers, params=params)
            if resp.status_code == 200:
                count = int(resp.headers.get("Content-Range", "0-0").split("/")[1])
                total += count
        except Exception:
            continue
    return total

st.title("REDIN Enforcement Index Generator (Supabase Read-Only)")
st.markdown("""
- Upload a CSV file of index constituents (columns: 'Company Name', 'Ticker').
- The app will search your Supabase DB for each constituent in all public tables/columns.
- Download the REDIN Enforcement Index as CSV.

**All database access is strictly read-only.**
""")

uploaded_file = st.file_uploader("Upload Index Constituent CSV", type=["csv"])

# Show all tables and columns
try:
    tables = fetch_user_tables()
    if not tables:
        st.warning("No tables found via API root. Please enter your table names below, comma-separated.")
        manual_tables = st.text_input("Enter table names:", "")
        tables = [t.strip() for t in manual_tables.split(",") if t.strip()]
    tables_columns = []
    for t in tables:
        cols = fetch_table_columns(t)
        if not cols:
            manual_cols = st.text_input(f"Enter columns for table `{t}` (comma-separated):", "")
            cols = [c.strip() for c in manual_cols.split(",") if c.strip()]
        tables_columns.append((t, cols))
    st.subheader("Database Tables & Columns (public schema)")
    for table, cols in tables_columns:
        st.write(f"**{table}**: {', '.join(cols)}")
except Exception as e:
    st.error(f"Could not fetch table/column info: {e}")
    st.stop()

if uploaded_file:
    try:
        df = pd.read_csv(uploaded_file)
        if not {'Company Name', 'Ticker'}.issubset(df.columns):
            st.error("CSV must have 'Company Name' and 'Ticker' columns.")
            st.stop()
        company_names = df['Company Name'].astype(str).tolist()
        tickers = df['Ticker'].astype(str).tolist()
        values = set([v.lower() for v in company_names + tickers])

        # Search all tables/columns for occurrences
        with st.spinner("Searching Supabase database..."):
            result_data = []
            for comp, ticker in zip(company_names, tickers):
                total_count = 0
                for table, columns in tables_columns:
                    for col in columns:
                        for val in (comp.lower(), ticker.lower()):
                            count = count_occurrences(table, col, [val])
                            total_count += count
                result_data.append({
                    "Company Name": comp,
                    "Ticker": ticker,
                    "REDIN Enforcement Index": float(total_count)
                })
            result_df = pd.DataFrame(result_data)
            st.subheader("REDIN Enforcement Index")
            st.dataframe(result_df)

            csv_buffer = io.StringIO()
            result_df.to_csv(csv_buffer, index=False)
            st.download_button(
                label="Download Index CSV",
                data=csv_buffer.getvalue(),
                file_name="REDIN_Enforcement_Index.csv",
                mime="text/csv"
            )
    except Exception as e:
        st.error(f"Error processing file or querying Supabase: {e}")

st.info("""
**Database access is strictly read-only.**
- The app uses Supabase REST API with your key, but only performs GET/HEAD/OPTIONS.
- No write/update/delete/DDL statements are possible through this UI.
""")
