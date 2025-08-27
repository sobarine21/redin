import streamlit as st
import pandas as pd
import io
import requests

# --- CONFIGURATION: Use Streamlit secrets ---
SUPABASE_URL = st.secrets["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

# --- GUARDRAILS: Only allow GET/HEAD/OPTIONS, never POST/PATCH/DELETE ---

def fetch_all_tables():
    # Use Supabase REST API to list all tables in the public schema
    # (Supabase does not expose a direct REST endpoint for listing tables, so we fake it using the information_schema)
    url = f"{SUPABASE_URL}/rest/v1/information_schema.tables"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    params = {
        "select": "table_name",
        "table_schema": "eq.public"
    }
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return [row["table_name"] for row in resp.json()]

def fetch_table_columns(table):
    # Use Supabase REST API to list columns for a given table
    url = f"{SUPABASE_URL}/rest/v1/information_schema.columns"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    params = {
        "select": "column_name",
        "table_name": f"eq.{table}",
        "table_schema": "eq.public"
    }
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return [row["column_name"] for row in resp.json()]

def count_occurrences(table, column, names_lower_set):
    # Use Supabase REST API for case-insensitive count
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "count=exact"
    }
    # Use ilike (case-insensitive) filter for each name/ticker
    # We'll do one call per name/ticker per column per table, for safety
    counts = {}
    for name in names_lower_set:
        params = {
            f"{column}.ilike": name  # ilike is case-insensitive in Supabase
        }
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 200:
            count = int(resp.headers.get("Content-Range", "0-0").split("/")[1])
        else:
            count = 0
        counts[name] = count
    return counts

# --- STREAMLIT APP ---

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
    tables = fetch_all_tables()
    tables_columns = []
    for t in tables:
        cols = fetch_table_columns(t)
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
        names_and_tickers = set(n.lower() for n in company_names + tickers)

        # Search all tables/columns for occurrences
        with st.spinner("Searching Supabase database..."):
            # For each (company, ticker) pair, count occurrences across all columns/tables
            result_data = []
            for comp, ticker in zip(company_names, tickers):
                total_count = 0
                for table, columns in tables_columns:
                    for col in columns:
                        # Count for company name and ticker (case-insensitive)
                        for val in (comp.lower(), ticker.lower()):
                            url = f"{SUPABASE_URL}/rest/v1/{table}"
                            headers = {
                                "apikey": SUPABASE_KEY,
                                "Authorization": f"Bearer {SUPABASE_KEY}",
                                "Prefer": "count=exact",
                            }
                            params = {
                                f"{col}.ilike": val
                            }
                            resp = requests.get(url, headers=headers, params=params)
                            if resp.status_code == 200:
                                count = int(resp.headers.get("Content-Range", "0-0").split("/")[1])
                                total_count += count
                            # If column type is not searchable, skip
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
- The app uses Supabase REST API with service key or anon key, but only performs GET/HEAD/OPTIONS.
- No write/update/delete/DDL statements are possible through this UI.
""")
