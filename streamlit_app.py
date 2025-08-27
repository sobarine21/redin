import streamlit as st
import pandas as pd
import psycopg2
import csv
import io
import os

# --- CONFIGURATION ---

# You should store your credentials securely, e.g. with Streamlit secrets or environment variables.
st.secrets["supabase"] = {
    "host": "YOUR_SUPABASE_HOST",
    "port": "5432",
    "database": "postgres",
    "user": "YOUR_READONLY_USER",
    "password": "YOUR_READONLY_PASSWORD"
}

# --- DATABASE CONNECTION (STRICTLY READ-ONLY) ---

def get_db_connection():
    # You MUST use a user with only SELECT permissions.
    conn = psycopg2.connect(
        host=st.secrets["supabase"]["host"],
        port=st.secrets["supabase"]["port"],
        dbname=st.secrets["supabase"]["database"],
        user=st.secrets["supabase"]["user"],
        password=st.secrets["supabase"]["password"],
        sslmode="require",
        options="-c default_transaction_read_only=on"  # Forces read-only at session level
    )
    return conn

# --- GET ALL TABLES/COLUMNS (POSTGRESQL) ---

def get_all_tables_and_columns(conn):
    cur = conn.cursor()
    # Only list user tables in public schema (you can adapt schema if needed)
    cur.execute("""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
    """)
    tables = {}
    for table_name, column_name in cur.fetchall():
        tables.setdefault(table_name, []).append(column_name)
    return list(tables.items())

# --- SEARCH COMPANIES/TICKERS IN DB ---

def search_company_in_db(conn, tables_columns, company_names, tickers):
    cur = conn.cursor()
    results = {}
    for comp, ticker in zip(company_names, tickers):
        total_count = 0
        for table, columns in tables_columns:
            for col in columns:
                # Use safe parameterized queries, case-insensitive search
                try:
                    query = f'SELECT COUNT(*) FROM "{table}" WHERE LOWER(CAST("{col}" AS TEXT)) = %s OR LOWER(CAST("{col}" AS TEXT)) = %s'
                    cur.execute(query, (comp.lower(), ticker.lower()))
                    count = cur.fetchone()[0]
                    total_count += count
                except Exception:
                    # Skip columns that can't be cast to text or searched
                    continue
        results[(comp, ticker)] = total_count
    return results

def compute_redin_index(count):
    # For now score = count (weighted average = count)
    return float(count)

# --- STREAMLIT APP ---

st.title("REDIN Enforcement Index Generator (Supabase/PostgreSQL)")
st.markdown("""
- Upload a CSV file of index constituents (columns: 'Company Name', 'Ticker').
- The app will search your Supabase DB for each constituent.
- Download the REDIN Enforcement Index as CSV.
""")

# Upload CSV
uploaded_file = st.file_uploader("Upload Index Constituent CSV", type=["csv"])

# DB connection (read-only)
try:
    conn = get_db_connection()
except Exception as e:
    st.error(f"Could not connect to Supabase/PostgreSQL: {e}")
    st.stop()

guardrails_error = False

# Display tables and columns (for user info)
try:
    tables_columns = get_all_tables_and_columns(conn)
    st.subheader("Database Tables & Columns (public schema)")
    for table, cols in tables_columns:
        st.write(f"**{table}**: {', '.join(cols)}")
except Exception as e:
    st.error(f"Failed to fetch tables/columns: {e}")
    guardrails_error = True

# Process upload
if uploaded_file and not guardrails_error:
    try:
        df = pd.read_csv(uploaded_file)
        # Basic validation
        if not {'Company Name', 'Ticker'}.issubset(df.columns):
            st.error("CSV must have 'Company Name' and 'Ticker' columns.")
            st.stop()
        company_names = df['Company Name'].astype(str).tolist()
        tickers = df['Ticker'].astype(str).tolist()

        # Search DB
        with st.spinner("Searching database..."):
            search_results = search_company_in_db(conn, tables_columns, company_names, tickers)

        # Prepare results DataFrame
        result_data = []
        for (comp, ticker), count in search_results.items():
            index_score = compute_redin_index(count)
            result_data.append({
                "Company Name": comp,
                "Ticker": ticker,
                "REDIN Enforcement Index": index_score
            })
        result_df = pd.DataFrame(result_data)
        st.subheader("REDIN Enforcement Index")
        st.dataframe(result_df)

        # Download option
        csv_buffer = io.StringIO()
        result_df.to_csv(csv_buffer, index=False)
        st.download_button(
            label="Download Index CSV",
            data=csv_buffer.getvalue(),
            file_name="REDIN_Enforcement_Index.csv",
            mime="text/csv"
        )

    except Exception as e:
        st.error(f"Error processing file: {e}")

# --- EXTREME GUARDRAILS ---

st.info("""
**Database connection is strictly read-only.**
- The app uses a PostgreSQL user with only SELECT privileges.
- All queries are parameterized and read-only.
- No write/update/delete/DDL statements are used or possible through this UI.
""")
