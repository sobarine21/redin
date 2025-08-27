import streamlit as st
import pandas as pd
import requests
import zipfile
from tempfile import NamedTemporaryFile

# --------------------------------------------------------------
# 🔑 Secrets – put these in .streamlit/secrets.toml or via UI
# --------------------------------------------------------------
SUPABASE_URL = st.secrets["SUPABASE_URL"].rstrip("/")
SUPABASE_ANON_KEY = st.secrets["SUPABASE_ANON_KEY"]
# Optional – only needed for the full‑SQL‑dump feature
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
# 1️⃣ OPTIONAL: auto‑discover tables (falls back to manual list)
# --------------------------------------------------------------
@st.cache_data(ttl=600)  # cache for 10 min
def list_user_tables() -> list[str]:
    """
    Try to obtain the list of tables in the `public` schema.
    Supabase does **not** let us run arbitrary SELECTs via /rpc,
    so this request often fails.  In that case we raise an exception
    and the UI will fall back to the manual table list.
    """
    # The built‑in `pg_tables` view is *not* exposed through the REST API,
    # so the only safe way is to call a **named** function that returns it.
    # If your project doesn’t have such a function, this call will 404.
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/get_public_tables",  # <-- must exist
        json={},
        headers=_auth_headers(),
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError("Auto‑discover failed – function not found.")
    rows = resp.json()
    # Expected format: [{ "name": "my_table" }, ...]
    return [r["name"] for r in rows]

# --------------------------------------------------------------
# 2️⃣ Paginated fetch for a single table (Range header)
# --------------------------------------------------------------
def fetch_table_paginated(table: str, chunk: int = 10_000) -> pd.DataFrame:
    """
    Pull *all* rows from `table` using Supabase’s Range‑Unit header.
    The loop ends when the server returns HTTP 200 (final page) or
    when we have reached the total row count reported in Content‑Range.
    """
    all_rows = []
    start = 0

    while True:
        end = start + chunk - 1
        headers = _auth_headers()
        headers.update(
            {
                "Range-Unit": "items",      # essential for correct pagination
                "Range": f"{start}-{end}",  # e.g. 0‑9999, 10000‑19999, …
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

        if resp.status_code == 200:  # last page
            break

        # Optional early‑stop if we know the total size
        content_range = resp.headers.get("Content-Range")
        if content_range:
            try:
                _, range_part = content_range.split(" ")
                _, total = range_part.split("/")
                total = int(total)
                if start + chunk >= total:
                    break
            except Exception:
                pass  # ignore parsing problems

        start += chunk

    return pd.DataFrame(all_rows)

# --------------------------------------------------------------
# 3️⃣ OPTIONAL: Full SQL dump (service‑role only)
# --------------------------------------------------------------
def download_sql_dump() -> bytes:
    """
    Calls Supabase’s hidden `pg_dump` RPC.
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
    # Supabase returns the dump under either "dump" or "data"
    b64 = payload.get("dump") or payload.get("data")
    return base64.b64decode(b64)

# --------------------------------------------------------------
# 🎨 UI
# --------------------------------------------------------------
st.title("🗂️ Supabase – Export Every Table (Full Data)")

# -----------------------------------------------------------------
# Step 1 – get the list of tables
# -----------------------------------------------------------------
# You already have a massive manual list.  Keep it as a fallback.
manual_table_list = [
    "iomfsa_press_releases","tsx_reviews_suspensions","us_doj_antitrust_cases","Eu_centralbank_enforcement","master_data",
    "sebi_circulars","policy_documents","india_myneta_all_parties_donorsss","Malaysia_sc_actions","indian_political_fund_doners",
    "policy_updates","policy_docs","policy_versions","trend_analysis","cssf_warnings","ICDR Fines_nse","asx_enforcement_notices",
    "banned by  Competent Authorities India","fsc_mauritius_documents4","ng_illegal_investments","maharera_non_compliance",
    "fsa_market_misconduct","disqualified_directors","mas_actions","Malaysia_enforcements","automation_settings","directors_struckoff",
    "indian_electoral_bond_owners","profiles","ActionTaken_Inspections_Report_nse","rbi_circulars","sescc_market_misconduct",
    "comcom.govt.nz","fsc_mauritius_documents2","federal_reserve_circulars","defaulting_clients_mcx","defaulting_clients_ncdex",
    "defaulting_clients_nse","ebsa_ocats","epa_data_list","fsc_mauritius_documents","sessc_cases","ssc_sanctions","sec_circulars",
    "sec_dil_proceedings","sec_admin_processings","malaysia_investor_alerts","fca_actions","euro_sanction","fca_publications",
    "apra_disqualified","sebi_reco","bse_enforcement","enforcement_details","mas_circulars","bafin_circulars","fca_circulars",
    "processing_batches","processing_metrics","performance_logs","validation_rules","cima_fines","mfsa_sanctions","sessc_press",
    "entity_enforcements","apra_disqualified2","ecb_enforcements","sec_litigation_releases","cypress_banned_domain",
    "bregg_activecreditor_notices","ridn_directory","redn_messages","fsc_mauritius_documents3","mfsa_warnings","sessc_press2",
    "sec_alj_orders","sfc_enforcement_news","suspended_websites","sniff_notifications","rss_feeds","cron_job_logs",
    "cfpb_enforcement_actions","cnv_alerts","enforcements_actions","fdic_enforcements","edo_orders","eu_rss_data",
    "ctfc_enforcements","asian_rss_data","americas_rss_data","middle_east_rss_data","african_rss_data","user_preferences",
    "index_configurations","indian_politicains","fi_financial_firms_sanctions","us_epa_gov_actions","bd_sec_enforcement",
    "canada_environmental_orders","sc_cases_compounded","sc_regulatory_settlements","sc_civil_actions",
    "sec_gov_gh_enforcement_actions","corporateinsolvency_proceedings","consolidatedLegacyByPRN","nse_suspended",
    "nse_banned_debared","esma_sanctions","uk environment_action","ibbi_nclt_orders","ibbi_nclat_orders",
    "ibbi_high_courts_orders","ibbi_orders","ibbi_supreme_court_orders","irdai_warnings_penalties","iscan_europe",
    "compliants_nse_listed","defaulting_clients_bse","struckoff_directors","amf_enforements","GLOBAL_SDN","user_uploads",
    "policy_pdf_updates","enforcement_entities","institutional_feeds","upload_entities","enforcement_matches","fsa_sanctions",
    "csa_investor_alters","ncua_enforcements","occ_enforcements","ots_enforcement","ots_enforcement_orders",
    "publicidad_liquidaciones","bangladesh_enforcement_archive","asic_banning_alerts","uk_liquidations","enheter_sokeresultat",
    "uk_disqualified_directors","newzealand_insolvancy","pcaob_enforcement_actions","uk_admin_proceedings",
    "newzealand_insolvent_company","asic_infringement_notices","brreg_bankruptcies","fma_media_releases",
    "new_zealand_insolvency","nz_removed_individuals_ceased","nfra_orders","penalties_exportoffice_india",
    "maharera_complaints","maharera_promoter_complaints","chat_conversations","action_exports_office_india",
    "cpcb_ngt_orders","cpcb_directions","dgft_adjudication_orders","ACRA_GOV_insolvant","chat_messages",
    "index_calculations","sql_query_history","saved_queries","bregg_insolvants","Indian_electoral_bondholders",
    "uk_tax_defaulters","sc_administrative_actions","superfinanciera_ordenes_suspension","banned _list_uapa",
    "iomfsa_public_warnings","Actiontaken_inspections_nse","ani_declarations","superfinanciera_actions",
    "epa_civil_cleanup_cases","sc_criminal_prosecution","alsu_bankruptcies","complaints_against_listed_nse",
    "delisted_under_liquidations_nse","crip_withdrawn","Companies_IBC_Moratorium_Debt","crip_nse_cases",
    "nse_under_liquidations","nse_actions","nse_Non_Compliant_MPS","NSE_List_SDD","nse_Non-compliant_Promoter freezing",
    "Archive SEBI DEBARRED entities","SEBI_DEACTIVATED","Defaulting_Client_Database nse_","nasdaq_disciplinary_actions",
    "ftc_cases","jpx_disciplinary_actions","finra_individuals_barred","finra_cases","fina_Actions Resulting from Referral",
    "finra_adjudication_decisions"
]

# Try auto‑discover; if it fails we just keep the manual list.
try:
    discovered_tables = list_user_tables()
    st.success(f"✅ Discovered **{len(discovered_tables)}** tables via RPC.")
except Exception as exc:
    st.warning(
        "🔍 Auto‑discover failed – using the manual table list you provided. "
        f"Details: {exc}"
    )
    discovered_tables = manual_table_list

st.success(f"✅ Ready to export **{len(discovered_tables)}** tables.")

# -----------------------------------------------------------------
# Step 2 – download CSV + SQL ZIP
# -----------------------------------------------------------------
if st.button("Export ALL tables as CSV + SQL ZIP"):
    with st.spinner("Downloading tables – this may take a while…"):
        with NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
            with zipfile.ZipFile(tmp_zip, "w") as zf:
                for tbl in discovered_tables:
                    st.info(f"📥 Fetching **{tbl}** …")
                    try:
                        df = fetch_table_paginated(tbl)
                        if df.empty:
                            st.warning(f"⚠️ `{tbl}` appears empty.")
                        csv_bytes = df.to_csv(index=False).encode("utf-8")
                        zf.writestr(f"{tbl}.csv", csv_bytes)
                        st.success(f"✅ `{tbl}` ({len(df)} rows) added to ZIP.")
                    except Exception as e:
                        st.error(f"❌ Failed to fetch `{tbl}`: {e}")

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
- **Read‑only** – only GET requests (or the optional service‑role RPC).  
- **RLS** – when using the anon key you receive only the rows the client is allowed to read.  
- **Full dump** – needs `SUPABASE_SERVICE_ROLE_KEY`; it bypasses RLS and returns the exact DB state.  
- **No external services** – everything runs locally in the Streamlit process.  
"""
)
