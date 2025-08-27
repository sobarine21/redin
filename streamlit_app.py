import streamlit as st
import pandas as pd
import requests
import zipfile
from tempfile import NamedTemporaryFile

SUPABASE_URL = st.secrets["SUPABASE_URL"].rstrip("/")
SUPABASE_ANON_KEY = st.secrets["SUPABASE_ANON_KEY"]

def _auth_headers() -> dict:
    return {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Accept": "application/json",
    }

def fetch_table_paginated(table: str, chunk: int = 10_000) -> pd.DataFrame:
    all_rows = []
    start = 0
    while True:
        end = start + chunk - 1
        headers = _auth_headers()
        headers["Range"] = f"{start}-{end}"
        url = f"{SUPABASE_URL}/rest/v1/{table}"
        resp = requests.get(url, headers=headers, timeout=300)
        if resp.status_code not in (200, 206):
            resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_rows.extend(batch)
        if resp.status_code == 200 or len(batch) < chunk:
            break
        start += chunk
    return pd.DataFrame(all_rows)

st.title("🗂️ Supabase – Export Every Table (Full Data)")

# Paste your table names here
tables = [
    "iomfsa_press_releases","tsx_reviews_suspensions","us_doj_antitrust_cases","Eu_centralbank_enforcement","master_data","sebi_circulars","policy_documents","india_myneta_all_parties_donorsss","Malaysia_sc_actions","indian_political_fund_doners","policy_updates","policy_docs","policy_versions","trend_analysis","cssf_warnings","ICDR Fines_nse","asx_enforcement_notices","banned by  Competent Authorities India","fsc_mauritius_documents4","ng_illegal_investments","maharera_non_compliance","fsa_market_misconduct","disqualified_directors","mas_actions","Malaysia_enforcements","automation_settings","directors_struckoff","indian_electoral_bond_owners","profiles","ActionTaken_Inspections_Report_nse","rbi_circulars","sescc_market_misconduct","comcom.govt.nz","fsc_mauritius_documents2","federal_reserve_circulars","defaulting_clients_mcx","defaulting_clients_ncdex","defaulting_clients_nse","ebsa_ocats","epa_data_list","fsc_mauritius_documents","sessc_cases","ssc_sanctions","sec_circulars","sec_dil_proceedings","sec_admin_processings","malaysia_investor_alerts","fca_actions","euro_sanction","fca_publications","apra_disqualified","sebi_reco","bse_enforcement","enforcement_details","mas_circulars","bafin_circulars","fca_circulars","processing_batches","processing_metrics","performance_logs","validation_rules","cima_fines","mfsa_sanctions","sessc_press","entity_enforcements","apra_disqualified2","ecb_enforcements","sec_litigation_releases","cypress_banned_domain","bregg_activecreditor_notices","ridn_directory","redn_messages","fsc_mauritius_documents3","mfsa_warnings","sessc_press2","sec_alj_orders","sfc_enforcement_news","suspended_websites","sniff_notifications","rss_feeds","cron_job_logs","cfpb_enforcement_actions","cnv_alerts","enforcements_actions","fdic_enforcements","edo_orders","eu_rss_data","ctfc_enforcements","asian_rss_data","americas_rss_data","middle_east_rss_data","african_rss_data","user_preferences","index_configurations","indian_politicains","fi_financial_firms_sanctions","us_epa_gov_actions","bd_sec_enforcement","canada_environmental_orders","sc_cases_compounded","sc_regulatory_settlements","sc_civil_actions","sec_gov_gh_enforcement_actions","corporateinsolvency_proceedings","consolidatedLegacyByPRN","nse_suspended","nse_banned_debared","esma_sanctions","uk environment_action","ibbi_nclt_orders","ibbi_nclat_orders","ibbi_high_courts_orders","ibbi_orders","ibbi_supreme_court_orders","irdai_warnings_penalties","iscan_europe","compliants_nse_listed","defaulting_clients_bse","struckoff_directors","amf_enforements","GLOBAL_SDN","user_uploads","policy_pdf_updates","enforcement_entities","institutional_feeds","upload_entities","enforcement_matches","fsa_sanctions","csa_investor_alters","ncua_enforcements","occ_enforcements","ots_enforcement","ots_enforcement_orders","publicidad_liquidaciones","bangladesh_enforcement_archive","asic_banning_alerts","uk_liquidations","enheter_sokeresultat","uk_disqualified_directors","newzealand_insolvancy","pcaob_enforcement_actions","uk_admin_proceedings","newzealand_insolvent_company","asic_infringement_notices","brreg_bankruptcies","fma_media_releases","new_zealand_insolvency","nz_removed_individuals_ceased","nfra_orders","penalties_exportoffice_india","maharera_complaints","maharera_promoter_complaints","chat_conversations","action_exports_office_india","cpcb_ngt_orders","cpcb_directions","dgft_adjudication_orders","ACRA_GOV_insolvant","chat_messages","index_calculations","sql_query_history","saved_queries","bregg_insolvants","Indian_electoral_bondholders","uk_tax_defaulters","sc_administrative_actions","superfinanciera_ordenes_suspension","banned _list_uapa","iomfsa_public_warnings","Actiontaken_inspections_nse","ani_declarations","superfinanciera_actions","epa_civil_cleanup_cases","sc_criminal_prosecution","alsu_bankruptcies","complaints_against_listed_nse","delisted_under_liquidations_nse","crip_withdrawn","Companies_IBC_Moratorium_Debt","crip_nse_cases","nse_under_liquidations","nse_actions","nse_Non_Compliant_MPS","NSE_List_SDD","nse_Non-compliant_Promoter freezing","Archive SEBI DEBARRED entities","SEBI_DEACTIVATED","Defaulting_Client_Database nse_","nasdaq_disciplinary_actions","ftc_cases","jpx_disciplinary_actions","finra_individuals_barred","finra_cases","fina_Actions Resulting from Referral","finra_adjudication_decisions"
]

st.success(f"✅ Ready to export **{len(tables)}** tables.")

if st.button("Export ALL tables as CSV ZIP"):
    with st.spinner("Downloading tables – this can take a few minutes…"):
        with NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
            with zipfile.ZipFile(tmp_zip, "w") as zf:
                for tbl in tables:
                    st.info(f"📥 Fetching **{tbl}** …")
                    try:
                        df = fetch_table_paginated(tbl)
                        if df.empty:
                            st.warning(f"⚠️ `{tbl}` appears empty.")
                        csv_bytes = df.to_csv(index=False).encode("utf-8")
                        zf.writestr(f"{tbl}.csv", csv_bytes)
                        st.success(f"✅ `{tbl}` ({len(df)} rows) added.")
                    except Exception as e:
                        st.warning(f"❌ Failed to fetch `{tbl}`: {e}")
            tmp_zip.flush()
            tmp_zip.seek(0)
            with open(tmp_zip.name, "rb") as f:
                st.download_button(
                    label="⬇️ Download ZIP (CSV export)",
                    data=f,
                    file_name="supabase_export.zip",
                    mime="application/zip",
                )
    st.success("✅ Export ready!")

st.info(
    """
- **Read‑only**: The app only issues GET requests.
- **RLS**: You only receive rows the client can read.
- **No external services** – everything runs locally in the Streamlit process.
"""
)
