"""
B2B Lead Enrichment Workflow - Streamlit UI
"""
import sys
import os
from enum import Enum
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import streamlit as st

st.set_page_config(
    page_title="B2B Lead Enrichment",
    page_icon="🎯",
    layout="wide",
)

import pandas as pd

# ============================================
# STATE MANAGEMENT (inlined to avoid import issues)
# ============================================

class StepStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

STEP_NAMES = {
    1: ("Pappers", "Recherche entreprises"),
    2: ("Enrich CRM", "URLs LinkedIn"),
    3: ("Export CSV", "Sauvegarde entreprises"),
    4: ("Phantombuster", "Extraction employes"),
    5: ("Claude AI", "Filtrage decideurs"),
    6: ("Captely", "Enrichissement contacts"),
    7: ("Export", "CSV final"),
    8: ("Sheets", "Google Sheets sync"),
    9: ("Emelia", "Campagne LinkedIn"),
}

STATUS_ICONS = {
    StepStatus.PENDING: "⏳",
    StepStatus.RUNNING: "🔄",
    StepStatus.COMPLETED: "✅",
    StepStatus.FAILED: "❌",
    StepStatus.SKIPPED: "⏭️",
}

STATUS_COLORS = {
    StepStatus.PENDING: "#6c757d",
    StepStatus.RUNNING: "#0d6efd",
    StepStatus.COMPLETED: "#198754",
    StepStatus.FAILED: "#dc3545",
    StepStatus.SKIPPED: "#ffc107",
}

def create_initial_steps():
    return {
        i: {
            "status": StepStatus.PENDING.value,
            "result_count": 0,
            "error_message": None,
        }
        for i in range(1, 10)
    }

def initialize_session_state():
    defaults = {
        "max_companies": 10,
        "skip_phantombuster": True,
        "steps": create_initial_steps(),
        "companies": [],
        "company_employees": {},
        "decision_makers": [],
        "enriched_contacts": [],
    }
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

def reset_pipeline_state():
    st.session_state.steps = create_initial_steps()
    st.session_state.companies = []
    st.session_state.company_employees = {}
    st.session_state.decision_makers = []
    st.session_state.enriched_contacts = []

def get_step_state(step):
    return st.session_state.steps.get(step, {})

def get_step_status(step):
    step_data = get_step_state(step)
    status_value = step_data.get("status", StepStatus.PENDING.value)
    return StepStatus(status_value)

def update_step_state(step, **kwargs):
    if step not in st.session_state.steps:
        st.session_state.steps[step] = {"status": StepStatus.PENDING.value, "result_count": 0}
    for key, value in kwargs.items():
        if key == "status" and isinstance(value, StepStatus):
            st.session_state.steps[step][key] = value.value
        else:
            st.session_state.steps[step][key] = value

initialize_session_state()

# ============================================
# PIPELINE WITH DETAILED LOGGING
# ============================================

def run_pipeline_with_logs(max_companies, skip_phantombuster):
    """Run pipeline with detailed live logging."""

    from main import (
        run_step_1_pappers,
        run_step_2_linkedin,
        run_step_3_save_companies,
        run_step_4_phantombuster,
        run_step_5_filter_decision_makers,
        run_step_6_enrich_contacts,
        run_step_7_export,
        run_step_8_google_sheets,
        run_step_9_emelia,
    )

    reset_pipeline_state()
    log_container = st.container()

    with log_container:
        st.subheader("📋 Logs d'exécution")

        # STEP 1
        with st.spinner("Étape 1/9: Recherche Pappers..."):
            st.write("🔄 **Étape 1:** Appel API Pappers...")
            try:
                companies = run_step_1_pappers(max_companies)
                st.session_state.companies = companies or []
                update_step_state(1, status=StepStatus.COMPLETED, result_count=len(companies or []))
                st.success(f"✅ Étape 1: {len(companies or [])} entreprises trouvées")
            except Exception as e:
                st.error(f"❌ Étape 1 ERREUR: {str(e)}")
                update_step_state(1, status=StepStatus.FAILED, error_message=str(e))
                return False

        if not companies:
            st.warning("⚠️ Aucune entreprise trouvée. Arrêt.")
            return False

        # STEP 2
        with st.spinner("Étape 2/9: LinkedIn URLs..."):
            st.write("🔄 **Étape 2:** Enrichissement LinkedIn...")
            try:
                companies = run_step_2_linkedin(companies)
                st.session_state.companies = companies
                with_li = len([c for c in companies if c.get("linkedin_url")])
                update_step_state(2, status=StepStatus.COMPLETED, result_count=with_li)
                st.success(f"✅ Étape 2: {with_li}/{len(companies)} avec LinkedIn")
            except Exception as e:
                st.error(f"❌ Étape 2 ERREUR: {str(e)}")
                update_step_state(2, status=StepStatus.FAILED, error_message=str(e))
                return False

        # STEP 3
        with st.spinner("Étape 3/9: Sauvegarde CSV..."):
            st.write("🔄 **Étape 3:** Sauvegarde fichier...")
            try:
                filepath = run_step_3_save_companies(companies)
                update_step_state(3, status=StepStatus.COMPLETED, result_count=len(companies))
                st.success(f"✅ Étape 3: Sauvegardé")
            except Exception as e:
                st.error(f"❌ Étape 3 ERREUR: {str(e)}")
                update_step_state(3, status=StepStatus.FAILED, error_message=str(e))

        # STEPS 4-5
        if skip_phantombuster:
            st.info("⏭️ **Étape 4:** Phantombuster sauté (option cochée)")
            update_step_state(4, status=StepStatus.SKIPPED)

            st.write("🔄 **Étape 5:** Extraction dirigeants depuis Pappers...")
            all_decision_makers = []
            for company in companies:
                for dirigeant in company.get("dirigeants", []):
                    all_decision_makers.append({
                        "name": dirigeant.get("nom"),
                        "title": dirigeant.get("qualite"),
                        "entreprise": company["nom"],
                        "siren": company["siren"],
                        "persona_type": "Dirigeant",
                    })
            st.session_state.decision_makers = all_decision_makers
            update_step_state(5, status=StepStatus.COMPLETED, result_count=len(all_decision_makers))
            st.success(f"✅ Étape 5: {len(all_decision_makers)} dirigeants extraits")
        else:
            # STEP 4 - Phantombuster
            st.info("🚀 **Étape 4:** Phantombuster - Mode parallèle activé")

            with st.spinner("Étape 4/9: Extraction LinkedIn (parallèle)..."):
                st.write("🔄 **Étape 4:** Lancement extraction LinkedIn...")
                update_step_state(4, status=StepStatus.RUNNING)

                try:
                    import config
                    if not config.PHANTOMBUSTER_API_KEY:
                        st.error("❌ PHANTOMBUSTER_API_KEY non configuré!")
                        update_step_state(4, status=StepStatus.FAILED)
                        return False
                    if not config.PHANTOMBUSTER_AGENT_ID:
                        st.error("❌ PHANTOMBUSTER_AGENT_ID non configuré!")
                        update_step_state(4, status=StepStatus.FAILED)
                        return False

                    companies_with_linkedin = [c for c in companies if c.get("linkedin_url")]
                    st.write(f"   → {len(companies_with_linkedin)} entreprises avec LinkedIn")

                    if not companies_with_linkedin:
                        st.warning("⚠️ Aucune entreprise avec URL LinkedIn")
                        update_step_state(4, status=StepStatus.SKIPPED)
                        company_employees = {}
                    else:
                        company_employees = run_step_4_phantombuster(companies)

                    st.session_state.company_employees = company_employees
                    update_step_state(4, status=StepStatus.COMPLETED, result_count=len(company_employees))
                    st.success(f"✅ Étape 4: {len(company_employees)} entreprises traitées")
                except Exception as e:
                    st.error(f"❌ Étape 4 ERREUR: {str(e)}")
                    update_step_state(4, status=StepStatus.FAILED, error_message=str(e))
                    return False

            # STEP 5 - Claude Filter
            with st.spinner("Étape 5/9: Filtrage Claude AI..."):
                st.write("🔄 **Étape 5:** Filtrage décideurs avec Claude...")
                try:
                    company_employees = st.session_state.get("company_employees", {})
                    all_decision_makers = run_step_5_filter_decision_makers(company_employees)
                    st.session_state.decision_makers = all_decision_makers
                    update_step_state(5, status=StepStatus.COMPLETED, result_count=len(all_decision_makers))
                    st.success(f"✅ Étape 5: {len(all_decision_makers)} décideurs identifiés")
                except Exception as e:
                    st.error(f"❌ Étape 5 ERREUR: {str(e)}")
                    update_step_state(5, status=StepStatus.FAILED, error_message=str(e))
                    return False

        all_decision_makers = st.session_state.get("decision_makers", [])

        if not all_decision_makers:
            st.warning("⚠️ Aucun décideur trouvé. Arrêt.")
            return False

        # STEP 6 - Captely
        with st.spinner("Étape 6/9: Enrichissement Captely..."):
            st.write("🔄 **Étape 6:** Enrichissement emails/téléphones...")
            try:
                enriched = run_step_6_enrich_contacts(all_decision_makers)
                st.session_state.enriched_contacts = enriched
                with_email = len([c for c in enriched if c.get("email")])
                update_step_state(6, status=StepStatus.COMPLETED, result_count=with_email)
                st.success(f"✅ Étape 6: {with_email}/{len(enriched)} avec email")
            except Exception as e:
                st.error(f"❌ Étape 6 ERREUR: {str(e)}")
                update_step_state(6, status=StepStatus.FAILED, error_message=str(e))
                return False

        # STEP 7 - Export
        with st.spinner("Étape 7/9: Export final..."):
            st.write("🔄 **Étape 7:** Export CSV final...")
            try:
                filepath = run_step_7_export(enriched)
                update_step_state(7, status=StepStatus.COMPLETED, result_count=len(enriched))
                st.success(f"✅ Étape 7: Export terminé!")
            except Exception as e:
                st.error(f"❌ Étape 7 ERREUR: {str(e)}")
                update_step_state(7, status=StepStatus.FAILED, error_message=str(e))

        # STEP 8 - Google Sheets
        with st.spinner("Étape 8/9: Google Sheets..."):
            st.write("🔄 **Étape 8:** Synchronisation Google Sheets...")
            try:
                import config
                if config.GOOGLE_SHEETS_SPREADSHEET_ID and config.GOOGLE_SHEETS_CREDENTIALS:
                    sheets_result = run_step_8_google_sheets(companies, enriched)
                    update_step_state(8, status=StepStatus.COMPLETED, result_count=sheets_result.get("contacts", 0))
                    st.success(f"✅ Étape 8: {sheets_result.get('contacts', 0)} contacts sync")
                else:
                    st.info("⏭️ Google Sheets non configuré - étape sautée")
                    update_step_state(8, status=StepStatus.SKIPPED)
            except Exception as e:
                st.warning(f"⚠️ Étape 8: {str(e)}")
                update_step_state(8, status=StepStatus.SKIPPED)

        # STEP 9 - Emelia
        with st.spinner("Étape 9/9: Envoi Emelia..."):
            st.write("🔄 **Étape 9:** Envoi vers campagne Emelia...")
            try:
                import config
                if config.EMELIA_API_KEY and config.EMELIA_CAMPAIGN_ID:
                    emelia_result = run_step_9_emelia(enriched)
                    update_step_state(9, status=StepStatus.COMPLETED, result_count=emelia_result.get("success", 0))
                    st.success(f"✅ Étape 9: {emelia_result.get('success', 0)} contacts envoyés")
                else:
                    st.info("⏭️ Emelia non configuré - étape sautée")
                    update_step_state(9, status=StepStatus.SKIPPED)
            except Exception as e:
                st.warning(f"⚠️ Étape 9: {str(e)}")
                update_step_state(9, status=StepStatus.SKIPPED)

        st.balloons()
        st.success("🎉 **Pipeline terminé avec succès!**")
        return True


# ============================================
# API CONFIG
# ============================================

def get_api_config():
    try:
        import config
        return {
            "Pappers": bool(config.PAPPERS_API_KEY),
            "Phantombuster": bool(config.PHANTOMBUSTER_API_KEY and config.PHANTOMBUSTER_AGENT_ID),
            "Enrich CRM": bool(config.ENRICH_CRM_API_KEY),
            "Captely": bool(config.CAPTELY_API_KEY),
            "Claude AI": bool(config.CLAUDE_API_KEY),
            "Google Sheets": bool(config.GOOGLE_SHEETS_CREDENTIALS and config.GOOGLE_SHEETS_SPREADSHEET_ID),
            "Emelia": bool(config.EMELIA_API_KEY and config.EMELIA_CAMPAIGN_ID),
        }
    except:
        return {}

# ============================================
# SIDEBAR
# ============================================

st.sidebar.title("⚙️ Configuration")

max_companies = st.sidebar.number_input(
    "Nb entreprises", min_value=1, max_value=100,
    value=st.session_state.get("max_companies", 3), step=1,
    help="Commence petit (3-5) pour tester"
)
st.session_state.max_companies = max_companies

skip_phantombuster = st.sidebar.checkbox(
    "⚡ Mode rapide (sans Phantombuster)",
    value=st.session_state.get("skip_phantombuster", True),
    help="Utilise les dirigeants Pappers au lieu de scraper LinkedIn (recommandé)"
)
st.session_state.skip_phantombuster = skip_phantombuster

if not skip_phantombuster:
    st.sidebar.warning("⚠️ Phantombuster = lent!")

st.sidebar.divider()

run_btn = st.sidebar.button("🚀 Lancer Pipeline", type="primary", use_container_width=True)

st.sidebar.divider()
st.sidebar.subheader("📡 APIs")
for api, ok in get_api_config().items():
    st.sidebar.write(f"{'✅' if ok else '❌'} {api}")

# ============================================
# MAIN
# ============================================

st.title("🎯 B2B Lead Enrichment")

# Progress cards - 9 steps in 2 rows
st.subheader("📊 Progression")
row1 = st.columns(5)
row2 = st.columns(4)
all_cols = row1 + row2

for i, step in enumerate(range(1, 10)):
    with all_cols[i]:
        state = get_step_state(step)
        status = get_step_status(step)
        icon = STATUS_ICONS.get(status, "⏳")
        color = STATUS_COLORS.get(status, "#6c757d")
        name = STEP_NAMES[step][0][:6]
        count = state.get("result_count", 0)

        st.markdown(f"""
        <div style="border:2px solid {color};border-radius:6px;padding:4px;text-align:center;background:{color}15;">
            <div style="font-size:1.2em;">{icon}</div>
            <div style="font-size:0.65em;"><b>{name}</b></div>
            <div style="font-size:0.55em;color:#666;">{count}</div>
        </div>
        """, unsafe_allow_html=True)

st.divider()

# Execute if button clicked
if run_btn:
    run_pipeline_with_logs(max_companies, skip_phantombuster)

# Data display
tab1, tab2 = st.tabs(["🏢 Entreprises", "👤 Contacts"])

with tab1:
    companies = st.session_state.get("companies", [])
    if companies:
        df = pd.DataFrame(companies)
        show_cols = [c for c in ["nom", "siren", "effectif", "linkedin_url"] if c in df.columns]
        st.dataframe(df[show_cols], use_container_width=True)
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 CSV", csv, "entreprises.csv")
    else:
        st.info("Lancez le pipeline pour voir les données")

with tab2:
    contacts = st.session_state.get("enriched_contacts", []) or st.session_state.get("decision_makers", [])
    if contacts:
        df = pd.DataFrame(contacts)
        show_cols = [c for c in ["name", "title", "entreprise", "email", "phone"] if c in df.columns]
        st.dataframe(df[show_cols], use_container_width=True)
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 CSV", csv, "contacts.csv")
    else:
        st.info("Lancez le pipeline pour voir les contacts")

# Footer
st.divider()
c1, c2, c3 = st.columns(3)
c1.metric("Entreprises", len(st.session_state.get("companies", [])))
c2.metric("Décideurs", len(st.session_state.get("decision_makers", [])))
c3.metric("Avec Email", len([c for c in st.session_state.get("enriched_contacts", []) if c.get("email")]))
