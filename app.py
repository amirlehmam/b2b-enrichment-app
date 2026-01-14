"""
B2B Lead Enrichment Workflow - Streamlit UI
Entry point for Streamlit Cloud deployment.
"""
import sys
import os
import tempfile
import time

# Setup path
ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import streamlit as st

# Page configuration - MUST be first Streamlit command
st.set_page_config(
    page_title="B2B Lead Enrichment Workflow",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Import components after set_page_config
from streamlit_app.core.state_manager import (
    initialize_session_state,
    reset_pipeline_state,
    STEP_NAMES,
    get_step_state,
    get_step_status,
    get_completed_steps_count,
    StepStatus,
    STATUS_ICONS,
    STATUS_COLORS,
    update_step_state,
    add_step_log,
)

import pandas as pd
from datetime import datetime

# Initialize session state
initialize_session_state()

# ============================================
# PIPELINE EXECUTION WITH LIVE PROGRESS
# ============================================

def run_pipeline_with_progress(max_companies, skip_phantombuster):
    """Run pipeline with live status updates."""

    # Import here to avoid circular imports
    from main import (
        run_step_1_pappers,
        run_step_2_linkedin,
        run_step_3_save_companies,
        run_step_4_phantombuster,
        run_step_5_filter_decision_makers,
        run_step_6_enrich_contacts,
        run_step_7_export,
    )

    reset_pipeline_state()

    with st.status("🚀 Pipeline en cours d'exécution...", expanded=True) as status:

        # Step 1: Pappers
        st.write("**Étape 1/7:** Recherche entreprises (Pappers)...")
        update_step_state(1, status=StepStatus.RUNNING)
        try:
            companies = run_step_1_pappers(max_companies)
            st.session_state.companies = companies or []
            update_step_state(1, status=StepStatus.COMPLETED, result_count=len(companies or []))
            st.write(f"✅ {len(companies or [])} entreprises trouvées")
        except Exception as e:
            update_step_state(1, status=StepStatus.FAILED, error_message=str(e))
            st.error(f"❌ Erreur: {e}")
            status.update(label="❌ Pipeline échoué", state="error")
            return False

        if not companies:
            status.update(label="⚠️ Aucune entreprise trouvée", state="error")
            return False

        # Step 2: LinkedIn URLs
        st.write("**Étape 2/7:** Récupération URLs LinkedIn...")
        update_step_state(2, status=StepStatus.RUNNING)
        try:
            companies = run_step_2_linkedin(companies)
            st.session_state.companies = companies
            with_linkedin = len([c for c in companies if c.get("linkedin_url")])
            update_step_state(2, status=StepStatus.COMPLETED, result_count=with_linkedin)
            st.write(f"✅ {with_linkedin}/{len(companies)} avec LinkedIn")
        except Exception as e:
            update_step_state(2, status=StepStatus.FAILED, error_message=str(e))
            st.error(f"❌ Erreur: {e}")
            status.update(label="❌ Pipeline échoué", state="error")
            return False

        # Step 3: Save CSV
        st.write("**Étape 3/7:** Sauvegarde CSV...")
        update_step_state(3, status=StepStatus.RUNNING)
        try:
            filepath = run_step_3_save_companies(companies)
            st.session_state.companies_csv_path = filepath
            update_step_state(3, status=StepStatus.COMPLETED, result_count=len(companies))
            st.write(f"✅ Sauvegardé: {filepath}")
        except Exception as e:
            update_step_state(3, status=StepStatus.FAILED, error_message=str(e))
            st.error(f"❌ Erreur: {e}")

        # Steps 4-5: Phantombuster or Skip
        if skip_phantombuster:
            st.write("**Étape 4/7:** ⏭️ Phantombuster sauté")
            update_step_state(4, status=StepStatus.SKIPPED)

            st.write("**Étape 5/7:** Extraction dirigeants Pappers...")
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
            st.write(f"✅ {len(all_decision_makers)} dirigeants extraits")
        else:
            # Step 4: Phantombuster
            st.write("**Étape 4/7:** Extraction employés LinkedIn (Phantombuster)...")
            update_step_state(4, status=StepStatus.RUNNING)
            try:
                company_employees = run_step_4_phantombuster(companies)
                st.session_state.company_employees = company_employees
                update_step_state(4, status=StepStatus.COMPLETED, result_count=len(company_employees))
                st.write(f"✅ {len(company_employees)} entreprises traitées")
            except Exception as e:
                update_step_state(4, status=StepStatus.FAILED, error_message=str(e))
                st.error(f"❌ Erreur: {e}")
                status.update(label="❌ Pipeline échoué", state="error")
                return False

            # Step 5: Claude Filter
            st.write("**Étape 5/7:** Filtrage décideurs (Claude AI)...")
            update_step_state(5, status=StepStatus.RUNNING)
            try:
                all_decision_makers = run_step_5_filter_decision_makers(company_employees)
                st.session_state.decision_makers = all_decision_makers
                update_step_state(5, status=StepStatus.COMPLETED, result_count=len(all_decision_makers))
                st.write(f"✅ {len(all_decision_makers)} décideurs identifiés")
            except Exception as e:
                update_step_state(5, status=StepStatus.FAILED, error_message=str(e))
                st.error(f"❌ Erreur: {e}")
                status.update(label="❌ Pipeline échoué", state="error")
                return False

        all_decision_makers = st.session_state.decision_makers

        if not all_decision_makers:
            status.update(label="⚠️ Aucun décideur trouvé", state="error")
            return False

        # Step 6: Captely
        st.write("**Étape 6/7:** Enrichissement contacts (Captely)...")
        update_step_state(6, status=StepStatus.RUNNING)
        try:
            enriched = run_step_6_enrich_contacts(all_decision_makers)
            st.session_state.enriched_contacts = enriched
            with_email = len([c for c in enriched if c.get("email")])
            update_step_state(6, status=StepStatus.COMPLETED, result_count=with_email)
            st.write(f"✅ {with_email}/{len(enriched)} avec email")
        except Exception as e:
            update_step_state(6, status=StepStatus.FAILED, error_message=str(e))
            st.error(f"❌ Erreur: {e}")
            status.update(label="❌ Pipeline échoué", state="error")
            return False

        # Step 7: Export
        st.write("**Étape 7/7:** Export final CSV...")
        update_step_state(7, status=StepStatus.RUNNING)
        try:
            filepath = run_step_7_export(enriched)
            st.session_state.contacts_csv_path = filepath
            update_step_state(7, status=StepStatus.COMPLETED, result_count=len(enriched))
            st.write(f"✅ Export: {filepath}")
        except Exception as e:
            update_step_state(7, status=StepStatus.FAILED, error_message=str(e))
            st.error(f"❌ Erreur: {e}")

        status.update(label="✅ Pipeline terminé avec succès!", state="complete")
        return True


def run_single_step_with_progress(step, max_companies):
    """Run a single step with progress."""
    from main import (
        run_step_1_pappers,
        run_step_2_linkedin,
        run_step_3_save_companies,
        run_step_4_phantombuster,
        run_step_5_filter_decision_makers,
        run_step_6_enrich_contacts,
        run_step_7_export,
    )

    with st.status(f"🔄 Exécution étape {step}...", expanded=True) as status:
        try:
            if step == 1:
                companies = run_step_1_pappers(max_companies)
                st.session_state.companies = companies or []
                update_step_state(1, status=StepStatus.COMPLETED, result_count=len(companies or []))
                st.write(f"✅ {len(companies or [])} entreprises")

            elif step == 2:
                companies = st.session_state.get("companies", [])
                if not companies:
                    st.error("❌ Pas de données. Exécutez l'étape 1 d'abord.")
                    return False
                enriched = run_step_2_linkedin(companies)
                st.session_state.companies = enriched
                update_step_state(2, status=StepStatus.COMPLETED, result_count=len(enriched))
                st.write(f"✅ {len(enriched)} entreprises enrichies")

            elif step == 3:
                companies = st.session_state.get("companies", [])
                if not companies:
                    st.error("❌ Pas de données.")
                    return False
                filepath = run_step_3_save_companies(companies)
                update_step_state(3, status=StepStatus.COMPLETED, result_count=len(companies))
                st.write(f"✅ Sauvegardé")

            elif step == 4:
                companies = st.session_state.get("companies", [])
                if not companies:
                    st.error("❌ Pas de données.")
                    return False
                company_employees = run_step_4_phantombuster(companies)
                st.session_state.company_employees = company_employees
                update_step_state(4, status=StepStatus.COMPLETED, result_count=len(company_employees))
                st.write(f"✅ {len(company_employees)} entreprises traitées")

            elif step == 5:
                company_employees = st.session_state.get("company_employees", {})
                if not company_employees:
                    st.error("❌ Pas de données employés. Exécutez l'étape 4.")
                    return False
                decision_makers = run_step_5_filter_decision_makers(company_employees)
                st.session_state.decision_makers = decision_makers
                update_step_state(5, status=StepStatus.COMPLETED, result_count=len(decision_makers))
                st.write(f"✅ {len(decision_makers)} décideurs")

            elif step == 6:
                decision_makers = st.session_state.get("decision_makers", [])
                if not decision_makers:
                    st.error("❌ Pas de décideurs. Exécutez l'étape 5.")
                    return False
                enriched = run_step_6_enrich_contacts(decision_makers)
                st.session_state.enriched_contacts = enriched
                update_step_state(6, status=StepStatus.COMPLETED, result_count=len(enriched))
                st.write(f"✅ {len(enriched)} contacts enrichis")

            elif step == 7:
                contacts = st.session_state.get("enriched_contacts", [])
                if not contacts:
                    st.error("❌ Pas de contacts. Exécutez l'étape 6.")
                    return False
                filepath = run_step_7_export(contacts)
                update_step_state(7, status=StepStatus.COMPLETED, result_count=len(contacts))
                st.write(f"✅ Exporté")

            status.update(label=f"✅ Étape {step} terminée!", state="complete")
            return True

        except Exception as e:
            update_step_state(step, status=StepStatus.FAILED, error_message=str(e))
            st.error(f"❌ Erreur: {e}")
            status.update(label=f"❌ Étape {step} échouée", state="error")
            return False


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
        }
    except:
        return {}

# ============================================
# SIDEBAR
# ============================================

st.sidebar.title("⚙️ Configuration")

max_companies = st.sidebar.number_input(
    "Nombre max entreprises", min_value=1, max_value=1000,
    value=st.session_state.get("max_companies", 5), step=1
)
st.session_state.max_companies = max_companies

skip_phantombuster = st.sidebar.checkbox(
    "Sauter Phantombuster (utiliser dirigeants Pappers)",
    value=st.session_state.get("skip_phantombuster", True)
)
st.session_state.skip_phantombuster = skip_phantombuster

st.sidebar.divider()

# Run buttons
run_pipeline = st.sidebar.button("🚀 Lancer Pipeline Complet", type="primary", use_container_width=True)

st.sidebar.divider()
st.sidebar.subheader("Étape individuelle")

selected_step = st.sidebar.selectbox(
    "Choisir étape", options=list(STEP_NAMES.keys()),
    format_func=lambda x: f"{x}. {STEP_NAMES[x][0]}"
)
run_step = st.sidebar.button(f"▶️ Exécuter Étape {selected_step}", use_container_width=True)

st.sidebar.divider()
st.sidebar.subheader("📡 APIs")
for api, ok in get_api_config().items():
    st.sidebar.markdown(f"{'✅' if ok else '❌'} {api}")

# ============================================
# MAIN CONTENT
# ============================================

st.title("🎯 B2B Lead Enrichment")
st.caption("Pipeline automatisé de prospection B2B")

# Execute pipeline if button clicked
if run_pipeline:
    run_pipeline_with_progress(max_companies, skip_phantombuster)

if run_step:
    run_single_step_with_progress(selected_step, max_companies)

# Progress overview
st.subheader("📊 Progression")
completed = get_completed_steps_count()
st.progress(completed / 7, f"{completed}/7 étapes complétées")

# Step cards
cols = st.columns(4)
for i, step in enumerate(range(1, 8)):
    with cols[i % 4]:
        state = get_step_state(step)
        status = get_step_status(step)
        icon = STATUS_ICONS.get(status, "⏳")
        color = STATUS_COLORS.get(status, "#6c757d")
        name = STEP_NAMES[step][0]
        count = state.get("result_count", 0)

        st.markdown(f"""
        <div style="border:2px solid {color};border-radius:8px;padding:8px;margin:2px 0;background:{color}15;text-align:center;">
            <div style="font-size:1.2em;">{icon}</div>
            <div><b>{step}. {name}</b></div>
            <div style="font-size:0.8em;color:#666;">{count} résultats</div>
        </div>
        """, unsafe_allow_html=True)
    if i == 3:
        cols = st.columns(4)

st.divider()

# Data tabs
tab1, tab2 = st.tabs(["🏢 Entreprises", "👤 Contacts"])

with tab1:
    companies = st.session_state.get("companies", [])
    if companies:
        df = pd.DataFrame(companies)
        cols_to_show = [c for c in ["nom", "siren", "effectif", "linkedin_url"] if c in df.columns]
        st.dataframe(df[cols_to_show], use_container_width=True, height=300)

        # Download button
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Télécharger CSV Entreprises", csv, "entreprises.csv", "text/csv")
    else:
        st.info("Aucune entreprise. Lancez le pipeline.")

with tab2:
    contacts = st.session_state.get("enriched_contacts", []) or st.session_state.get("decision_makers", [])
    if contacts:
        df = pd.DataFrame(contacts)
        cols_to_show = [c for c in ["name", "title", "entreprise", "email", "phone", "persona_type"] if c in df.columns]
        st.dataframe(df[cols_to_show], use_container_width=True, height=300)

        # Download button
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Télécharger CSV Contacts", csv, "contacts.csv", "text/csv")
    else:
        st.info("Aucun contact. Lancez le pipeline.")

# Stats footer
st.divider()
c1, c2, c3, c4 = st.columns(4)
companies = st.session_state.get("companies", [])
contacts = st.session_state.get("enriched_contacts", [])
c1.metric("Entreprises", len(companies))
c2.metric("Décideurs", len(st.session_state.get("decision_makers", [])))
c3.metric("Avec Email", len([c for c in contacts if c.get("email")]))
c4.metric("Avec Tél", len([c for c in contacts if c.get("phone")]))
