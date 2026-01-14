"""
B2B Lead Enrichment Workflow - Streamlit UI
"""
import sys
import os
import tempfile

# Add project root to path for imports (works locally and on Streamlit Cloud)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import streamlit as st

# Page configuration - MUST be first Streamlit command
st.set_page_config(
    page_title="B2B Lead Enrichment Workflow",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Now import components (after set_page_config)
from streamlit_app.core.state_manager import (
    initialize_session_state,
    reset_pipeline_state,
)
from streamlit_app.core.pipeline_runner import (
    run_full_pipeline,
    run_single_step,
    load_companies_from_csv,
)
from streamlit_app.components.sidebar import render_sidebar
from streamlit_app.components.progress_tracker import render_progress_tracker, render_step_logs
from streamlit_app.components.data_tables import render_companies_table, render_contacts_table
from streamlit_app.components.download_buttons import render_download_section

# Initialize session state
initialize_session_state()


def handle_run_pipeline():
    """Handle running the full pipeline."""
    reset_pipeline_state()

    max_companies = st.session_state.get("max_companies", 10)
    skip_phantombuster = st.session_state.get("skip_phantombuster", False)

    success = run_full_pipeline(
        max_companies=max_companies,
        skip_phantombuster=skip_phantombuster
    )

    if success:
        st.success("Pipeline termine avec succes!")
        st.balloons()
    else:
        if st.session_state.get("stop_requested"):
            st.warning("Pipeline arrete par l'utilisateur.")
        else:
            st.error("Le pipeline a echoue. Consultez les logs pour plus de details.")


def handle_run_step(step: int):
    """Handle running a single step."""
    max_companies = st.session_state.get("max_companies", 10)

    success = run_single_step(step, max_companies=max_companies)

    if success:
        st.success(f"Etape {step} terminee!")
    else:
        st.error(f"Etape {step} echouee. Consultez les logs.")


def handle_load_csv(uploaded_file):
    """Handle loading companies from uploaded CSV."""
    try:
        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(uploaded_file.getvalue())
            tmp_path = tmp.name

        # Load companies
        companies = load_companies_from_csv(tmp_path)

        # Clean up temp file
        os.unlink(tmp_path)

        if companies:
            st.success(f"Charge {len(companies)} entreprises depuis le CSV")
        else:
            st.error("Erreur lors du chargement du CSV")

    except Exception as e:
        st.error(f"Erreur: {str(e)}")


# ============================================
# MAIN APP
# ============================================

# Header
st.title("🎯 B2B Lead Enrichment Workflow")
st.markdown("Pipeline automatise pour la prospection et l'enrichissement de contacts B2B")

# Render sidebar and get actions
actions = render_sidebar()

# Handle sidebar actions
if actions["run_pipeline"]:
    handle_run_pipeline()
    st.rerun()

if actions["run_step"]:
    handle_run_step(actions["selected_step"])
    st.rerun()

if actions["load_csv"] and actions["uploaded_file"]:
    handle_load_csv(actions["uploaded_file"])
    st.rerun()

# Main content area with tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Pipeline",
    "🏢 Entreprises",
    "👤 Contacts",
    "📋 Logs"
])

with tab1:
    render_progress_tracker()
    st.divider()
    render_download_section()

with tab2:
    companies = st.session_state.get("companies", [])
    render_companies_table(companies)

with tab3:
    # Show enriched contacts if available, otherwise decision makers
    enriched = st.session_state.get("enriched_contacts", [])
    decision_makers = st.session_state.get("decision_makers", [])

    if enriched:
        render_contacts_table(enriched, "Contacts Enrichis")
    elif decision_makers:
        render_contacts_table(decision_makers, "Decideurs")
    else:
        st.info("Aucun contact disponible. Executez le pipeline pour extraire les contacts.")

with tab4:
    st.subheader("Logs d'execution")
    render_step_logs()

# Footer with stats
st.divider()

companies = st.session_state.get("companies", [])
decision_makers = st.session_state.get("decision_makers", [])
enriched = st.session_state.get("enriched_contacts", [])

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Entreprises", len(companies))

with col2:
    st.metric("Decideurs", len(decision_makers))

with col3:
    with_email = len([c for c in enriched if c.get("email")]) if enriched else 0
    st.metric("Avec Email", with_email)

with col4:
    with_phone = len([c for c in enriched if c.get("phone")]) if enriched else 0
    st.metric("Avec Telephone", with_phone)
