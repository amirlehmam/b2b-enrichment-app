"""
B2B Lead Enrichment Workflow - Streamlit UI
Entry point for Streamlit Cloud deployment.
"""
import sys
import os
import tempfile

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
)
from streamlit_app.core.pipeline_runner import (
    run_full_pipeline,
    run_single_step,
    load_companies_from_csv,
)

import pandas as pd
from datetime import datetime

# Initialize session state
initialize_session_state()

# ============================================
# HELPER FUNCTIONS
# ============================================

def get_api_config():
    """Get API configuration."""
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

def handle_run_pipeline():
    reset_pipeline_state()
    max_companies = st.session_state.get("max_companies", 10)
    skip_phantombuster = st.session_state.get("skip_phantombuster", False)
    success = run_full_pipeline(max_companies=max_companies, skip_phantombuster=skip_phantombuster)
    if success:
        st.success("Pipeline termine avec succes!")
        st.balloons()
    else:
        if st.session_state.get("stop_requested"):
            st.warning("Pipeline arrete par l'utilisateur.")
        else:
            st.error("Le pipeline a echoue. Consultez les logs.")

def handle_run_step(step):
    max_companies = st.session_state.get("max_companies", 10)
    success = run_single_step(step, max_companies=max_companies)
    if success:
        st.success(f"Etape {step} terminee!")
    else:
        st.error(f"Etape {step} echouee.")

def handle_load_csv(uploaded_file):
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(uploaded_file.getvalue())
            tmp_path = tmp.name
        companies = load_companies_from_csv(tmp_path)
        os.unlink(tmp_path)
        if companies:
            st.success(f"Charge {len(companies)} entreprises depuis le CSV")
    except Exception as e:
        st.error(f"Erreur: {str(e)}")

# ============================================
# SIDEBAR
# ============================================

st.sidebar.title("Configuration")
st.sidebar.header("Parametres")

max_companies = st.sidebar.number_input(
    "Nombre max entreprises", min_value=1, max_value=1000,
    value=st.session_state.get("max_companies", 10), step=1
)
st.session_state.max_companies = max_companies

skip_phantombuster = st.sidebar.checkbox(
    "Sauter Phantombuster",
    value=st.session_state.get("skip_phantombuster", False)
)
st.session_state.skip_phantombuster = skip_phantombuster

st.sidebar.divider()
st.sidebar.header("Controle Pipeline")

is_running = st.session_state.get("is_running", False)
col1, col2 = st.sidebar.columns(2)

with col1:
    run_clicked = st.button("Lancer", type="primary", use_container_width=True, disabled=is_running)
with col2:
    stop_clicked = st.button("Stop", type="secondary", use_container_width=True, disabled=not is_running)

if stop_clicked:
    st.session_state.stop_requested = True

st.sidebar.divider()
st.sidebar.header("Etape individuelle")

selected_step = st.sidebar.selectbox(
    "Selectionner", options=list(STEP_NAMES.keys()),
    format_func=lambda x: f"{x}: {STEP_NAMES[x][0]}"
)
run_step_clicked = st.sidebar.button(f"Executer Etape {selected_step}", use_container_width=True, disabled=is_running)

st.sidebar.divider()
st.sidebar.header("Charger CSV")
uploaded_file = st.sidebar.file_uploader("CSV entreprises", type=["csv"])
load_csv_clicked = st.sidebar.button("Charger", use_container_width=True) if uploaded_file else False

st.sidebar.divider()
st.sidebar.header("APIs")
for api, ok in get_api_config().items():
    st.sidebar.markdown(f"{'✅' if ok else '❌'} {api}")

# Handle actions
if run_clicked:
    handle_run_pipeline()
    st.rerun()
if run_step_clicked:
    handle_run_step(selected_step)
    st.rerun()
if load_csv_clicked and uploaded_file:
    handle_load_csv(uploaded_file)
    st.rerun()

# ============================================
# MAIN CONTENT
# ============================================

st.title("🎯 B2B Lead Enrichment Workflow")
st.markdown("Pipeline automatise pour la prospection B2B")

# Progress
completed = get_completed_steps_count()
st.progress(completed / 7, f"{completed}/7 etapes")

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
        <div style="border:2px solid {color};border-radius:8px;padding:10px;margin:4px 0;background:{color}15;min-height:80px;">
            <b>{icon} {step}. {name}</b><br/>
            <small>{count} resultats</small>
        </div>
        """, unsafe_allow_html=True)
    if i == 3:
        cols = st.columns(4)

st.divider()

# Tabs
tab1, tab2, tab3 = st.tabs(["🏢 Entreprises", "👤 Contacts", "📋 Logs"])

with tab1:
    companies = st.session_state.get("companies", [])
    if companies:
        df = pd.DataFrame(companies)
        display_cols = [c for c in ["nom", "siren", "effectif", "linkedin_url"] if c in df.columns]
        st.dataframe(df[display_cols], use_container_width=True, height=400)
        st.caption(f"{len(companies)} entreprises")
    else:
        st.info("Aucune entreprise. Lancez le pipeline.")

with tab2:
    contacts = st.session_state.get("enriched_contacts", []) or st.session_state.get("decision_makers", [])
    if contacts:
        df = pd.DataFrame(contacts)
        display_cols = [c for c in ["name", "title", "entreprise", "email", "phone"] if c in df.columns]
        st.dataframe(df[display_cols], use_container_width=True, height=400)
        st.caption(f"{len(contacts)} contacts")
    else:
        st.info("Aucun contact. Lancez le pipeline.")

with tab3:
    for step in range(1, 8):
        state = get_step_state(step)
        logs = state.get("logs", [])
        if logs:
            with st.expander(f"Etape {step}: {STEP_NAMES[step][0]} ({len(logs)} logs)"):
                st.code("\n".join(logs))

# Footer stats
st.divider()
c1, c2, c3, c4 = st.columns(4)
companies = st.session_state.get("companies", [])
contacts = st.session_state.get("enriched_contacts", [])
c1.metric("Entreprises", len(companies))
c2.metric("Decideurs", len(st.session_state.get("decision_makers", [])))
c3.metric("Avec Email", len([c for c in contacts if c.get("email")]))
c4.metric("Avec Tel", len([c for c in contacts if c.get("phone")]))
