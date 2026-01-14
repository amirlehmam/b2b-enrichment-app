"""
Sidebar component with configuration and controls.
"""
import streamlit as st
import sys
import os

# Add project root to path for imports
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from streamlit_app.core.state_manager import STEP_NAMES


def get_api_config():
    """Get API configuration (lazy load to avoid import issues)."""
    import config
    return {
        "Pappers": bool(config.PAPPERS_API_KEY),
        "Phantombuster": bool(config.PHANTOMBUSTER_API_KEY and config.PHANTOMBUSTER_AGENT_ID),
        "Enrich CRM": bool(config.ENRICH_CRM_API_KEY),
        "Captely": bool(config.CAPTELY_API_KEY),
        "Claude AI": bool(config.CLAUDE_API_KEY),
    }


def render_sidebar():
    """Render the sidebar with configuration options and controls."""

    st.sidebar.title("Configuration")

    # === SECTION 1: Pipeline Parameters ===
    st.sidebar.header("Parametres")

    max_companies = st.sidebar.number_input(
        "Nombre max entreprises",
        min_value=1,
        max_value=1000,
        value=st.session_state.get("max_companies", 10),
        step=1,
        help="Limite le nombre d'entreprises a recuperer"
    )
    st.session_state.max_companies = max_companies

    skip_phantombuster = st.sidebar.checkbox(
        "Sauter Phantombuster",
        value=st.session_state.get("skip_phantombuster", False),
        help="Utilise les dirigeants Pappers au lieu de l'extraction LinkedIn"
    )
    st.session_state.skip_phantombuster = skip_phantombuster

    st.sidebar.divider()

    # === SECTION 2: Pipeline Control ===
    st.sidebar.header("Controle Pipeline")

    is_running = st.session_state.get("is_running", False)

    col1, col2 = st.sidebar.columns(2)

    with col1:
        run_clicked = st.button(
            "Lancer Pipeline",
            type="primary",
            use_container_width=True,
            disabled=is_running,
            key="btn_run_pipeline"
        )

    with col2:
        stop_clicked = st.button(
            "Arreter",
            type="secondary",
            use_container_width=True,
            disabled=not is_running,
            key="btn_stop"
        )

    if stop_clicked:
        st.session_state.stop_requested = True

    st.sidebar.divider()

    # === SECTION 3: Individual Step Execution ===
    st.sidebar.header("Executer une etape")

    selected_step = st.sidebar.selectbox(
        "Selectionner etape",
        options=list(STEP_NAMES.keys()),
        format_func=lambda x: f"Etape {x}: {STEP_NAMES[x][0]}",
        key="selected_step"
    )

    # Show step description
    step_name, step_desc = STEP_NAMES[selected_step]
    st.sidebar.caption(f"{step_desc}")

    # Show prerequisites
    prerequisites = {
        1: "Aucun prerequis",
        2: "Necessite: Donnees entreprises (Etape 1)",
        3: "Necessite: Donnees entreprises (Etapes 1-2)",
        4: "Necessite: URLs LinkedIn (Etapes 1-2)",
        5: "Necessite: Donnees employes (Etape 4)",
        6: "Necessite: Liste decideurs (Etapes 4-5)",
        7: "Necessite: Contacts enrichis (Etape 6)"
    }
    st.sidebar.caption(f"*{prerequisites[selected_step]}*")

    run_step_clicked = st.sidebar.button(
        f"Executer Etape {selected_step}",
        use_container_width=True,
        disabled=is_running,
        key="btn_run_step"
    )

    st.sidebar.divider()

    # === SECTION 4: Resume from CSV ===
    st.sidebar.header("Reprendre depuis CSV")

    uploaded_file = st.sidebar.file_uploader(
        "Charger CSV entreprises",
        type=["csv"],
        help="Reprendre le pipeline depuis un fichier CSV existant",
        key="csv_uploader"
    )

    load_csv_clicked = False
    if uploaded_file:
        st.session_state.uploaded_csv = uploaded_file
        load_csv_clicked = st.sidebar.button(
            "Charger le fichier",
            use_container_width=True,
            key="btn_load_csv"
        )

    st.sidebar.divider()

    # === SECTION 5: API Status ===
    st.sidebar.header("Statut APIs")
    render_api_status()

    # Return action flags
    return {
        "run_pipeline": run_clicked,
        "stop": stop_clicked,
        "run_step": run_step_clicked,
        "selected_step": selected_step,
        "load_csv": load_csv_clicked,
        "uploaded_file": uploaded_file,
    }


def render_api_status():
    """Show API configuration status indicators."""
    try:
        apis = get_api_config()
        for api_name, is_configured in apis.items():
            if is_configured:
                st.sidebar.markdown(f"✅ **{api_name}**: Configure")
            else:
                st.sidebar.markdown(f"❌ **{api_name}**: Manquant")
    except Exception as e:
        st.sidebar.warning(f"Erreur config: {e}")
