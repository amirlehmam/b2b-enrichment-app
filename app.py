"""
B2B Lead Enrichment Workflow - Streamlit UI
"""
import sys
import os

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import streamlit as st

st.set_page_config(
    page_title="B2B Lead Enrichment",
    page_icon="🎯",
    layout="wide",
)

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
)

import pandas as pd

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
    )

    reset_pipeline_state()
    log_container = st.container()

    with log_container:
        st.subheader("📋 Logs d'exécution")

        # STEP 1
        with st.spinner("Étape 1/7: Recherche Pappers..."):
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
        with st.spinner("Étape 2/7: LinkedIn URLs..."):
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
        with st.spinner("Étape 3/7: Sauvegarde CSV..."):
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
            # STEP 4 - Phantombuster (Optimized - Parallel Processing)
            st.info("🚀 **Étape 4:** Phantombuster - Mode parallèle activé (3 entreprises en simultané)")

            with st.spinner("Étape 4/7: Extraction LinkedIn (parallèle)..."):
                st.write("🔄 **Étape 4:** Lancement extraction LinkedIn...")
                update_step_state(4, status=StepStatus.RUNNING)

                try:
                    # Check if Phantombuster is properly configured
                    import config
                    if not config.PHANTOMBUSTER_API_KEY:
                        st.error("❌ PHANTOMBUSTER_API_KEY non configuré!")
                        update_step_state(4, status=StepStatus.FAILED, error_message="API key missing")
                        return False
                    if not config.PHANTOMBUSTER_AGENT_ID:
                        st.error("❌ PHANTOMBUSTER_AGENT_ID non configuré!")
                        st.info("💡 L'Agent ID se trouve dans l'URL de votre agent Phantombuster: phantombuster.com/agents/XXXXX")
                        update_step_state(4, status=StepStatus.FAILED, error_message="Agent ID missing")
                        return False

                    st.write(f"   ✓ API Key configurée")
                    st.write(f"   ✓ Agent ID: {config.PHANTOMBUSTER_AGENT_ID[:8]}...")

                    # Show which companies we're processing
                    companies_with_linkedin = [c for c in companies if c.get("linkedin_url")]
                    st.write(f"   → {len(companies_with_linkedin)} entreprises avec LinkedIn à traiter")

                    if not companies_with_linkedin:
                        st.warning("⚠️ Aucune entreprise avec URL LinkedIn - saut de Phantombuster")
                        update_step_state(4, status=StepStatus.SKIPPED)
                        company_employees = {}
                    else:
                        for i, company in enumerate(companies_with_linkedin):
                            st.write(f"   📍 [{i+1}/{len(companies_with_linkedin)}] {company['nom']}: {company['linkedin_url']}")

                        company_employees = run_step_4_phantombuster(companies)

                    st.session_state.company_employees = company_employees
                    update_step_state(4, status=StepStatus.COMPLETED, result_count=len(company_employees))
                    st.success(f"✅ Étape 4: {len(company_employees)} entreprises traitées")
                except Exception as e:
                    st.error(f"❌ Étape 4 ERREUR: {str(e)}")
                    st.error(f"   Détail: {type(e).__name__}")
                    import traceback
                    st.code(traceback.format_exc(), language="python")
                    update_step_state(4, status=StepStatus.FAILED, error_message=str(e))
                    return False

            # STEP 5 - Claude Filter
            with st.spinner("Étape 5/7: Filtrage Claude AI..."):
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
        with st.spinner("Étape 6/7: Enrichissement Captely..."):
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
        with st.spinner("Étape 7/7: Export final..."):
            st.write("🔄 **Étape 7:** Export CSV final...")
            try:
                filepath = run_step_7_export(enriched)
                update_step_state(7, status=StepStatus.COMPLETED, result_count=len(enriched))
                st.success(f"✅ Étape 7: Export terminé!")
            except Exception as e:
                st.error(f"❌ Étape 7 ERREUR: {str(e)}")
                update_step_state(7, status=StepStatus.FAILED, error_message=str(e))

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
    st.sidebar.warning("⚠️ Phantombuster = 2-5 min/entreprise!")

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

# Progress cards
st.subheader("📊 Progression")
cols = st.columns(7)
for i, step in enumerate(range(1, 8)):
    with cols[i]:
        state = get_step_state(step)
        status = get_step_status(step)
        icon = STATUS_ICONS.get(status, "⏳")
        color = STATUS_COLORS.get(status, "#6c757d")
        name = STEP_NAMES[step][0][:8]
        count = state.get("result_count", 0)

        st.markdown(f"""
        <div style="border:2px solid {color};border-radius:6px;padding:6px;text-align:center;background:{color}15;">
            <div style="font-size:1.5em;">{icon}</div>
            <div style="font-size:0.7em;"><b>{step}</b></div>
            <div style="font-size:0.6em;color:#666;">{count}</div>
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
