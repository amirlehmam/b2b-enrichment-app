"""
Pipeline runner that integrates the existing backend with Streamlit.
"""
import sys
import os
import io
from datetime import datetime
from contextlib import contextmanager
import streamlit as st

# Add project root to path for imports
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from main import (
    run_step_1_pappers,
    run_step_2_linkedin,
    run_step_3_save_companies,
    run_step_4_phantombuster,
    run_step_5_filter_decision_makers,
    run_step_6_enrich_contacts,
    run_step_7_export,
)
from services.csv_export import read_companies_csv

from .state_manager import (
    StepStatus,
    update_step_state,
    add_step_log,
    get_step_state,
)


class OutputCapture:
    """Captures print output and forwards to session state logs."""

    def __init__(self, step: int):
        self.step = step
        self.buffer = io.StringIO()
        self._original_stdout = None
        self._original_stderr = None

    def write(self, text):
        if text.strip():
            add_step_log(self.step, text.strip())
        self.buffer.write(text)

    def flush(self):
        self.buffer.flush()

    def __enter__(self):
        self._original_stdout = sys.stdout
        self._original_stderr = sys.stderr
        sys.stdout = self
        sys.stderr = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout = self._original_stdout
        sys.stderr = self._original_stderr
        return False


def check_stop_requested() -> bool:
    """Check if stop was requested."""
    return st.session_state.get("stop_requested", False)


def run_step_with_capture(step: int, func, *args, **kwargs):
    """Run a step function with output capture and state management."""

    # Check for stop request
    if check_stop_requested():
        update_step_state(step, status=StepStatus.FAILED, error_message="Arrete par l'utilisateur")
        return None

    # Update state to running
    st.session_state.current_step = step
    update_step_state(
        step,
        status=StepStatus.RUNNING,
        started_at=datetime.now().isoformat(),
        error_message=None
    )

    try:
        with OutputCapture(step):
            result = func(*args, **kwargs)

        update_step_state(
            step,
            status=StepStatus.COMPLETED,
            completed_at=datetime.now().isoformat()
        )
        return result

    except Exception as e:
        update_step_state(
            step,
            status=StepStatus.FAILED,
            error_message=str(e),
            completed_at=datetime.now().isoformat()
        )
        add_step_log(step, f"ERREUR: {str(e)}")
        raise


def execute_step_1(max_companies: int = None):
    """Execute Step 1: Pappers - Get companies."""
    companies = run_step_with_capture(1, run_step_1_pappers, max_companies)

    if companies:
        st.session_state.companies = companies
        update_step_state(1, result_count=len(companies))

    return companies


def execute_step_2():
    """Execute Step 2: Enrich CRM - Get LinkedIn URLs."""
    companies = st.session_state.companies

    if not companies:
        raise ValueError("Pas de donnees entreprises. Executez l'etape 1 d'abord.")

    enriched = run_step_with_capture(2, run_step_2_linkedin, companies)

    if enriched:
        st.session_state.companies = enriched
        with_linkedin = len([c for c in enriched if c.get("linkedin_url")])
        update_step_state(2, result_count=with_linkedin)

    return enriched


def execute_step_3():
    """Execute Step 3: Save companies to CSV."""
    companies = st.session_state.companies

    if not companies:
        raise ValueError("Pas de donnees entreprises. Executez les etapes 1-2 d'abord.")

    filepath = run_step_with_capture(3, run_step_3_save_companies, companies)

    if filepath:
        st.session_state.companies_csv_path = filepath
        update_step_state(3, result_count=len(companies))

    return filepath


def execute_step_4():
    """Execute Step 4: Phantombuster - Extract employees."""
    companies = st.session_state.companies

    if not companies:
        raise ValueError("Pas de donnees entreprises.")

    company_employees = run_step_with_capture(4, run_step_4_phantombuster, companies)

    if company_employees:
        st.session_state.company_employees = company_employees
        update_step_state(4, result_count=len(company_employees))

    return company_employees


def execute_step_5():
    """Execute Step 5: Claude Filter - Identify decision makers."""
    company_employees = st.session_state.company_employees

    if not company_employees:
        raise ValueError("Pas de donnees employes. Executez l'etape 4 d'abord.")

    decision_makers = run_step_with_capture(5, run_step_5_filter_decision_makers, company_employees)

    if decision_makers:
        st.session_state.decision_makers = decision_makers
        update_step_state(5, result_count=len(decision_makers))

    return decision_makers


def execute_step_6():
    """Execute Step 6: Captely - Enrich contacts."""
    decision_makers = st.session_state.decision_makers

    if not decision_makers:
        raise ValueError("Pas de decideurs. Executez les etapes 4-5 d'abord.")

    enriched = run_step_with_capture(6, run_step_6_enrich_contacts, decision_makers)

    if enriched:
        st.session_state.enriched_contacts = enriched
        with_email = len([c for c in enriched if c.get("email")])
        update_step_state(6, result_count=with_email)

    return enriched


def execute_step_7():
    """Execute Step 7: Export final CSV."""
    contacts = st.session_state.enriched_contacts

    if not contacts:
        raise ValueError("Pas de contacts enrichis. Executez l'etape 6 d'abord.")

    filepath = run_step_with_capture(7, run_step_7_export, contacts)

    if filepath:
        st.session_state.contacts_csv_path = filepath
        update_step_state(7, result_count=len(contacts))

    return filepath


def use_pappers_leaders():
    """Use Pappers leaders when skipping Phantombuster (steps 4-5)."""
    companies = st.session_state.companies

    # Mark step 4 as skipped
    update_step_state(4, status=StepStatus.SKIPPED)

    # Extract decision makers from Pappers dirigeants
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

    # Mark step 5 as completed (we got decision makers from Pappers)
    update_step_state(
        5,
        status=StepStatus.COMPLETED,
        result_count=len(all_decision_makers),
        completed_at=datetime.now().isoformat()
    )
    add_step_log(5, f"Mode simplifie: {len(all_decision_makers)} dirigeants extraits de Pappers")

    return all_decision_makers


def run_full_pipeline(max_companies: int = None, skip_phantombuster: bool = False):
    """
    Run the complete pipeline.

    Returns:
        bool: True if completed successfully, False if stopped/failed
    """
    st.session_state.is_running = True
    st.session_state.pipeline_started_at = datetime.now().isoformat()

    try:
        # Step 1: Pappers
        companies = execute_step_1(max_companies)
        if not companies or check_stop_requested():
            return False

        # Step 2: LinkedIn URLs
        companies = execute_step_2()
        if not companies or check_stop_requested():
            return False

        # Step 3: Save Companies
        execute_step_3()
        if check_stop_requested():
            return False

        # Steps 4-5: Phantombuster & Claude Filter (or skip)
        if skip_phantombuster:
            use_pappers_leaders()
        else:
            # Step 4: Phantombuster
            company_employees = execute_step_4()
            if not company_employees or check_stop_requested():
                return False

            # Step 5: Claude Filter
            decision_makers = execute_step_5()
            if not decision_makers or check_stop_requested():
                return False

        # Step 6: Captely
        enriched = execute_step_6()
        if not enriched or check_stop_requested():
            return False

        # Step 7: Export
        execute_step_7()

        st.session_state.pipeline_completed_at = datetime.now().isoformat()
        return True

    except Exception as e:
        return False

    finally:
        st.session_state.is_running = False


def run_single_step(step: int, max_companies: int = None) -> bool:
    """
    Run a single pipeline step.

    Returns:
        bool: True if successful, False if failed
    """
    st.session_state.is_running = True

    try:
        step_functions = {
            1: lambda: execute_step_1(max_companies),
            2: execute_step_2,
            3: execute_step_3,
            4: execute_step_4,
            5: execute_step_5,
            6: execute_step_6,
            7: execute_step_7,
        }

        if step not in step_functions:
            raise ValueError(f"Etape invalide: {step}")

        result = step_functions[step]()
        return result is not None

    except Exception as e:
        st.error(f"Erreur etape {step}: {str(e)}")
        return False

    finally:
        st.session_state.is_running = False


def load_companies_from_csv(filepath: str):
    """Load companies from an existing CSV file."""
    try:
        companies = read_companies_csv(filepath)
        st.session_state.companies = companies

        # Mark steps 1-3 as completed since we have company data
        for step in [1, 2, 3]:
            update_step_state(
                step,
                status=StepStatus.COMPLETED,
                result_count=len(companies),
                completed_at=datetime.now().isoformat()
            )
            add_step_log(step, f"Charge depuis CSV: {len(companies)} entreprises")

        return companies

    except Exception as e:
        st.error(f"Erreur chargement CSV: {str(e)}")
        return None
