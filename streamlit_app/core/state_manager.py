"""
Session state management for the Streamlit workflow UI.
"""
import streamlit as st
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum


class StepStatus(Enum):
    """Status of a pipeline step."""
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
    7: ("Export Final", "CSV pour Emilia"),
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


@dataclass
class StepState:
    """State for an individual pipeline step."""
    status: StepStatus = StepStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result_count: int = 0
    error_message: Optional[str] = None
    logs: List[str] = field(default_factory=list)


def create_initial_steps() -> Dict[int, dict]:
    """Create initial step states as dicts (for JSON serialization)."""
    return {
        i: {
            "status": StepStatus.PENDING.value,
            "started_at": None,
            "completed_at": None,
            "result_count": 0,
            "error_message": None,
            "logs": []
        }
        for i in range(1, 8)
    }


def initialize_session_state():
    """Initialize all session state variables."""

    defaults = {
        # Configuration
        "max_companies": 10,
        "skip_phantombuster": False,

        # Pipeline state
        "is_running": False,
        "current_step": 0,
        "stop_requested": False,
        "pipeline_started_at": None,
        "pipeline_completed_at": None,
        "steps": create_initial_steps(),

        # Workflow data
        "companies": [],
        "company_employees": {},
        "decision_makers": [],
        "enriched_contacts": [],
        "companies_csv_path": None,
        "contacts_csv_path": None,

        # UI state
        "uploaded_csv": None,
    }

    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value


def reset_pipeline_state():
    """Reset pipeline to initial state for a new run."""
    st.session_state.is_running = False
    st.session_state.current_step = 0
    st.session_state.stop_requested = False
    st.session_state.pipeline_started_at = None
    st.session_state.pipeline_completed_at = None
    st.session_state.steps = create_initial_steps()

    # Clear workflow data
    st.session_state.companies = []
    st.session_state.company_employees = {}
    st.session_state.decision_makers = []
    st.session_state.enriched_contacts = []
    st.session_state.companies_csv_path = None
    st.session_state.contacts_csv_path = None


def get_step_state(step: int) -> dict:
    """Get state for a specific step."""
    return st.session_state.steps.get(step, {})


def update_step_state(step: int, **kwargs):
    """Update state for a specific step."""
    if step not in st.session_state.steps:
        st.session_state.steps[step] = create_initial_steps()[step]

    for key, value in kwargs.items():
        if key == "status" and isinstance(value, StepStatus):
            st.session_state.steps[step][key] = value.value
        else:
            st.session_state.steps[step][key] = value


def add_step_log(step: int, message: str):
    """Add a log message to a step."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    if step in st.session_state.steps:
        st.session_state.steps[step]["logs"].append(f"[{timestamp}] {message}")


def get_completed_steps_count() -> int:
    """Get count of completed or skipped steps."""
    count = 0
    for step_data in st.session_state.steps.values():
        status = step_data.get("status", "")
        if status in [StepStatus.COMPLETED.value, StepStatus.SKIPPED.value]:
            count += 1
    return count


def get_step_status(step: int) -> StepStatus:
    """Get the status enum for a step."""
    step_data = get_step_state(step)
    status_value = step_data.get("status", StepStatus.PENDING.value)
    return StepStatus(status_value)
