"""
Progress tracker component for visualizing pipeline execution.
"""
import streamlit as st

from ..core.state_manager import (
    STEP_NAMES,
    STATUS_ICONS,
    STATUS_COLORS,
    StepStatus,
    get_step_state,
    get_completed_steps_count,
    get_step_status,
)


def render_progress_tracker():
    """Render the pipeline progress tracker."""

    st.subheader("Progression du Pipeline")

    # Overall progress bar
    completed = get_completed_steps_count()
    progress_value = completed / 7

    st.progress(progress_value, f"{completed}/7 etapes completees")

    # Step cards in rows
    render_step_cards()


def render_step_cards():
    """Render step cards in a grid layout."""

    # First row: Steps 1-4
    cols1 = st.columns(4)
    for i, col in enumerate(cols1):
        step = i + 1
        with col:
            render_step_card(step)

    # Second row: Steps 5-7 (with empty 4th column)
    cols2 = st.columns(4)
    for i, col in enumerate(cols2[:3]):
        step = i + 5
        with col:
            render_step_card(step)


def render_step_card(step: int):
    """Render a single step card."""

    step_state = get_step_state(step)
    status = get_step_status(step)
    name, description = STEP_NAMES[step]

    icon = STATUS_ICONS.get(status, "⏳")
    color = STATUS_COLORS.get(status, "#6c757d")

    # Format result info
    result_count = step_state.get("result_count", 0)
    error_msg = step_state.get("error_message", "")

    if status == StepStatus.PENDING:
        result_text = "En attente..."
    elif status == StepStatus.RUNNING:
        result_text = "En cours..."
    elif status == StepStatus.SKIPPED:
        result_text = "Saute"
    elif status == StepStatus.FAILED:
        result_text = f"Erreur"
    else:
        result_text = f"{result_count} resultats"

    # Render card with custom styling
    st.markdown(f"""
    <div style="
        border: 2px solid {color};
        border-radius: 8px;
        padding: 12px;
        margin: 4px 0;
        background-color: {color}15;
        min-height: 120px;
    ">
        <div style="font-size: 0.9em; color: #666;">Etape {step}</div>
        <div style="font-size: 1.1em; font-weight: bold; color: {color}; margin: 4px 0;">
            {icon} {name}
        </div>
        <div style="font-size: 0.8em; color: #888; margin-bottom: 8px;">
            {description}
        </div>
        <div style="font-size: 0.9em; font-weight: 500;">
            {result_text}
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_step_logs(step: int = None):
    """Render logs for a specific step or all steps."""

    if step:
        _render_single_step_logs(step)
    else:
        for s in range(1, 8):
            _render_single_step_logs(s)


def _render_single_step_logs(step: int):
    """Render logs for a single step."""

    step_state = get_step_state(step)
    logs = step_state.get("logs", [])
    status = get_step_status(step)
    name, _ = STEP_NAMES[step]

    # Only show if there are logs or step has been run
    if not logs and status == StepStatus.PENDING:
        return

    icon = STATUS_ICONS.get(status, "⏳")
    expanded = status == StepStatus.RUNNING

    with st.expander(f"{icon} Etape {step}: {name} ({len(logs)} logs)", expanded=expanded):
        if logs:
            log_text = "\n".join(logs)
            st.code(log_text, language="text")
        else:
            st.caption("Aucun log disponible")

        # Show error if failed
        error_msg = step_state.get("error_message", "")
        if error_msg and status == StepStatus.FAILED:
            st.error(f"Erreur: {error_msg}")
