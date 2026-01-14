"""
Download buttons component for CSV exports.
"""
import streamlit as st
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any


def render_download_section():
    """Render the download section with CSV export buttons."""

    st.subheader("Telechargements")

    companies = st.session_state.get("companies", [])
    enriched_contacts = st.session_state.get("enriched_contacts", [])
    companies_csv_path = st.session_state.get("companies_csv_path")
    contacts_csv_path = st.session_state.get("contacts_csv_path")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**CSV Entreprises**")

        if companies:
            csv_data = create_companies_csv(companies)
            filename = f"entreprises_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            st.download_button(
                label=f"Telecharger ({len(companies)} lignes)",
                data=csv_data,
                file_name=filename,
                mime="text/csv",
                type="primary",
                key="download_companies"
            )

            if companies_csv_path:
                st.caption(f"Sauvegarde: `{companies_csv_path}`")
        else:
            st.button("Telecharger Entreprises", disabled=True, key="download_companies_disabled")
            st.caption("Aucune donnee disponible")

    with col2:
        st.markdown("**CSV Contacts Enrichis**")

        if enriched_contacts:
            csv_data = create_contacts_csv(enriched_contacts)
            filename = f"contacts_enrichis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            st.download_button(
                label=f"Telecharger ({len(enriched_contacts)} lignes)",
                data=csv_data,
                file_name=filename,
                mime="text/csv",
                type="primary",
                key="download_contacts"
            )

            if contacts_csv_path:
                st.caption(f"Sauvegarde: `{contacts_csv_path}`")
        else:
            st.button("Telecharger Contacts", disabled=True, key="download_contacts_disabled")
            st.caption("Aucune donnee disponible")


def create_companies_csv(companies: List[Dict[str, Any]]) -> bytes:
    """Create CSV bytes from companies data."""

    df = pd.DataFrame(companies)

    # Handle nested data (dirigeants)
    if "dirigeants" in df.columns:
        df["dirigeants"] = df["dirigeants"].apply(
            lambda x: "; ".join([
                f"{d.get('nom', '')} ({d.get('qualite', '')})"
                for d in x
            ]) if isinstance(x, list) else str(x) if x else ""
        )

    # Remove complex nested data
    columns_to_drop = ["enrich_data"]
    df = df.drop(columns=[c for c in columns_to_drop if c in df.columns], errors='ignore')

    return df.to_csv(index=False).encode("utf-8")


def create_contacts_csv(contacts: List[Dict[str, Any]]) -> bytes:
    """Create CSV bytes from contacts data (formatted for Emilia)."""

    export_rows = []

    for contact in contacts:
        full_name = contact.get("name", "")
        parts = full_name.split(" ", 1) if full_name else ["", ""]

        export_rows.append({
            "first_name": parts[0] if parts else "",
            "last_name": parts[1] if len(parts) > 1 else "",
            "full_name": full_name,
            "title": contact.get("title", ""),
            "company": contact.get("entreprise", ""),
            "linkedin_url": contact.get("linkedin_url", ""),
            "email": contact.get("email", ""),
            "phone": contact.get("phone", ""),
            "persona_type": contact.get("persona_type", ""),
            "siren": contact.get("siren", ""),
        })

    df = pd.DataFrame(export_rows)
    return df.to_csv(index=False).encode("utf-8")
