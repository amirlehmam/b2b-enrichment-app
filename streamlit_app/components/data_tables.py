"""
Data table components for displaying companies and contacts.
"""
import streamlit as st
import pandas as pd
from typing import List, Dict, Any


def render_companies_table(companies: List[Dict[str, Any]]):
    """Render the companies data table."""

    if not companies:
        st.info("Aucune donnee entreprise. Executez l'etape 1 pour recuperer les entreprises.")
        return

    st.subheader(f"Entreprises ({len(companies)})")

    # Convert to DataFrame
    df = pd.DataFrame(companies)

    # Select and reorder columns for display
    display_columns = [
        "nom", "siren", "forme_juridique", "effectif",
        "adresse", "activite", "linkedin_url"
    ]

    # Filter to existing columns
    available_cols = [c for c in display_columns if c in df.columns]
    df_display = df[available_cols].copy()

    # Rename columns for display
    column_names = {
        "nom": "Nom Entreprise",
        "siren": "SIREN",
        "forme_juridique": "Forme Juridique",
        "effectif": "Effectif",
        "adresse": "Adresse",
        "activite": "Activite",
        "linkedin_url": "URL LinkedIn"
    }
    df_display.rename(columns={k: v for k, v in column_names.items() if k in df_display.columns}, inplace=True)

    # Filters
    col1, col2 = st.columns(2)

    with col1:
        search = st.text_input(
            "Rechercher par nom",
            key="company_search",
            placeholder="Entrez le nom de l'entreprise..."
        )

    with col2:
        has_linkedin = st.checkbox("Uniquement avec LinkedIn", key="filter_linkedin")

    # Apply filters
    if search:
        mask = df_display["Nom Entreprise"].str.contains(search, case=False, na=False)
        df_display = df_display[mask]

    if has_linkedin and "URL LinkedIn" in df_display.columns:
        df_display = df_display[
            df_display["URL LinkedIn"].notna() &
            (df_display["URL LinkedIn"] != "")
        ]

    # Display dataframe
    st.dataframe(
        df_display,
        use_container_width=True,
        height=400,
        column_config={
            "URL LinkedIn": st.column_config.LinkColumn("URL LinkedIn", display_text="Ouvrir")
        }
    )

    # Summary stats
    total = len(companies)
    with_linkedin = len([c for c in companies if c.get("linkedin_url")])
    pct = (100 * with_linkedin // total) if total > 0 else 0

    st.caption(f"Total: {total} entreprises | Avec LinkedIn: {with_linkedin} ({pct}%)")


def render_contacts_table(contacts: List[Dict[str, Any]], title: str = "Contacts"):
    """Render the contacts/decision makers table."""

    if not contacts:
        st.info("Aucune donnee contacts disponible.")
        return

    st.subheader(f"{title} ({len(contacts)})")

    df = pd.DataFrame(contacts)

    # Select columns for display
    display_columns = [
        "name", "title", "entreprise", "persona_type",
        "linkedin_url", "email", "phone"
    ]

    available_cols = [c for c in display_columns if c in df.columns]
    df_display = df[available_cols].copy()

    # Rename columns
    column_names = {
        "name": "Nom",
        "title": "Titre",
        "entreprise": "Entreprise",
        "persona_type": "Persona",
        "linkedin_url": "LinkedIn",
        "email": "Email",
        "phone": "Telephone"
    }
    df_display.rename(columns={k: v for k, v in column_names.items() if k in df_display.columns}, inplace=True)

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        personas_available = df_display["Persona"].unique().tolist() if "Persona" in df_display.columns else []
        persona_filter = st.multiselect(
            "Filtrer par Persona",
            options=personas_available,
            key="persona_filter"
        )

    with col2:
        has_email = st.checkbox("Avec Email", key="filter_email")

    with col3:
        has_phone = st.checkbox("Avec Telephone", key="filter_phone")

    # Apply filters
    if persona_filter:
        df_display = df_display[df_display["Persona"].isin(persona_filter)]

    if has_email and "Email" in df_display.columns:
        df_display = df_display[
            df_display["Email"].notna() &
            (df_display["Email"] != "")
        ]

    if has_phone and "Telephone" in df_display.columns:
        df_display = df_display[
            df_display["Telephone"].notna() &
            (df_display["Telephone"] != "")
        ]

    # Display
    column_config = {}
    if "LinkedIn" in df_display.columns:
        column_config["LinkedIn"] = st.column_config.LinkColumn("LinkedIn", display_text="Profil")

    st.dataframe(
        df_display,
        use_container_width=True,
        height=400,
        column_config=column_config
    )

    # Stats
    total = len(contacts)
    with_email = len([c for c in contacts if c.get("email")])
    with_phone = len([c for c in contacts if c.get("phone")])

    st.caption(f"Total: {total} | Avec Email: {with_email} | Avec Telephone: {with_phone}")
