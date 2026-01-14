"""
Configuration pour le workflow d'enrichissement B2B

Supports both:
- Streamlit Cloud secrets (st.secrets)
- Environment variables
- Local defaults
"""
import os


def get_secret(key: str, default: str = "") -> str:
    """
    Get a secret value from Streamlit secrets or environment variables.
    Priority: st.secrets > os.environ > default
    """
    # Try Streamlit secrets first (for Streamlit Cloud)
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass

    # Fall back to environment variable
    return os.getenv(key, default)


# ============================================
# API KEYS
# ============================================

PAPPERS_API_KEY = get_secret("PAPPERS_API_KEY")
PHANTOMBUSTER_API_KEY = get_secret("PHANTOMBUSTER_API_KEY")
ENRICH_CRM_API_KEY = get_secret("ENRICH_CRM_API_KEY")
CAPTELY_API_KEY = get_secret("CAPTELY_API_KEY")
CLAUDE_API_KEY = get_secret("CLAUDE_API_KEY")
EMELIA_API_KEY = get_secret("EMELIA_API_KEY")

# ============================================
# API ENDPOINTS
# ============================================

PAPPERS_BASE_URL = "https://api.pappers.fr/v2"
PHANTOMBUSTER_BASE_URL = "https://api.phantombuster.com/api/v2"
ENRICH_CRM_BASE_URL = "https://gateway.enrich-crm.com/api/ingress/v4"
CAPTELY_BASE_URL = "https://app.captely.com/api"
CLAUDE_BASE_URL = "https://api.anthropic.com/v1"

# ============================================
# PHANTOMBUSTER CONFIG
# ============================================

PHANTOMBUSTER_AGENT_ID = get_secret("PHANTOMBUSTER_AGENT_ID")

# ============================================
# EMELIA CONFIG
# ============================================

EMELIA_CAMPAIGN_ID = get_secret("EMELIA_CAMPAIGN_ID")

# ============================================
# GOOGLE SHEETS CONFIG
# ============================================

# JSON string des credentials Service Account
# Peut être le contenu du fichier JSON téléchargé depuis Google Cloud Console
GOOGLE_SHEETS_CREDENTIALS = get_secret("GOOGLE_SHEETS_CREDENTIALS")

# ID du spreadsheet (visible dans l'URL: docs.google.com/spreadsheets/d/{ID}/edit)
GOOGLE_SHEETS_SPREADSHEET_ID = get_secret("GOOGLE_SHEETS_SPREADSHEET_ID")

# ============================================
# FILTRES PAPPERS
# ============================================

PAPPERS_FILTERS = {
    "entreprise_cessee": "false",
    "par_page": 100,
    "convention_collective": "0045",
    "tranche_effectif": "00,01,02,03,11,12,21,22",
    "categorie_juridique": "5710,5720,5599,5499,5498",
}

# ============================================
# ICP - PERSONAS CIBLES
# ============================================

TARGET_PERSONAS = [
    "CEO", "Directeur General", "Gerant", "President", "Fondateur",
    "DSI", "CTO", "Directeur Technique", "Responsable IT", "Directeur Informatique",
    "DRH", "Directeur des Ressources Humaines", "Responsable RH", "HR Director",
    "DAF", "Directeur Financier", "CFO", "Responsable Administratif et Financier",
    "COO", "Directeur des Operations", "Directeur Commercial",
]

# ============================================
# OUTPUT
# ============================================

OUTPUT_DIR = "output"
COMPANIES_CSV = f"{OUTPUT_DIR}/companies.csv"
CONTACTS_CSV = f"{OUTPUT_DIR}/contacts.csv"
ENRICHED_CONTACTS_CSV = f"{OUTPUT_DIR}/enriched_contacts.csv"
