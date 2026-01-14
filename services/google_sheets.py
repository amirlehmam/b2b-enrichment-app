"""
Client Google Sheets pour synchroniser les données du workflow.
Documentation: https://docs.gspread.org/

Utilise gspread avec authentification Service Account.
"""
import json
from typing import List, Optional
import config

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False
    print("⚠ gspread non installé. Installez avec: pip install gspread google-auth")


class GoogleSheetsClient:
    """Client pour Google Sheets API via gspread"""

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    def __init__(self, credentials_json: str = None):
        """
        Initialise le client Google Sheets.

        Args:
            credentials_json: JSON des credentials Service Account
                             (peut être string JSON ou dict)
        """
        if not GSPREAD_AVAILABLE:
            raise ImportError("gspread non installé")

        self.credentials_json = credentials_json or config.GOOGLE_SHEETS_CREDENTIALS
        self.client = self._authenticate()

    def _authenticate(self):
        """Authentifie avec le Service Account."""
        if not self.credentials_json:
            raise ValueError("GOOGLE_SHEETS_CREDENTIALS non configuré")

        # Parser le JSON si c'est une string
        if isinstance(self.credentials_json, str):
            creds_dict = json.loads(self.credentials_json)
        else:
            creds_dict = self.credentials_json

        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=self.SCOPES
        )

        return gspread.authorize(credentials)

    def open_spreadsheet(self, spreadsheet_id: str):
        """
        Ouvre un spreadsheet par son ID.

        Args:
            spreadsheet_id: ID du spreadsheet (dans l'URL)

        Returns:
            Spreadsheet object
        """
        return self.client.open_by_key(spreadsheet_id)

    def get_or_create_worksheet(
        self,
        spreadsheet_id: str,
        worksheet_name: str,
        headers: List[str] = None
    ):
        """
        Récupère ou crée une feuille dans le spreadsheet.

        Args:
            spreadsheet_id: ID du spreadsheet
            worksheet_name: Nom de la feuille
            headers: En-têtes à ajouter si création

        Returns:
            Worksheet object
        """
        spreadsheet = self.open_spreadsheet(spreadsheet_id)

        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            # Créer la feuille
            worksheet = spreadsheet.add_worksheet(
                title=worksheet_name,
                rows=1000,
                cols=20
            )
            # Ajouter les headers si fournis
            if headers:
                worksheet.append_row(headers)

        return worksheet

    def append_rows(
        self,
        spreadsheet_id: str,
        worksheet_name: str,
        rows: List[List],
        headers: List[str] = None
    ) -> int:
        """
        Ajoute plusieurs lignes à une feuille.

        Args:
            spreadsheet_id: ID du spreadsheet
            worksheet_name: Nom de la feuille
            rows: Liste de lignes (chaque ligne est une liste de valeurs)
            headers: En-têtes si la feuille doit être créée

        Returns:
            Nombre de lignes ajoutées
        """
        worksheet = self.get_or_create_worksheet(
            spreadsheet_id,
            worksheet_name,
            headers
        )

        if rows:
            worksheet.append_rows(rows, value_input_option="USER_ENTERED")

        return len(rows)

    def clear_worksheet(self, spreadsheet_id: str, worksheet_name: str):
        """Efface le contenu d'une feuille (garde les headers)."""
        worksheet = self.get_or_create_worksheet(spreadsheet_id, worksheet_name)

        # Garder la première ligne (headers)
        if worksheet.row_count > 1:
            worksheet.delete_rows(2, worksheet.row_count)

    def update_worksheet(
        self,
        spreadsheet_id: str,
        worksheet_name: str,
        data: List[dict],
        headers: List[str]
    ) -> int:
        """
        Remplace tout le contenu d'une feuille avec de nouvelles données.

        Args:
            spreadsheet_id: ID du spreadsheet
            worksheet_name: Nom de la feuille
            data: Liste de dictionnaires
            headers: Liste des colonnes à extraire

        Returns:
            Nombre de lignes écrites
        """
        worksheet = self.get_or_create_worksheet(
            spreadsheet_id,
            worksheet_name,
            headers
        )

        # Effacer le contenu existant
        worksheet.clear()

        # Ajouter headers
        worksheet.append_row(headers)

        # Convertir les dicts en lignes
        rows = []
        for item in data:
            row = [str(item.get(h, "")) for h in headers]
            rows.append(row)

        if rows:
            worksheet.append_rows(rows, value_input_option="USER_ENTERED")

        return len(rows)


# ============================================
# FONCTIONS HELPER POUR LE WORKFLOW
# ============================================

def sync_companies_to_sheets(companies: List[dict], spreadsheet_id: str = None) -> int:
    """
    Synchronise les entreprises vers Google Sheets.

    Args:
        companies: Liste des entreprises
        spreadsheet_id: ID du spreadsheet (utilise config si non fourni)

    Returns:
        Nombre d'entreprises synchronisées
    """
    spreadsheet_id = spreadsheet_id or config.GOOGLE_SHEETS_SPREADSHEET_ID

    if not spreadsheet_id:
        print("⚠ GOOGLE_SHEETS_SPREADSHEET_ID non configuré")
        return 0

    if not GSPREAD_AVAILABLE:
        print("⚠ gspread non installé")
        return 0

    try:
        client = GoogleSheetsClient()

        headers = [
            "siren", "nom", "forme_juridique", "effectif",
            "adresse", "code_naf", "activite", "linkedin_url",
            "convention_collective", "dirigeants"
        ]

        # Préparer les données
        data = []
        for company in companies:
            row = company.copy()
            # Convertir les dirigeants en string
            if isinstance(row.get("dirigeants"), list):
                row["dirigeants"] = "; ".join([
                    f"{d.get('nom', '')} ({d.get('qualite', '')})"
                    for d in row["dirigeants"]
                ])
            data.append(row)

        count = client.update_worksheet(
            spreadsheet_id,
            "Entreprises",
            data,
            headers
        )

        print(f"✅ {count} entreprises synchronisées vers Google Sheets")
        return count

    except Exception as e:
        print(f"❌ Erreur sync Google Sheets: {e}")
        return 0


def sync_contacts_to_sheets(contacts: List[dict], spreadsheet_id: str = None) -> int:
    """
    Synchronise les contacts enrichis vers Google Sheets.

    Args:
        contacts: Liste des contacts
        spreadsheet_id: ID du spreadsheet (utilise config si non fourni)

    Returns:
        Nombre de contacts synchronisés
    """
    spreadsheet_id = spreadsheet_id or config.GOOGLE_SHEETS_SPREADSHEET_ID

    if not spreadsheet_id:
        print("⚠ GOOGLE_SHEETS_SPREADSHEET_ID non configuré")
        return 0

    if not GSPREAD_AVAILABLE:
        print("⚠ gspread non installé")
        return 0

    try:
        client = GoogleSheetsClient()

        headers = [
            "name", "title", "entreprise", "siren", "persona_type",
            "linkedin_url", "email", "email_verified", "phone", "phone_type"
        ]

        count = client.update_worksheet(
            spreadsheet_id,
            "Contacts",
            contacts,
            headers
        )

        print(f"✅ {count} contacts synchronisés vers Google Sheets")
        return count

    except Exception as e:
        print(f"❌ Erreur sync Google Sheets: {e}")
        return 0


def append_contacts_to_sheets(contacts: List[dict], spreadsheet_id: str = None) -> int:
    """
    Ajoute des contacts à la feuille existante (sans effacer).
    Ajoute automatiquement un timestamp pour chaque contact.

    Args:
        contacts: Liste des contacts à ajouter
        spreadsheet_id: ID du spreadsheet

    Returns:
        Nombre de contacts ajoutés
    """
    from datetime import datetime

    spreadsheet_id = spreadsheet_id or config.GOOGLE_SHEETS_SPREADSHEET_ID

    if not spreadsheet_id or not GSPREAD_AVAILABLE:
        return 0

    try:
        client = GoogleSheetsClient()

        headers = [
            "date_ajout", "name", "title", "entreprise", "siren", "persona_type",
            "linkedin_url", "email", "email_verified", "phone", "phone_type"
        ]

        # Timestamp pour ce batch
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

        # Convertir en lignes avec timestamp
        rows = []
        for contact in contacts:
            row = [timestamp] + [str(contact.get(h, "")) for h in headers[1:]]
            rows.append(row)

        count = client.append_rows(
            spreadsheet_id,
            "Contacts_Historique",  # Nouvelle feuille pour l'historique
            rows,
            headers
        )

        print(f"✅ {count} contacts ajoutés à l'historique Google Sheets")
        return count

    except Exception as e:
        print(f"❌ Erreur append Google Sheets: {e}")
        return 0


def get_all_contacts_from_sheets(spreadsheet_id: str = None) -> List[dict]:
    """
    Récupère tous les contacts sauvegardés depuis Google Sheets.

    Args:
        spreadsheet_id: ID du spreadsheet

    Returns:
        Liste de tous les contacts avec leur historique
    """
    spreadsheet_id = spreadsheet_id or config.GOOGLE_SHEETS_SPREADSHEET_ID

    if not spreadsheet_id or not GSPREAD_AVAILABLE:
        return []

    try:
        client = GoogleSheetsClient()
        spreadsheet = client.open_spreadsheet(spreadsheet_id)

        try:
            worksheet = spreadsheet.worksheet("Contacts_Historique")
        except:
            print("  ℹ Aucun historique trouvé")
            return []

        # Récupérer toutes les données
        records = worksheet.get_all_records()
        print(f"  ✓ {len(records)} contacts trouvés dans l'historique")
        return records

    except Exception as e:
        print(f"❌ Erreur lecture Google Sheets: {e}")
        return []


def get_contacts_stats(spreadsheet_id: str = None) -> dict:
    """
    Récupère des stats sur les contacts sauvegardés.

    Returns:
        dict avec total, avec_email, avec_phone, entreprises_uniques
    """
    contacts = get_all_contacts_from_sheets(spreadsheet_id)

    if not contacts:
        return {"total": 0, "avec_email": 0, "avec_phone": 0, "entreprises": 0}

    return {
        "total": len(contacts),
        "avec_email": len([c for c in contacts if c.get("email")]),
        "avec_phone": len([c for c in contacts if c.get("phone")]),
        "entreprises": len(set(c.get("entreprise", "") for c in contacts if c.get("entreprise"))),
    }


if __name__ == "__main__":
    print("Test de l'intégration Google Sheets...")

    if not GSPREAD_AVAILABLE:
        print("❌ gspread non installé")
        print("   pip install gspread google-auth")
    elif not config.GOOGLE_SHEETS_CREDENTIALS:
        print("⚠ GOOGLE_SHEETS_CREDENTIALS non configuré")
        print("\nInstructions:")
        print("1. Créez un projet sur Google Cloud Console")
        print("2. Activez l'API Google Sheets et Google Drive")
        print("3. Créez un Service Account")
        print("4. Téléchargez le JSON des credentials")
        print("5. Ajoutez le JSON dans config.py ou .env")
        print("6. Partagez votre Google Sheet avec l'email du Service Account")
    else:
        print("✅ Configuration Google Sheets détectée")
        client = GoogleSheetsClient()
        print("✅ Authentification réussie")
