"""
Client API Captely pour l'enrichissement des contacts (emails et téléphones)
Documentation: https://captely.readme.io/reference/
"""
import requests
import uuid
from typing import Optional
import config


class CaptelyClient:
    """Client pour l'API Captely"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or config.CAPTELY_API_KEY
        self.base_url = "https://api.captely.com/api"

    def _headers(self) -> dict:
        return {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def enrich_contact(
        self,
        first_name: str,
        last_name: str,
        company: str,
        linkedin_url: str = None,
        enrich_email: bool = True,
        enrich_phone: bool = True
    ) -> dict:
        """
        Enrichit un contact avec email et téléphone.

        Args:
            first_name: Prénom (requis)
            last_name: Nom (requis)
            company: Nom de l'entreprise (requis)
            linkedin_url: URL LinkedIn (améliore précision de 30%)
            enrich_email: Chercher l'email (1 crédit si trouvé)
            enrich_phone: Chercher le téléphone (15 crédits si trouvé)

        Returns:
            Données enrichies (email, phone, etc.)
        """
        payload = {
            "first_name": first_name,
            "last_name": last_name,
            "company": company,
            "enrich_email": enrich_email,
            "enrich_phone": enrich_phone,
            "idempotency_key": str(uuid.uuid4()),
        }

        if linkedin_url:
            payload["linkedin_url"] = linkedin_url

        try:
            response = requests.post(
                f"{self.base_url}/enrich",
                headers=self._headers(),
                json=payload,
                timeout=35  # Max 30s selon doc + marge
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Erreur enrichissement Captely: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"  Response: {e.response.text}")
            return {}

    def enrich_phone(
        self,
        first_name: str,
        last_name: str,
        company: str,
        linkedin_url: str = None
    ) -> dict:
        """
        Récupère le numéro de téléphone d'un contact.

        Args:
            first_name: Prénom
            last_name: Nom
            company: Entreprise
            linkedin_url: URL LinkedIn

        Returns:
            Données téléphone (phone, type, carrier)
        """
        payload = {
            "first_name": first_name,
            "last_name": last_name,
            "company": company,
            "idempotency_key": str(uuid.uuid4()),
        }

        if linkedin_url:
            payload["linkedin_url"] = linkedin_url

        try:
            response = requests.post(
                f"{self.base_url}/enrich/phone",
                headers=self._headers(),
                json=payload,
                timeout=35
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Erreur récupération téléphone: {e}")
            return {}

    def get_credits(self) -> dict:
        """
        Récupère le solde de crédits.

        Returns:
            dict avec:
                - balance: Solde total de crédits
                - email_credits: Crédits disponibles pour emails
                - phone_credits: Crédits disponibles pour téléphones
        """
        try:
            response = requests.get(
                f"{self.base_url}/credits/balance",
                headers=self._headers(),
                timeout=10
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Erreur récupération crédits: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"  Response: {e.response.text}")
            return {"balance": 0, "email_credits": 0, "phone_credits": 0}


def enrich_contacts_with_captely(decision_makers: list, enrich_phone: bool = True) -> list:
    """
    Enrichit une liste de décideurs avec emails et téléphones.

    Args:
        decision_makers: Liste des décideurs (avec name, firstName, lastName, linkedin_url, entreprise)
        enrich_phone: Enrichir aussi avec téléphone (15 crédits/contact)

    Returns:
        Liste enrichie avec emails et téléphones
    """
    client = CaptelyClient()
    enriched = []

    for dm in decision_makers:
        # Extraire prénom/nom
        first_name = dm.get("firstName") or ""
        last_name = dm.get("lastName") or ""

        # Si pas de prénom/nom séparés, parser le nom complet
        if not first_name or not last_name:
            full_name = dm.get("name", "")
            parts = full_name.split(" ", 1)
            first_name = parts[0] if parts else ""
            last_name = parts[1] if len(parts) > 1 else ""

        company = dm.get("entreprise", dm.get("company", ""))
        linkedin_url = dm.get("linkedin_url", "")

        if not first_name or not last_name or not company:
            print(f"  Données manquantes pour {dm.get('name', 'inconnu')}, skip")
            enriched.append(dm)
            continue

        print(f"  Enrichissement de {first_name} {last_name} ({company})...")

        # Enrichir avec email et téléphone
        result = client.enrich_contact(
            first_name=first_name,
            last_name=last_name,
            company=company,
            linkedin_url=linkedin_url,
            enrich_email=True,
            enrich_phone=enrich_phone
        )

        dm["email"] = result.get("email")
        dm["email_verified"] = result.get("email_verified", False)
        dm["phone"] = result.get("phone")
        dm["phone_type"] = result.get("phone_type")

        enriched.append(dm)

        if dm["email"] or dm["phone"]:
            print(f"    ✓ Email: {dm.get('email', 'N/A')} | Tel: {dm.get('phone', 'N/A')}")
        else:
            print(f"    ✗ Non trouvé")

    return enriched


if __name__ == "__main__":
    print("Test de l'API Captely...")

    client = CaptelyClient()

    # Test enrichissement
    result = client.enrich_contact(
        first_name="Helene",
        last_name="Chatillon",
        company="Tenderlift",
        linkedin_url="https://linkedin.com/in/helene-chatillon-2b07424",
        enrich_email=True,
        enrich_phone=False  # Économiser les crédits pour le test
    )
    print(f"Résultat: {result}")
