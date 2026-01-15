"""
Client API Enrich CRM pour récupérer les données d'entreprises
Documentation: https://enrich-crm.readme.io/reference/
"""
import requests
from typing import Optional
import config


class EnrichCRMClient:
    """Client pour l'API Enrich CRM"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or config.ENRICH_CRM_API_KEY
        self.base_url = "https://gateway.enrich-crm.com/api/ingress/v4"

    def enrich_by_domain(self, domain: str, firmographic: bool = True) -> dict:
        """
        Enrichit une entreprise à partir de son domaine.

        Args:
            domain: Domaine de l'entreprise (ex: anthropic.com)
            firmographic: Inclure données firmographiques (taille, secteur, etc.)

        Returns:
            Données enrichies de l'entreprise
        """
        params = {
            "apiId": self.api_key,
            "data": domain,
            "firmographic": str(firmographic).lower(),
        }

        try:
            response = requests.get(
                f"{self.base_url}/full",
                params=params,
                headers={"accept": "application/json"}
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Erreur Enrich CRM pour {domain}: {e}")
            return {}

    def enrich_by_email(self, email: str, firmographic: bool = True) -> dict:
        """
        Enrichit à partir d'une adresse email.

        Args:
            email: Adresse email
            firmographic: Inclure données firmographiques

        Returns:
            Données enrichies
        """
        params = {
            "apiId": self.api_key,
            "data": email,
            "firmographic": str(firmographic).lower(),
        }

        try:
            response = requests.get(
                f"{self.base_url}/full",
                params=params,
                headers={"accept": "application/json"}
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Erreur Enrich CRM pour {email}: {e}")
            return {}

    def enrich_by_company_name(self, company_name: str, firmographic: bool = True, debug: bool = True) -> dict:
        """
        Enrichit à partir du nom d'entreprise (fuzzy search).

        Args:
            company_name: Nom de l'entreprise
            firmographic: Inclure données firmographiques
            debug: True pour avoir tous les résultats (non filtrés)

        Returns:
            Données enrichies
        """
        params = {
            "apiId": self.api_key,
            "data": company_name,
            "firmographic": str(firmographic).lower(),
            "debug": str(debug).lower(),
        }

        try:
            response = requests.get(
                f"{self.base_url}/full",
                params=params,
                headers={"accept": "application/json"}
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Erreur Enrich CRM pour {company_name}: {e}")
            return {}

    def get_company_linkedin(self, company_name: str) -> Optional[str]:
        """
        Récupère l'URL LinkedIn d'une entreprise.

        Args:
            company_name: Nom de l'entreprise

        Returns:
            URL LinkedIn ou None
        """
        data = self.enrich_by_company_name(company_name)

        # Chercher l'URL LinkedIn dans la réponse
        linkedin_url = (
            data.get("linkedinUrl") or
            data.get("linkedin_url") or
            data.get("company", {}).get("linkedinUrl") or
            data.get("company", {}).get("linkedin_url")
        )

        return linkedin_url


def enrich_companies_with_linkedin(companies: list) -> list:
    """
    Enrichit une liste d'entreprises avec leurs URLs LinkedIn.

    Args:
        companies: Liste d'entreprises (dictionnaires avec clé 'nom')

    Returns:
        Liste d'entreprises avec la clé 'linkedin_url' ajoutée
    """
    client = EnrichCRMClient()
    enriched = []

    # Debug: vérifier API key
    print(f"  [DEBUG] Enrich CRM API Key configurée: {'✅' if client.api_key else '❌'}")
    if client.api_key:
        print(f"  [DEBUG] API Key: {client.api_key[:20]}...")

    for company in companies:
        print(f"  Enrichissement de {company['nom']}...")

        # Essayer par nom d'entreprise
        data = client.enrich_by_company_name(company["nom"])

        # DEBUG: afficher la structure de réponse
        if data:
            print(f"    [DEBUG] Réponse keys: {list(data.keys())[:10]}")
            if "company" in data:
                print(f"    [DEBUG] company keys: {list(data['company'].keys())[:10]}")
                if "firmographics" in data.get("company", {}):
                    firmographics = data["company"]["firmographics"]
                    print(f"    [DEBUG] firmographics keys: {list(firmographics.keys())[:10]}")
        else:
            print(f"    [DEBUG] Réponse vide ou erreur")

        # Chercher LinkedIn URL dans différents emplacements possibles
        linkedin_url = None
        if data:
            # Structure: company.firmographics.linkedinUrl
            firmographics = data.get("company", {}).get("firmographics", {})
            linkedin_url = (
                firmographics.get("linkedinUrl") or
                firmographics.get("linkedin") or
                data.get("linkedinUrl") or
                data.get("linkedin") or
                data.get("company", {}).get("linkedinUrl") or
                data.get("company", {}).get("linkedin")
            )

            # Aussi chercher dans socials si présent
            socials = data.get("company", {}).get("socials", {})
            if not linkedin_url and socials:
                linkedin_url = socials.get("linkedin") or socials.get("linkedinUrl")

        company["linkedin_url"] = linkedin_url
        company["enrich_data"] = data  # Garder toutes les données
        enriched.append(company)

        if linkedin_url:
            print(f"    ✓ LinkedIn: {linkedin_url}")
        else:
            print(f"    ✗ LinkedIn non trouvé")

    return enriched


if __name__ == "__main__":
    print("Test de l'API Enrich CRM...")
    client = EnrichCRMClient()

    # Test avec un domaine connu
    result = client.enrich_by_domain("anthropic.com")
    print(f"Résultat Anthropic: {result}")
