"""
Client API Emelia pour l'envoi de contacts vers les campagnes LinkedIn/Email
Documentation: https://docs.emelia.io/

Emelia utilise GraphQL pour son API.
"""
import requests
import json
from typing import Optional, List
import config


class EmeliaClient:
    """Client pour l'API GraphQL Emelia"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or config.EMELIA_API_KEY
        self.base_url = "https://graphql.emelia.io/graphql"

    def _headers(self) -> dict:
        return {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
        }

    def _execute_query(self, query: str, variables: dict = None) -> dict:
        """Exécute une requête GraphQL."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        try:
            response = requests.post(
                self.base_url,
                headers=self._headers(),
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()

            if "errors" in result:
                print(f"Erreur GraphQL: {result['errors']}")
                return {}

            return result.get("data", {})

        except requests.exceptions.RequestException as e:
            print(f"Erreur Emelia API: {e}")
            return {}

    def get_campaigns(self) -> List[dict]:
        """
        Récupère toutes les campagnes.

        Returns:
            Liste des campagnes avec id, name, status
        """
        query = """
        query {
            all_campaigns {
                _id
                name
                status
                stats {
                    contacted
                    replied
                }
            }
        }
        """
        result = self._execute_query(query)
        return result.get("all_campaigns", [])

    def get_campaign(self, campaign_id: str) -> dict:
        """
        Récupère les détails d'une campagne.

        Args:
            campaign_id: ID de la campagne

        Returns:
            Détails de la campagne
        """
        query = """
        query campaign($id: ID!) {
            campaign(id: $id) {
                _id
                name
                status
                stats {
                    contacted
                    replied
                    total
                }
            }
        }
        """
        result = self._execute_query(query, {"id": campaign_id})
        return result.get("campaign", {})

    def add_contact_to_campaign(
        self,
        campaign_id: str,
        contact: dict
    ) -> bool:
        """
        Ajoute un contact à une campagne existante.

        Si la campagne est en cours (RUNNING), le contact est automatiquement
        ajouté à la séquence. Si la campagne est terminée (FINISHED), elle
        redémarre automatiquement pour ce contact.

        Args:
            campaign_id: ID de la campagne
            contact: Dict avec les données du contact:
                - email (requis pour email campaigns)
                - firstName, lastName
                - linkedinUrl (requis pour LinkedIn campaigns)
                - company, phone, etc.

        Returns:
            True si succès, False sinon
        """
        query = """
        mutation addContactToCampaignHook($id: ID!, $contact: JSON!) {
            addContactToCampaignHook(id: $id, contact: $contact)
        }
        """

        variables = {
            "id": campaign_id,
            "contact": contact
        }

        result = self._execute_query(query, variables)

        if result.get("addContactToCampaignHook"):
            return True
        return False

    def add_contacts_to_campaign(
        self,
        campaign_id: str,
        contacts: List[dict]
    ) -> dict:
        """
        Ajoute plusieurs contacts à une campagne.

        Args:
            campaign_id: ID de la campagne
            contacts: Liste de contacts

        Returns:
            Dict avec succès/échecs
        """
        results = {"success": 0, "failed": 0, "errors": []}

        for contact in contacts:
            try:
                if self.add_contact_to_campaign(campaign_id, contact):
                    results["success"] += 1
                    print(f"  ✓ {contact.get('firstName', '')} {contact.get('lastName', '')} ajouté")
                else:
                    results["failed"] += 1
                    results["errors"].append(f"Échec pour {contact.get('email', 'inconnu')}")
            except Exception as e:
                results["failed"] += 1
                results["errors"].append(str(e))

        return results

    def add_contact_to_list(
        self,
        list_id: str,
        contact: dict
    ) -> bool:
        """
        Ajoute un contact à une liste (pas directement à une campagne).

        Args:
            list_id: ID de la liste
            contact: Données du contact

        Returns:
            True si succès
        """
        query = """
        mutation addContactsToListHook($id: ID!, $contact: JSON!) {
            addContactsToListHook(id: $id, contact: $contact)
        }
        """

        variables = {
            "id": list_id,
            "contact": contact
        }

        result = self._execute_query(query, variables)
        return bool(result.get("addContactsToListHook"))

    def remove_contact_from_campaign(
        self,
        campaign_id: str,
        email: str
    ) -> bool:
        """
        Retire un contact d'une campagne.

        Args:
            campaign_id: ID de la campagne
            email: Email du contact

        Returns:
            True si succès
        """
        query = """
        mutation removeOneContactFromCampaign($id: ID!, $email: String!) {
            removeOneContactFromCampaign(id: $id, email: $email)
        }
        """

        variables = {
            "id": campaign_id,
            "email": email
        }

        result = self._execute_query(query, variables)
        return bool(result.get("removeOneContactFromCampaign"))


def send_contacts_to_emelia(
    contacts: List[dict],
    campaign_id: str = None
) -> dict:
    """
    Envoie les contacts enrichis vers une campagne Emelia.

    Args:
        contacts: Liste des contacts enrichis
        campaign_id: ID de la campagne (utilise config si non fourni)

    Returns:
        Résultats de l'envoi
    """
    campaign_id = campaign_id or config.EMELIA_CAMPAIGN_ID

    if not campaign_id:
        print("⚠ EMELIA_CAMPAIGN_ID non configuré!")
        return {"success": 0, "failed": len(contacts), "errors": ["Campaign ID missing"]}

    client = EmeliaClient()

    # Vérifier que la campagne existe
    campaign = client.get_campaign(campaign_id)
    if not campaign:
        print(f"⚠ Campagne {campaign_id} non trouvée")
        return {"success": 0, "failed": len(contacts), "errors": ["Campaign not found"]}

    print(f"\n📧 Envoi vers campagne Emelia: {campaign.get('name', campaign_id)}")
    print(f"   Status: {campaign.get('status', 'inconnu')}")

    # Formater les contacts pour Emelia
    emelia_contacts = []
    for contact in contacts:
        # Parser nom si nécessaire
        full_name = contact.get("name", "")
        parts = full_name.split(" ", 1)
        first_name = contact.get("firstName") or (parts[0] if parts else "")
        last_name = contact.get("lastName") or (parts[1] if len(parts) > 1 else "")

        emelia_contact = {
            "firstName": first_name,
            "lastName": last_name,
            "email": contact.get("email", ""),
            "phone": contact.get("phone", ""),
            "linkedinUrl": contact.get("linkedin_url", ""),
            "company": contact.get("entreprise", contact.get("company", "")),
            "position": contact.get("title", ""),
            # Variables personnalisées
            "custom1": contact.get("persona_type", ""),
            "custom2": contact.get("siren", ""),
        }
        emelia_contacts.append(emelia_contact)

    # Envoyer les contacts
    results = client.add_contacts_to_campaign(campaign_id, emelia_contacts)

    print(f"\n✅ Envoi Emelia terminé:")
    print(f"   Succès: {results['success']}")
    print(f"   Échecs: {results['failed']}")

    return results


if __name__ == "__main__":
    print("Test de l'API Emelia...")

    client = EmeliaClient()

    if not config.EMELIA_API_KEY:
        print("⚠ EMELIA_API_KEY non configuré")
        print("Configurez la clé API dans config.py ou .env")
    else:
        # Lister les campagnes
        campaigns = client.get_campaigns()
        print(f"\nCampagnes trouvées: {len(campaigns)}")
        for c in campaigns[:5]:
            print(f"  - {c.get('name')} (ID: {c.get('_id')}) - {c.get('status')}")
