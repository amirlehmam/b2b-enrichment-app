"""
Client API Captely pour l'enrichissement des contacts (emails et téléphones)
Documentation: https://captely.readme.io/reference/

Supporte:
- Enrichissement unitaire (/api/enrich)
- Enrichissement en masse (/api/enrich/bulk) - RECOMMANDÉ pour >5 contacts
"""
import requests
import uuid
import time
from typing import Optional, List
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
        Enrichit un contact avec email et téléphone (unitaire).
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
                timeout=35
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Erreur enrichissement Captely: {e}")
            return {}

    def enrich_bulk(
        self,
        contacts: List[dict],
        enrich_email: bool = True,
        enrich_phone: bool = True
    ) -> str:
        """
        Lance un enrichissement en masse (async).

        Args:
            contacts: Liste de dicts avec first_name, last_name, company, linkedin_url
            enrich_email: Enrichir avec email
            enrich_phone: Enrichir avec téléphone

        Returns:
            job_id pour suivre le statut
        """
        payload = {
            "contacts": contacts,
            "enrich_email": enrich_email,
            "enrich_phone": enrich_phone,
        }

        try:
            print(f"  → Soumission de {len(contacts)} contacts pour enrichissement bulk...")

            response = requests.post(
                f"{self.base_url}/enrich/bulk",
                headers=self._headers(),
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()

            job_id = data.get("job_id") or data.get("jobId") or data.get("id")
            print(f"  ✓ Job créé: {job_id}")
            return job_id

        except requests.exceptions.RequestException as e:
            print(f"  ✗ Erreur bulk enrichment: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"    Response: {e.response.text[:200]}")
            return None

    def get_bulk_status(self, job_id: str) -> dict:
        """
        Vérifie le statut d'un job bulk.

        Returns:
            dict avec status (pending, processing, completed, failed)
        """
        try:
            response = requests.get(
                f"{self.base_url}/enrich/status/{job_id}",
                headers=self._headers(),
                timeout=10
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"  ⚠ Erreur status: {e}")
            return {"status": "error"}

    def get_bulk_results(self, job_id: str, page: int = 1, per_page: int = 100) -> dict:
        """
        Récupère les résultats d'un job bulk.

        Returns:
            dict avec contacts enrichis
        """
        try:
            response = requests.get(
                f"{self.base_url}/enrich/results/{job_id}",
                headers=self._headers(),
                params={"page": page, "per_page": per_page},
                timeout=30
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"  ✗ Erreur résultats: {e}")
            return {"contacts": [], "results": []}

    def wait_for_bulk_completion(
        self,
        job_id: str,
        timeout: int = 600,
        poll_interval: int = 5
    ) -> bool:
        """
        Attend la fin d'un job bulk.

        Args:
            job_id: ID du job
            timeout: Timeout en secondes (défaut 10 min)
            poll_interval: Intervalle de polling en secondes

        Returns:
            True si terminé avec succès
        """
        start_time = time.time()
        last_progress = 0

        while time.time() - start_time < timeout:
            status_data = self.get_bulk_status(job_id)
            status = status_data.get("status", "unknown")
            progress = status_data.get("progress", 0)
            total = status_data.get("total", 0)

            if progress != last_progress:
                print(f"  ... {status}: {progress}/{total} traités")
                last_progress = progress

            if status == "completed":
                print(f"  ✓ Job terminé!")
                return True
            elif status in ["failed", "error"]:
                print(f"  ✗ Job échoué: {status_data.get('error', 'N/A')}")
                return False

            time.sleep(poll_interval)

        print(f"  ✗ Timeout après {timeout}s")
        return False

    def get_credits(self) -> dict:
        """Récupère le solde de crédits."""
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
            return {"balance": 0}


def enrich_contacts_with_captely(decision_makers: list, enrich_phone: bool = True) -> list:
    """
    Enrichit une liste de décideurs avec emails et téléphones.
    Utilise l'API BULK pour >3 contacts (beaucoup plus rapide).

    Args:
        decision_makers: Liste des décideurs
        enrich_phone: Enrichir aussi avec téléphone

    Returns:
        Liste enrichie avec emails et téléphones
    """
    if not decision_makers:
        return []

    client = CaptelyClient()

    # Préparer les contacts pour l'API
    contacts_for_api = []
    contact_map = {}  # Pour mapper les résultats

    for i, dm in enumerate(decision_makers):
        first_name = dm.get("firstName") or ""
        last_name = dm.get("lastName") or ""

        if not first_name or not last_name:
            full_name = dm.get("name", "")
            parts = full_name.split(" ", 1)
            first_name = parts[0] if parts else ""
            last_name = parts[1] if len(parts) > 1 else ""

        company = dm.get("entreprise", dm.get("company", ""))
        linkedin_url = dm.get("linkedin_url", "")

        if not first_name or not last_name or not company:
            print(f"  ⚠ Données manquantes pour {dm.get('name', 'inconnu')}, skip")
            continue

        contact_data = {
            "first_name": first_name,
            "last_name": last_name,
            "company": company,
        }
        if linkedin_url:
            contact_data["linkedin_url"] = linkedin_url

        contacts_for_api.append(contact_data)
        contact_map[f"{first_name}_{last_name}_{company}".lower()] = i

    if not contacts_for_api:
        print("  ⚠ Aucun contact valide à enrichir")
        return decision_makers

    print(f"\n📧 Enrichissement BULK de {len(contacts_for_api)} contacts...")

    # Utiliser BULK API
    if len(contacts_for_api) >= 1:
        job_id = client.enrich_bulk(
            contacts=contacts_for_api,
            enrich_email=True,
            enrich_phone=enrich_phone
        )

        if not job_id:
            print("  ✗ Échec création job bulk")
            return decision_makers

        # Attendre la fin
        if not client.wait_for_bulk_completion(job_id, timeout=600, poll_interval=5):
            print("  ✗ Job bulk non terminé")
            return decision_makers

        # Récupérer les résultats
        results_data = client.get_bulk_results(job_id, per_page=1000)
        results = results_data.get("contacts", []) or results_data.get("results", []) or results_data.get("data", [])

        print(f"  ✓ {len(results)} résultats reçus")

        # Mapper les résultats aux contacts originaux
        enriched_count = 0
        for result in results:
            first = result.get("first_name", "")
            last = result.get("last_name", "")
            comp = result.get("company", "")
            key = f"{first}_{last}_{comp}".lower()

            if key in contact_map:
                idx = contact_map[key]
                decision_makers[idx]["email"] = result.get("email")
                decision_makers[idx]["email_verified"] = result.get("email_verified", False)
                decision_makers[idx]["phone"] = result.get("phone")
                decision_makers[idx]["phone_type"] = result.get("phone_type")

                if result.get("email") or result.get("phone"):
                    enriched_count += 1

        print(f"  ✓ {enriched_count} contacts enrichis avec email/téléphone")

    return decision_makers


if __name__ == "__main__":
    print("Test de l'API Captely...")

    client = CaptelyClient()

    # Test crédits
    credits = client.get_credits()
    print(f"Crédits: {credits}")

    # Test bulk avec 2 contacts
    test_contacts = [
        {
            "first_name": "Elon",
            "last_name": "Musk",
            "company": "Tesla",
        },
        {
            "first_name": "Tim",
            "last_name": "Cook",
            "company": "Apple",
        }
    ]

    job_id = client.enrich_bulk(test_contacts, enrich_email=True, enrich_phone=False)
    if job_id:
        print(f"Job ID: {job_id}")
        if client.wait_for_bulk_completion(job_id, timeout=120):
            results = client.get_bulk_results(job_id)
            print(f"Résultats: {results}")
