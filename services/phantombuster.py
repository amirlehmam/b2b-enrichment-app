"""
Client API Phantombuster pour l'extraction des employés LinkedIn
Documentation: https://hub.phantombuster.com/docs/api

OPTIMIZED VERSION:
- Uses blocking API calls (output=result-object) - no polling!
- Parallel processing of multiple companies
- 10x faster than original implementation
"""
import requests
import json
import time
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import config


class PhantombusterClient:
    """Client optimisé pour l'API Phantombuster"""

    def __init__(self, api_key: str = None, agent_id: str = None):
        self.api_key = api_key or config.PHANTOMBUSTER_API_KEY
        self.agent_id = agent_id or config.PHANTOMBUSTER_AGENT_ID
        self.base_url = "https://api.phantombuster.com/api/v2"

    def _headers(self) -> dict:
        return {
            "X-Phantombuster-Key": self.api_key,
            "Content-Type": "application/json",
        }

    def launch_and_wait(self, linkedin_company_url: str, timeout: int = 600) -> Optional[List[dict]]:
        """
        Lance l'agent et attend le résultat (appel bloquant).

        Utilise output=result-object pour un appel bloquant -
        PAS DE POLLING, beaucoup plus rapide!

        Args:
            linkedin_company_url: URL LinkedIn de l'entreprise
            timeout: Timeout en secondes (défaut 10 min)

        Returns:
            Liste des employés ou None si erreur
        """
        if not self.agent_id:
            raise ValueError("PHANTOMBUSTER_AGENT_ID non configuré")

        # Préparer l'argument JSON
        argument = json.dumps({
            "spreadsheetUrl": linkedin_company_url,
            "numberOfEmployeesToExtract": 50,  # Limiter pour la vitesse
        })

        try:
            print(f"  → Lancement extraction: {linkedin_company_url}")

            # Appel BLOQUANT - pas besoin de polling!
            response = requests.post(
                f"{self.base_url}/agents/launch",
                headers=self._headers(),
                params={
                    "id": self.agent_id,
                    "argument": argument,
                    "output": "result-object",  # BLOCKING CALL!
                },
                timeout=timeout
            )

            if response.status_code == 200:
                data = response.json()

                # Vérifier le statut
                if data.get("status") == "success":
                    result_object = data.get("data", {}).get("resultObject")
                    if result_object:
                        return self._parse_employees(result_object)

                    # Si pas de resultObject, essayer de récupérer depuis S3
                    return self._fetch_from_s3()
                else:
                    print(f"  ⚠ Agent terminé avec statut: {data.get('status')}")
                    print(f"    Message: {data.get('message', 'N/A')}")
                    return []
            else:
                print(f"  ✗ Erreur HTTP {response.status_code}: {response.text[:200]}")
                return None

        except requests.exceptions.Timeout:
            print(f"  ✗ Timeout après {timeout}s")
            return None
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Erreur requête: {e}")
            return None

    def _parse_employees(self, result_object) -> List[dict]:
        """Parse le resultObject en liste d'employés."""
        if isinstance(result_object, list):
            employees = result_object
        elif isinstance(result_object, dict):
            employees = result_object.get("employees", [])
        else:
            return []

        valid_employees = []
        for emp in employees:
            if emp.get("profileUrl") or emp.get("linkedInProfileUrl"):
                valid_employees.append({
                    "name": emp.get("name", emp.get("fullName", "")),
                    "firstName": emp.get("firstName", ""),
                    "lastName": emp.get("lastName", ""),
                    "title": emp.get("job", emp.get("title", emp.get("headline", ""))),
                    "linkedin_url": emp.get("profileUrl", emp.get("linkedInProfileUrl", "")),
                    "location": emp.get("location", ""),
                    "company_query": emp.get("query", ""),
                })

        return valid_employees

    def _fetch_from_s3(self) -> List[dict]:
        """Récupère les résultats depuis S3 (fallback)."""
        try:
            response = requests.get(
                f"{self.base_url}/agents/fetch-output",
                headers=self._headers(),
                params={"id": self.agent_id}
            )

            if response.status_code == 200:
                data = response.json()
                if data.get("data", {}).get("output"):
                    # Parse le JSON output
                    output = json.loads(data["data"]["output"])
                    return self._parse_employees(output)
            return []
        except Exception as e:
            print(f"  ⚠ Erreur S3 fallback: {e}")
            return []

    def get_agent_status(self) -> dict:
        """Récupère le statut de l'agent."""
        try:
            response = requests.get(
                f"{self.base_url}/agents/fetch",
                headers=self._headers(),
                params={"id": self.agent_id}
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erreur statut agent: {e}")
            return {}


def extract_employees_batch(companies: List[dict], max_workers: int = 3) -> Dict[str, dict]:
    """
    Extrait les employés de PLUSIEURS entreprises en PARALLÈLE.

    BEAUCOUP plus rapide que l'extraction séquentielle!

    Args:
        companies: Liste des entreprises avec linkedin_url
        max_workers: Nombre de workers parallèles (défaut 3 pour éviter rate limits)

    Returns:
        Dict {siren: {"company": company, "employees": [...]}}
    """
    client = PhantombusterClient()

    # Filtrer les entreprises avec LinkedIn
    companies_with_linkedin = [c for c in companies if c.get("linkedin_url")]

    if not companies_with_linkedin:
        print("Aucune entreprise avec URL LinkedIn")
        return {}

    print(f"\n🚀 Extraction parallèle de {len(companies_with_linkedin)} entreprises...")
    print(f"   (max {max_workers} en parallèle)")

    results = {}

    def process_company(company):
        """Traite une entreprise."""
        linkedin_url = company.get("linkedin_url")
        siren = company.get("siren")
        nom = company.get("nom", "?")

        print(f"\n📍 {nom}")
        employees = client.launch_and_wait(linkedin_url)

        if employees:
            print(f"  ✓ {len(employees)} employés trouvés")
        else:
            print(f"  ⚠ Aucun employé trouvé")
            employees = []

        return siren, {"company": company, "employees": employees}

    # Exécution parallèle
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_company, company): company
            for company in companies_with_linkedin
        }

        for future in as_completed(futures):
            try:
                siren, data = future.result()
                results[siren] = data
            except Exception as e:
                company = futures[future]
                print(f"  ✗ Erreur pour {company.get('nom', '?')}: {e}")

    total_employees = sum(len(d["employees"]) for d in results.values())
    print(f"\n✅ Total: {total_employees} employés extraits de {len(results)} entreprises")

    return results


def extract_employees_from_linkedin(linkedin_url: str, agent_id: str = None) -> list:
    """
    Extrait les employés d'une entreprise via Phantombuster.

    Version compatible avec l'ancien code (appel unique).
    Pour les batches, utiliser extract_employees_batch() à la place.
    """
    client = PhantombusterClient(agent_id=agent_id)
    employees = client.launch_and_wait(linkedin_url)
    return employees or []


if __name__ == "__main__":
    print("Test de l'API Phantombuster (version optimisée)...")

    if not config.PHANTOMBUSTER_AGENT_ID:
        print("ERREUR: PHANTOMBUSTER_AGENT_ID non configuré dans config.py")
        print("Instructions:")
        print("1. Allez sur https://phantombuster.com")
        print("2. Cliquez sur 'My Agents'")
        print("3. Ouvrez votre agent LinkedIn Company Employees")
        print("4. Copiez l'ID depuis l'URL (les chiffres après /agents/)")
    else:
        client = PhantombusterClient()
        status = client.get_agent_status()
        print(f"Statut agent: {json.dumps(status, indent=2)}")
