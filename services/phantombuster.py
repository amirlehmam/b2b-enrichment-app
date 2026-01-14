"""
Client API Phantombuster pour l'extraction des employés LinkedIn
Documentation: https://hub.phantombuster.com/reference/post_agents-launch

API v2 - Le body JSON doit contenir:
- id: agent ID
- argument: objet ou string JSON avec les paramètres
"""
import requests
import json
import time
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import config


class PhantombusterClient:
    """Client pour l'API Phantombuster v2"""

    def __init__(self, api_key: str = None, agent_id: str = None):
        self.api_key = api_key or config.PHANTOMBUSTER_API_KEY
        self.agent_id = agent_id or config.PHANTOMBUSTER_AGENT_ID
        self.base_url = "https://api.phantombuster.com/api/v2"

    def _headers(self) -> dict:
        return {
            "X-Phantombuster-Key": self.api_key,
            "Content-Type": "application/json",
        }

    def launch_agent(self, linkedin_company_url: str) -> Optional[str]:
        """
        Lance un agent Phantombuster.

        Args:
            linkedin_company_url: URL LinkedIn de l'entreprise

        Returns:
            Container ID ou None si erreur
        """
        if not self.agent_id:
            raise ValueError("PHANTOMBUSTER_AGENT_ID non configuré")

        # Body JSON avec id et argument (argument en JSON string pour compatibilité)
        argument_obj = {
            "spreadsheetUrl": linkedin_company_url,
            "numberOfEmployeesToExtract": 50,
        }

        body = {
            "id": self.agent_id,
            "argument": json.dumps(argument_obj),  # Convert to JSON string
        }

        try:
            print(f"  → Lancement extraction: {linkedin_company_url}")
            print(f"  [DEBUG] Agent ID: '{self.agent_id}'")
            print(f"  [DEBUG] API Key: '{self.api_key[:15]}...'")
            print(f"  [DEBUG] Body: {json.dumps(body)}")

            response = requests.post(
                f"{self.base_url}/agents/launch",
                headers=self._headers(),
                json=body,
                timeout=30
            )

            print(f"  [DEBUG] Response status: {response.status_code}")
            print(f"  [DEBUG] Response body: {response.text[:300]}")

            if response.status_code == 200:
                data = response.json()
                container_id = data.get("containerId")
                print(f"  ✓ Agent lancé, container: {container_id}")
                return container_id
            else:
                print(f"  ✗ Erreur HTTP {response.status_code}: {response.text[:200]}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"  ✗ Erreur requête: {e}")
            return None

    def wait_for_completion(self, container_id: str, timeout: int = 300, poll_interval: int = 10) -> bool:
        """
        Attend la fin de l'exécution d'un agent.

        Args:
            container_id: ID du container
            timeout: Timeout en secondes
            poll_interval: Intervalle de polling en secondes

        Returns:
            True si terminé avec succès
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = requests.get(
                    f"{self.base_url}/containers/fetch",
                    headers=self._headers(),
                    params={"id": container_id},
                    timeout=10
                )

                if response.status_code == 200:
                    data = response.json()
                    status = data.get("status")

                    if status == "finished":
                        print(f"  ✓ Container terminé")
                        return True
                    elif status == "error":
                        print(f"  ✗ Container erreur: {data.get('error', 'N/A')}")
                        return False

                print(f"  ... en cours (attente {poll_interval}s)")
                time.sleep(poll_interval)

            except Exception as e:
                print(f"  ⚠ Erreur polling: {e}")
                time.sleep(poll_interval)

        print(f"  ✗ Timeout après {timeout}s")
        return False

    def get_output(self) -> List[dict]:
        """
        Récupère les résultats de l'agent depuis S3.

        Returns:
            Liste des employés
        """
        try:
            # Récupérer infos agent
            response = requests.get(
                f"{self.base_url}/agents/fetch",
                headers=self._headers(),
                params={"id": self.agent_id},
                timeout=10
            )

            if response.status_code != 200:
                print(f"  ⚠ Erreur fetch agent: {response.status_code}")
                return []

            agent_data = response.json()
            s3_folder = agent_data.get("s3Folder")
            org_s3_folder = agent_data.get("orgS3Folder")

            if not s3_folder or not org_s3_folder:
                print("  ⚠ S3 folder non trouvé")
                return []

            # Récupérer résultat depuis S3
            result_url = f"https://phantombuster.s3.amazonaws.com/{org_s3_folder}/{s3_folder}/result.json"
            result_response = requests.get(result_url, timeout=30)

            if result_response.status_code != 200:
                print(f"  ⚠ Erreur S3: {result_response.status_code}")
                return []

            employees = result_response.json()
            return self._parse_employees(employees)

        except Exception as e:
            print(f"  ✗ Erreur output: {e}")
            return []

    def _parse_employees(self, data) -> List[dict]:
        """Parse les données en liste d'employés."""
        if isinstance(data, list):
            employees = data
        elif isinstance(data, dict):
            employees = data.get("employees", data.get("results", [data]))
        else:
            return []

        valid_employees = []
        for emp in employees:
            profile_url = emp.get("profileUrl") or emp.get("linkedInProfileUrl") or emp.get("linkedin_url")
            name = emp.get("name") or emp.get("fullName") or f"{emp.get('firstName', '')} {emp.get('lastName', '')}".strip()

            if profile_url and name:
                valid_employees.append({
                    "name": name,
                    "firstName": emp.get("firstName", ""),
                    "lastName": emp.get("lastName", ""),
                    "title": emp.get("job") or emp.get("title") or emp.get("headline", ""),
                    "linkedin_url": profile_url,
                    "location": emp.get("location", ""),
                    "company_query": emp.get("query", ""),
                })

        return valid_employees


def extract_employees_from_linkedin(linkedin_url: str, agent_id: str = None) -> list:
    """
    Extrait les employés d'une entreprise via Phantombuster.

    Args:
        linkedin_url: URL LinkedIn de l'entreprise
        agent_id: ID de l'agent (optionnel)

    Returns:
        Liste des employés
    """
    client = PhantombusterClient(agent_id=agent_id)

    # Lancer l'agent
    container_id = client.launch_agent(linkedin_url)
    if not container_id:
        return []

    # Attendre la fin
    if not client.wait_for_completion(container_id):
        return []

    # Récupérer les résultats
    employees = client.get_output()
    return employees


def extract_employees_batch(companies: List[dict], max_workers: int = 2) -> Dict[str, dict]:
    """
    Extrait les employés de plusieurs entreprises.

    Args:
        companies: Liste des entreprises
        max_workers: Nombre de workers parallèles

    Returns:
        Dict {siren: {"company": company, "employees": [...]}}
    """
    # Filtrer les entreprises avec LinkedIn
    companies_with_linkedin = [c for c in companies if c.get("linkedin_url")]

    if not companies_with_linkedin:
        print("Aucune entreprise avec URL LinkedIn")
        return {}

    print(f"\n🚀 Extraction de {len(companies_with_linkedin)} entreprises...")

    results = {}

    for company in companies_with_linkedin:
        linkedin_url = company.get("linkedin_url")
        siren = company.get("siren")
        nom = company.get("nom", "?")

        print(f"\n📍 {nom}")
        employees = extract_employees_from_linkedin(linkedin_url)

        if employees:
            print(f"  ✓ {len(employees)} employés trouvés")
        else:
            print(f"  ⚠ Aucun employé trouvé")
            employees = []

        results[siren] = {"company": company, "employees": employees}

        # Pause entre les appels
        time.sleep(2)

    total_employees = sum(len(d["employees"]) for d in results.values())
    print(f"\n✅ Total: {total_employees} employés extraits de {len(results)} entreprises")

    return results


if __name__ == "__main__":
    print("Test de l'API Phantombuster...")

    if not config.PHANTOMBUSTER_AGENT_ID:
        print("ERREUR: PHANTOMBUSTER_AGENT_ID non configuré")
        print("Instructions:")
        print("1. Allez sur https://phantombuster.com")
        print("2. Ouvrez votre agent LinkedIn Company Employees")
        print("3. Copiez l'ID depuis l'URL (après /phantoms/)")
    else:
        client = PhantombusterClient()
        print(f"Agent ID: {client.agent_id}")
        print(f"API Key: {client.api_key[:10]}...")
