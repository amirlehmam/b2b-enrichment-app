"""
Client API Phantombuster pour l'extraction des employés LinkedIn
Documentation: https://hub.phantombuster.com/docs/api
"""
import requests
import time
from typing import Optional
import config


class PhantombusterClient:
    """Client pour l'API Phantombuster"""

    def __init__(self, api_key: str = None, agent_id: str = None):
        self.api_key = api_key or config.PHANTOMBUSTER_API_KEY
        self.agent_id = agent_id or config.PHANTOMBUSTER_AGENT_ID
        self.base_url = config.PHANTOMBUSTER_BASE_URL

    def _headers(self) -> dict:
        return {
            "X-Phantombuster-Key-1": self.api_key,
            "Content-Type": "application/json",
        }

    def launch_agent(self, linkedin_company_url: str) -> Optional[str]:
        """
        Lance un agent Phantombuster pour extraire les employés d'une entreprise.

        Args:
            linkedin_company_url: URL LinkedIn de l'entreprise

        Returns:
            ID du container (pour suivre l'exécution) ou None si erreur
        """
        if not self.agent_id:
            raise ValueError("PHANTOMBUSTER_AGENT_ID non configuré. Voir config.py")

        payload = {
            "id": self.agent_id,
            "argument": {
                "spreadsheetUrl": linkedin_company_url,
                # Paramètres typiques pour LinkedIn Company Employees
                "numberOfEmployeesToExtract": 100,
            }
        }

        try:
            response = requests.post(
                f"{self.base_url}/agents/launch",
                headers=self._headers(),
                json=payload
            )
            response.raise_for_status()
            data = response.json()

            container_id = data.get("containerId")
            print(f"Agent lancé, container ID: {container_id}")
            return container_id

        except requests.exceptions.RequestException as e:
            print(f"Erreur lancement agent: {e}")
            return None

    def get_agent_output(self, container_id: str = None) -> Optional[list]:
        """
        Récupère les résultats d'un agent depuis le fichier S3.

        Args:
            container_id: ID du container (optionnel)

        Returns:
            Liste des employés extraits
        """
        if not self.agent_id:
            raise ValueError("PHANTOMBUSTER_AGENT_ID non configuré")

        try:
            # D'abord récupérer les infos de l'agent pour avoir le s3Folder
            response = requests.get(
                f"{self.base_url}/agents/fetch",
                headers=self._headers(),
                params={"id": self.agent_id}
            )
            response.raise_for_status()
            agent_data = response.json()

            s3_folder = agent_data.get("s3Folder")
            org_s3_folder = agent_data.get("orgS3Folder")

            if not s3_folder or not org_s3_folder:
                print("Impossible de trouver le dossier S3")
                return []

            # Récupérer le JSON depuis S3
            result_url = f"https://phantombuster.s3.amazonaws.com/{org_s3_folder}/{s3_folder}/result.json"
            result_response = requests.get(result_url)
            result_response.raise_for_status()

            employees = result_response.json()

            # Filtrer les erreurs et formater
            valid_employees = []
            for emp in employees:
                if emp.get("profileUrl") and emp.get("name"):
                    valid_employees.append({
                        "name": emp.get("name"),
                        "firstName": emp.get("firstName"),
                        "lastName": emp.get("lastName"),
                        "title": emp.get("job", ""),
                        "linkedin_url": emp.get("profileUrl"),
                        "location": emp.get("location", ""),
                        "company_query": emp.get("query", ""),
                    })

            return valid_employees

        except requests.exceptions.RequestException as e:
            print(f"Erreur récupération output: {e}")
            return None

    def get_agent_status(self) -> dict:
        """Récupère le statut de l'agent"""
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

    def wait_for_completion(self, container_id: str, timeout: int = 300, poll_interval: int = 10) -> bool:
        """
        Attend la fin de l'exécution d'un agent.

        Args:
            container_id: ID du container
            timeout: Timeout en secondes
            poll_interval: Intervalle de polling en secondes

        Returns:
            True si terminé avec succès, False sinon
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            status = self.get_agent_status()
            launch_status = status.get("lastEndMessage", "")

            if "finished" in launch_status.lower():
                print("Agent terminé avec succès")
                return True
            elif "error" in launch_status.lower():
                print(f"Agent terminé avec erreur: {launch_status}")
                return False

            print(f"Agent en cours... (attente {poll_interval}s)")
            time.sleep(poll_interval)

        print("Timeout atteint")
        return False


def extract_employees_from_linkedin(linkedin_url: str, agent_id: str = None) -> list:
    """
    Extrait les employés d'une entreprise via Phantombuster.

    Args:
        linkedin_url: URL LinkedIn de l'entreprise
        agent_id: ID de l'agent (optionnel, utilise config si non fourni)

    Returns:
        Liste des employés avec leurs profils LinkedIn
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
    employees = client.get_agent_output()
    return employees or []


if __name__ == "__main__":
    print("Test de l'API Phantombuster...")

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
        print(f"Statut agent: {status}")
