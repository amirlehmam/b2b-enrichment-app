"""
Client API Phantombuster pour l'extraction des employés LinkedIn
Documentation: https://hub.phantombuster.com/reference/post_agents-launch

API v2 - Le body JSON doit contenir:
- id: agent ID
- argument: objet ou string JSON avec les paramètres

IMPORTANT: L'agent doit être configuré sur Phantombuster avec:
- Session cookie LinkedIn (li_at)
- Paramètres par défaut
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

    def get_agent_info(self) -> dict:
        """Récupère les infos de l'agent pour debug."""
        try:
            response = requests.get(
                f"{self.base_url}/agents/fetch",
                headers=self._headers(),
                params={"id": self.agent_id},
                timeout=10
            )
            if response.status_code == 200:
                return response.json()
            return {}
        except:
            return {}

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

        # L'argument peut être passé en JSON string ou objet
        # On utilise bonusArgument pour fusionner avec la config existante de l'agent
        # Cela permet de garder le sessionCookie configuré dans l'agent
        argument_obj = {
            "spreadsheetUrl": linkedin_company_url,
            "numberOfEmployeesToExtract": 50,
        }

        # Si un sessionCookie est configuré dans config.py, l'ajouter
        linkedin_cookie = getattr(config, 'LINKEDIN_SESSION_COOKIE', None)
        if linkedin_cookie:
            argument_obj["sessionCookie"] = linkedin_cookie
            print(f"  [DEBUG] Session cookie ajouté depuis config")

        body = {
            "id": self.agent_id,
            "bonusArgument": argument_obj,  # Fusionne avec l'argument par défaut (garde sessionCookie)
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
            print(f"  [DEBUG] Response body: {response.text[:500]}")

            if response.status_code == 200:
                data = response.json()
                container_id = data.get("containerId")
                print(f"  ✓ Agent lancé, container: {container_id}")
                return container_id
            else:
                print(f"  ✗ Erreur HTTP {response.status_code}: {response.text[:200]}")
                # Si erreur 400, afficher plus de détails
                if response.status_code == 400:
                    print(f"  [ERREUR] Vérifiez que l'agent est configuré avec un sessionCookie LinkedIn valide")
                return None

        except requests.exceptions.RequestException as e:
            print(f"  ✗ Erreur requête: {e}")
            return None

    def wait_for_completion(self, container_id: str = None, timeout: int = 300, poll_interval: int = 10) -> bool:
        """
        Attend la fin de l'exécution d'un agent via fetch-output.

        Args:
            container_id: ID du container (optionnel, utilise agent_id sinon)
            timeout: Timeout en secondes
            poll_interval: Intervalle de polling en secondes

        Returns:
            True si terminé avec succès
        """
        start_time = time.time()
        last_status = None

        while time.time() - start_time < timeout:
            try:
                # Utiliser fetch-output qui est fait pour le polling
                response = requests.get(
                    f"{self.base_url}/agents/fetch-output",
                    headers=self._headers(),
                    params={"id": self.agent_id},
                    timeout=15
                )

                if response.status_code == 200:
                    data = response.json()
                    status = data.get("status")
                    progress = data.get("progress", 0)
                    progress_label = data.get("progressLabel", "")
                    is_running = data.get("isAgentRunning", False)

                    if status != last_status:
                        print(f"  [STATUS] {status} - {progress_label or progress}")
                        last_status = status

                    if status == "finished":
                        print(f"  ✓ Agent terminé!")
                        return True
                    elif status == "launch error":
                        print(f"  ✗ Erreur de lancement")
                        output = data.get("output", "")
                        if output:
                            print(f"  [OUTPUT] {output[:500]}")
                        return False
                    elif not is_running and status not in ["starting", "running"]:
                        # Agent pas en cours et pas fini = problème
                        if status == "never launched":
                            print(f"  ⚠ Agent jamais lancé - vérifiez la configuration")
                        return False

                time.sleep(poll_interval)

            except Exception as e:
                print(f"  ⚠ Erreur polling: {e}")
                time.sleep(poll_interval)

        print(f"  ✗ Timeout après {timeout}s")
        return False

    def get_output(self) -> List[dict]:
        """
        Récupère les résultats de l'agent.

        Essaie d'abord fetch-output, puis S3 en fallback.

        Returns:
            Liste des employés
        """
        try:
            # Méthode 1: Via fetch-output (contient parfois le résultat directement)
            response = requests.get(
                f"{self.base_url}/agents/fetch-output",
                headers=self._headers(),
                params={"id": self.agent_id},
                timeout=15
            )

            if response.status_code == 200:
                data = response.json()
                output = data.get("output", "")
                print(f"  [DEBUG] fetch-output status: {data.get('status')}")

                # Essayer de parser le JSON du output si présent
                if output and "[" in output:
                    try:
                        # Chercher un array JSON dans le output
                        start_idx = output.find("[")
                        end_idx = output.rfind("]") + 1
                        if start_idx >= 0 and end_idx > start_idx:
                            json_str = output[start_idx:end_idx]
                            employees = json.loads(json_str)
                            if employees:
                                print(f"  ✓ {len(employees)} employés trouvés via output")
                                return self._parse_employees(employees)
                    except json.JSONDecodeError:
                        pass

            # Méthode 2: Récupérer depuis S3 (result.json)
            print(f"  → Récupération depuis S3...")
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

            print(f"  [DEBUG] S3 folders: org={org_s3_folder}, agent={s3_folder}")

            if not s3_folder or not org_s3_folder:
                print("  ⚠ S3 folder non trouvé")
                return []

            # Essayer result.json
            result_url = f"https://phantombuster.s3.amazonaws.com/{org_s3_folder}/{s3_folder}/result.json"
            print(f"  [DEBUG] S3 URL: {result_url}")

            result_response = requests.get(result_url, timeout=30)

            if result_response.status_code == 200:
                employees = result_response.json()
                print(f"  ✓ {len(employees) if isinstance(employees, list) else 'N/A'} depuis S3")
                return self._parse_employees(employees)
            else:
                print(f"  ⚠ Erreur S3 result.json: {result_response.status_code}")

            # Essayer database-result.csv si result.json échoue
            csv_url = f"https://phantombuster.s3.amazonaws.com/{org_s3_folder}/{s3_folder}/database-result.csv"
            csv_response = requests.get(csv_url, timeout=30)

            if csv_response.status_code == 200:
                print(f"  → Parsing CSV...")
                return self._parse_csv_employees(csv_response.text)

            print(f"  ⚠ Aucun résultat trouvé")
            return []

        except Exception as e:
            print(f"  ✗ Erreur output: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _parse_csv_employees(self, csv_text: str) -> List[dict]:
        """Parse les employés depuis un CSV."""
        employees = []
        lines = csv_text.strip().split("\n")

        if len(lines) < 2:
            return []

        # Header
        headers = [h.strip().strip('"') for h in lines[0].split(",")]
        print(f"  [DEBUG] CSV headers: {headers[:10]}")

        for line in lines[1:]:
            try:
                # Simple CSV parsing (peut échouer si virgules dans les valeurs)
                values = [v.strip().strip('"') for v in line.split(",")]
                row = dict(zip(headers, values))

                profile_url = row.get("profileUrl") or row.get("linkedInProfileUrl") or row.get("linkedin")
                name = row.get("name") or row.get("fullName") or f"{row.get('firstName', '')} {row.get('lastName', '')}".strip()

                if profile_url and name:
                    employees.append({
                        "name": name,
                        "firstName": row.get("firstName", ""),
                        "lastName": row.get("lastName", ""),
                        "title": row.get("job") or row.get("title") or row.get("headline", ""),
                        "linkedin_url": profile_url,
                        "location": row.get("location", ""),
                    })
            except:
                continue

        return employees

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

    # Debug: afficher info agent
    print(f"  [DEBUG] Vérification agent...")
    agent_info = client.get_agent_info()
    if agent_info:
        print(f"  [DEBUG] Agent trouvé: {agent_info.get('name', 'N/A')}")
    else:
        print(f"  ⚠ Impossible de récupérer info agent - vérifiez API key et agent ID")

    # Lancer l'agent
    container_id = client.launch_agent(linkedin_url)
    if not container_id:
        print(f"  ⚠ Échec lancement agent - voir erreurs ci-dessus")
        return []

    # Attendre la fin (utilise l'agent ID, pas le container)
    if not client.wait_for_completion(container_id):
        print(f"  ⚠ Agent n'a pas terminé correctement")
        # Essayer quand même de récupérer les résultats
        pass

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
