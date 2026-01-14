"""
Filtrage IA des décideurs via Claude API
"""
import json
from anthropic import Anthropic
import config


class ClaudeFilter:
    """Filtre les employés pour identifier les décideurs clés"""

    def __init__(self, api_key: str = None):
        self.client = Anthropic(api_key=api_key or config.CLAUDE_API_KEY)
        self.target_personas = config.TARGET_PERSONAS

    def filter_decision_makers(self, employees: list, company_name: str, max_personas: int = 3) -> list:
        """
        Identifie les décideurs clés parmi une liste d'employés.

        Args:
            employees: Liste d'employés (nom, titre, linkedin_url)
            company_name: Nom de l'entreprise
            max_personas: Nombre max de décideurs à retourner

        Returns:
            Liste des décideurs identifiés
        """
        if not employees:
            return []

        # Préparer la liste des employés pour le prompt
        employees_text = "\n".join([
            f"- {e.get('name', e.get('nom', 'N/A'))}: {e.get('title', e.get('titre', 'N/A'))} ({e.get('linkedin_url', e.get('profileUrl', 'N/A'))})"
            for e in employees[:50]  # Limiter pour le contexte
        ])

        personas_text = ", ".join(self.target_personas)

        prompt = f"""Analyse cette liste d'employés de l'entreprise "{company_name}" et identifie les {max_personas} décideurs clés les plus pertinents.

Personas cibles: {personas_text}

Liste des employés:
{employees_text}

Instructions:
1. Sélectionne uniquement les profils correspondant aux personas cibles
2. Priorise: CEO/Gérant > DSI/CTO > DRH > DAF > Autres directeurs
3. Retourne EXACTEMENT au format JSON suivant, sans texte additionnel:

[
  {{"name": "Prénom Nom", "title": "Titre", "linkedin_url": "URL", "persona_type": "CEO|DSI|DRH|DAF|Autre"}},
  ...
]

Si aucun décideur n'est trouvé, retourne: []"""

        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1000,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )

            # Parser la réponse JSON
            content = response.content[0].text.strip()

            # Nettoyer le markdown si présent
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
                content = content.strip()

            decision_makers = json.loads(content)
            return decision_makers[:max_personas]

        except json.JSONDecodeError as e:
            print(f"Erreur parsing JSON: {e}")
            print(f"Réponse: {content}")
            return []
        except Exception as e:
            print(f"Erreur Claude API: {e}")
            return []


def filter_employees_for_company(employees: list, company_name: str) -> list:
    """
    Filtre les employés d'une entreprise pour identifier les décideurs.

    Args:
        employees: Liste des employés
        company_name: Nom de l'entreprise

    Returns:
        Liste des décideurs (2-3 par entreprise)
    """
    filter_client = ClaudeFilter()
    decision_makers = filter_client.filter_decision_makers(employees, company_name)

    print(f"  {len(decision_makers)} décideur(s) identifié(s) pour {company_name}")
    for dm in decision_makers:
        print(f"    - {dm.get('name')}: {dm.get('title')} ({dm.get('persona_type')})")

    return decision_makers


if __name__ == "__main__":
    print("Test du filtre Claude...")

    # Données de test
    test_employees = [
        {"name": "Jean Dupont", "title": "CEO & Founder", "linkedin_url": "https://linkedin.com/in/jean-dupont"},
        {"name": "Marie Martin", "title": "Software Engineer", "linkedin_url": "https://linkedin.com/in/marie-martin"},
        {"name": "Pierre Bernard", "title": "Directeur Technique", "linkedin_url": "https://linkedin.com/in/pierre-bernard"},
        {"name": "Sophie Leroy", "title": "Marketing Manager", "linkedin_url": "https://linkedin.com/in/sophie-leroy"},
        {"name": "François Moreau", "title": "DRH", "linkedin_url": "https://linkedin.com/in/francois-moreau"},
    ]

    decision_makers = filter_employees_for_company(test_employees, "Entreprise Test")
    print(f"\nRésultat: {json.dumps(decision_makers, indent=2, ensure_ascii=False)}")
