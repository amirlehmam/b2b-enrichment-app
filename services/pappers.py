"""
Client API Pappers pour la recherche d'entreprises
Documentation: https://www.pappers.fr/api/documentation
"""
import requests
from typing import Generator
import config


class PappersClient:
    """Client pour l'API Pappers"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or config.PAPPERS_API_KEY
        self.base_url = config.PAPPERS_BASE_URL

    def search_companies(
        self,
        convention_collective: str = None,
        tranche_effectif: str = None,
        categorie_juridique: str = None,
        entreprise_cessee: str = "false",
        par_page: int = 100,
        max_results: int = None
    ) -> Generator[dict, None, None]:
        """
        Recherche d'entreprises avec filtres.

        Args:
            convention_collective: Code convention (ex: "0045" pour filtrer 0045*)
            tranche_effectif: Codes tranches effectif séparés par virgule
            categorie_juridique: Codes catégories juridiques séparés par virgule
            entreprise_cessee: "false" pour entreprises actives uniquement
            par_page: Nombre de résultats par page (max 100)
            max_results: Limite totale de résultats (None = tous)

        Yields:
            dict: Données de chaque entreprise
        """
        params = {
            "api_token": self.api_key,
            "par_page": min(par_page, 100),
            "entreprise_cessee": entreprise_cessee,
        }

        if convention_collective:
            params["convention_collective"] = convention_collective
        if tranche_effectif:
            params["tranche_effectif"] = tranche_effectif
        if categorie_juridique:
            params["categorie_juridique"] = categorie_juridique

        page = 1
        total_fetched = 0

        while True:
            params["page"] = page

            response = requests.get(
                f"{self.base_url}/recherche",
                params=params
            )
            response.raise_for_status()
            data = response.json()

            results = data.get("resultats", [])
            if not results:
                break

            for company in results:
                yield self._parse_company(company)
                total_fetched += 1

                if max_results and total_fetched >= max_results:
                    return

            # Vérifier s'il y a plus de pages
            total = data.get("total", 0)
            if page * par_page >= total:
                break

            page += 1

    def _parse_company(self, raw: dict) -> dict:
        """Parse les données brutes d'une entreprise"""
        return {
            "siren": raw.get("siren"),
            "siret": raw.get("siret_siege"),
            "nom": raw.get("nom_entreprise"),
            "forme_juridique": raw.get("forme_juridique"),
            "effectif": raw.get("effectif"),
            "tranche_effectif": raw.get("tranche_effectif"),
            "date_creation": raw.get("date_creation"),
            "adresse": self._format_address(raw),
            "code_naf": raw.get("code_naf"),
            "activite": raw.get("libelle_code_naf"),
            "convention_collective": raw.get("convention_collective"),
            "dirigeants": self._parse_dirigeants(raw.get("representants", [])),
        }

    def _format_address(self, raw: dict) -> str:
        """Formate l'adresse complète"""
        parts = [
            raw.get("siege", {}).get("adresse_ligne_1", ""),
            raw.get("siege", {}).get("code_postal", ""),
            raw.get("siege", {}).get("ville", ""),
        ]
        return " ".join(p for p in parts if p)

    def _parse_dirigeants(self, representants: list) -> list:
        """Parse la liste des dirigeants"""
        dirigeants = []
        for rep in representants:
            if rep.get("qualite"):  # Seulement les personnes avec un rôle
                dirigeants.append({
                    "nom": f"{rep.get('prenom', '')} {rep.get('nom', '')}".strip(),
                    "qualite": rep.get("qualite"),
                    "date_naissance": rep.get("date_de_naissance"),
                })
        return dirigeants

    def get_company_by_siren(self, siren: str) -> dict:
        """Récupère les détails d'une entreprise par SIREN"""
        response = requests.get(
            f"{self.base_url}/entreprise",
            params={
                "api_token": self.api_key,
                "siren": siren,
            }
        )
        response.raise_for_status()
        return self._parse_company(response.json())


def get_target_companies(max_results: int = None, fetch_details: bool = True) -> list:
    """
    Récupère les entreprises correspondant aux critères ICP.

    Args:
        max_results: Limite le nombre de résultats (pour tests)
        fetch_details: Si True, récupère les détails complets (dirigeants) pour chaque entreprise

    Returns:
        Liste des entreprises
    """
    client = PappersClient()
    filters = config.PAPPERS_FILTERS

    companies = list(client.search_companies(
        convention_collective=filters.get("convention_collective"),
        tranche_effectif=filters.get("tranche_effectif"),
        categorie_juridique=filters.get("categorie_juridique"),
        entreprise_cessee=filters.get("entreprise_cessee", "false"),
        par_page=filters.get("par_page", 100),
        max_results=max_results,
    ))

    # Récupérer les détails complets pour avoir les dirigeants
    if fetch_details:
        detailed_companies = []
        for company in companies:
            try:
                detailed = client.get_company_by_siren(company["siren"])
                detailed_companies.append(detailed)
                print(f"  Détails récupérés pour {company['nom']} ({len(detailed.get('dirigeants', []))} dirigeants)")
            except Exception as e:
                print(f"  Erreur détails pour {company['nom']}: {e}")
                detailed_companies.append(company)
        return detailed_companies

    return companies


if __name__ == "__main__":
    # Test rapide
    print("Test de l'API Pappers...")
    companies = get_target_companies(max_results=5)
    for c in companies:
        print(f"- {c['nom']} (SIREN: {c['siren']}) - {c['effectif']} salariés")
