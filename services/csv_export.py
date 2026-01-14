"""
Export des données vers CSV
"""
import csv
import os
from datetime import datetime
import config


def ensure_output_dir():
    """Crée le répertoire output s'il n'existe pas"""
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)


def export_companies(companies: list, filename: str = None) -> str:
    """
    Exporte les entreprises vers un fichier CSV.

    Args:
        companies: Liste des entreprises
        filename: Nom du fichier (optionnel)

    Returns:
        Chemin du fichier créé
    """
    ensure_output_dir()
    filepath = filename or config.COMPANIES_CSV

    fieldnames = [
        "siren", "siret", "nom", "forme_juridique", "effectif",
        "adresse", "code_naf", "activite", "convention_collective",
        "linkedin_url", "dirigeants"
    ]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for company in companies:
            # Convertir la liste des dirigeants en string
            row = company.copy()
            if "dirigeants" in row and isinstance(row["dirigeants"], list):
                row["dirigeants"] = "; ".join([
                    f"{d.get('nom', '')} ({d.get('qualite', '')})"
                    for d in row["dirigeants"]
                ])
            writer.writerow(row)

    print(f"Exporté {len(companies)} entreprises vers {filepath}")
    return filepath


def export_contacts(contacts: list, filename: str = None) -> str:
    """
    Exporte les contacts/décideurs vers un fichier CSV.

    Args:
        contacts: Liste des contacts
        filename: Nom du fichier (optionnel)

    Returns:
        Chemin du fichier créé
    """
    ensure_output_dir()
    filepath = filename or config.CONTACTS_CSV

    fieldnames = [
        "entreprise", "siren", "name", "title", "persona_type",
        "linkedin_url", "email", "email_verified", "phone"
    ]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for contact in contacts:
            writer.writerow(contact)

    print(f"Exporté {len(contacts)} contacts vers {filepath}")
    return filepath


def export_enriched_contacts(contacts: list, filename: str = None) -> str:
    """
    Exporte les contacts enrichis vers un fichier CSV prêt pour Emilia.

    Args:
        contacts: Liste des contacts enrichis
        filename: Nom du fichier (optionnel)

    Returns:
        Chemin du fichier créé
    """
    ensure_output_dir()

    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{config.OUTPUT_DIR}/enriched_contacts_{timestamp}.csv"

    # Format adapté pour Emilia / campagnes LinkedIn
    fieldnames = [
        "first_name", "last_name", "full_name", "title", "company",
        "linkedin_url", "email", "phone", "persona_type", "siren"
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for contact in contacts:
            # Parser le nom complet
            full_name = contact.get("name", "")
            parts = full_name.split(" ", 1)
            first_name = parts[0] if parts else ""
            last_name = parts[1] if len(parts) > 1 else ""

            row = {
                "first_name": first_name,
                "last_name": last_name,
                "full_name": full_name,
                "title": contact.get("title", ""),
                "company": contact.get("entreprise", ""),
                "linkedin_url": contact.get("linkedin_url", ""),
                "email": contact.get("email", ""),
                "phone": contact.get("phone", ""),
                "persona_type": contact.get("persona_type", ""),
                "siren": contact.get("siren", ""),
            }
            writer.writerow(row)

    print(f"Exporté {len(contacts)} contacts enrichis vers {filename}")
    return filename


def read_companies_csv(filepath: str = None) -> list:
    """Lit un fichier CSV d'entreprises"""
    filepath = filepath or config.COMPANIES_CSV

    if not os.path.exists(filepath):
        return []

    companies = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            companies.append(dict(row))

    return companies


def read_contacts_csv(filepath: str = None) -> list:
    """Lit un fichier CSV de contacts"""
    filepath = filepath or config.CONTACTS_CSV

    if not os.path.exists(filepath):
        return []

    contacts = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            contacts.append(dict(row))

    return contacts


if __name__ == "__main__":
    print("Test export CSV...")

    # Données de test
    test_companies = [
        {
            "siren": "123456789",
            "nom": "Entreprise Test",
            "forme_juridique": "SAS",
            "effectif": 50,
            "linkedin_url": "https://linkedin.com/company/test",
        }
    ]

    test_contacts = [
        {
            "entreprise": "Entreprise Test",
            "siren": "123456789",
            "name": "Jean Dupont",
            "title": "CEO",
            "linkedin_url": "https://linkedin.com/in/jean-dupont",
            "email": "jean@test.com",
            "phone": "+33612345678",
            "persona_type": "CEO",
        }
    ]

    export_companies(test_companies)
    export_enriched_contacts(test_contacts)
    print("Export réussi!")
