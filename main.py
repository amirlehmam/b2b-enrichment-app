"""
Orchestrateur principal du workflow d'enrichissement B2B

Pipeline complet:
1. Pappers → Récupérer entreprises ciblées
2. Enrich CRM → Trouver URLs LinkedIn entreprises
3. CSV → Sauvegarder les entreprises
4. Phantombuster → Extraire employés LinkedIn
5. Claude → Filtrer les décideurs (2-3 par entreprise)
6. Captely → Enrichir avec emails et téléphones
7. CSV → Exporter les contacts enrichis
8. Google Sheets → Synchroniser entreprises et contacts
9. Emelia → Envoyer vers campagne LinkedIn/Email
"""
import argparse
import time
from datetime import datetime

import config
from services.pappers import get_target_companies
from services.enrich_crm import enrich_companies_with_linkedin
from services.phantombuster import extract_employees_from_linkedin, extract_employees_batch
from services.claude_filter import filter_employees_for_company
from services.captely import enrich_contacts_with_captely
from services.csv_export import (
    export_companies,
    export_enriched_contacts,
    read_companies_csv,
)
from services.google_sheets import (
    sync_companies_to_sheets,
    sync_contacts_to_sheets,
)
from services.emelia import send_contacts_to_emelia


def run_step_1_pappers(max_companies: int = None) -> list:
    """Étape 1: Récupérer les entreprises via Pappers"""
    print("\n" + "=" * 60)
    print("ÉTAPE 1: Récupération des entreprises (Pappers)")
    print("=" * 60)

    companies = get_target_companies(max_results=max_companies)
    print(f"\n✓ {len(companies)} entreprises récupérées")

    return companies


def run_step_2_linkedin(companies: list) -> list:
    """Étape 2: Enrichir avec URLs LinkedIn via Enrich CRM"""
    print("\n" + "=" * 60)
    print("ÉTAPE 2: Récupération des URLs LinkedIn (Enrich CRM)")
    print("=" * 60)

    enriched = enrich_companies_with_linkedin(companies)

    with_linkedin = [c for c in enriched if c.get("linkedin_url")]
    print(f"\n✓ {len(with_linkedin)}/{len(enriched)} entreprises avec LinkedIn")

    return enriched


def run_step_3_save_companies(companies: list) -> str:
    """Étape 3: Sauvegarder les entreprises en CSV"""
    print("\n" + "=" * 60)
    print("ÉTAPE 3: Sauvegarde des entreprises (CSV)")
    print("=" * 60)

    filepath = export_companies(companies)
    print(f"✓ Fichier sauvegardé: {filepath}")

    return filepath


def run_step_4_phantombuster(companies: list) -> dict:
    """Étape 4: Extraire les employés via Phantombuster (OPTIMISÉ - PARALLÈLE)"""
    print("\n" + "=" * 60)
    print("ÉTAPE 4: Extraction des employés (Phantombuster - Mode Rapide)")
    print("=" * 60)

    if not config.PHANTOMBUSTER_AGENT_ID:
        print("⚠ PHANTOMBUSTER_AGENT_ID non configuré!")
        print("  Configurez l'ID dans config.py ou via variable d'environnement")
        return {}

    # Utiliser l'extraction BATCH parallèle (10x plus rapide!)
    company_employees = extract_employees_batch(companies, max_workers=3)

    print(f"\n✓ Employés extraits pour {len(company_employees)} entreprises")
    return company_employees


def run_step_5_filter_decision_makers(company_employees: dict) -> list:
    """Étape 5: Filtrer les décideurs via Claude"""
    print("\n" + "=" * 60)
    print("ÉTAPE 5: Filtrage des décideurs (Claude AI)")
    print("=" * 60)

    all_decision_makers = []

    for siren, data in company_employees.items():
        company = data["company"]
        employees = data["employees"]

        if not employees:
            continue

        print(f"\nFiltrage pour: {company['nom']}")
        decision_makers = filter_employees_for_company(employees, company["nom"])

        # Ajouter les infos entreprise à chaque décideur
        for dm in decision_makers:
            dm["entreprise"] = company["nom"]
            dm["siren"] = siren
            all_decision_makers.append(dm)

    print(f"\n✓ {len(all_decision_makers)} décideurs identifiés au total")
    return all_decision_makers


def run_step_6_enrich_contacts(decision_makers: list) -> list:
    """Étape 6: Enrichir les contacts via Captely"""
    print("\n" + "=" * 60)
    print("ÉTAPE 6: Enrichissement des contacts (Captely)")
    print("=" * 60)

    enriched = enrich_contacts_with_captely(decision_makers)

    with_email = [c for c in enriched if c.get("email")]
    with_phone = [c for c in enriched if c.get("phone")]

    print(f"\n✓ {len(with_email)} contacts avec email")
    print(f"✓ {len(with_phone)} contacts avec téléphone")

    return enriched


def run_step_7_export(contacts: list) -> str:
    """Étape 7: Exporter les contacts enrichis"""
    print("\n" + "=" * 60)
    print("ÉTAPE 7: Export final (CSV)")
    print("=" * 60)

    filepath = export_enriched_contacts(contacts)
    print(f"✓ Fichier exporté: {filepath}")

    return filepath


def run_step_8_google_sheets(companies: list, contacts: list) -> dict:
    """Étape 8: Synchroniser vers Google Sheets"""
    print("\n" + "=" * 60)
    print("ÉTAPE 8: Synchronisation Google Sheets")
    print("=" * 60)

    results = {"companies": 0, "contacts": 0}

    if not config.GOOGLE_SHEETS_SPREADSHEET_ID:
        print("⚠ GOOGLE_SHEETS_SPREADSHEET_ID non configuré - étape sautée")
        return results

    if not config.GOOGLE_SHEETS_CREDENTIALS:
        print("⚠ GOOGLE_SHEETS_CREDENTIALS non configuré - étape sautée")
        return results

    try:
        # Sync entreprises
        if companies:
            results["companies"] = sync_companies_to_sheets(companies)

        # Sync contacts
        if contacts:
            results["contacts"] = sync_contacts_to_sheets(contacts)

        print(f"\n✓ {results['companies']} entreprises synchronisées")
        print(f"✓ {results['contacts']} contacts synchronisés")

    except Exception as e:
        print(f"❌ Erreur Google Sheets: {e}")

    return results


def run_step_9_emelia(contacts: list) -> dict:
    """Étape 9: Envoyer vers campagne Emelia"""
    print("\n" + "=" * 60)
    print("ÉTAPE 9: Envoi vers campagne Emelia")
    print("=" * 60)

    if not config.EMELIA_API_KEY:
        print("⚠ EMELIA_API_KEY non configuré - étape sautée")
        return {"success": 0, "failed": 0}

    if not config.EMELIA_CAMPAIGN_ID:
        print("⚠ EMELIA_CAMPAIGN_ID non configuré - étape sautée")
        return {"success": 0, "failed": 0}

    # Filtrer les contacts avec email (requis pour Emelia)
    contacts_with_email = [c for c in contacts if c.get("email")]

    if not contacts_with_email:
        print("⚠ Aucun contact avec email - rien à envoyer")
        return {"success": 0, "failed": 0}

    print(f"📧 {len(contacts_with_email)} contacts avec email à envoyer")

    results = send_contacts_to_emelia(contacts_with_email)

    print(f"\n✓ {results.get('success', 0)} contacts envoyés vers Emelia")
    if results.get('failed', 0) > 0:
        print(f"⚠ {results.get('failed', 0)} échecs")

    return results


def run_full_pipeline(max_companies: int = None, skip_phantombuster: bool = False):
    """
    Exécute le pipeline complet.

    Args:
        max_companies: Limite le nombre d'entreprises (pour tests)
        skip_phantombuster: Saute l'extraction Phantombuster (utilise les dirigeants Pappers)
    """
    start_time = datetime.now()
    print("\n" + "#" * 60)
    print("# WORKFLOW ENRICHISSEMENT B2B")
    print(f"# Démarré le: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("#" * 60)

    # Étape 1: Pappers
    companies = run_step_1_pappers(max_companies)
    if not companies:
        print("❌ Aucune entreprise trouvée. Arrêt.")
        return

    # Étape 2: LinkedIn URLs
    companies = run_step_2_linkedin(companies)

    # Étape 3: Sauvegarder
    run_step_3_save_companies(companies)

    if skip_phantombuster:
        # Mode simplifié: utiliser les dirigeants de Pappers
        print("\n⚠ Mode simplifié: utilisation des dirigeants Pappers")
        all_decision_makers = []
        for company in companies:
            for dirigeant in company.get("dirigeants", []):
                all_decision_makers.append({
                    "name": dirigeant.get("nom"),
                    "title": dirigeant.get("qualite"),
                    "entreprise": company["nom"],
                    "siren": company["siren"],
                    "persona_type": "Dirigeant",
                })
    else:
        # Étape 4: Phantombuster
        company_employees = run_step_4_phantombuster(companies)

        # Étape 5: Filtrage Claude
        all_decision_makers = run_step_5_filter_decision_makers(company_employees)

    if not all_decision_makers:
        print("❌ Aucun décideur trouvé. Arrêt.")
        return

    # Étape 6: Enrichissement Captely
    enriched_contacts = run_step_6_enrich_contacts(all_decision_makers)

    # Étape 7: Export final CSV
    output_file = run_step_7_export(enriched_contacts)

    # Étape 8: Google Sheets (optionnel)
    sheets_results = run_step_8_google_sheets(companies, enriched_contacts)

    # Étape 9: Emelia (optionnel)
    emelia_results = run_step_9_emelia(enriched_contacts)

    # Résumé
    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "#" * 60)
    print("# RÉSUMÉ")
    print("#" * 60)
    print(f"Entreprises traitées: {len(companies)}")
    print(f"Décideurs identifiés: {len(all_decision_makers)}")
    print(f"Contacts enrichis: {len(enriched_contacts)}")
    print(f"Fichier output: {output_file}")
    print(f"Google Sheets: {sheets_results.get('contacts', 0)} contacts sync")
    print(f"Emelia: {emelia_results.get('success', 0)} contacts envoyés")
    print(f"Durée totale: {duration}")
    print("#" * 60)


def main():
    parser = argparse.ArgumentParser(description="Workflow d'enrichissement B2B")

    parser.add_argument(
        "--max-companies",
        type=int,
        default=None,
        help="Limite le nombre d'entreprises (pour tests)"
    )

    parser.add_argument(
        "--skip-phantombuster",
        action="store_true",
        help="Saute Phantombuster, utilise les dirigeants Pappers"
    )

    parser.add_argument(
        "--step",
        type=int,
        choices=[1, 2, 3, 4, 5, 6, 7],
        help="Exécute uniquement une étape spécifique"
    )

    parser.add_argument(
        "--from-csv",
        type=str,
        help="Reprend depuis un fichier CSV d'entreprises existant"
    )

    args = parser.parse_args()

    # Vérifications de configuration
    print("Vérification de la configuration...")

    if not config.PAPPERS_API_KEY:
        print("❌ PAPPERS_API_KEY non configuré")
        return

    if not config.PHANTOMBUSTER_AGENT_ID and not args.skip_phantombuster:
        print("⚠ PHANTOMBUSTER_AGENT_ID non configuré")
        print("  Utilisez --skip-phantombuster ou configurez l'ID")

    # Exécution
    if args.from_csv:
        companies = read_companies_csv(args.from_csv)
        print(f"Chargé {len(companies)} entreprises depuis {args.from_csv}")
        # Continuer le pipeline depuis l'étape 4
        # ...
    else:
        run_full_pipeline(
            max_companies=args.max_companies,
            skip_phantombuster=args.skip_phantombuster
        )


if __name__ == "__main__":
    main()
