
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict

import requests
from bs4 import BeautifulSoup  # noqa: F401  # importée si besoin d'analyse supplémentaire
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Constantes HTTP
# ---------------------------------------------------------------------------
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
}

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def extract_json_from_next(html: str) -> Dict[str, Any] | None:
    """Retourne le JSON contenu dans <script id="__NEXT_DATA__"> (si présent)."""
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None


def parse_company_data(next_data: Dict[str, Any]) -> Dict[str, Any]:
    """Sélectionne quelques champs utiles du JSON Next.js de Pappers."""
    entreprise = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("pageData", {})
        .get("entreprise", {})
    )
    siege = entreprise.get("siege", {})
    dirigeants = entreprise.get("dirigeants", []) or []
    return {
        "siren": entreprise.get("siren"),
        "denomination": entreprise.get("nom_entreprise"),
        "forme_juridique": entreprise.get("forme_juridique"),
        "date_creation": entreprise.get("date_creation"),
        "capital": entreprise.get("capital"),
        "effectif": entreprise.get("effectif"),
        "naf_code": entreprise.get("activite_principale"),
        "adresse_siege": siege.get("adresse_complete"),
        "dirigeants": "; ".join(d.get("nom") for d in dirigeants if d.get("nom")),
    }


# ---------------------------------------------------------------------------
# Scraping d'un SIREN
# ---------------------------------------------------------------------------

def scrape_siren(session: requests.Session, siren: str, sleep_time: float) -> Dict[str, Any] | None:
    """Retourne un dict de données pour un SIREN, ou None si introuvable."""
    url = f"https://www.pappers.fr/entreprise/{siren}"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = extract_json_from_next(resp.text)
        if not data:
            return None
        return parse_company_data(data)
    except requests.RequestException:
        return None
    finally:
        time.sleep(sleep_time)


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def csv_exists_and_has_header(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


def already_scraped(path: Path) -> set[str]:
    """Renvoie l'ensemble des SIREN déjà présents dans un CSV existant."""
    if not csv_exists_and_has_header(path):
        return set()
    with path.open(newline="", encoding="utf-8") as f:
        return {row["siren"] for row in csv.DictReader(f) if row.get("siren")}


# ---------------------------------------------------------------------------
# Programme principal
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scrape Pappers pour enrichir un JSONL de SIRET.")
    parser.add_argument("input", help="Fichier ND‑JSON d'entrée (1 objet par ligne).")
    parser.add_argument("output", help="CSV de sortie enrichi.")
    parser.add_argument("--sleep", type=float, default=0.5, help="Pause entre requêtes (sec).")
    parser.add_argument("--threads", type=int, default=1, help="Nombre de threads parallèles.")
    parser.add_argument("--resume", action="store_true", help="Reprendre en ignorant les SIREN déjà dans le CSV.")
    args = parser.parse_args()

    # SIREN déjà traités ?
    done = already_scraped(Path(args.output)) if args.resume else set()

    # Lecture du fichier source et extraction des SIREN uniques
    target_siren: set[str] = set()
    with open(args.input, encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            siret = obj.get("registration_number")
            if not siret:
                continue
            siret = str(siret)  # <-- conversion sûre
            if len(siret) < 9 or not siret.isdigit():
                continue
            siren = siret[:9]
            if siren not in done:
                target_siren.add(siren)

    if not target_siren:
        sys.exit("Tout est déjà enrichi ou aucun SIREN valide trouvé.")

    fieldnames = [
        "siren",
        "denomination",
        "forme_juridique",
        "date_creation",
        "capital",
        "effectif",
        "naf_code",
        "adresse_siege",
        "dirigeants",
    ]

    mode = "a" if args.resume and csv_exists_and_has_header(Path(args.output)) else "w"
    with open(args.output, mode, newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if mode == "w":
            writer.writeheader()

        with ThreadPoolExecutor(max_workers=args.threads) as pool:
            futures = {
                pool.submit(scrape_siren, requests.Session(), siren, args.sleep): siren
                for siren in target_siren
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc="Scraping"):
                siren = futures[future]
                try:
                    data = future.result()
                except Exception as exc:
                    tqdm.write(f"[ERREUR] {siren}: {exc}")
                    continue
                if data:
                    writer.writerow(data)
                    csvfile.flush()

    print(f"✔︎ CSV enrichi : {args.output}")


if __name__ == "__main__":
    main()
