import csv
from io import TextIOWrapper
from typing import Set, Dict, Any

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

import scrape_pappers


def extract_sirens_from_csv(file_stream) -> Set[str]:
    """Extract unique SIREN numbers from an uploaded CSV file."""
    reader = csv.DictReader(TextIOWrapper(file_stream, encoding='utf-8'))
    sirens: Set[str] = set()
    known_cols = {'siren', 'siret', 'registration_number'}
    for row in reader:
        found = False
        for col in known_cols:
            val = row.get(col)
            if val:
                digits = ''.join(ch for ch in str(val) if ch.isdigit())
                if len(digits) >= 9:
                    sirens.add(digits[:9])
                    found = True
        if not found:
            for val in row.values():
                if not val:
                    continue
                digits = ''.join(ch for ch in str(val) if ch.isdigit())
                if len(digits) >= 9:
                    sirens.add(digits[:9])
                    break
    return sirens


def scrape_sirens(sirens: Set[str], sleep: float = 0.5, threads: int = 4) -> list[Dict[str, Any]]:
    results: list[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = {pool.submit(scrape_pappers.scrape_siren, requests.Session(), s, sleep): s for s in sirens}
        for future in as_completed(futures):
            data = future.result()
            if data:
                results.append(data)
    return results


FIELDNAMES = [
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
