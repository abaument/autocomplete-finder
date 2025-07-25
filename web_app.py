import csv
from io import StringIO, TextIOWrapper
from typing import Set, Dict, Any

from flask import Flask, request, send_file, render_template_string

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

app = Flask(__name__)

INDEX_HTML = """
<!doctype html>
<title>Pappers Scraper</title>
<h1>Upload a CSV file</h1>
<form method=post enctype=multipart/form-data action="/process">
  <input type=file name=file required>
  <input type=submit value="Submit">
</form>
"""


@app.route('/')
def index():
    return render_template_string(INDEX_HTML)


@app.route('/process', methods=['POST'])
def process():
    uploaded_file = request.files.get('file')
    if not uploaded_file:
        return 'No file uploaded', 400

    sirens = extract_sirens_from_csv(uploaded_file.stream)
    if not sirens:
        return 'No valid SIREN found in CSV', 400

    results = scrape_sirens(sirens)

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=FIELDNAMES)
    writer.writeheader()
    for row in results:
        writer.writerow(row)
    output.seek(0)

    return send_file(output, mimetype='text/csv', as_attachment=True, download_name='pappers.csv')


if __name__ == '__main__':
    app.run(debug=True)
