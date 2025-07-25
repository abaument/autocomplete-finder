import csv
from io import StringIO
from typing import Set, Dict, Any

from flask import Flask, request, send_file, render_template_string

import requests

import scrape_pappers
from scraper_utils import extract_sirens_from_csv, scrape_sirens, FIELDNAMES

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
